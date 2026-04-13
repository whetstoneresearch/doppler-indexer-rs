# Doppler Indexer

## Overview

### Description

Doppler Indexer is a high-performance Ethereum blockchain indexer written in Rust. It collects raw blockchain data (blocks, receipts, logs), tracks dynamically created contracts via factory events, reads historical on-chain state via `eth_call`, decodes events and call results into typed Parquet files, and runs custom transformation handlers that write to PostgreSQL.

Operating modes: Full (historical + live), Decode-Only (`--decode-only`), Live-Only (`--live-only`), Catch-Up-Only (`--catch-up-only`).

### Subsystems

| Subsystem | Description |
|-----------|-------------|
| **Raw Data Collection** | Fetches blocks, receipts, logs, factory addresses, and eth_call results from EVM chains |
| **Decoding** | Parses raw log and eth_call data into typed columns using Solidity ABI signatures |
| **Transformations** | Custom Rust handlers process decoded data and write to PostgreSQL with DAG-based dependency scheduling and per-handler progress |
| **Live Mode** | Real-time block processing via WebSocket with reorg detection and bincode-to-parquet compaction |
| **RPC Layer** | Unified RPC client with sliding-window rate limiting, streaming, and batch support |
| **S3 Storage** | Optional S3-compatible storage with write-through caching and manifest management |
| **Database** | PostgreSQL connection pool, migrations, and typed operation helpers |
| **Metrics** | Prometheus metrics server for RPC and pipeline observability |
| **Configuration** | JSON-based chain, contract, and pipeline configuration |

### Data Flow

#### Historical Pipeline

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Blocks    │────►│    Receipts      │────►│    Logs     │────► Log Decoder ────┐
└──────┬──────┘     └────────┬─────────┘     └─────────────┘                      │
       │                     │                                                     │
       │                     ├────────────────►┌─────────────┐                     │
       │                     │   ◄─────────────│  Factories  │────► Decoder        │
       │                     │    (recollect)  └─────────────┘                     │
       │                     │                                                     ▼
       │                     └─────────────────►┌──────────────────┐       ┌──────────────┐
       │                    (event triggers)    │  Event-Triggered │       │              │
       └─────────────────────────────────────►  │    ETH Calls     │──────►│Transformations│──► PostgreSQL
                                                └──────────────────┘       │    Engine    │
                                                        │                  └──────────────┘
                                              ┌─────────┴──────────┐
                                              │   ETH Call Decoder │
                                              └────────────────────┘
```

Each collector runs independently with async tasks. Synchronization barriers (oneshot channels) enforce ordering: factory catchup before factory-dependent eth_call catchup, eth_call catchup before decode catchup, decode catchup before transformation catchup.

#### Live Pipeline

```
                            WebSocket (eth_subscribe "newHeads")
                                          │
                                          ▼
                            ┌───────────────────────────┐
                            │      LiveCollector        │◄────── ReorgDetector
                            │     (block headers)       │
                            └───────────────────────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
                    ▼                     ▼                     ▼
        ┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
        │   HTTP: Receipts  │   │  FactoryParser    │   │  HTTP: eth_calls  │
        │   & Logs          │   │  (from logs)      │   │  (configured)     │
        └───────────────────┘   └───────────────────┘   └───────────────────┘
                    │                     │                       │
                    ▼                     ▼                       ▼
        ┌─────────────────────────────────────────────────────────────────────┐
        │                     LiveStorage (bincode) + Transformations         │
        └─────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
        ┌─────────────────────────────────────────────────────────────────────┐
        │                      CompactionService                               │
        │               (bincode → parquet at range_size)                      │
        └─────────────────────────────────────────────────────────────────────┘
```

Live mode writes bincode per-block for fast ingestion, then compacts to parquet for efficient storage. Reorg detection rolls back affected blocks atomically.

### Output Structure

```
data/{chain}/
├── historical/
│   ├── decoded/{eth_calls,logs}/{contract}/{event_or_fn}/
│   ├── factories/{collection}/
│   └── raw/{blocks,receipts,logs,eth_calls}/
└── live/
    ├── decoded/, factories/, raw/
    ├── snapshots/    # Upsert snapshots for reorg rollback
    └── status/       # Per-block processing status

migrations/
├── 000_handler_progress.sql
├── 001_active_versions.sql
├── 002_live_progress.sql
└── tables/           # Per-handler table migrations
```

---

## Features Index

### raw_data_collection
- **description**: Fetches blocks, transaction receipts, and logs from EVM chains. Two-phase processing (catchup then current). Parquet output with configurable field selection.
- **entry_points**: `src/raw_data/historical/blocks.rs`, `src/raw_data/historical/receipts.rs`, `src/raw_data/historical/logs.rs`, `src/raw_data/historical/catchup/`, `src/raw_data/historical/current/`
- **depends_on**: [rpc]
- **doc**: `docs/features/block_collection.md`, `docs/features/receipts_collection.md`, `docs/features/logs_collection.md`

### factory_collection
- **description**: Discovers dynamically created contracts from factory events. Supports multiple creation event types, nested tuple parameters, and incremental forwarding of discovered addresses.
- **entry_points**: `src/raw_data/historical/factories.rs`
- **depends_on**: [raw_data_collection, rpc]
- **doc**: `docs/features/factory_collection.md`

### eth_call_collection
- **description**: Makes `eth_call` requests at historical block heights. Supports every-block, every-N-blocks, duration-based, once, and on-event call patterns. Multicall3 batching optimization.
- **entry_points**: `src/raw_data/historical/eth_calls/`
- **depends_on**: [raw_data_collection, factory_collection, rpc]
- **doc**: `docs/features/eth_call_collection.md`

### on_event_calls
- **description**: Executes eth_calls in response to specific on-chain events (e.g., read pool state after a Swap). Target override allows routing calls to different contracts.
- **entry_points**: `src/raw_data/historical/eth_calls/event_triggers.rs`
- **depends_on**: [eth_call_collection]
- **doc**: `docs/features/on_event_calls.md`

### decoding
- **description**: Parses raw log data and eth_call results into typed Parquet columns using Solidity ABI signatures. Supports named tuples, incremental decoding, and column index tracking.
- **entry_points**: `src/decoding/`
- **depends_on**: [raw_data_collection]
- **doc**: `docs/features/decoding.md`

### transformations
- **description**: Custom Rust handlers process decoded events/calls and produce PostgreSQL operations. Single-submit continuous DAG scheduler: all `(handler, range)` work items are submitted upfront with call-dep readiness gated by a background file scanner, eliminating pass-based batching. Per-handler progress tracking, version injection, sequential execution support, and transactional execution with deadlock retry.
- **entry_points**: `src/transformations/traits.rs`, `src/transformations/registry.rs`, `src/transformations/engine.rs`, `src/transformations/executor.rs`, `src/transformations/scheduler/`
- **depends_on**: [decoding, database]
- **doc**: `docs/features/transformations.md`

### transformation_utils
- **description**: Shared utilities for transformation handlers: pool metadata cache, tick math, database helpers, metadata extraction, and address sanitization.
- **entry_points**: `src/transformations/util/`
- **depends_on**: [transformations]
- **doc**: `docs/features/transformation_utils.md`

### pool_metrics
- **description**: Per-block OHLC snapshots and hot-query pool_state table for all pool types (V3, LockableV3, V4 hooks). Shared BlockAccumulator and process_swaps/process_liquidity_deltas functions. liquidity_deltas append-only log for future TVL reconstruction.
- **entry_points**: `src/transformations/event/v3/metrics.rs`, `src/transformations/event/metrics/`
- **depends_on**: [transformations, transformation_utils]
- **doc**: `docs/pool-metrics/metrics_implementation_phases.md`

### live_mode
- **description**: Real-time block processing via WebSocket. Bincode storage for fast per-block writes, reorg detection, automatic compaction to parquet, and gap backfill on reconnect.
- **entry_points**: `src/live/`
- **depends_on**: [rpc, decoding, transformations]
- **doc**: `docs/features/live_mode.md`

### rpc
- **description**: Unified RPC client layer with auto-selection (Alchemy CU-based or standard), sliding-window rate limiting, streaming block fetches, batch requests, and WebSocket subscriptions.
- **entry_points**: `src/rpc/`
- **depends_on**: []
- **doc**: `docs/features/rpc.md`

### s3_storage
- **description**: Optional S3-compatible object storage with write-through caching, manifest management, retry queues, and initial sync.
- **entry_points**: `src/storage/`
- **depends_on**: [raw_data_collection]
- **doc**: `docs/features/s3_storage.md`

### configuration
- **description**: JSON-based configuration for chains, contracts, tokens, RPC settings, raw data collection, transformations, storage, and metrics. Environment variable references for secrets.
- **entry_points**: `src/types/config/`
- **depends_on**: []
- **doc**: `docs/features/config.md`

### parallelism
- **description**: Cross-cutting architecture documentation covering async task design, channel-based communication, synchronization barriers, and concurrency controls across the pipeline.
- **entry_points**: `src/main.rs`, `src/runtime.rs` (cross-cutting)
- **depends_on**: []
- **doc**: `docs/features/parallelism.md`

### database
- **description**: PostgreSQL connection pool (deadpool_postgres), migration runner, typed DbOperation/DbValue/WhereClause abstractions, deadlock retry, and structured error types.
- **entry_points**: `src/db/`
- **depends_on**: []
- **doc**: `docs/features/database.md`

### metrics
- **description**: Prometheus metrics server exposing RPC, live mode, collection, and transformation observability metrics via HTTP endpoint. RAII guards for safe in-flight tracking.
- **entry_points**: `src/metrics/`
- **depends_on**: [rpc]
- **doc**: `docs/features/metrics.md`

### solana_raw_data
- **description**: Solana raw data collection behind the `solana` feature flag. Extracts events from transaction log messages via ProgramLogParser (tracking CPI depth and instruction indices) and instructions from the transaction instruction tree (resolving account indices through Address Lookup Tables). Combined single-pass block extraction function processes both in one iteration. All extraction filters by a configured set of program IDs.
- **entry_points**: `src/solana/raw_data/events.rs`, `src/solana/raw_data/instructions.rs`, `src/solana/raw_data/extraction.rs`
- **depends_on**: [solana_rpc, configuration]
- **doc**: `docs/features/solana_raw_data.md`

---

## Project Structure

```
src/
├── main.rs              # Entry point, orchestrates the pipeline
├── runtime.rs           # Chain runtime setup (RPC clients, features, channels)
├── db/                  # Database layer (pool, migrations, types, errors)
├── rpc/                 # RPC clients (unified, alchemy, standard, websocket)
├── raw_data/historical/ # Raw data collection (blocks, receipts, logs, eth_calls, factories)
├── decoding/            # ABI decoding (logs, eth_calls, catchup, current)
├── live/                # Live mode (collector, storage, compaction, reorg, progress)
├── storage/             # S3 storage (local, s3, cached, manifest, upload, retry, contract_index)
├── metrics/             # Prometheus metrics server and RPC metrics
├── transformations/     # Transformation engine, handlers, registry, scheduler, utils
└── types/               # Config types, shared types, decoded value types
```

## Example Configuration

```json
{
  "chains": [
    {
      "name": "base",
      "chain_id": 8453,
      "rpc_url_env_var": "BASE_RPC_URL",
      "ws_url_env_var": "BASE_WS_URL",
      "start_block": 26602741,
      "contracts": "contracts/base",
      "tokens": "tokens/base.json",
      "block_receipts_method": "eth_getBlockReceipts",
      "rpc": {
        "concurrency": 100,
        "requests_per_second": 7500,
        "batch_size": 1000
      }
    }
  ],
  "raw_data_collection": {
    "channel_capacity": 5000,
    "factory_channel_capacity": 5000,
    "block_receipt_concurrency": 100,
    "parquet_block_range": 10000,
    "rpc_batch_size": 1000,
    "contract_logs_only": false
  },
  "transformations": {
    "database_url_env_var": "DATABASE_URL",
    "handler_concurrency": 4,
    "max_batch_size": 1000
  },
  "storage": {
    "s3": {
      "endpoint_env_var": "S3_ENDPOINT",
      "access_key_env_var": "S3_ACCESS_KEY",
      "secret_key_env_var": "S3_SECRET_KEY",
      "bucket_env_var": "S3_BUCKET"
    }
  },
  "metrics": {
    "addr": "0.0.0.0:9090"
  }
}
```
