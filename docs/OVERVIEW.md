# Doppler Indexer

Doppler Indexer is a high-performance Ethereum blockchain indexer written in Rust. It collects raw blockchain data, tracks dynamically created contracts, reads on-chain state, decodes events and call results into typed Parquet files, and supports custom transformations that write to PostgreSQL.

## Features

- **Raw Data Collection** - Fetches blocks, transaction receipts, and logs from any EVM-compatible chain
- **Factory Contract Tracking** - Automatically discovers and indexes dynamically created contracts (e.g., Uniswap pairs, token deployments)
- **Historical State Reading** - Makes `eth_call` requests at historical block heights to track on-chain state over time
- **Event-Triggered Calls** - Execute eth_calls in response to specific events (e.g., read pool state after a Swap)
- **Event Decoding** - Parses raw log data into typed columns using Solidity event signatures
- **Transformations** - Custom Rust handlers process decoded data and write to PostgreSQL with per-handler progress tracking
- **Live Mode** - Real-time block processing via WebSocket with reorg detection and automatic compaction
- **Parallel Processing** - Async task-based architecture with configurable concurrency for maximum throughput
- **Resumable** - Automatic catchup on restart, skipping already-collected ranges
- **Flexible Output** - Parquet files for analytics, PostgreSQL for application data

## Operating Modes

The indexer supports multiple operating modes:

| Mode | Description |
|------|-------------|
| **Full Mode** | Historical collection followed by live WebSocket processing |
| **Decode-Only** (`--decode-only`) | Run decoders on existing raw parquet files without collection |
| **Live-Only** (`--live-only`) | Skip historical processing and start directly in live mode |

## Project Structure

```
src/
├── main.rs              # Entry point, orchestrates the pipeline
├── db/                  # Database layer
│   ├── pool.rs          # PostgreSQL connection pool (deadpool_postgres)
│   ├── types.rs         # DbValue, DbOperation, WhereClause types
│   ├── migrations.rs    # Migration runner
│   └── error.rs         # Database error types
├── rpc/                 # RPC client layer
│   ├── unified.rs       # UnifiedRpcClient (Standard or Alchemy)
│   ├── alchemy.rs       # Alchemy client with CU rate limiting
│   ├── rpc.rs           # Standard RPC client
│   └── websocket.rs     # WebSocket client for live mode
├── raw_data/            # Data collection and decoding
│   ├── historical/      # Historical collection pipeline
│   │   ├── catchup/     # Catchup phase (process existing ranges)
│   │   ├── current/     # Current phase (process new blocks)
│   │   ├── blocks.rs    # Block collection
│   │   ├── receipts.rs  # Receipt collection
│   │   ├── logs.rs      # Log collection
│   │   ├── eth_calls.rs # ETH call collection
│   │   └── factories.rs # Factory contract discovery
│   └── decoding/        # ABI decoding
│       ├── logs.rs      # Log decoder
│       ├── eth_calls.rs # ETH call decoder
│       ├── catchup/     # Catchup decoding
│       └── current/     # Live decoding
├── live/                # Live mode components
│   ├── collector.rs     # WebSocket block collection
│   ├── storage.rs       # Bincode storage for live data
│   ├── compaction.rs    # Bincode to parquet compaction
│   ├── progress.rs      # Per-block progress tracking
│   └── reorg.rs         # Chain reorganization detection
├── transformations/     # Transformation system
│   ├── traits.rs        # TransformationHandler, EventHandler, EthCallHandler traits
│   ├── registry.rs      # Handler registration
│   ├── engine.rs        # Transformation engine (orchestrates handlers)
│   ├── context.rs       # TransformationContext, DecodedEvent, DecodedCall
│   ├── historical.rs    # HistoricalDataReader (parquet queries)
│   ├── event/           # Event handlers
│   ├── eth_call/        # ETH call handlers
│   └── util/            # Shared utilities (price, market, db helpers)
└── types/               # Type definitions
    ├── config/          # Configuration types
    │   ├── indexer.rs   # IndexerConfig
    │   ├── chain.rs     # ChainConfig
    │   ├── contract.rs  # Contract definitions
    │   ├── eth_call.rs  # ETH call config (frequency, triggers)
    │   └── ...
    └── shared/          # Shared types
```

## Architecture

The indexer runs as multiple concurrent async tasks connected by channels:

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

Each collector runs independently, maximizing throughput while respecting RPC rate limits. The optional transformation layer processes decoded data and writes to PostgreSQL.

**Recollection Channel**: When the factory collector encounters a corrupted log file during catchup, it deletes the file and sends a recollection request back to the receipt collector. The receipt collector then re-fetches the data and sends it through the normal channels, ensuring data integrity without manual intervention.

### Two-Phase Processing

Each collector operates in two phases:
1. **Catchup Phase** - Process existing parquet files and fill gaps
2. **Current Phase** - Process new blocks as they arrive via channels

Synchronization barriers (oneshot channels) ensure proper ordering:
- Factory catchup completes before factory-dependent eth_call catchup
- ETH call catchup completes before decode catchup
- Decode catchup completes before transformation engine catchup

### Live Mode Architecture

After historical catchup completes, the indexer transitions to live mode for real-time processing:

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
        │                        LiveStorage (bincode)                         │
        │         raw/{blocks,receipts,logs,eth_calls}/, decoded/, factories/  │
        └─────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
        ┌─────────────────────────────────────────────────────────────────────┐
        │                     TransformationEngine                             │
        │                (handlers with call dependencies)                     │
        └─────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
        ┌─────────────────────────────────────────────────────────────────────┐
        │                      CompactionService                               │
        │               (bincode → parquet at range_size)                      │
        └─────────────────────────────────────────────────────────────────────┘
```

Live mode uses bincode format for fast per-block writes, later compacted to parquet for efficient storage.

## Output Structure

```
data/{chain}/
├── historical/
│   ├── decoded/              # Decoded parquet files
│   │   ├── eth_calls/        # Decoded call results
│   │   │   └── {contract}/{function}/
│   │   └── logs/             # Decoded events
│   │       └── {contract}/{event}/
│   ├── factories/            # Discovered factory addresses
│   │   └── {collection}/
│   └── raw/                  # Raw blockchain data
│       ├── blocks/           # Block headers
│       ├── eth_calls/        # Raw eth_call results
│       │   ├── {contract}/
│       │   │   ├── {function}/
│       │   │   ├── once/     # One-time calls (name, symbol, etc.)
│       │   │   └── {function}/on_events/  # Event-triggered calls
│       │   └── {collection}/ # Factory contract calls
│       ├── logs/             # Raw event logs
│       └── receipts/         # Transaction receipts
│
└── live/                     # Live mode bincode storage
    ├── decoded/
    │   ├── eth_calls/
    │   └── logs/
    ├── factories/
    ├── raw/
    │   ├── blocks/
    │   ├── eth_calls/
    │   ├── logs/
    │   └── receipts/
    ├── snapshots/            # Upsert snapshots for reorg rollback
    └── status/               # Per-block processing status

migrations/
├── tables/              # Core table migrations
│   ├── tokens.sql
│   ├── pools.sql
│   └── ...
└── handlers/            # Per-handler migrations
    └── {handler_name}/
        └── {migration}.sql
```

## Transformation System

The transformation system processes decoded events and eth_calls, transforming them into PostgreSQL records.

### Key Concepts

**TransformationHandler Trait**: All handlers implement this trait:
```rust
#[async_trait]
pub trait TransformationHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32 { 1 }              // Bump to reprocess all data
    fn handler_key(&self) -> String;            // "{name}_v{version}"
    fn migration_paths(&self) -> Vec<&'static str>;  // Handler-specific SQL
    fn reorg_tables(&self) -> Vec<&'static str> { vec![] }  // Tables for reorg rollback
    async fn handle(&self, ctx: &TransformationContext) -> Result<Vec<DbOperation>, TransformationError>;
    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> { Ok(()) }
}
```

**EventHandler / EthCallHandler**: Marker traits that declare triggers:
```rust
pub trait EventHandler: TransformationHandler {
    fn triggers(&self) -> Vec<EventTrigger>;
    fn call_dependencies(&self) -> Vec<(String, String)> { vec![] }
}

pub trait EthCallHandler: TransformationHandler {
    fn triggers(&self) -> Vec<EthCallTrigger>;
}
```

**Source/Version Injection**: The engine automatically injects `source` and `source_version` columns into all database operations, enabling multiple handler versions to coexist.

**TransformationContext**: Provides handlers with:
- Chain info (name, chain_id, block range)
- Decoded events and calls for the current range
- Transaction address lookup (from/to)
- Contract configuration helpers
- Historical data queries (parquet files)
- Ad-hoc RPC calls

**DbOperation**: Database operations returned by handlers:
- `Upsert` - INSERT with ON CONFLICT DO UPDATE
- `Insert` - Simple INSERT
- `Update` - UPDATE with WHERE clause
- `Delete` - DELETE with WHERE clause
- `RawSql` - Raw SQL for complex operations

### Handler Registration

Handlers are registered at compile-time in `src/transformations/registry.rs`:
```rust
pub fn build_registry() -> TransformationRegistry {
    let mut registry = TransformationRegistry::new();
    super::event::register_handlers(&mut registry);
    super::eth_call::register_handlers(&mut registry);
    registry
}
```

### Progress Tracking

Each handler's progress is tracked independently in the `_handler_progress` table:
- `chain_id` - Chain identifier
- `handler_key` - Handler identity ("{name}_v{version}")
- `range_start` - Last processed block range start

In live mode, per-block progress is tracked in `_live_progress` and migrated during compaction.

## RPC Layer

The RPC layer provides unified access to blockchain data with intelligent rate limiting.

### Client Types

**UnifiedRpcClient**: Auto-selects the appropriate client based on URL:
- `AlchemyClient` - For Alchemy endpoints (CU-based rate limiting)
- `RpcClient` - Standard client for other providers

**WsClient**: WebSocket client for live mode with:
- `eth_subscribe("newHeads")` subscription
- Automatic reconnection with exponential backoff
- Gap detection on reconnect

### Rate Limiting

**SlidingWindowRateLimiter**: Implements a 10-second sliding window:
- Matches Alchemy's rate limiting behavior
- Shared across all clients for account-level limiting
- Per-request CU acquisition (not batch-based)

### Streaming Collection

The block collector uses streaming for efficient throughput:
```rust
// Blocks are sent as they arrive, not waiting for the full batch
client.get_blocks_streaming(block_numbers, full_transactions, result_tx)
```

This enables pipeline parallelism where downstream collectors process data while upstream is still fetching.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RPC_CONCURRENCY` | 100 | Max concurrent RPC requests |
| `ALCHEMY_CU_PER_SECOND` | 7500 | Alchemy compute units per second |
| `RPC_BATCH_SIZE` | (config) | Override for batch size |
| `DB_POOL_SIZE` | 16 | PostgreSQL connection pool size |

## Factory Collection

Factories allow tracking contracts that are dynamically created by other contracts. The system supports:

- **Factory Collection Types**: Define shared calls/events configuration at the chain level, reused across multiple factory contracts
- **Multiple Factory Events**: A single factory can track multiple creation event types
- **Nested Tuple Parameters**: Extract addresses from complex event data structures
- **Incremental Forwarding**: Factory addresses are sent to eth_calls as they're discovered

See [Factory Collection](./FACTORY_COLLECTION.md) for detailed documentation.

## ETH Call Collection

The eth_call collector supports multiple call patterns:

| Pattern | Description |
|---------|-------------|
| Every Block | Call at every block (default) |
| Every N Blocks | Call every N blocks |
| Duration-based | Call at time intervals (e.g., `"1h"`) |
| Once | Call once per contract address |
| On-Events | Call when specific events are emitted |

### Multicall3 Optimization

When a `Multicall3` contract is configured, all eth_calls for the same block are automatically batched into a single `aggregate3` RPC call, significantly reducing RPC requests.

### Target Override

The `target` field allows routing calls to a different contract than the one emitting events, useful for oracles or shared state contracts.

See [ETH Call Collection](./ETH_CALL_COLLECTION.md) and [On-Event Calls](./ON_EVENT_CALLS.md) for detailed documentation.

## Decoding

The decoding module transforms raw blockchain data into decoded, typed parquet files:

- **Event Signature Parsing**: Extracts event names, parameter types, and topic0 hashes
- **Named Tuple Support**: Automatically flattens named tuples into dot-notation columns
- **Incremental Decoding**: Adding new events only processes the new event type, not all historical data
- **Column Index Tracking**: For "once" calls, tracks which columns exist in each file for efficient incremental collection

See [Decoding](./DECODING.md) for detailed documentation.

## Live Mode

Live mode enables real-time block processing via WebSocket subscription:

- **Bincode Storage**: Fast serialization for per-block writes
- **Reorg Detection**: Tracks parent hashes to detect chain reorganizations
- **Automatic Compaction**: Converts bincode to parquet when ranges are complete
- **Gap Detection**: Backfills missing blocks on WebSocket reconnection
- **Atomic Writes**: Write to temp file, flush, sync, then atomic rename

See [Live Mode](./LIVE_MODE.md) for detailed documentation.

## Quick Start

1. Create a configuration file (see [Configuration](./CONFIG.md))
2. Set your RPC URL environment variable
3. Run the indexer:

```bash
cargo run --release
```

## Data Flow

### Collection Phase
1. **Block Collector** fetches block headers and sends to receipts
2. **Receipt Collector** fetches transaction receipts and extracts logs
3. **Log Collector** writes raw logs to parquet, forwards to factory collector
4. **Factory Collector** discovers new contracts from factory events
5. **ETH Call Collector** makes historical state queries (block-based and event-triggered)

### Decoding Phase
1. **Log Decoder** parses raw logs using ABI event signatures
2. **ETH Call Decoder** parses raw call results using ABI function signatures
3. Both write decoded parquet files and forward to transformation engine

### Transformation Phase
1. **Transformation Engine** receives decoded events/calls
2. Looks up registered handlers by trigger (source, event/function)
3. Invokes handlers with `TransformationContext`
4. Executes returned `DbOperation`s transactionally per handler
5. Injects `source` and `source_version` columns automatically

## Documentation

| Document | Description |
|----------|-------------|
| [Configuration](./CONFIG.md) | JSON configuration format, contracts, tokens, and options |
| [Block Collection](./BLOCK_COLLECTION.md) | Block fetching and storage |
| [Receipt Collection](./RECEIPTS_COLLECTION.md) | Transaction receipt processing |
| [Log Collection](./LOGS_COLLECTION.md) | Event log filtering and storage |
| [Factory Collection](./FACTORY_COLLECTION.md) | Dynamic contract discovery |
| [ETH Call Collection](./ETH_CALL_COLLECTION.md) | Historical state reading |
| [On-Event Calls](./ON_EVENT_CALLS.md) | Event-triggered eth_calls |
| [Decoding](./DECODING.md) | ABI-based event and call decoding |
| [Transformations](./TRANSFORMATIONS.md) | Custom handlers, PostgreSQL output, historical queries |
| [Transformation Utils](./TRANSFORMATION_UTILS.md) | Price computation, market metrics, database helpers |
| [Live Mode](./LIVE_MODE.md) | Real-time block processing via WebSocket |
| [Parallelism](./PARALLELISM.md) | Task architecture and data flow |
| [RPC](./RPC.md) | RPC client, rate limiting, and retries |

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
      "factory_collections": "factory_collections/base.json",
      "block_receipts_method": "eth_getBlockReceipts"
    }
  ],
  "raw_data_collection": {
    "parquet_block_range": 10000,
    "rpc_batch_size": 100,
    "contract_logs_only": true,
    "live_mode": true,
    "reorg_depth": 128,
    "compaction_interval_secs": 10,
    "decoding_concurrency": 4,
    "factory_concurrency": 4,
    "fields": {
      "block_fields": ["number", "timestamp", "transactions"],
      "receipt_fields": ["block_number", "timestamp", "transaction_hash", "from", "to"],
      "log_fields": ["block_number", "timestamp", "transaction_hash", "log_index", "address", "topics", "data"]
    }
  },
  "transformations": {
    "database_url_env_var": "DATABASE_URL",
    "handler_concurrency": 4,
    "mode": {
      "batch_for_catchup": true,
      "catchup_batch_size": 10000
    }
  },
  "metrics": {
    "addr": "0.0.0.0:9090"
  }
}
```

## Use Cases

- **DEX Analytics** - Track swaps, liquidity changes, and pool state across Uniswap V2/V3/V4
- **Token Tracking** - Monitor token deployments, transfers, and balances
- **Protocol Indexing** - Index any smart contract's events and state
- **Historical Analysis** - Query on-chain data with SQL using DuckDB or similar tools
- **Application Backends** - Use transformations to power real-time APIs with PostgreSQL
- **Real-time Applications** - Use live mode for low-latency event processing

## Querying Data

Example using DuckDB to query decoded swap events:

```sql
SELECT
    block_number,
    block_timestamp,
    encode(contract_address, 'hex') as pool,
    amount0,
    amount1,
    sqrtPriceX96
FROM read_parquet('data/base/historical/decoded/logs/UniswapV4PoolManager/Swap/*.parquet')
WHERE block_timestamp > 1700000000
ORDER BY block_number DESC
LIMIT 100;
```

## Performance

The indexer is designed for high throughput:

- **Parallel collection** - Multiple collectors run concurrently via async tasks
- **Streaming block fetches** - Blocks sent immediately as they arrive, not waiting for batches
- **Batch RPC requests** - Configurable batch sizes reduce overhead
- **Multicall3 batching** - Combine all eth_calls per block into a single RPC call
- **Rate limit aware** - Sliding window rate limiter matches Alchemy's 10-second window
- **Shared rate limiter** - Single limiter across all clients for account-level limiting
- **Semaphore-bounded concurrency** - `RPC_CONCURRENCY` controls in-flight requests
- **Efficient storage** - Snappy-compressed Parquet with configurable schemas
- **Resumable** - Automatic catchup skips already-processed ranges
- **Incremental decoding** - New events/calls only process what's missing
- **Per-handler transactions** - Handler failures don't affect other handlers
- **Concurrent handler execution** - `handler_concurrency` config controls parallelism
- **Concurrent decoding** - `decoding_concurrency` config controls catchup parallelism
