# Metrics / Observability

Prometheus-based observability subsystem that exposes operational metrics about the indexer pipeline via an HTTP endpoint.

**Important disambiguation:** This document covers the *infrastructure observability* module at `src/metrics/`. The similarly named files at `src/transformations/event/*/metrics.rs` are *domain-specific transformation handlers* that compute pool state, snapshots, and liquidity deltas from on-chain swap/mint/burn events. Those are documented in the [transformations](transformations.md) feature doc.

## Scope

- Prometheus metric registration and HTTP exposition via `metrics-exporter-prometheus`
- RPC request instrumentation: duration histograms, request/error counters, in-flight gauges
- Live mode instrumentation: WebSocket disconnects, reorg depth, compaction cycles, block arrival intervals
- Collection instrumentation: parquet writes, block processing, eth_call batches
- Transformation instrumentation: handler duration, events/calls processed, retry tracking, handler concurrency
- RAII guard pattern for safe metric lifecycle (in-flight tracking, duration recording)
- Convenience `with_metrics()` async wrapper for ergonomic instrumentation
- Chain label extraction and sanitization from RPC URLs
- Configuration: optional metrics server enabled via `MetricsConfig` in the indexer config

## Non-scope

- Pool metrics handlers (swap metrics, liquidity deltas, OHLC snapshots) -- those are transformation handlers, not observability
- Application-level dashboards or alerting rules

## Data / Control Flow

### Initialization

```
main.rs
  |
  +--> config.metrics is Some?
         |
         +--> metrics::init_metrics_server(addr)
         |      |
         |      +--> PrometheusBuilder::new()
         |             .with_http_listener(addr)
         |             .install()
         |
         +--> metrics::describe_rpc_metrics()
                |
                +--> describe_histogram!("rpc_request_duration_seconds", ...)
                +--> describe_counter!("rpc_requests_total", ...)
                +--> describe_counter!("rpc_errors_total", ...)
                +--> describe_gauge!("rpc_requests_in_flight", ...)
```

`init_metrics_server` installs the global Prometheus recorder and spawns an HTTP server that serves metrics at the `/metrics` endpoint. `describe_rpc_metrics` registers human-readable descriptions for all RPC metric names. Both are called exactly once during startup, before any RPC calls occur.

### RPC Request Instrumentation

Every RPC call in `AlchemyClient` (the primary production client) is wrapped with `with_metrics()`:

```
AlchemyClient::get_block(block_number)
  |
  +--> with_metrics(RpcMethod::GetBlock, &chain_label, || async { ... })
         |
         +--> RpcMetricsGuard::new(method, chain)
         |      |
         |      +--> gauge!("rpc_requests_in_flight").increment(1.0)
         |
         +--> operation().await
         |
         +--> match result:
                Ok(_)  => guard.success()
                           |
                           +--> histogram!("rpc_request_duration_seconds", status="success").record(duration)
                           +--> counter!("rpc_requests_total", status="success").increment(1)
                           +--> gauge!("rpc_requests_in_flight").decrement(1.0)
                |
                Err(e) => guard.failure(&e)
                           |
                           +--> counter!("rpc_errors_total", error_type=category).increment(1)
                           +--> histogram!("rpc_request_duration_seconds", status="error").record(duration)
                           +--> counter!("rpc_requests_total", status="error").increment(1)
                           +--> gauge!("rpc_requests_in_flight").decrement(1.0)
```

If the guard is dropped without calling `success()` or `failure()` (e.g., due to a panic), the `Drop` impl still decrements the in-flight gauge, preventing counter leaks.

### Chain Label Extraction

`chain_label_from_url(url)` produces sanitized Prometheus-safe label values from RPC endpoint URLs:

| URL pattern | Label |
|---|---|
| `https://eth-mainnet.g.alchemy.com/v2/...` | `alchemy_eth_mainnet` |
| `https://base-mainnet.g.alchemy.com/v2/...` | `alchemy_base_mainnet` |
| `https://mainnet.infura.io/v3/...` | `infura_mainnet` |
| `https://rpc.ankr.com/eth` | `rpc_ankr_com` |

Dots and hyphens in hostnames are replaced with underscores. For Alchemy and Infura, the subdomain is extracted and prefixed with the provider name.

## Live Mode Metrics

`describe_live_metrics()` registers metrics for the WebSocket-based live processing pipeline:

| Name | Type | Labels | Description |
|---|---|---|---|
| `live_ws_disconnects_total` | counter | `chain` | WebSocket disconnection count |
| `live_ws_reconnects_total` | counter | `chain` | WebSocket reconnection count |
| `live_reorgs_total` | counter | `chain` | Chain reorganizations detected |
| `live_reorg_depth` | histogram | `chain` | Depth of detected reorgs (blocks rolled back) |
| `live_compaction_cycle_duration_seconds` | histogram | `chain` | Duration of compaction cycles |
| `live_compaction_stuck_blocks` | gauge | `chain` | Blocks stuck awaiting transformation |
| `live_compaction_blocks_pending` | gauge | `chain` | Blocks pending compaction |
| `live_block_arrival_interval_seconds` | histogram | `chain` | Time between consecutive block arrivals |
| `live_block_processing_duration_seconds` | histogram | `chain` | Time to process a single live block |
| `live_blocks_processed_total` | counter | `chain` | Total live blocks processed |
| `live_gap_detections_total` | counter | `chain` | Gap detections (missing block sequences) |
| `live_backfill_duration_seconds` | histogram | `chain` | Duration of gap backfill operations |
| `live_backfill_blocks_total` | counter | `chain` | Total blocks backfilled |

## Collection Metrics

`describe_collection_metrics()` registers metrics for raw data collection:

| Name | Type | Labels | Description |
|---|---|---|---|
| `collection_blocks_processed_total` | counter | `chain` | Blocks processed during collection |
| `collection_block_processing_duration_seconds` | histogram | `chain` | Per-block processing time |
| `collection_block_transactions` | histogram | `chain` | Transaction count per block |
| `collection_parquet_write_duration_seconds` | histogram | `chain`, `data_type` | Parquet file write duration |
| `collection_parquet_file_size_bytes` | histogram | `chain`, `data_type` | Parquet file sizes |
| `collection_eth_calls_total` | counter | `chain` | Total eth_calls executed |
| `collection_eth_call_batch_duration_seconds` | histogram | `chain` | Eth_call batch duration |
| `collection_receipts_processed_total` | counter | `chain` | Receipts processed |

`record_parquet_write(chain, data_type, duration_secs, file_path)` records both the write duration and the resulting file size by reading file metadata.

## Transformation Metrics

`describe_transformation_metrics()` registers metrics for the handler execution pipeline:

| Name | Type | Labels | Description |
|---|---|---|---|
| `transformation_handler_duration_seconds` | histogram | `handler_key`, `mode`, `status` | Handler execution time |
| `transformation_handler_errors_total` | counter | `handler_key`, `error_type` | Handler error count |
| `transformation_events_processed_total` | counter | `handler_key` | Events processed by handler |
| `transformation_calls_processed_total` | counter | `handler_key` | Calls processed by handler |
| `transformation_retry_attempts_total` | counter | `chain` | Live mode retry attempts |
| `transformation_range_finalization_duration_seconds` | histogram | `chain` | Range finalization time |
| `transformation_pending_events` | gauge | `chain` | Events buffered pending call dependencies |
| `transformation_handlers_in_flight` | gauge | `handler_key`, `mode` | Handlers currently executing |

**HandlerMetricsGuard** - RAII guard for handler execution: increments `transformation_handlers_in_flight` on creation, records `transformation_handler_duration_seconds` with status label on `success()`/`failure()`, decrements the gauge on drop. Labels include `handler_key` and `mode` (either `"catchup"` or `"live"`).

## Related Files

### Core metrics module

| File | Role | Key exports |
|---|---|---|
| `src/metrics/mod.rs` | Module root, Prometheus server initialization | `init_metrics_server(addr: SocketAddr)`, re-exports from submodules |
| `src/metrics/rpc.rs` | RPC request metrics: registration, RAII guard, async wrapper | `RpcMethod`, `RpcMetricsGuard`, `with_metrics()`, `describe_rpc_metrics()`, `chain_label_from_url()` |
| `src/metrics/live.rs` | Live mode metrics registration | `describe_live_metrics()` |
| `src/metrics/collection.rs` | Collection metrics registration and recording | `describe_collection_metrics()`, `record_parquet_write()` |
| `src/metrics/transformations.rs` | Transformation metrics registration, RAII guard | `describe_transformation_metrics()`, `HandlerMetricsGuard` |

### Configuration

| File | Role | Key exports |
|---|---|---|
| `src/types/config/metrics.rs` | Metrics config deserialization | `MetricsConfig { addr: String }` |
| `src/types/config/indexer.rs` | Top-level config, holds optional `MetricsConfig` | `IndexerConfig.metrics: Option<MetricsConfig>` |

### Consumer (RPC client)

| File | Role | Key exports |
|---|---|---|
| `src/rpc/alchemy.rs` | Primary production RPC client; wraps every RPC method with `with_metrics()` | `AlchemyClient` |

### Pool metrics handlers (domain logic, not observability)

These files are named `metrics.rs` but implement `TransformationHandler`/`EventHandler` traits to compute pool state from on-chain events. They are listed here only for disambiguation.

| File | Role |
|---|---|
| `src/transformations/event/v3/metrics.rs` | V3 pool swap/liquidity metrics handlers |
| `src/transformations/event/v4/metrics.rs` | V4 base hook cumulative swap metrics handler |
| `src/transformations/event/multicurve/metrics.rs` | Multicurve swap/liquidity metrics handlers |
| `src/transformations/event/dhook/metrics.rs` | Doppler hook swap/liquidity metrics handlers |
| `src/transformations/event/scheduled_multicurve/metrics.rs` | Scheduled multicurve swap/liquidity metrics handlers |
| `src/transformations/event/decay_multicurve/metrics.rs` | Decay multicurve swap/liquidity metrics handlers |
| `src/transformations/event/metrics/mod.rs` | Shared metrics processing module root |
| `src/transformations/event/metrics/swap_data.rs` | Normalized `SwapInput`/`LiquidityInput` types, `process_swaps()`, `process_liquidity_deltas()` |
| `src/transformations/event/metrics/v4_hook_extract.rs` | Shared V4 hook event extraction (`extract_v4_hook_swaps()`, `extract_*_modify_liquidity()`) |
| `src/transformations/event/metrics/accumulator.rs` | `BlockAccumulator` for OHLC and volume aggregation within a single block |

## RPC Metrics Reference

### Registered metrics

| Name | Type | Labels | Description |
|---|---|---|---|
| `rpc_request_duration_seconds` | histogram | `method`, `chain`, `status` | Duration of RPC requests in seconds |
| `rpc_requests_total` | counter | `method`, `chain`, `status` | Total number of RPC requests |
| `rpc_errors_total` | counter | `method`, `chain`, `error_type` | Total number of RPC errors |
| `rpc_requests_in_flight` | gauge | `method`, `chain` | Number of RPC requests currently in progress |

### RpcMethod variants

Each variant maps to a string label value used in `method`:

| Variant | Label value |
|---|---|
| `GetBlockNumber` | `eth_blockNumber` |
| `GetBlock` | `eth_getBlockByNumber` |
| `GetTransaction` | `eth_getTransactionByHash` |
| `GetTransactionReceipt` | `eth_getTransactionReceipt` |
| `GetBlockReceipts` | `eth_getBlockReceipts` |
| `GetLogs` | `eth_getLogs` |
| `GetBalance` | `eth_getBalance` |
| `GetCode` | `eth_getCode` |
| `EthCall` | `eth_call` |
| `GetBlocksBatch` | `eth_getBlockByNumber_batch` |
| `GetTransactionReceiptsBatch` | `eth_getTransactionReceipt_batch` |
| `GetLogsBatch` | `eth_getLogs_batch` |
| `EthCallBatch` | `eth_call_batch` |
| `GetBlockReceiptsConcurrent` | `eth_getBlockReceipts_concurrent` |

### RpcErrorCategory variants

Errors are categorized into label values used in `error_type`:

| Variant | Label value | Source error |
|---|---|---|
| `Transport` | `transport` | `RpcError::Transport` |
| `RateLimit` | `rate_limit` | `RpcError::RateLimitExceeded` |
| `InvalidUrl` | `invalid_url` | `RpcError::InvalidUrl` |
| `Batch` | `batch` | `RpcError::BatchError` |
| `Provider` | `provider` | `RpcError::ProviderError` |

## Configuration

Metrics are optional. When the `metrics` key is absent from the indexer config, no metrics server is started and no metrics are recorded (the global `metrics` crate recorder remains a no-op).

```json
{
  "metrics": {
    "addr": "0.0.0.0:9090"
  }
}
```

The `addr` field is a `SocketAddr` string parsed at startup. The Prometheus exporter serves metrics at `http://{addr}/metrics`.

## Invariants and Constraints

1. **Single initialization**: `init_metrics_server` must be called at most once. The underlying `PrometheusBuilder::install()` sets a global recorder; calling it twice will panic.

2. **Describe before record**: `describe_rpc_metrics()` must be called before any RPC metrics are recorded. This is enforced by startup ordering in `main.rs` -- the describe call happens immediately after `init_metrics_server`, before any chain workers or RPC clients are created.

3. **RAII in-flight safety**: `RpcMetricsGuard` guarantees that the `rpc_requests_in_flight` gauge is always decremented, even on panic. The `Drop` impl checks a `completed` flag; if the guard was dropped without an explicit `success()`/`failure()` call, it still decrements the gauge. This prevents in-flight counter leaks that would produce incorrect dashboard values.

4. **Label sanitization**: `chain_label_from_url` replaces dots and hyphens with underscores in URL hostnames. This produces labels safe for Prometheus (which restricts label values to `[a-zA-Z0-9_]` for best compatibility). API keys in URL paths are never included in labels.

5. **Optional metrics**: When no `MetricsConfig` is provided, no Prometheus recorder is installed. The `metrics` crate's macros (`counter!`, `gauge!`, `histogram!`) become no-ops in this case, so instrumented code paths incur negligible overhead.

6. **No cross-thread state**: The `metrics` crate uses a global atomic recorder. All metric operations are lock-free and safe to call from any async task or thread without synchronization.
