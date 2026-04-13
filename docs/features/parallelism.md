# Parallelism and Data Flow

The indexer uses an async task-based architecture with channels for communication between collectors. This design enables concurrent data collection, efficient resource utilization, and clean separation of concerns.

## Overview

Raw data collection runs as multiple concurrent async tasks connected by channels:

1. **Blocks Collector** - Fetches blocks from RPC, sends block info downstream
2. **Receipts Collector** - Fetches transaction receipts, extracts logs
3. **Logs Collector** - Writes logs to parquet (via pipelined JoinSet), optionally filters by contract
4. **Factories Collector** - Extracts factory-created contract addresses from logs, forwards to downstream channels in parallel via `tokio::join!`
5. **Eth Calls Collector** - Executes eth_call requests at historical block heights
6. **Decoders** - Decode raw logs and eth_call results into typed parquet files

Each collector runs independently and communicates via typed channels, allowing the system to maximize throughput while respecting RPC rate limits.

## Data Flow Diagram

### Basic Flow (without factories)

```
┌─────────────┐     (block_number, timestamp, tx_hashes)     ┌──────────────────┐     LogMessage     ┌─────────────┐
│   blocks    │ ────────────────────────────────────────────▶│    receipts      │ ─────────────────▶ │    logs     │
└──────┬──────┘                                              │                  │                    │             │
       │                                                     │  1. Fetch RPC    │                    │ Write logs  │
       │                                                     │  2. Extract logs │                    │ to parquet  │
       │                                                     │  3. Write parquet│                    └──────┬──────┘
       │                                                     └──────────────────┘                           │
       │                                                                                                    │
       │         (block_number, timestamp)                                                                  │
       └────────────────────────────────────────────────────────────────────────┐                           │
                                                                                │                           ▼
                                                                                ▼                    ┌─────────────┐
                                                                         ┌─────────────┐            │ log_decoder │
                                                                         │  eth_calls  │            └─────────────┘
                                                                         │             │
                                                                         │ Execute at  │
                                                                         │ block height│
                                                                         └──────┬──────┘
                                                                                │
                                                                                ▼
                                                                         ┌─────────────┐
                                                                         │call_decoder │
                                                                         └─────────────┘
```

### Full Flow (with factories and `contract_logs_only: true`)

```
┌─────────────┐                              ┌──────────────────┐
│   blocks    │ ───(block info)────────────▶ │    receipts      │
└──────┬──────┘                              │                  │
       │                                     │  extracts logs   │
       │                                     │  from receipts   │
       │                                     └────────┬─────────┘
       │                                              │
       │                                              │ LogMessage
       │                            ┌─────────────────┼─────────────────┐
       │                            │                 │                 │
       │                            ▼                 ▼                 │
       │                     ┌─────────────┐  ┌─────────────┐           │
       │                     │    logs     │  │  factories  │           │
       │                     │             │  │             │           │
       │                     │ waits for   │◀─│  extracts   │           │
       │                     │ factory     │  │  factory    │           │
       │                     │ addresses   │  │  addresses  │           │
       │                     └──────┬──────┘  └──────┬──────┘           │
       │                            │                │                  │
       │                            │                │ FactoryMessage
       │                            ▼                │                  │
       │                     ┌─────────────┐         │                  │
       │                     │  filtered   │         │                  │
       │                     │  parquet    │         │                  │
       │                     │  output     │         │                  │
       │                     └─────────────┘         │                  │
       │                                             │                  │
       │        (block_number, timestamp)            │                  │
       └──────────────────────────┬──────────────────┼──────────────────┘
                                  │                  │
                                  ▼                  │
                           ┌─────────────┐           │
                           │  eth_calls  │◀──────────┘
                           │             │   (factory addresses for
                           │ Regular +   │    factory eth_calls)
                           │ Factory     │
                           │ calls       │
                           └─────────────┘
```

## Channel Message Types

### Block to Receipts / Eth Calls

```rust
// To receipts collector
(u64, u64, Vec<B256>)  // (block_number, timestamp, tx_hashes)

// To eth_calls collector
(u64, u64)  // (block_number, timestamp)
```

### Receipts to Logs / Factories

```rust
pub enum LogMessage {
    Logs(Vec<LogData>),
    RangeComplete { range_start: u64, range_end: u64 },
    AllRangesComplete,
}
```

- `Logs` - Batch of extracted log entries
- `RangeComplete` - Signals a block range is fully processed, triggers parquet writes
- `AllRangesComplete` - Signals end of collection, receivers should shut down

### Factories to Eth Calls

The factory-to-eth_calls channel uses `FactoryMessage`, which supports incremental address forwarding:

```rust
pub enum FactoryMessage {
    /// Incremental batch of factory addresses discovered (sent per rpc_batch_size logs)
    IncrementalAddresses(FactoryAddressData),
    /// A block range is complete - parquet files written
    RangeComplete { range_start: u64, range_end: u64 },
    /// All processing is complete
    AllComplete,
}

pub struct FactoryAddressData {
    pub range_start: u64,
    pub range_end: u64,
    pub addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
}
```

`FactoryAddressData` contains discovered factory addresses grouped by block number, with `(timestamp, address, collection_name)` tuples. The `IncrementalAddresses` variant enables early RPC fetching before the full range is processed.

### Receipts to Event Trigger Channel

When `on_events` eth_calls are configured, the receipts collector also sends matched events to the eth_calls collector:

```rust
pub enum EventTriggerMessage {
    Triggers(Vec<EventTriggerData>),
    RangeComplete { range_start: u64, range_end: u64 },
    AllComplete,
}
```

### Collectors to Decoders

```rust
pub enum DecoderMessage {
    LogsReady { range_start: u64, range_end: u64, logs: Vec<LogData>, live_mode: bool, has_factory_matchers: bool },
    EthCallsReady { range_start: u64, range_end: u64, contract_name: String, function_name: String, results: Vec<EthCallResult>, live_mode: bool, retry_transform_after_decode: bool },
    OnceCallsReady { range_start: u64, range_end: u64, contract_name: String, results: Vec<OnceCallResult>, live_mode: bool, retry_transform_after_decode: bool },
    EventCallsReady { range_start: u64, range_end: u64, contract_name: String, function_name: String, results: Vec<EventCallResult>, live_mode: bool, retry_transform_after_decode: bool },
    EthCallsBlockComplete { range_start: u64, range_end: u64, retry_transform_after_decode: bool },
    FactoryAddresses { range_start: u64, range_end: u64, addresses: HashMap<String, Vec<Address>> },
    OnceFileBackfilled { range_start: u64, range_end: u64, contract_name: String },
    Reorg { _common_ancestor: u64, orphaned: Vec<u64> },
    AllComplete,
}
```

The `live_mode` flag controls whether decoded output is written to bincode (live) or parquet (historical). The `retry_transform_after_decode` flag defers transform execution until a block-complete retry request is processed.

### Decoders to Transformation Engine

```rust
pub struct DecodedEventsMessage {
    pub range_start: u64, pub range_end: u64,
    pub source_name: String, pub event_name: String,
    pub events: Vec<DecodedEvent>,
}

pub struct DecodedCallsMessage {
    pub range_start: u64, pub range_end: u64,
    pub source_name: String, pub function_name: String,
    pub calls: Vec<DecodedCall>,
}

pub struct RangeCompleteMessage {
    pub range_start: u64, pub range_end: u64,
    pub kind: RangeCompleteKind,  // Logs or EthCalls
}

pub struct ReorgMessage {
    pub common_ancestor: u64,
    pub orphaned: Vec<u64>,
}
```

## Channel Configuration

Channel capacity is configurable in `raw_data_collection`:

| Setting | Default | Description |
|---------|---------|-------------|
| `channel_capacity` | 1000 | Capacity for main channels (blocks→receipts, receipts→logs, receipts→factories, blocks→eth_calls) |
| `factory_channel_capacity` | 1000 | Capacity for factory address channels (factories→logs, factories→eth_calls) |

```json
{
  "raw_data_collection": {
    "channel_capacity": 2000,
    "factory_channel_capacity": 1000
  }
}
```

Larger channel capacities allow more buffering between collectors, which can improve throughput when downstream collectors are temporarily slower than upstream.

## Streaming Collection

The block collector uses a **streaming approach** for maximum throughput:

1. All block requests for a range are dispatched concurrently (rate-limited by semaphore + CU limiter)
2. Each block is processed **immediately as it arrives** from the RPC
3. Block data is forwarded to downstream collectors (receipts, eth_calls) **instantly**
4. Records are buffered in a BTreeMap for ordered parquet output

### Benefits

- **Immediate downstream forwarding**: Receipts collector starts receiving blocks as soon as they're fetched, not after the whole batch completes
- **Better rate limit utilization**: Requests continuously flow to the RPC, limited only by semaphore and CU budget
- **Pipelining**: While blocks are still being fetched, downstream collectors are already processing

### Architecture

```
                     ┌─────────────────────────────────────────────────────────────┐
                     │                    RPC Layer                                 │
                     │  ┌─────────┐    ┌─────────────────────┐    ┌─────────────┐  │
                     │  │Semaphore│───▶│Sliding Window Limiter│───▶│   Execute   │  │
                     │  │(100-500)│    │   (75000 CU/10sec)   │    │   Request   │  │
                     │  └─────────┘    └─────────────────────┘    └──────┬──────┘  │
                     └───────────────────────────────────────────────────┼─────────┘
                                                                         │
                                              Stream results via channel │
                                                                         ▼
┌──────────────┐              ┌──────────────────────────────────────────────────────┐
│    Blocks    │◀─────────────│           Block Collector (Streaming)                 │
│   Parquet    │              │  ┌──────────────┐   ┌──────────────────────────────┐  │
│   (ordered)  │◀─────────────│  │ BTreeMap     │   │  Forward to downstream       │  │
└──────────────┘              │  │ (buffered)   │   │  immediately as received     │──┼──▶ Receipts
                              │  └──────────────┘   └──────────────────────────────┘  │
                              └───────────────────────────────────────────────────────┘
                                                                                       │
                                                                                       ▼
                                                                                   Eth Calls
```

Note: The sliding window tracks CU usage over 10-second windows (7500 CU/s * 10 = 75000 CU per window).

## Concurrency Settings

### Per-Chain RPC Configuration

RPC concurrency, rate limiting, and batch size are configured per chain in the `rpc` block. There are no environment variables for these settings.

| Setting | Default | Description |
|---------|---------|-------------|
| `rpc.concurrency` | 100 | Max concurrent in-flight RPC requests across all collectors |
| `rpc.requests_per_second` | 7500 | Generic rate-limit units per second. For standard providers this is requests/sec; for Alchemy-like providers it is CU/sec |
| `rpc.batch_size` | 100 | Max batch size for RPC requests (fallback: `raw_data_collection.rpc_batch_size`) |

```json
{
  "chains": [{
    "name": "base",
    "rpc": {
      "concurrency": 500,
      "requests_per_second": 7500,
      "batch_size": 100
    }
  }]
}
```

### Shared Rate Limiter

All RPC clients share a single `SlidingWindowRateLimiter` for account-level rate limiting:

```rust
// Create shared limiter for account-level rate limiting across all clients
let shared_limiter = Arc::new(SlidingWindowRateLimiter::new(cu_per_second));

// All clients share the same limiter
let blocks_client = UnifiedRpcClient::from_url_with_options(
    &rpc_url, cu_per_second, rpc_concurrency, rpc_batch_size as usize, Some(shared_limiter.clone())
)?;
let receipts_client = UnifiedRpcClient::from_url_with_options(
    &rpc_url, cu_per_second, rpc_concurrency, rpc_batch_size as usize, Some(shared_limiter.clone())
)?;
```

This ensures combined CU usage across all collectors (blocks, receipts, eth_calls) stays within your Alchemy plan limits.

### Block Receipt Concurrency

When using `block_receipts_method` (e.g., `eth_getBlockReceipts`), receipts are fetched block-by-block rather than per-transaction. The concurrency is configurable:

| Setting | Default | Description |
|---------|---------|-------------|
| `block_receipt_concurrency` | 10 | Number of blocks to fetch receipts for concurrently |

```json
{
  "chains": [{
    "name": "optimism",
    "block_receipts_method": "eth_getBlockReceipts"
  }],
  "raw_data_collection": {
    "block_receipt_concurrency": 20
  }
}
```

### RPC Batch Size

Controls how many RPC requests are batched together. Resolution order: `chain.rpc.batch_size` -> `raw_data_collection.rpc_batch_size` -> default (100).

| Setting | Default | Description |
|---------|---------|-------------|
| `rpc.batch_size` | 100 | Per-chain batch size override |
| `raw_data_collection.rpc_batch_size` | 100 | Global fallback batch size |

### Catchup Concurrency Settings

Additional concurrency settings in `raw_data_collection` for catchup phases:

| Setting | Default | Description |
|---------|---------|-------------|
| `decoding_concurrency` | 4 | Number of concurrent decoding tasks for log and eth_call catchup |
| `factory_concurrency` | 4 | Number of concurrent tasks for factory collection catchup |

## Coordination Between Collectors

### Logs Waiting for Factories

When `contract_logs_only: true` is configured and factories exist:

1. Logs collector receives `LogMessage::Logs` and accumulates them
2. Logs collector receives `LogMessage::RangeComplete`
3. **Before filtering/writing**, logs collector waits for `FactoryAddressData` for that range
4. Logs are filtered to include:
   - Configured contract addresses
   - Factory-discovered addresses for that range
5. Filtered logs are written to parquet

This ensures logs from newly discovered factory contracts are included in the output.

### Eth Calls with Factory Contracts

1. Regular eth_calls execute immediately when block ranges complete (no waiting)
2. Factory eth_calls execute when `FactoryAddressData` arrives from factories collector
3. Both types of results are written independently

## Backpressure Monitoring

The receipts collector monitors channel backpressure to help diagnose bottlenecks:

- **High pressure warning**: Logged when channel is >90% full
- **Summary metrics** at completion:
  - Total sends and average send time
  - High pressure sends (>50% full)
  - Critical pressure sends (>90% full)
  - Maximum single send time

If you see frequent high-pressure warnings, consider:
- Increasing `channel_capacity`
- Optimizing downstream consumers (logs/factories)
- Reducing `parquet_block_range` to process smaller batches

## Resumability and Catchup

Each collector implements resumability to handle crashes and restarts gracefully.

### Catchup Phase

On startup, before processing new data from channels, collectors perform catchup:

1. **Blocks Collector**: Scans existing block parquet files, skips already-collected ranges
2. **Receipts Collector**:
   - Scans block files to find available ranges
   - Checks if receipts, logs, and factory files exist for each range
   - Re-processes ranges where any downstream files are missing
3. **Logs Collector**: Relies on receipts collector for catchup (receipts re-sends data if logs missing)
4. **Factories Collector**:
   - Scans log files to find available ranges
   - Re-processes ranges where factory files are missing
   - Sends cached factory addresses to downstream collectors on startup
5. **Eth Calls Collector**: Scans block files and re-processes missing eth_call files

### Empty Range Handling

Collectors write empty parquet files for ranges with no data (e.g., no transactions, no matching logs). This marks ranges as processed and prevents unnecessary re-processing.

### Cascade Re-processing

If you delete a downstream file, the upstream collector's catchup will detect the missing file and re-process that range:

- Delete `logs/*.parquet` → Receipts collector re-sends log data
- Delete `factories/*.parquet` → Receipts collector re-processes via factories
- Delete `eth_calls/*.parquet` → Eth calls collector re-processes from block data

## Threading Model

The indexer uses Tokio's async runtime with two JoinSets for task lifecycle management. Each collector is split into a catchup phase and a current phase that runs sequentially within its task.

**Historical tasks** (`tasks` JoinSet) -- complete after historical processing:

```rust
let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

tasks.spawn(collect_blocks(...));       // catchup then current phase
tasks.spawn(collect_receipts(...));     // catchup then current phase
tasks.spawn(collect_logs(...));         // catchup then current phase
tasks.spawn(collect_factories(...));    // if factories configured
tasks.spawn(collect_eth_calls(...));    // catchup then current phase
tasks.spawn(decode_logs(...));          // if events configured
tasks.spawn(decode_eth_calls(...));     // if eth_calls configured

while let Some(result) = tasks.join_next().await {
    result??;
}
```

**Live tasks** (`live_tasks` JoinSet) -- continue running into live mode:

```rust
let mut live_tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

live_tasks.spawn(engine.run(...));      // if transformations configured

// After historical tasks complete, live mode tasks are spawned into live_tasks:
// - WebSocket subscription
// - LiveCollector (block processing, reorg detection)
// - CompactionService (periodic parquet compaction)
// - LiveMessage drain task

while let Some(result) = live_tasks.join_next().await {
    result??;
}
```

All RPC tasks share the same rate limiter (`SlidingWindowRateLimiter`), which manages CU consumption across all operations. This ensures the combined RPC usage stays within provider limits.

### Catchup Synchronization Barriers

The system uses `oneshot` channels as barriers to coordinate catchup ordering:

1. **Factory catchup barrier**: Factory catchup must complete before factory-dependent eth_call catchup begins.
2. **Eth_call decode catchup barrier**: Eth_call decode catchup must complete before the transformation engine runs its own catchup, ensuring decoded parquet files are available. After this barrier is released, the transformation engine uses the DAG scheduler (see [DAG-Based Catchup Execution](#dag-based-catchup-execution)) rather than sequential execution, enabling pipelined, dependency-aware handler processing across all pending ranges.

## Transformation Engine Parallelism

The transformation engine uses semaphore-bounded concurrency for handler execution:

### Handler Concurrency

| Setting | Default | Description |
|---------|---------|-------------|
| `handler_concurrency` | 4 | Number of handlers that can execute concurrently |

```json
{
  "transformations": {
    "handler_concurrency": 4,
    "mode": {
      "batch_for_catchup": true,
      "catchup_batch_size": 10000
    }
  }
}
```

### Execution Pattern

Handlers process ranges concurrently using a semaphore to limit parallelism:

```rust
let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
let mut join_set: JoinSet<Result<...>> = JoinSet::new();

for range in ranges_to_process {
    let permit = semaphore.clone().acquire_owned().await?;
    join_set.spawn(async move {
        let result = handler.handle(events).await;
        drop(permit);
        result
    });
}
```

This ensures:
- **Bounded memory usage**: Only `handler_concurrency` handlers run simultaneously
- **Independent progress**: Each handler tracks its own progress in `_handler_progress`
- **Independent transactions**: Each handler's operations execute in their own transaction
- **Fault isolation**: One handler's failure doesn't affect others

### DAG-Based Catchup Execution

The transformation engine uses a DAG (Directed Acyclic Graph) scheduler for catchup processing that enables pipelined, dependency-aware execution:

```
┌─────────────────────────────────────────────────┐
│              CompletionTracker                    │
│   Tracks per-(handler, range) completion state   │
│   Seeded from _handler_progress on startup       │
└─────────────────────────┬───────────────────────┘
                          │
┌─────────────────────────▼───────────────────────┐
│              DagScheduler                         │
│   1. Spawn task per WorkItem                     │
│   2. Await dependency satisfaction               │
│   3. Acquire sequential semaphore (if needed)    │
│   4. Acquire global concurrency permit           │
│   5. Execute handler                             │
│   6. Mark completed/failed, wake dependents      │
└─────────────────────────────────────────────────┘
```

Key properties:
- **Pipelined execution**: Range R+1 for handler A can start while R is still running for dependent handler B
- **Cascade failures**: If handler A fails on range R, all dependents immediately cascade-fail on that range
- **Sequential handlers**: Per-handler FIFO semaphore (capacity 1) ensures strict range ordering for state-dependent handlers
- **Multi-pass deferral**: Catchup runs multiple passes, deferring ranges where call-dependency files don't yet exist

Configuration:
| Setting | Default | Description |
|---------|---------|-------------|
| `handler_concurrency` | 4 | Global concurrency limit for DAG scheduler |

The DAG scheduler replaces the previous sequential per-handler catchup, enabling much higher throughput when handlers have independent dependency chains.

## Performance Tuning

### For High-Throughput Scenarios

```json
{
  "chains": [{
    "rpc": {
      "concurrency": 500,
      "compute_units_per_second": 7500,
      "batch_size": 100
    }
  }],
  "raw_data_collection": {
    "parquet_block_range": 10000,
    "channel_capacity": 5000,
    "block_receipt_concurrency": 20,
    "decoding_concurrency": 8,
    "factory_concurrency": 8
  },
  "transformations": {
    "handler_concurrency": 8,
    "mode": {
      "batch_for_catchup": true,
      "catchup_batch_size": 10000
    }
  }
}
```

### For Memory-Constrained Environments

```json
{
  "chains": [{
    "rpc": {
      "concurrency": 50,
      "batch_size": 50
    }
  }],
  "raw_data_collection": {
    "parquet_block_range": 1000,
    "rpc_batch_size": 50,
    "channel_capacity": 500,
    "factory_channel_capacity": 500,
    "decoding_concurrency": 2,
    "factory_concurrency": 2
  },
  "transformations": {
    "handler_concurrency": 2,
    "mode": {
      "batch_for_catchup": true,
      "catchup_batch_size": 1000
    }
  }
}
```

### Monitoring Tips

1. **Watch for backpressure warnings** - Indicates downstream is slower than upstream
2. **Check RPC rate limit errors** - May need to reduce batch size or concurrency
3. **Monitor file sizes** - Very large parquet files may indicate `parquet_block_range` is too high
4. **Track catchup duration** - Long catchup suggests many ranges need re-processing
5. **Handler execution time** - If handlers are slow, consider increasing `handler_concurrency`

### ConcurrentExecutor Helper

The `AlchemyClient` uses a `ConcurrentExecutor` helper struct that encapsulates semaphore + rate limiter concurrency control:

```rust
// Internal structure (not public API)
struct ConcurrentExecutor {
    semaphore: Arc<Semaphore>,
    rate_limiter: Arc<SlidingWindowRateLimiter>,
    cost_per_request: u32,
}
```

This abstraction is used by both ordered and streaming execution methods.

### Concurrent Execution Methods

The codebase uses two main patterns for concurrent execution:

**1. Semaphore-bounded JoinSet (for ordered results)**

Used by `execute_concurrent_ordered` in AlchemyClient for batch RPC calls:

```rust
let semaphore = Arc::new(Semaphore::new(rpc_concurrency));
let mut join_set = JoinSet::new();

for (idx, request) in requests.into_iter().enumerate() {
    let semaphore = semaphore.clone();
    join_set.spawn(async move {
        let permit = semaphore.acquire_owned().await?;
        let result = make_request(request).await;
        drop(permit);
        (idx, result)
    });
}

// Collect and sort by index to preserve order
let mut results = Vec::new();
while let Some(result) = join_set.join_next().await {
    results.push(result?);
}
results.sort_by_key(|(idx, _)| *idx);
```

**2. Streaming execution (for immediate forwarding)**

Used by `execute_streaming` in AlchemyClient for block fetching:

```rust
let semaphore = Arc::new(Semaphore::new(rpc_concurrency));

for request in requests {
    let result_tx = result_tx.clone();
    join_set.spawn(async move {
        let permit = semaphore.acquire_owned().await?;
        let result = make_request(request).await;
        drop(permit);
        let _ = result_tx.send(result).await;
    });
}
```

Results are sent immediately as they complete, enabling pipeline parallelism.

**3. Semaphore-bounded JoinSet for Decoding**

The catchup decoders (`src/decoding/catchup/logs.rs` and `src/decoding/catchup/eth_calls.rs`) use semaphore-bounded JoinSet for parallel parquet file processing:

```rust
let semaphore = Arc::new(Semaphore::new(decoding_concurrency));
let mut join_set: JoinSet<Result<...>> = JoinSet::new();

for file in files_to_process {
    let permit = semaphore.clone().acquire_owned().await?;
    join_set.spawn(async move {
        let result = decode_file(file).await;
        drop(permit);
        result
    });
}
```

This ensures bounded memory usage during catchup while maximizing parallelism.

## Retry and Error Handling

All RPC operations use automatic retry with exponential backoff:

| Setting | Default | Description |
|---------|---------|-------------|
| `max_retries` | 10 | Maximum retry attempts |
| `initial_delay` | 500ms | Delay before first retry |
| `max_delay` | 30s | Maximum delay between retries |
| `backoff_multiplier` | 2.0 | Exponential backoff factor |

Retryable errors include:
- Network/connection errors
- Rate limit responses (429)
- Server errors (502, 503, 504)
- Timeout errors

Non-retryable errors (invalid URL, malformed requests) fail immediately.

### Panic Handling

Concurrent batch methods handle task panics gracefully:
- Panics are logged and tracked
- Method returns `Err(RpcError::ProviderError("One or more concurrent tasks panicked"))`
- Prevents silent partial results

## Live Mode

After historical processing completes, the system transitions to live mode (enabled by default when `ws_url_env_var` is configured). Live mode adds several concurrent tasks:

1. **WebSocket Subscription** - Subscribes to new block headers via `eth_subscribe("newHeads")`
2. **LiveCollector** - Processes each new block: fetches full block data, extracts logs, runs factory matchers, triggers eth_calls, sends decoded data to decoders
3. **LiveEthCallCollector** - Executes eth_call requests for each new block, supporting multicall3 batching
4. **CompactionService** - Periodically compacts per-block bincode files into parquet ranges

Live mode reuses the same decoder and transformation engine channels from historical mode, so the transformation engine processes both historical catchup data and live data through the same `select!` loop.

### Transformation Retry

The `CompactionService` detects blocks with incomplete or failed transformations and sends `TransformRetryRequest` messages to the transformation engine:

```rust
pub struct TransformRetryRequest {
    pub block_number: u64,
    pub missing_handlers: Option<HashSet<String>>,
}
```

A configurable grace period (`transform_retry_grace_period_secs`, default 300s) prevents retrying blocks that are still being actively processed.

## See Also

- [Block Collection](./BLOCK_COLLECTION.md) - Block fetching and channel output
- [Receipt Collection](./RECEIPTS_COLLECTION.md) - Receipt fetching and log extraction
- [Log Collection](./LOGS_COLLECTION.md) - Log filtering and parquet writing
- [Factory Collection](./FACTORY_COLLECTION.md) - Factory address discovery
- [eth_call Collection](./ETH_CALL_COLLECTION.md) - eth_call execution and storage
- [Decoding](./DECODING.md) - ABI-based log and call decoding
- [RPC](./RPC.md) - Rate limiting and retry configuration
- [Transformations](./TRANSFORMATIONS.md) - Handler concurrency and batch mode
- [Configuration](./CONFIG.md) - Full configuration reference
