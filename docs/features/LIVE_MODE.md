# Live Mode

Live mode enables real-time block processing via WebSocket subscription, complementing the historical HTTP-based collection. Instead of processing blocks in large ranges, live mode processes blocks individually as they arrive at the chain tip, providing lower latency for real-time applications.

## Overview

The indexer operates in two modes:

1. **Historical Mode** (HTTP): Processes blocks in configurable ranges (default: 1000 blocks), writes to parquet files
2. **Live Mode** (WebSocket): Processes blocks individually as they arrive, writes to bincode files, later compacted to parquet

After historical catchup completes, the indexer transitions to live mode for real-time processing.

## Architecture

```
                            WebSocket (eth_subscribe "newHeads")
                                          │
                                          ▼
                            ┌───────────────────────────┐
                            │      LiveCollector        │◄────── ReorgDetector
                            │     (block headers)       │             │
                            └───────────────────────────┘             │
                                          │                           │ reorg?
                    ┌─────────────────────┼─────────────────────┐     ▼
                    │                     │                     │  Delete orphaned
                    ▼                     ▼                     │  Notify all stages
        ┌───────────────────┐   ┌───────────────────┐          │
        │   HTTP: Receipts  │   │  HTTP: eth_calls  │          │
        │   & Logs          │   │  (configured)     │◄─────────┤
        └───────────────────┘   └───────────────────┘          │
                    │                     │                     │
                    │                     │ ◄── wait for        │
                    ▼                     │     factory addrs   │
        ┌───────────────────┐             │                     │
        │  FactoryParser    │─────────────┤                     │
        │  (from logs)      │             │                     │
        └───────────────────┘             │                     │
                    │                     │                     │
                    │ new factory         ▼                     │
                    │ addresses   ┌───────────────────┐         │
                    └────────────►│  HTTP: eth_calls  │         │
                                  │  (factory pools)  │         │
                                  └───────────────────┘         │
                                          │                     │
        ┌─────────────────────────────────┴─────────────────────┤
        │                                                       │
        ▼                                                       │
┌───────────────────┐                                           │
│   LiveStorage     │  raw/{blocks,receipts,logs,eth_calls}/     │
│  (bincode files)  │                                           │
└───────────────────┘                                           │
        │                                                       │
        ├───────────────────────────────────────┐               │
        ▼                                       ▼               │
┌───────────────────┐                   ┌───────────────────┐   │
│   Log Decoder     │                   │  EthCall Decoder  │   │
│  (live_mode=true) │                   │  (live_mode=true) │   │
└───────────────────┘                   └───────────────────┘   │
        │                                       │               │
        │         ┌─────────────────────────────┘               │
        ▼         ▼                                             │
┌─────────────────────────────────────┐                         │
│        LiveStorage (decoded)        │                         │
│  decoded/logs/, decoded/eth_calls/  │                         │
└─────────────────────────────────────┘                         │
                    │                                           │
                    ▼                                           │
┌─────────────────────────────────────┐                         │
│     TransformationEngine            │◄────────────────────────┘
│  (handlers with call dependencies)  │      ReorgMessage
└─────────────────────────────────────┘
                    │
                    │ mark block complete
                    ▼
┌─────────────────────────────────────┐
│       LiveProgressTracker           │
│  (per-block, per-handler tracking)  │
└─────────────────────────────────────┘
                    │
                    │ all handlers complete?
                    │ range_end < tip - reorg_depth?
                    ▼
┌─────────────────────────────────────┐
│        CompactionService            │
│  (bincode → parquet at range_size)  │
└─────────────────────────────────────┘
                    │
                    ▼
            data/raw/{chain}/
            data/derived/{chain}/
```

### Data Flow Summary

1. **Block Header**: WebSocket delivers new block header
2. **Reorg Check**: Verify parent hash matches; if not, trigger reorg cleanup
3. **Parallel Collection**:
   - Fetch receipts and extract logs
   - Fetch configured eth_calls for non-factory contracts
4. **Factory Parsing**: Parse factory events from logs to discover new addresses
5. **Factory Calls**: Fetch eth_calls for newly discovered factory addresses
6. **Storage**: Write raw data to bincode files
7. **Decoding**: Decode logs and eth_calls, write to decoded bincode
8. **Transformation**: Run handlers (waiting for call dependencies)
9. **Progress**: Mark block complete per handler
10. **Compaction**: When range complete AND beyond reorg depth, compact to parquet

## Storage Structure

Live mode uses a dedicated storage directory with bincode format for fast serialization:

```
data/{chain}/live/
├── decoded/
│   ├── eth_calls/{block_number}/{contract}/{function}.bin
│   └── logs/{block_number}/{contract}/{event}.bin
├── factories/{block_number}.bin    # Factory addresses discovered in block
├── raw/
│   ├── blocks/{block_number}.bin   # Block headers
│   ├── eth_calls/{block_number}.bin # ETH call results
│   ├── logs/{block_number}.bin     # Raw logs
│   └── receipts/{block_number}.bin # Transaction receipts
├── snapshots/{block_number}.bin    # Upsert snapshots for reorg rollback
└── status/{block_number}.json      # Processing status (JSON for debugging)
```

### Block Status Tracking

Each block's processing status is tracked in a JSON file:

```json
{
  "collected": true,
  "block_fetched": true,
  "receipts_collected": true,
  "logs_collected": true,
  "factories_extracted": true,
  "eth_calls_collected": true,
  "logs_decoded": true,
  "eth_calls_decoded": true,
  "transformed": true,
  "completed_handlers": ["handler_v1"],
  "failed_handlers": []
}
```

A block is ready for compaction when all flags are `true`. The completeness check uses `is_complete_with(expectations)` which respects `LivePipelineExpectations` -- optional stages (eth_call collection, decoding, transformations) are skipped if not expected by the runtime pipeline configuration.

### LiveLog (Raw Log Storage)

Raw logs are stored with full transaction context for downstream traceability:

```rust
pub struct LiveLog {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
    pub log_index: u32,
    pub transaction_index: u32,
    pub transaction_hash: [u8; 32],  // For log-receipt correlation
}
```

The `transaction_hash` field enables correlation between logs and their originating transactions, which is essential for downstream processing and debugging.

## DecoderMessage Live Mode

The `DecoderMessage` variants include a `live_mode` flag:

```rust
DecoderMessage::LogsReady {
    range_start: u64,
    range_end: u64,
    logs: Vec<LogData>,
    live_mode: bool,              // true for live mode
    has_factory_matchers: bool,   // whether factory address matching is active
}
```

When `live_mode: true`:
- Decoded data is written to bincode files in `data/{chain}/live/decoded/`
- Block status is updated to mark decoding complete
- Data is later compacted to parquet by the CompactionService

When `live_mode: false`:
- Decoded data is written directly to parquet in `data/{chain}/historical/decoded/`

## Live Decoded Data Types

Decoded data in live mode reuses the shared `DecodedValue` enum from `crate::types::decoded`, which supports bincode serialization:

```rust
pub enum DecodedValue {
    Address([u8; 20]),
    Uint256(U256),
    Int256(I256),
    Uint128(u128),
    Int128(i128),
    Uint64(u64),
    Int64(i64),
    Uint32(u32),
    Int32(i32),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    NamedTuple(Vec<(String, DecodedValue)>),
    UnnamedTuple(Vec<DecodedValue>),
    Array(Vec<DecodedValue>),
}
```

### LiveDecodedLog

```rust
pub struct LiveDecodedLog {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub contract_address: [u8; 20],
    pub decoded_values: Vec<DecodedValue>,
}
```

### LiveDecodedCall

```rust
pub struct LiveDecodedCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub decoded_value: DecodedValue,
}
```

### LiveDecodedEventCall

```rust
pub struct LiveDecodedEventCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub decoded_value: DecodedValue,
}
```

### LiveDecodedOnceCall

```rust
pub struct LiveDecodedOnceCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub decoded_values: Vec<(String, DecodedValue)>,  // function_name -> value
}
```

## Reorg Handling

When the LiveCollector detects a chain reorganization (parent hash mismatch):

1. **Detection**: ReorgDetector compares incoming block's parent_hash with stored hash
2. **Orphan Identification**: Walk back to find the common ancestor
3. **Cleanup**:
   - Delete raw data for orphaned blocks from `data/{chain}/live/`
   - Delete decoded data for orphaned blocks
   - Clear progress tracker entries for orphaned blocks
   - Send `DecoderMessage::Reorg` to log and eth_call decoder channels
   - Send `ReorgMessage` to the transformation engine to clean up pending events
   - Send `LiveMessage::Reorg` downstream

```rust
DecoderMessage::Reorg {
    _common_ancestor: u64,  // Last valid block
    orphaned: Vec<u64>,     // Blocks to delete
}
```

## Compaction

The CompactionService periodically checks for complete block ranges and compacts them:

### Compaction Criteria

A range is ready for compaction when:
1. All blocks in the range are present **and sequential** (e.g., blocks 1000-1999 with no gaps)
2. All blocks have `status.is_complete_with(expectations) == true` (respects `LivePipelineExpectations` for optional stages)
3. All transformation handlers have completed for all blocks (checked via `LiveProgressTracker`)
4. The range end is beyond the reorg safety boundary (`latest_block - reorg_depth`)

**Sequential validation:** The compaction service validates that blocks form a complete sequential range, not just that the count matches. This prevents compacting sparse ranges (e.g., `[0,2,4,6,8...]`) that happen to have the correct count:

```rust
// Sort and verify blocks are sequential
block_numbers.sort_unstable();
if block_numbers.first() != Some(&range_start)
    || block_numbers.last() != Some(&range_end) {
    continue;  // Wrong boundaries
}

// Verify no gaps
let has_gap = block_numbers.windows(2).any(|w| w[1] != w[0] + 1);
if has_gap {
    continue;  // Gaps in sequence
}
```

### Compaction Process

1. Read all bincode files for the range
2. Convert to Arrow arrays
3. Write parquet files:
   - Blocks to `data/raw/{chain}/blocks/{start}_{end}.parquet`
   - Logs to `data/raw/{chain}/logs/{start}_{end}.parquet`
   - Eth calls to `data/raw/{chain}/eth_calls/{start}_{end}.parquet`
   - Factory addresses to `data/derived/{chain}/factories/{collection}/{start}-{end}.parquet`
4. Optionally upload parquet files to S3 (if `StorageManager` is configured)
5. Migrate progress from `_live_progress` to `_handler_progress`
6. Delete bincode files, status files, and decoded data
7. Clear progress tracker in-memory state for the range

### Configuration

```json
{
  "raw_data_collection": {
    "live_mode": true,
    "reorg_depth": 128,
    "compaction_interval_secs": 10
  }
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `live_mode` | true | Enable WebSocket live mode (enabled by default when `ws_url_env_var` is set) |
| `reorg_depth` | 128 | Blocks to track for reorg detection |
| `compaction_interval_secs` | 10 | Check interval for compaction |
| `transform_retry_grace_period_secs` | 300 | Minimum seconds before retrying stuck transformations |

## Progress Tracking

Live mode uses per-block progress tracking instead of per-range:

### _live_progress Table

```sql
CREATE TABLE _live_progress (
    chain_id BIGINT NOT NULL,
    handler_key TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    PRIMARY KEY (chain_id, handler_key, block_number)
);
```

During compaction, entries are migrated to `_handler_progress` with the range information.

**Progress migration semantics:** The `range_start` column in `_handler_progress` means "next range to process starts at". After compacting range 0-999, the value inserted is 1000 (not 999). This matches historical mode semantics where `range_start` indicates the starting point for the next processing run.

### LiveProgressTracker

The `LiveProgressTracker` coordinates between the transformation engine and compaction service:

```rust
pub struct LiveProgressTracker {
    chain_id: i64,
    chain_name: String,                        // for LiveStorage access
    completed: HashMap<u64, HashSet<String>>,   // block -> completed handler keys
    handler_keys: HashSet<String>,              // all registered handlers
    db_pool: Option<Arc<DbPool>>,
}
```

**Initialization:**
1. Created in `process_chain()` before the transformation engine
2. All handlers are registered via `register_handler(&handler.handler_key())`
3. Passed to both `TransformationEngine` and `spawn_live_mode()`

**Block completion flow:**
1. Handler processes a block and calls `record_completed_range_for_handler()`
2. Engine calls `progress_tracker.mark_complete(block, handler_key)` for single-block ranges
3. Tracker persists to `_live_progress` table and updates in-memory state
4. Tracker atomically updates the status file via `update_status_atomic()` (file-locked read-modify-write) to add the handler key to `completed_handlers` and set `transformed = true` when all handlers are done
5. `is_block_complete(block)` returns `true` when all handlers have marked complete
6. Compaction service checks completion before compacting ranges

**Why this matters:** Without handler registration, `is_block_complete()` returns `true` immediately (empty handler set), allowing compaction before handlers finish processing.

## Mode Transition

After historical catchup completes:

1. Get the last processed block number
2. Connect to WebSocket and subscribe to `eth_subscribe("newHeads")`
3. Receive first live block
4. If gap exists between catchup and first live block, backfill via HTTP
5. Start live processing loop
6. Start compaction service

```rust
let last_catchup_block = get_last_processed_block();
let first_live_block = ws_client.subscribe().await?;

// Backfill gap if needed
if first_live_block.number > last_catchup_block + 1 {
    backfill_via_http(last_catchup_block + 1, first_live_block.number).await?;
}

// Start live collectors
spawn(live_collector.run(...));
spawn(compaction_service.run(...));
```

## CLI Modes

### Live-Only Mode

Start directly in live mode without historical processing:

```bash
./doppler-indexer --live-only
```

This mode:
- Skips historical catchup entirely
- Requires `ws_url_env_var` to be configured
- Useful for testing live mode in isolation
- Can run on a dedicated machine while historical runs elsewhere

### Behavior Summary

| `live_mode` config | `ws_url_env_var` | WS URL env set | Result |
|--------------------|------------------|----------------|--------|
| not set (default)  | set              | yes            | **Live mode enabled** |
| not set (default)  | set              | no             | Live mode skipped |
| not set (default)  | not set          | -              | Live mode skipped |
| `true`             | set              | yes            | Live mode enabled |
| `true`             | not set          | -              | Live mode skipped (warning) |
| `false`            | -                | -              | Live mode disabled |

## WebSocket Reconnection

On WebSocket disconnect:
1. Log disconnect with last known block
2. Attempt reconnection with exponential backoff (100ms initial, 30s max)
3. On reconnect, calculate missed block range
4. Backfill missed blocks via HTTP
5. Resume live processing

## LiveStorage API

```rust
impl LiveStorage {
    // Block operations
    fn write_block(&self, block: &LiveBlock) -> Result<()>;
    fn read_block(&self, block_number: u64) -> Result<LiveBlock>;
    fn delete_block(&self, block_number: u64) -> Result<()>;
    fn list_blocks(&self) -> Result<Vec<u64>>;

    // Receipt operations
    fn write_receipts(&self, block_number: u64, receipts: &[LiveReceipt]) -> Result<()>;
    fn read_receipts(&self, block_number: u64) -> Result<Vec<LiveReceipt>>;
    fn delete_receipts(&self, block_number: u64) -> Result<()>;

    // Log operations
    fn write_logs(&self, block_number: u64, logs: &[LiveLog]) -> Result<()>;
    fn read_logs(&self, block_number: u64) -> Result<Vec<LiveLog>>;
    fn delete_logs(&self, block_number: u64) -> Result<()>;

    // Eth call operations
    fn write_eth_calls(&self, block_number: u64, calls: &[LiveEthCall]) -> Result<()>;
    fn read_eth_calls(&self, block_number: u64) -> Result<Vec<LiveEthCall>>;
    fn delete_eth_calls(&self, block_number: u64) -> Result<()>;

    // Factory operations
    fn write_factories(&self, block_number: u64, addresses: &LiveFactoryAddresses) -> Result<()>;
    fn read_factories(&self, block_number: u64) -> Result<LiveFactoryAddresses>;
    fn delete_factories(&self, block_number: u64) -> Result<()>;
    fn list_factory_blocks(&self) -> Result<Vec<u64>>;

    // Snapshot operations (for reorg rollback)
    fn write_snapshots(&self, block_number: u64, snapshots: &[LiveUpsertSnapshot]) -> Result<()>;
    fn read_snapshots(&self, block_number: u64) -> Result<Vec<LiveUpsertSnapshot>>;
    fn delete_snapshots(&self, block_number: u64) -> Result<()>;

    // Decoded log operations
    fn write_decoded_logs(&self, block_number: u64, contract: &str, event: &str, logs: &[LiveDecodedLog]) -> Result<()>;
    fn read_decoded_logs(&self, block_number: u64, contract: &str, event: &str) -> Result<Vec<LiveDecodedLog>>;
    fn delete_all_decoded_logs(&self, block_number: u64) -> Result<()>;
    fn list_decoded_log_types(&self, block_number: u64) -> Result<Vec<(String, String)>>;

    // Decoded eth_call operations
    fn write_decoded_calls(&self, block_number: u64, contract: &str, function: &str, calls: &[LiveDecodedCall]) -> Result<()>;
    fn write_decoded_once_calls(&self, block_number: u64, contract: &str, calls: &[LiveDecodedOnceCall]) -> Result<()>;
    fn write_decoded_event_calls(&self, block_number: u64, contract: &str, function: &str, calls: &[LiveDecodedEventCall]) -> Result<()>;
    fn delete_all_decoded_calls(&self, block_number: u64) -> Result<()>;
    fn list_decoded_call_types(&self, block_number: u64) -> Result<Vec<(String, String)>>;

    // Status operations
    fn write_status(&self, block_number: u64, status: &LiveBlockStatus) -> Result<()>;
    fn read_status(&self, block_number: u64) -> Result<LiveBlockStatus>;
    fn update_status_atomic<F>(&self, block_number: u64, update_fn: F) -> Result<()>;  // File-locked read-modify-write
    fn delete_status(&self, block_number: u64) -> Result<()>;

    // Reorg detection helpers
    fn get_recent_blocks_for_reorg(&self, count: u64) -> Result<Vec<LiveBlock>>;
    fn max_block_number(&self) -> Result<Option<u64>>;

    // Gap detection
    fn find_gaps(&self) -> Result<Vec<(u64, u64)>>;  // Returns (start, end) ranges of missing blocks

    // Bulk operations
    fn delete_all(&self, block_number: u64) -> Result<()>;  // Deletes all data for a block
    fn delete_range(&self, start: u64, end: u64) -> Result<()>;  // Deletes all data for a range
}
```

## Performance Considerations

- **Bincode format**: Much faster serialization/deserialization than parquet for single-block writes
- **Per-block files**: Enables efficient deletion during reorgs without rewriting entire parquet files
- **Status files as JSON**: Easy to debug and inspect processing state
- **Compaction batching**: Processes complete ranges to minimize parquet file count
- **Reorg depth**: 128 blocks covers most reorgs while limiting memory usage

## Data Safety

### Atomic Writes

All file writes use atomic operations to prevent corruption from crashes:

```rust
// Write to temp file with unique suffix, flush, sync, then atomic rename
let random_suffix: u32 = rand::random();
let temp_name = format!("{}.tmp.{}", path.file_name()..., random_suffix);
let temp_path = path.with_file_name(temp_name);
let file = fs::File::create(&temp_path)?;
let mut writer = BufWriter::new(file);
bincode::serialize_into(&mut writer, data)?;
writer.flush()?;
writer.into_inner()?.sync_all()?;
fs::rename(&temp_path, path)?;
```

Temp files use a random suffix (`.tmp.{random}`) to avoid race conditions between concurrent writers (e.g., collector and decoder both updating status for the same block).

On startup, `ensure_dirs()` cleans up any leftover `.tmp` files from interrupted writes, including in nested subdirectories.

### Atomic Status Updates

Status files support atomic read-modify-write via `update_status_atomic()`, which uses a separate `.json.lock` file with exclusive file locking to prevent concurrent updates from overwriting each other:

```rust
storage.update_status_atomic(block_number, |status| {
    status.completed_handlers.insert(handler_key);
    if all_complete {
        status.transformed = true;
    }
});
```

### TOCTOU Race Prevention

Read operations handle `NotFound` from the actual read instead of checking `exists()` first:

```rust
// Correct: handle error from actual operation
pub fn read_block(&self, block_number: u64) -> Result<LiveBlock, StorageError> {
    let path = self.block_path(block_number);
    read_bincode(&path).map_err(|e| map_not_found(e, block_number))
}
```

This prevents race conditions where a file could be deleted between the exists check and the read.

### Reorg Detector Seeding

On restart, the `ReorgDetector` is seeded from existing storage to detect reorgs for the first 128 blocks:

```rust
// In LiveCollector::new()
match storage.get_recent_blocks_for_reorg(config.reorg_depth) {
    Ok(recent) if !recent.is_empty() => {
        reorg_detector.seed(recent);
    }
    // ...
}
```

### Gap Detection

Live mode has multiple layers of gap detection to ensure no blocks are missed:

#### 1. Storage Gap Detection (on startup)

On startup, before processing new blocks, gaps within existing storage are detected and backfilled:

```rust
// In LiveStorage
pub fn find_gaps(&self) -> Result<Vec<(u64, u64)>, StorageError> {
    let blocks = self.list_blocks()?;
    let mut gaps = Vec::new();
    for window in blocks.windows(2) {
        if window[1] > window[0] + 1 {
            gaps.push((window[0] + 1, window[1] - 1));
        }
    }
    Ok(gaps)
}
```

This catches incomplete backfills from previous sessions. For example, if blocks 100-110 exist but 103-107 are missing:

```
INFO  Found 1 gaps (5 missing blocks) in storage, backfilling
INFO  Backfilling 5 blocks (103 to 107)
```

#### 2. Transition Gap Detection (on first WS block)

On the first WebSocket block received, gaps between storage and the new block are detected:

```rust
// On first block received
if block_number > expected_start_block + 1 {
    tracing::warn!("Gap detected, backfilling...");
    backfill_blocks(expected_start + 1, block_number - 1).await?;
}
```

#### 3. Reconnection Gap Detection

When WebSocket reconnects after a disconnect, missed blocks are backfilled:

```rust
WsEvent::Reconnected { missed_from, missed_to } => {
    backfill_blocks(missed_from, missed_to, ...).await?;
}
```

### Reorg During Compaction

The progress tracker is shared between collector and compaction service. When a reorg occurs:

1. Collector deletes orphaned blocks and clears their progress
2. Compaction gracefully handles `NotFound` errors (skips the range)

```rust
// In compact_range()
match self.storage.read_block(block_number) {
    Ok(block) => blocks.push(block),
    Err(StorageError::NotFound(_)) => {
        tracing::warn!("Block deleted during compaction (reorg?), skipping range");
        return Ok(());
    }
    Err(e) => return Err(e.into()),
}
```

## Stuck Block Detection and Transform Retry

The `CompactionService` monitors for blocks that are decoded but stuck waiting for transformation. This handles cases where the transformation engine drops a block due to transient errors (DB failures, handler bugs).

**Detection criteria** (checked each compaction cycle):
1. Block's transform inputs are ready (`logs_decoded` and `eth_calls_decoded`)
2. Block is NOT yet `transformed`
3. Block is NOT complete in the progress tracker
4. Block has been in this state for at least `transform_retry_grace_period_secs` (default: 300s)

**Retry mechanism:**
- The compaction service sends a `TransformRetryRequest` (with the block number and set of missing/failed handler keys) via a bounded channel to the transformation engine
- Failed handlers from the status file's `failed_handlers` set are included alongside pending handlers from the progress tracker
- Grace period prevents premature retries for blocks that are simply still being processed
- If the retry channel is full, the block keeps its original grace timer and will be retried on the next cycle

```rust
pub struct TransformRetryRequest {
    pub block_number: u64,
    pub missing_handlers: Option<HashSet<String>>,
}
```

## LivePipelineExpectations

Runtime expectations for optional pipeline stages, derived from the active pipeline wiring:

```rust
pub struct LivePipelineExpectations {
    pub expect_log_decode: bool,
    pub expect_eth_call_collection: bool,
    pub expect_eth_call_decode: bool,
    pub expect_transformations: bool,
}
```

These control which status flags are required for completeness checks. When a stage is not expected, its status flag is automatically set to `true` via `apply_expectations()`. This avoids blocking compaction on stages that aren't wired up in the current pipeline.

## LiveEthCallCollector

The `LiveEthCallCollector` (`src/live/eth_calls.rs`) handles all eth_call collection for live mode blocks, supporting:

1. **Regular frequency-based calls** - calls made at configured intervals (every block, duration-based)
2. **Factory calls** - calls to all known factory-discovered addresses
3. **Once calls** - one-time calls for newly discovered factory addresses
4. **Event-triggered calls** - calls triggered by matching log events

The collector uses a staged batch pattern (`CollectedEthCallBatch`): calls are collected, then the caller persists storage state before applying runtime updates or dispatching decoder messages. This ensures crash safety -- if the process dies between collection and persistence, the catchup service can re-collect.

## Outstanding Issues

> **Note**: Most critical issues have been resolved. The following items remain.

### Resolved Issues

The following issues from earlier versions have been fixed:

- **Issue 4 (LiveBlockStatus Never Set to Complete):** Now fixed - `status.decoded = true` is set in multiple places in the log decoder and eth_call decoder. The `status.transformed = true` is set in `progress.rs` when all handlers complete.
- **Issue 8 (Logs Parquet Missing transaction_hash):** Fixed - `compaction.rs` now includes `transaction_hash` field in the logs parquet schema.
- **Issue 9 (Progress Tracker Handlers Never Registered):** Fixed - Handlers are registered in `main.rs` during initialization.

- **Issue 3 (Transformation Engine in Live-Only Mode):** Now fixed - `process_chain_live_only()` spawns the `TransformationEngine` when `transformations_enabled` is true, alongside decoders and the live collector.

### Remaining Issues

#### 10. Decoded Data Discarded During Compaction

Per the TODO in `compact_range()`:

```rust
// TODO: Implement full decoded data compaction in Phase 7.
// The historical decoder will re-decode from the raw parquet files as needed.
```

Decoded bincode files are deleted during compaction without being converted to parquet. The data must be re-decoded from raw parquet later, which is inefficient.

## New Live Mode Types

### LiveUpsertSnapshot and LiveDbValue

For reorg rollback support, live mode captures snapshots of database state before upserts:

```rust
pub struct LiveUpsertSnapshot {
    pub table: String,
    pub source: String,                                // Handler source name (for scoped restoration)
    pub source_version: u32,                           // Handler source version
    pub key_columns: Vec<(String, LiveDbValue)>,       // Key columns with their values
    pub previous_row: Option<Vec<(String, LiveDbValue)>>,  // None = INSERT (reorg should DELETE)
}

pub enum LiveDbValue {
    Null,
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Int2(u8),
    Uint64(u64),
    Text(String),
    VarChar(String),
    Bytes(Vec<u8>),
    Address([u8; 20]),
    Bytes32([u8; 32]),
    Numeric(String),
    Timestamp(i64),
    Json(String),
    JsonB(String),
}
```

### LiveFactoryAddresses

Tracks factory addresses discovered in each block:

```rust
pub struct LiveFactoryAddresses {
    /// collection_name -> list of (timestamp, address) discovered
    pub addresses_by_collection: HashMap<String, Vec<(u64, [u8; 20])>>,
}
```

## Unified Error Type

Live mode uses a unified `LiveError` type (`src/live/error.rs`) that consolidates errors from various modules:

```rust
pub enum LiveError {
    Storage(StorageError),
    Rpc(RpcError),
    Database(DbError),
    ChannelClosed,
    BlockNotFound(u64),
}
```

Additionally, `CollectorError` (`src/live/collector.rs`) and `CompactionError` (`src/live/compaction.rs`) are separate error types specific to their modules.

## Recovery and Catchup

### Catchup for Incomplete Blocks

Live mode now includes a catchup mechanism for blocks that were partially processed in a previous session:

#### Problem

On restart, live mode previously had no way to recover from:
1. Blocks collected but not decoded (decoder crashed)
2. Blocks decoded but not transformed (engine crashed)
3. Blocks partially transformed (some handlers ran, others didn't)

#### Solution

**Enhanced LiveBlockStatus with Handler Tracking**

The status file now tracks which handlers have completed:

```json
{
  "collected": true,
  "block_fetched": true,
  "receipts_collected": true,
  "logs_collected": true,
  "factories_extracted": true,
  "eth_calls_collected": true,
  "logs_decoded": true,
  "eth_calls_decoded": true,
  "transformed": true,
  "completed_handlers": ["v3_handler_v1", "v4_handler_v1"],
  "failed_handlers": []
}
```

The `completed_handlers` and `failed_handlers` fields (with `#[serde(default)]` for backwards compatibility) enable catchup to determine exactly which handlers still need to run without querying the database. Failed handlers are retried via the stuck block detection mechanism.

**LiveCatchupService**

New service (`src/live/catchup.rs`) that:
- **Reconstructs missing status files** from local data and database
- Scans storage for incomplete blocks on startup
- Replays raw logs to decoder channel for blocks needing decode
- Replays raw eth_calls to decoder channel for blocks needing decode
- Identifies blocks needing transformation with specific missing handlers

**Status File Reconstruction**

If status files are missing but block data exists, the catchup service reconstructs them:

```rust
// Check what data exists for the block
let block_exists = storage.read_block(block_number).is_ok();
let logs_exist = storage.read_logs(block_number).is_ok();
let decoded_logs_exist = !storage.list_decoded_log_types(block_number)?.is_empty();

// Query database for completed handlers
let completed_handlers = db_pool.query(
    "SELECT handler_key FROM _live_progress WHERE chain_id = $1 AND block_number = $2",
    &[&chain_id, &block_number]
).await?;

// Reconstruct status
status.collected = block_exists;
status.logs_decoded = decoded_logs_exist || logs_empty;
status.completed_handlers = completed_handlers;
status.transformed = all_handlers_complete;
```

This enables recovery from:
- Accidentally deleted status files
- Status files corrupted during a crash
- Migration from older versions without certain status fields

**Catchup Flow**

```
Startup:
  1. LiveCollector::run() starts
  2. run_catchup_phase()
     ├── Reconstruct missing status files from data + database
     ├── Load progress from storage status files
     ├── scan_incomplete_blocks() → CatchupScanResult
     ├── resume_collection_blocks() for blocks needing re-collection
     │   (FetchBlock / FetchReceiptsAndLogs / CollectEthCalls stages)
     ├── replay_logs_for_decode() → log_decoder channel
     ├── replay_calls_for_decode() → eth_call_decoder channel
     └── queue_transform_retry() for blocks needing transformation
  3. backfill_storage_gaps()
     ├── Find gaps in block number sequence
     └── Backfill missing blocks via HTTP
  4. Catchup complete
  5. Start WebSocket subscription (normal live mode)
```

**Progress Tracker Updates**

- `mark_complete()` now persists handler key to status file alongside database
- `load_from_storage()` seeds in-memory state from status files on restart

### Improved Logging

Live mode now has clearer logging to understand pipeline progress:

| Log Message | Meaning |
|-------------|---------|
| `Reconstructed status file for block X` | Missing status rebuilt from data |
| `Reconstructed N missing status files` | Status recovery complete |
| `Found N gaps (M missing blocks) in storage, backfilling` | Gap detection found missing blocks |
| `Backfilling N blocks (X to Y)` | Gap backfill in progress |
| `Block X collected: Y txs` | Block header and transactions fetched |
| `Block X receipts collected: Y logs` | Receipts fetched, logs extracted |
| `Block X eth_calls collected: Y` | Eth calls collected (only if any) |
| `Block X logs decoded` | Log decoder finished |
| `Block X eth_calls decoded` | Eth call decoder finished |
| `Block X fully transformed (N handlers)` | All handlers complete |
| `Handler X buffering block Y: waiting for eth_call deps` | Handler waiting for call dependencies |
| `Handler X unblocked for block Y` | Buffered events now processing |
| `Log decoder live phase started` | Decoder task running |
| `No incomplete blocks found, skipping catchup phase` | Clean startup |

### Module Structure

| File | Purpose |
|------|---------|
| `src/live/mod.rs` | Module exports |
| `src/live/types.rs` | All live mode types: `LiveBlock`, `LiveBlockStatus`, `LiveLog`, `LiveReceipt`, `LiveEthCall`, `LiveFactoryAddresses`, `LiveDecodedLog/Call/EventCall/OnceCall`, `LiveUpsertSnapshot`, `LiveDbValue`, `LiveModeConfig`, `LivePipelineExpectations`, `LiveMessage` |
| `src/live/collector.rs` | `LiveCollector` - WebSocket block processing, catchup, backfill, factory extraction |
| `src/live/compaction.rs` | `CompactionService` - bincode-to-parquet compaction, stuck block detection, S3 upload |
| `src/live/progress.rs` | `LiveProgressTracker` - per-block, per-handler completion tracking |
| `src/live/storage.rs` | `LiveStorage` - bincode/JSON file operations with atomic writes |
| `src/live/reorg.rs` | `ReorgDetector` - parent hash verification and common ancestor search |
| `src/live/catchup.rs` | `LiveCatchupService` - status reconstruction and incomplete block scanning |
| `src/live/eth_calls.rs` | `LiveEthCallCollector` - frequency, factory, once, and event-triggered eth_call collection |
| `src/live/error.rs` | `LiveError` - unified error type |
