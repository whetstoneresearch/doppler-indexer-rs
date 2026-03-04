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
│   LiveStorage     │  blocks/, receipts/, logs/, eth_calls/    │
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
            data/derived/{chain}/decoded/
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
data/live/{chain}/
├── blocks/{block_number}.bin       # Block headers
├── receipts/{block_number}.bin     # Transaction receipts
├── logs/{block_number}.bin         # Raw logs
├── eth_calls/{block_number}.bin    # ETH call results
├── factories/{block_number}.bin    # Factory addresses discovered in block
├── snapshots/{block_number}.bin    # Upsert snapshots for reorg rollback
├── status/{block_number}.json      # Processing status (JSON for debugging)
└── decoded/
    ├── logs/{block_number}/{contract}/{event}.bin
    └── eth_calls/{block_number}/{contract}/{function}.bin
```

### Block Status Tracking

Each block's processing status is tracked in a JSON file:

```json
{
  "collected": true,
  "block_fetched": true,
  "receipts_collected": true,
  "logs_collected": true,
  "decoded": true,
  "transformed": true
}
```

A block is ready for compaction when all flags are `true`.

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
    live_mode: bool,  // true for live mode
}
```

When `live_mode: true`:
- Decoded data is written to bincode files in `data/live/{chain}/decoded/`
- Block status is updated to mark decoding complete
- Data is later compacted to parquet by the CompactionService

When `live_mode: false`:
- Decoded data is written directly to parquet in `data/derived/{chain}/decoded/`

## Live Decoded Data Types

Decoded data in live mode uses serializable types optimized for bincode:

### LiveDecodedValue

```rust
pub enum LiveDecodedValue {
    Address([u8; 20]),
    Uint256(String),    // Stored as string for bincode compatibility
    Int256(String),
    Uint64(u64),
    Int64(i64),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    NamedTuple(Vec<(String, LiveDecodedValue)>),
    UnnamedTuple(Vec<LiveDecodedValue>),
    Array(Vec<LiveDecodedValue>),
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
    pub decoded_values: Vec<LiveDecodedValue>,
}
```

### LiveDecodedCall

```rust
pub struct LiveDecodedCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub decoded_value: LiveDecodedValue,
}
```

### LiveDecodedEventCall

```rust
pub struct LiveDecodedEventCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub decoded_value: LiveDecodedValue,
}
```

### LiveDecodedOnceCall

```rust
pub struct LiveDecodedOnceCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub decoded_values: Vec<(String, LiveDecodedValue)>,  // function_name -> value
}
```

## Reorg Handling

When the LiveCollector detects a chain reorganization (parent hash mismatch):

1. **Detection**: ReorgDetector compares incoming block's parent_hash with stored hash
2. **Orphan Identification**: Walk back to find the common ancestor
3. **Cleanup**:
   - Delete raw data for orphaned blocks from `data/live/{chain}/`
   - Delete decoded data for orphaned blocks
   - Send `DecoderMessage::Reorg` to decoder for additional cleanup
   - Send `LiveMessage::Reorg` downstream

```rust
DecoderMessage::Reorg {
    common_ancestor: u64,  // Last valid block
    orphaned: Vec<u64>,    // Blocks to delete
}
```

## Compaction

The CompactionService periodically checks for complete block ranges and compacts them:

### Compaction Criteria

A range is ready for compaction when:
1. All blocks in the range are present **and sequential** (e.g., blocks 1000-1999 with no gaps)
2. All blocks have `status.is_complete() == true`
3. All transformation handlers have completed for all blocks

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
3. Write parquet to `data/raw/{chain}/` and `data/derived/{chain}/decoded/`
4. Migrate progress from `_live_progress` to `_handler_progress`
5. Delete bincode files and status files

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
    completed: HashMap<u64, HashSet<String>>,  // block -> completed handler keys
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
4. `is_block_complete(block)` returns `true` when all handlers have marked complete
5. Compaction service checks completion before compacting ranges

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

    // Decoded eth_call operations
    fn write_decoded_calls(&self, block_number: u64, contract: &str, function: &str, calls: &[LiveDecodedCall]) -> Result<()>;
    fn write_decoded_once_calls(&self, block_number: u64, contract: &str, calls: &[LiveDecodedOnceCall]) -> Result<()>;
    fn write_decoded_event_calls(&self, block_number: u64, contract: &str, function: &str, calls: &[LiveDecodedEventCall]) -> Result<()>;

    // Status operations
    fn write_status(&self, block_number: u64, status: &LiveBlockStatus) -> Result<()>;
    fn read_status(&self, block_number: u64) -> Result<LiveBlockStatus>;

    // Reorg detection helpers
    fn get_recent_blocks_for_reorg(&self, count: u64) -> Result<Vec<(u64, [u8; 32])>>;
    fn max_block_number(&self) -> Result<Option<u64>>;

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
// Write to temp file, flush, sync, then atomic rename
let temp_path = path.with_extension("tmp");
let file = fs::File::create(&temp_path)?;
let mut writer = BufWriter::new(file);
bincode::serialize_into(&mut writer, data)?;
writer.flush()?;
writer.into_inner()?.sync_all()?;
fs::rename(&temp_path, path)?;
```

On startup, `ensure_dirs()` cleans up any leftover `.tmp` files from interrupted writes.

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

On transition to live mode, gaps between historical and live blocks are detected and backfilled:

```rust
// On first block received
if block_number > expected_start_block + 1 {
    tracing::warn!("Gap detected, backfilling...");
    backfill_blocks(expected_start + 1, block_number - 1).await?;
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

## Outstanding Issues

> **Note**: Most critical issues have been resolved. The following items remain.

### Resolved Issues

The following issues from earlier versions have been fixed:

- **Issue 4 (LiveBlockStatus Never Set to Complete):** Now fixed - `status.decoded = true` is set in multiple places in the log decoder and eth_call decoder. The `status.transformed = true` is set in `progress.rs` when all handlers complete.
- **Issue 8 (Logs Parquet Missing transaction_hash):** Fixed - `compaction.rs` now includes `transaction_hash` field in the logs parquet schema.
- **Issue 9 (Progress Tracker Handlers Never Registered):** Fixed - Handlers are registered in `main.rs` during initialization.

### Remaining Issues

#### 3. Transformation Engine in Live-Only Mode

When using `--live-only` mode, the transformation engine is NOT running (the engine only continues into live mode after completing historical processing). This means:

- In normal mode (historical → live transition): transformation engine DOES continue running in live mode
- In `--live-only` mode: transformation engine is not started

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
    pub key_columns: Vec<String>,
    pub key_values: Vec<LiveDbValue>,
    pub previous_row: Option<HashMap<String, LiveDbValue>>,
}

pub enum LiveDbValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Text(String),
    Bytes(Vec<u8>),
    Numeric(String),
    Json(String),
}
```

### LiveFactoryAddresses

Tracks factory addresses discovered in each block:

```rust
pub struct LiveFactoryAddresses {
    pub addresses: HashMap<String, Vec<[u8; 20]>>,  // collection_name -> addresses
}
```

## Unified Error Type

Live mode uses a unified `LiveError` type that consolidates errors from various modules:

```rust
pub enum LiveError {
    Storage(StorageError),
    Rpc(RpcError),
    Channel(String),
    Decode(String),
    Io(std::io::Error),
    Database(DbError),
    Progress(String),
}
```

`ProgressError` and `LiveEthCallError` are now type aliases to `LiveError`.
