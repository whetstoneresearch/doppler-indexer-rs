# Live Mode

Live mode enables real-time block processing via WebSocket subscription, complementing the historical HTTP-based collection. Instead of processing blocks in large ranges, live mode processes blocks individually as they arrive at the chain tip, providing lower latency for real-time applications.

## Overview

The indexer operates in two modes:

1. **Historical Mode** (HTTP): Processes blocks in configurable ranges (default: 1000 blocks), writes to parquet files
2. **Live Mode** (WebSocket): Processes blocks individually as they arrive, writes to bincode files, later compacted to parquet

After historical catchup completes, the indexer transitions to live mode for real-time processing.

## Architecture

```
WebSocket (eth_subscribe)
         │
         ▼
┌─────────────────────┐
│   LiveCollector     │ ───► ReorgDetector
│   (block headers)   │            │
└─────────────────────┘            │ reorg detected?
         │                         ▼
         │                  Delete orphaned data
         │                  Send Reorg message
         ▼
┌─────────────────────┐
│  HTTP RPC Backfill  │ (receipts, full block data)
│                     │
└─────────────────────┘
         │
         ├────────────────────────────────────┐
         ▼                                    ▼
┌─────────────────────┐              ┌─────────────────────┐
│    LiveStorage      │              │     Decoder         │
│  (bincode files)    │              │  (live_mode=true)   │
└─────────────────────┘              └─────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────────┐              ┌─────────────────────┐
│  CompactionService  │              │  LiveStorage        │
│ (bincode → parquet) │              │  (decoded bincode)  │
└─────────────────────┘              └─────────────────────┘
```

## Storage Structure

Live mode uses a dedicated storage directory with bincode format for fast serialization:

```
data/live/{chain}/
├── blocks/{block_number}.bin       # Block headers
├── receipts/{block_number}.bin     # Transaction receipts
├── logs/{block_number}.bin         # Raw logs
├── eth_calls/{block_number}.bin    # ETH call results
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
1. All blocks in the range are present (e.g., blocks 1000-1999 all exist)
2. All blocks have `status.is_complete() == true`
3. All transformation handlers have completed for all blocks

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

    // Decoded log operations
    fn write_decoded_logs(&self, block_number: u64, contract: &str, event: &str, logs: &[LiveDecodedLog]) -> Result<()>;
    fn read_decoded_logs(&self, block_number: u64, contract: &str, event: &str) -> Result<Vec<LiveDecodedLog>>;
    fn delete_all_decoded_logs(&self, block_number: u64) -> Result<()>;

    // Status operations
    fn write_status(&self, block_number: u64, status: &LiveBlockStatus) -> Result<()>;
    fn read_status(&self, block_number: u64) -> Result<LiveBlockStatus>;

    // Bulk operations
    fn delete_all(&self, block_number: u64) -> Result<()>;  // Deletes all data for a block
}
```

## Performance Considerations

- **Bincode format**: Much faster serialization/deserialization than parquet for single-block writes
- **Per-block files**: Enables efficient deletion during reorgs without rewriting entire parquet files
- **Status files as JSON**: Easy to debug and inspect processing state
- **Compaction batching**: Processes complete ranges to minimize parquet file count
- **Reorg depth**: 128 blocks covers most reorgs while limiting memory usage

## Current Limitations

- Decoded data compaction relies on re-decoding from compacted raw parquet (avoids schema synchronization complexity)
- ETH call live mode writes to parquet (placeholder for full bincode support)
