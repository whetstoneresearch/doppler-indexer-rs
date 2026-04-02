# Receipt Collection

The receipt collection module fetches transaction receipts via RPC and writes them to Parquet files. It receives block information from the block collector and extracts logs to forward to the log collector.

Receipt collection is split into two phases:

1. **Catchup Phase** (`catchup/receipts.rs`): On startup, processes any block ranges where blocks exist but receipts or logs are missing
2. **Current Phase** (`current/receipts.rs`): Processes new blocks from a channel with early batch-based RPC fetching

## Usage

### Catchup Phase

```rust
use doppler_indexer_rs::raw_data::historical::catchup::receipts::collect_receipts;
use doppler_indexer_rs::raw_data::historical::receipts::LogMessage;
use doppler_indexer_rs::rpc::UnifiedRpcClient;
use tokio::sync::mpsc;

let client = UnifiedRpcClient::from_url(&rpc_url)?;

// Channels to downstream collectors
let (log_tx, log_rx) = mpsc::channel::<LogMessage>(1000);
let (factory_log_tx, factory_log_rx) = mpsc::channel::<LogMessage>(1000);

// Run catchup phase (no block_rx needed - reads from existing block parquet files)
// Returns ReceiptsCatchupState to pass to the current phase
let catchup_state = collect_receipts(
    &chain_config,
    &client,
    &raw_data_config,
    &Some(log_tx),
    &Some(factory_log_tx),
    &None,  // event_trigger_tx
    &[],    // event_matchers
    None,   // s3_manifest
    None,   // storage_manager
).await?;
```

### Current Phase

```rust
use doppler_indexer_rs::raw_data::historical::current::receipts::collect_receipts;
use doppler_indexer_rs::raw_data::historical::receipts::LogMessage;
use doppler_indexer_rs::raw_data::historical::factories::RecollectRequest;
use doppler_indexer_rs::rpc::UnifiedRpcClient;
use tokio::sync::mpsc;

let client = UnifiedRpcClient::from_url(&rpc_url)?;

// Channel from block collector
let (block_tx, block_rx) = mpsc::channel(1000);
// Channels to downstream collectors
let (log_tx, log_rx) = mpsc::channel::<LogMessage>(1000);
let (factory_log_tx, factory_log_rx) = mpsc::channel::<LogMessage>(1000);
// Channel for recollection requests from factory collector (optional)
let (recollect_tx, recollect_rx) = mpsc::channel::<RecollectRequest>(100);

// Run current phase (processes new blocks from channel)
// catchup_state is returned from the catchup phase collect_receipts call
collect_receipts(
    &chain_config,
    &client,
    &raw_data_config,
    block_rx,
    Some(log_tx),
    Some(factory_log_tx),
    None,  // event_trigger_tx
    vec![], // event_matchers
    Some(recollect_rx),
    catchup_state,
    None,  // storage_manager
).await?;
```

## Function Signatures

### Catchup Phase

```rust
// src/raw_data/historical/catchup/receipts.rs
pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    log_tx: &Option<Sender<LogMessage>>,
    factory_log_tx: &Option<Sender<LogMessage>>,
    event_trigger_tx: &Option<Sender<EventTriggerMessage>>,
    event_matchers: &[EventTriggerMatcher],
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<ReceiptsCatchupState, ReceiptCollectionError>
```

### Current Phase

```rust
// src/raw_data/historical/current/receipts.rs
pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    block_rx: Receiver<(u64, u64, Vec<B256>)>,
    log_tx: Option<Sender<LogMessage>>,
    factory_log_tx: Option<Sender<LogMessage>>,
    event_trigger_tx: Option<Sender<EventTriggerMessage>>,
    event_matchers: Vec<EventTriggerMatcher>,
    recollect_rx: Option<Receiver<RecollectRequest>>,
    catchup_state: ReceiptsCatchupState,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<(), ReceiptCollectionError>
```

### Parameters

#### Catchup Phase Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with name and optional `block_receipts_method` |
| `client` | `&UnifiedRpcClient` | Shared RPC client for fetching receipts |
| `raw_data_config` | `&RawDataCollectionConfig` | Parquet range size, batch sizes, and field configuration |
| `log_tx` | `&Option<Sender<LogMessage>>` | Optional channel for forwarding logs to log collector (reference) |
| `factory_log_tx` | `&Option<Sender<LogMessage>>` | Optional channel for forwarding logs to factory collector (reference) |
| `event_trigger_tx` | `&Option<Sender<EventTriggerMessage>>` | Optional channel for event-triggered eth_calls (reference) |
| `event_matchers` | `&[EventTriggerMatcher]` | Matchers for detecting events that trigger eth_calls (slice reference) |
| `s3_manifest` | `Option<S3Manifest>` | Optional S3 manifest for checking remote file availability |
| `storage_manager` | `Option<Arc<StorageManager>>` | Optional storage manager for S3 uploads and downloads |

#### Return Value

The catchup phase returns `ReceiptsCatchupState`, which is passed to the current phase:

```rust
pub struct ReceiptsCatchupState {
    pub existing_files: HashSet<String>,
    pub existing_logs_files: HashSet<String>,
    pub s3_manifest: Option<S3Manifest>,
}
```

This transfers pre-scanned file lists and the S3 manifest to the current phase, avoiding redundant filesystem/S3 checks.

#### Current Phase Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with name and optional `block_receipts_method` |
| `client` | `&UnifiedRpcClient` | Shared RPC client for fetching receipts |
| `raw_data_config` | `&RawDataCollectionConfig` | Parquet range size, batch sizes, and field configuration |
| `block_rx` | `Receiver<(u64, u64, Vec<B256>)>` | Channel receiving block info from block collector |
| `log_tx` | `Option<Sender<LogMessage>>` | Optional channel for forwarding logs to log collector |
| `factory_log_tx` | `Option<Sender<LogMessage>>` | Optional channel for forwarding logs to factory collector |
| `event_trigger_tx` | `Option<Sender<EventTriggerMessage>>` | Optional channel for event-triggered eth_calls |
| `event_matchers` | `Vec<EventTriggerMatcher>` | Matchers for detecting events that trigger eth_calls |
| `recollect_rx` | `Option<Receiver<RecollectRequest>>` | Optional channel for receiving recollection requests from factory collector |
| `catchup_state` | `ReceiptsCatchupState` | State from catchup phase with pre-scanned file lists and S3 manifest |
| `storage_manager` | `Option<Arc<StorageManager>>` | Optional storage manager for S3 uploads |

### Input Channel Message Format (Current Phase)

Receives from block collector:
- `u64` - Block number
- `u64` - Block timestamp
- `Vec<B256>` - Transaction hashes in the block

### Output Channel Message Format

Both `log_tx` and `factory_log_tx` receive `LogMessage` enum variants:

```rust
pub enum LogMessage {
    Logs(Vec<LogData>),
    RangeComplete { range_start: u64, range_end: u64 },
    AllRangesComplete,
}
```

- `Logs(Vec<LogData>)` - Batch of extracted logs
- `RangeComplete` - Signals a block range has been fully processed
- `AllRangesComplete` - Signals all ranges are done (sent when channel closes)

Each `LogData` contains:
- `block_number: u64`
- `block_timestamp: u64`
- `transaction_hash: B256`
- `log_index: u32`
- `address: [u8; 20]`
- `topics: Vec<[u8; 32]>`
- `data: Vec<u8>`

### Event Trigger Types

When event-triggered eth_calls are configured, the receipt collector extracts matching events and sends them via the `event_trigger_tx` channel.

#### EventTriggerMessage

```rust
pub enum EventTriggerMessage {
    /// Batch of event triggers from a range
    Triggers(Vec<EventTriggerData>),
    /// A block range is complete
    RangeComplete { range_start: u64, range_end: u64 },
    /// All ranges are complete
    AllComplete,
}
```

#### EventTriggerData

```rust
pub struct EventTriggerData {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    /// Address that emitted the event
    pub emitter_address: [u8; 20],
    /// Matched source name (contract or factory collection)
    pub source_name: String,
    /// Event signature hash (topic[0])
    pub event_signature: [u8; 32],
    /// All topics including topic0
    pub topics: Vec<[u8; 32]>,
    /// ABI-encoded event data
    pub data: Vec<u8>,
}
```

#### EventTriggerMatcher

```rust
pub struct EventTriggerMatcher {
    /// Source name (contract name or factory collection name)
    pub source_name: String,
    /// Addresses to match (empty for factory collections - matched dynamically)
    pub addresses: HashSet<[u8; 20]>,
    /// Whether this is a factory collection (addresses discovered dynamically)
    pub is_factory: bool,
    /// Event signature hash (keccak256 of signature)
    pub event_topic0: [u8; 32],
}
```

Matchers are built from contract configurations via `build_event_trigger_matchers()`. For factory collections, `addresses` is empty and matching is done dynamically against discovered factory addresses.

## Output

Receipts are written to Parquet files named by block range:

- **Historical mode**: `data/{chain}/historical/raw/receipts/`
- **Live mode**: `data/{chain}/live/raw/receipts/`

```
data/ethereum/historical/raw/receipts/
├── receipts_0-999.parquet
├── receipts_1000-1999.parquet
├── receipts_2000-2999.parquet
└── ...
```

## Processing Flow

### Catchup Phase

1. Scans existing block parquet files in `data/{chain}/historical/raw/blocks/` (and S3 manifest if provided)
2. For each block range, checks if receipts and logs parquet files exist (locally or in S3)
3. If missing, reads block info from existing block parquet file (downloading from S3 if needed)
4. Fetches receipts via RPC and processes them
5. Uploads receipt parquet files to S3 if `storage_manager` is configured
6. Sends `RangeComplete` signals for each range (even if already complete) to synchronize downstream collectors

### Current Phase (Mode-Specific Scheduling)

The current phase uses mode-specific scheduling to maximize rate-limit saturation:

1. Receives block info (block number, timestamp, tx hashes) from block collector
2. Skips ranges that already exist (checked against catchup state's file lists and S3 manifest)
3. Tracks blocks in batch states per range (`ReceiptBatchState`)
4. **Block-receipt mode** (`block_receipts_method` set): Spawns one JoinSet task per block, bounded at `block_receipt_concurrency`. Each task calls `get_block_receipts` for a single block. The client's internal rate limiter and semaphore handle RPC pacing.
5. **Fallback mode** (per-tx receipts): Accumulates blocks in a pending buffer. Flushes when the accumulated tx count reaches `rpc_batch_size`, when a range boundary is crossed, or after a 150ms timeout. Each flush dispatches all accumulated tx hashes as a single `get_transaction_receipts_batch` call, maximizing batch utilization across sparse blocks.
6. Sends logs immediately to downstream collectors as fetches complete
7. When all blocks in a range are received and completed:
   - Sorts records by block number
   - Writes receipt data to Parquet file
   - Uploads to S3 if `storage_manager` is configured
8. Handles recollection requests via `tokio::select!` alongside normal block processing
9. On shutdown, flushes any pending fallback blocks and processes incomplete ranges

## Fetching Strategies

The receipt collector supports two fetching strategies, configured per-chain via `block_receipts_method`:

### Per-Transaction Fetching (Default / Fallback)

When `block_receipts_method` is not set, receipts are fetched individually using `eth_getTransactionReceipt`:

- In catchup mode: all tx hashes across a range's blocks are collected and sent as a single `get_transaction_receipts_batch` call
- In current mode: blocks accumulate in a pending buffer; tx hashes are flushed as a batch when count reaches `rpc_batch_size`, at range boundaries, or after a 150ms timeout
- Cross-block micro-batching maximizes batch utilization even on chains with sparse blocks
- Rate limiting is applied per-request by the RPC client

This is the most compatible approach and works with all RPC providers.

### Block-Level Fetching

When `block_receipts_method` is set (e.g., `"eth_getBlockReceipts"`), all receipts for a block are fetched in a single RPC call:

```json
{
    "name": "optimism",
    "chain_id": 10,
    "rpc_url_env_var": "OPTIMISM_RPC_URL",
    "block_receipts_method": "eth_getBlockReceipts"
}
```

**Advantages:**
- Fewer RPC calls (1 per block vs N per block)
- More efficient for blocks with many transactions
- Reduces rate limiting overhead

**Behavior:**
- Blocks are fetched concurrently (configurable via `block_receipt_concurrency`, default 10)
- Rate limiting is applied per block request
- For Alchemy, each call consumes 500 compute units

**Unsupported Transaction Types:**

Some chains (e.g., Optimism, Base) have transaction types that may not deserialize correctly with standard Ethereum receipt types (e.g., L2 deposit transactions). The collector handles this gracefully:

1. The response is first deserialized as raw JSON (`Vec<serde_json::Value>`)
2. Each receipt is then parsed individually
3. Receipts that fail to parse are logged at debug level and skipped
4. Processing continues with the successfully parsed receipts

This matches the behavior of per-transaction fetching, where individual failures don't abort the entire batch.

## Resumability

Collection is fully resumable with comprehensive catchup logic. The catchup and current phases are now separate functions that run sequentially:

### Catchup Phase (Separate Function)

The catchup phase runs first as a separate function (`catchup::receipts::collect_receipts`):

1. **Scans existing block files** - Reads `data/{chain}/historical/raw/blocks/` to find all available block ranges (also checks S3 manifest if provided)
2. **Checks downstream files** - For each block range, checks if:
   - Receipt parquet file exists (locally or in S3)
   - Logs parquet file exists (locally or in S3, if log collection is enabled)
3. **Re-processes missing ranges** - If any downstream files are missing, reads block info from the existing block parquet file (downloading from S3 if needed) and re-processes that range
4. **Signals completion** - Sends `RangeComplete` for all ranges (including already-complete ones) to synchronize downstream collectors
5. **Returns state** - Returns `ReceiptsCatchupState` containing pre-scanned file lists and S3 manifest to pass to the current phase

Note: Factory catchup is handled separately by the factories module reading from logs files directly.

### Current Phase (Separate Function)

The current phase runs after catchup completes (`current::receipts::collect_receipts`):

1. **Receives catchup state** - Uses `ReceiptsCatchupState` from the catchup phase, which includes pre-scanned file lists and S3 manifest to skip already-processed ranges
2. **Processes new blocks** - Receives block info from the block collector channel
3. **Uses early batch fetching** - Starts RPC work before full range is complete for better throughput
4. **Handles recollection requests** - Processes requests from factory collector in parallel via `tokio::select!`
5. **Uploads to S3** - When `storage_manager` is provided, uploads completed parquet files to S3
6. **Shutdown cleanup** - Processes any incomplete ranges when the block channel closes

This separation ensures that catchup completes fully before processing new blocks, preventing race conditions between historical and live data.

### Empty Range Handling

If a block range contains no transactions, the collector writes an empty parquet file to mark the range as processed. This prevents unnecessary re-processing on subsequent runs.

### Manual Re-collection

To re-collect a range, delete the corresponding file from `data/{chain}/historical/raw/receipts/`. Note that you may also need to delete the corresponding logs and factory files if you want those to be regenerated.

## Automatic Recollection (Corrupted File Recovery)

The current phase receipt collector supports automatic recollection of ranges when downstream collectors detect corrupted files. This is primarily used by the factory collector during its catchup phase.

### How It Works

1. **Detection**: During catchup, the factory collector reads existing log parquet files. If a file is corrupted (fails to parse), the factory collector:
   - Deletes the corrupted file
   - Sends a `RecollectRequest` through the `recollect_tx` channel

2. **Re-processing**: The current phase receipt collector listens on `recollect_rx` using `tokio::select!`:
   - Reads block info from the corresponding block parquet file
   - Re-fetches receipts via RPC for that range
   - Sends logs through the normal `factory_log_tx` channel

3. **Normal Processing**: The factory collector receives the re-fetched logs through its normal `log_rx` channel and processes them as usual

Note: Only the current phase function accepts `recollect_rx`. The catchup phase does not handle recollection requests since it runs before the current phase and any corrupted files would be naturally re-processed.

### RecollectRequest Structure

```rust
pub struct RecollectRequest {
    pub range_start: u64,
    pub range_end: u64,
    pub _file_path: PathBuf,
}
```

### Data Flow

```
Factory Catchup                    Receipt Collector
     │                                    │
     │ (reads corrupted log file)         │
     │                                    │
     ▼                                    │
  Delete file                             │
     │                                    │
     │──── RecollectRequest ─────────────►│
     │                                    │
     │                                    ▼
     │                          Read block info
     │                          Fetch receipts (RPC)
     │                                    │
     │◄─────── LogMessage ────────────────│
     │      (via factory_log_tx)          │
     ▼                                    │
  Process normally                        │
```

### Benefits

- **Automatic recovery**: No manual intervention required for corrupted files
- **Data integrity**: Ensures all ranges are properly processed
- **Efficient**: Only re-fetches the specific corrupted range, not the entire dataset
- **Non-blocking**: Uses `tokio::select!` to handle recollection requests alongside normal block processing

## Field Configuration

### Minimal Schema (when `receipt_fields` is specified)

Configure specific fields in your config:

```json
{
  "raw_data_collection": {
    "parquet_block_range": 1000,
    "fields": {
      "receipt_fields": ["block_number", "timestamp", "transaction_hash", "from", "to"]
    }
  }
}
```

Available fields:
- `block_number` → `block_number: UInt64`
- `timestamp` → `block_timestamp: UInt64`
- `transaction_hash` → `transaction_hash: FixedSizeBinary(32)`
- `from` → `from_address: FixedSizeBinary(20)`
- `to` → `to_address: FixedSizeBinary(20)` (nullable for contract creation)
- `logs` → `log_count: UInt32`

### Full Schema (when `receipt_fields` is null/omitted)

When no fields are specified, all receipt fields are stored:

| Column | Type | Nullable |
|--------|------|----------|
| `block_number` | UInt64 | No |
| `block_timestamp` | UInt64 | No |
| `transaction_hash` | FixedSizeBinary(32) | No |
| `transaction_index` | UInt32 | No |
| `from_address` | FixedSizeBinary(20) | No |
| `to_address` | FixedSizeBinary(20) | Yes |
| `cumulative_gas_used` | UInt64 | No |
| `gas_used` | UInt64 | No |
| `contract_address` | FixedSizeBinary(20) | Yes |
| `status` | Boolean | No |
| `log_count` | UInt32 | No |

## Module Structure

The receipt collection code is organized into multiple modules:

```
src/raw_data/historical/
├── receipts.rs          # Shared types, helpers, and processing logic
├── catchup/
│   └── receipts.rs      # Catchup phase: process existing block ranges
└── current/
    └── receipts.rs      # Current phase: process new blocks from channel
```

- **`receipts.rs`**: Contains shared types (`LogMessage`, `EventTriggerMessage`, `ReceiptCollectionError`, etc.), helper functions (`fetch_block_receipts_bounded`, `fetch_tx_receipts_batched`, `process_range`, `send_logs_to_channels`), and schema/parquet utilities
- **`catchup/receipts.rs`**: Standalone catchup function that scans existing block files and re-processes missing ranges
- **`current/receipts.rs`**: Main collection function with early batch fetching and recollection support

## Data Flow

```
                          ┌─────────────────────────────────────────────────────────┐
                          │                    Startup                              │
                          │                                                         │
                          │  ┌─────────────────┐    RangeComplete     ┌───────────┐ │
                          │  │ catchup/        │──────────────────────▶│ downstream│ │
                          │  │ receipts.rs     │     (all ranges)     │ collectors│ │
                          │  │ (reads blocks)  │                      └───────────┘ │
                          │  └─────────────────┘                                    │
                          └─────────────────────────────────────────────────────────┘
                                               ▼
┌─────────────┐     (block_number, timestamp, tx_hashes)     ┌──────────────────────┐
│   blocks.rs │ ──────────────────────────────────────────▶  │ current/receipts.rs  │
└─────────────┘                                              │                      │
                                                             │ 1. Track in batch    │
                                                             │ 2. Early fetch RPC   │
                                                             │ 3. Send logs immed.  │
                                                             │ 4. Write parquet     │
                                                             └──────────┬───────────┘
                                                                        │
                                                                 LogMessage
                                                                ┌───────┴───────┐
                                                                │               │
                                                                ▼               ▼
                                                       ┌──────────────┐  ┌──────────────┐
                                                       │   logs.rs    │  │ factories.rs │
                                                       └──────────────┘  └──────────────┘
```

## Configuration Options

The following `raw_data_collection` options affect receipt collection:

| Option | Default | Description |
|--------|---------|-------------|
| `parquet_block_range` | 1000 | Number of blocks per Parquet file |
| `rpc_batch_size` | 100 | Transactions per RPC batch. Also controls early batch fetch threshold in current phase. |
| `block_receipt_concurrency` | 10 | Concurrent block receipt requests (block-level fetching only) |

### Block-Receipt Mode (Current Phase)

When `block_receipts_method` is set, each arriving block is dispatched as a separate `get_block_receipts` RPC call into a bounded JoinSet. The concurrency cap is `block_receipt_concurrency` (default 10). The client's internal rate limiter and semaphore handle per-request pacing. This maximizes in-flight blocks up to the hard cap.

### Fallback Micro-Batching (Current Phase)

When `block_receipts_method` is not set, the collector accumulates arriving blocks in a pending buffer and flushes them as batched `get_transaction_receipts_batch` calls. Flush triggers:

- **Batch full**: accumulated tx count reaches `rpc_batch_size` (default 100)
- **Range boundary**: a block from a new range arrives while blocks from the old range are pending
- **Timeout**: 150ms after the first block entered the buffer (prevents stalling on sparse chains)

This cross-block micro-batching keeps tx batches full regardless of per-block tx density.

## Backpressure Monitoring

The receipt collector monitors channel backpressure to help diagnose performance bottlenecks:

- **High pressure warning**: Logged when channel is >90% full
- **Summary metrics**: Logged at completion with:
  - Total sends and average send time
  - High pressure sends (>50% full)
  - Critical pressure sends (>90% full)
  - Maximum single send time

If you see frequent high-pressure warnings, consider:
- Increasing channel capacity
- Optimizing downstream consumers (logs/factories)
- Reducing `parquet_block_range` to process smaller batches

## Error Handling

| Error | Cause |
|-------|-------|
| `Rpc` | RPC request failed |
| `Io` | File system error |
| `Parquet` | Parquet write error |
| `Arrow` | Arrow array construction error |
| `ReceiptNotFound` | RPC returned null for a transaction hash |
| `ChannelSend` | Log or factory receiver dropped the channel |
| `JoinError` | Blocking task (parquet write) failed to join |

## Example Configuration

### Standard (Per-Transaction Fetching)

```json
{
  "chains": [
    {
      "name": "ethereum",
      "chain_id": 1,
      "rpc_url_env_var": "ETHEREUM_RPC_URL",
      "start_block": "17000000"
    }
  ],
  "raw_data_collection": {
    "parquet_block_range": 10000,
    "fields": {
      "block_fields": ["number", "timestamp", "transactions"],
      "receipt_fields": ["block_number", "timestamp", "transaction_hash", "from", "to"],
      "log_fields": ["block_number", "timestamp", "transaction_hash", "log_index", "address", "topics", "data"]
    }
  }
}
```

### With Block-Level Fetching (L2 Chains)

```json
{
  "chains": [
    {
      "name": "optimism",
      "chain_id": 10,
      "rpc_url_env_var": "OPTIMISM_RPC_URL",
      "start_block": "100000000",
      "block_receipts_method": "eth_getBlockReceipts"
    }
  ],
  "raw_data_collection": {
    "parquet_block_range": 1000,
    "fields": {
      "receipt_fields": ["block_number", "timestamp", "transaction_hash", "from", "to"]
    }
  }
}
```

This configuration:
- Uses `eth_getBlockReceipts` for efficient block-level receipt fetching
- Handles L2 deposit transactions gracefully (skips unsupported types)
- Writes 1,000 blocks worth of receipts per Parquet file
- Stores only the specified receipt fields (minimal schema)
