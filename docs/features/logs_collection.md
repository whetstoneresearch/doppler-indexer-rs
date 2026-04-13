# Log Collection

The log collection module receives logs extracted from transaction receipts and writes them to Parquet files. Logs are obtained from the receipt collector rather than via separate RPC calls, ensuring consistency and efficiency.

## Architecture

Log collection is split into two phases to support resumable operation:

1. **Catchup Phase** (`catchup/logs.rs`): Scans existing parquet files, builds the schema and configured addresses, and returns a `LogsCatchupState` containing all initialization data.

2. **Current Phase** (`current/logs.rs`): Receives logs from the receipt collector via channels and writes them to parquet files, using the state from the catchup phase.

This separation allows the indexer to:
- Quickly determine which ranges have already been processed
- Pre-compute schemas and address sets once
- Seamlessly transition from catchup to live processing

### Module Structure

```
src/raw_data/historical/
├── logs.rs              # Shared types, helpers, and parquet writing functions
├── catchup/
│   └── logs.rs          # Catchup phase: initialization and state building
└── current/
    └── logs.rs          # Current phase: channel processing loop
```

- **`logs.rs`**: Contains `LogsCatchupState`, `LogCollectionError`, `LogWriteTask`, `prepare_completed_range`, `execute_log_write`, schema building, and parquet writing utilities shared by both phases
- **`catchup/logs.rs`**: Initializes state by scanning existing files and building configuration
- **`current/logs.rs`**: Processes `LogMessage` from channels using the pre-built state

## Usage

```rust
use doppler_indexer_rs::raw_data::historical::catchup::logs as catchup_logs;
use doppler_indexer_rs::raw_data::historical::current::logs as current_logs;
use doppler_indexer_rs::raw_data::historical::receipts::{LogData, LogMessage};
use doppler_indexer_rs::raw_data::historical::factories::FactoryAddressData;
use doppler_indexer_rs::decoding::DecoderMessage;
use doppler_indexer_rs::storage::{S3Manifest, StorageManager};
use tokio::sync::mpsc;
use std::sync::Arc;

// Channel from receipt collector (sends LogMessage)
let (log_tx, log_rx) = mpsc::channel::<LogMessage>(1000);

// Optional factory channel (for contract_logs_only filtering)
let factory_rx: Option<Receiver<FactoryAddressData>> = None;

// Optional decoder channel (for ABI decoding)
let decoder_tx: Option<Sender<DecoderMessage>> = None;

// Optional S3 manifest and storage manager for remote storage
let s3_manifest: Option<S3Manifest> = None;
let storage_manager: Option<Arc<StorageManager>> = None;

// Phase 1: Catchup - initialize state
let catchup_state = catchup_logs::collect_logs(
    &chain_config, &raw_data_config, s3_manifest, storage_manager,
).await?;

// Phase 2: Current - process logs from channel
current_logs::collect_logs(&chain_config, log_rx, factory_rx, decoder_tx, catchup_state).await?;
```

## Function Signatures

### Catchup Phase

```rust
pub async fn collect_logs(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<LogsCatchupState, LogCollectionError>
```

Returns a `LogsCatchupState` containing:
- `output_dir`: Path to the logs parquet directory
- `range_size`: Number of blocks per parquet file
- `schema`: Arrow schema for writing parquet files
- `configured_addresses`: Set of contract addresses for filtering
- `existing_files`: Set of already-processed parquet file names
- `contract_logs_only`: Whether to filter to configured contracts only
- `needs_factory_wait`: Whether to wait for factory addresses before writing
- `log_fields`: Optional list of specific fields to include
- `s3_manifest`: Optional S3 manifest for skipping ranges already uploaded
- `storage_manager`: Optional storage manager for uploading parquet files to S3
- `chain_name`: Chain name used for S3 upload paths

### Current Phase

```rust
pub async fn collect_logs(
    chain: &ChainConfig,
    log_rx: Receiver<LogMessage>,
    factory_rx: Option<Receiver<FactoryAddressData>>,
    decoder_tx: Option<Sender<DecoderMessage>>,
    state: LogsCatchupState,
) -> Result<(), LogCollectionError>
```

### Parameters

#### Catchup Phase

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with name and contracts |
| `raw_data_config` | `&RawDataCollectionConfig` | Parquet range size and field configuration |
| `s3_manifest` | `Option<S3Manifest>` | Optional S3 manifest for skipping already-uploaded ranges |
| `storage_manager` | `Option<Arc<StorageManager>>` | Optional storage manager for uploading parquet files to S3 |

#### Current Phase

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with name and contracts |
| `log_rx` | `Receiver<LogMessage>` | Channel receiving log messages from receipt collector |
| `factory_rx` | `Option<Receiver<FactoryAddressData>>` | Optional channel receiving factory addresses for filtering |
| `decoder_tx` | `Option<Sender<DecoderMessage>>` | Optional channel for sending logs to the decoder for ABI decoding |
| `state` | `LogsCatchupState` | Pre-computed state from the catchup phase |

### Input Channel Message Format

Receives `LogMessage` enum from receipt collector with the following variants:

```rust
pub enum LogMessage {
    Logs(Vec<LogData>),
    RangeComplete { range_start: u64, range_end: u64 },
    AllRangesComplete,
}
```

- **`Logs(Vec<LogData>)`** - A batch of log data to accumulate
- **`RangeComplete`** - Signals that all logs for a block range have been sent and the range can be written
- **`AllRangesComplete`** - Signals that all ranges are finished (end of collection)

Each `LogData` contains:
- `block_number: u64`
- `block_timestamp: u64`
- `transaction_hash: B256`
- `log_index: u32`
- `address: [u8; 20]`
- `topics: Vec<[u8; 32]>`
- `data: Vec<u8>`

### Output Channel (Decoder)

When `decoder_tx` is provided, the collector sends `DecoderMessage` for each completed range:

```rust
pub enum DecoderMessage {
    LogsReady {
        range_start: u64, range_end: u64, logs: Vec<LogData>,
        live_mode: bool, has_factory_matchers: bool,
    },
    EthCallsReady {
        range_start: u64, range_end: u64, contract_name: String,
        function_name: String, results: Vec<EthCallResult>,
        live_mode: bool, retry_transform_after_decode: bool,
    },
    OnceCallsReady {
        range_start: u64, range_end: u64, contract_name: String,
        results: Vec<OnceCallResult>,
        live_mode: bool, retry_transform_after_decode: bool,
    },
    EventCallsReady {
        range_start: u64, range_end: u64, contract_name: String,
        function_name: String, results: Vec<EventCallResult>,
        live_mode: bool, retry_transform_after_decode: bool,
    },
    EthCallsBlockComplete {
        range_start: u64, range_end: u64,
        retry_transform_after_decode: bool,
    },
    FactoryAddresses {
        range_start: u64, range_end: u64,
        addresses: HashMap<String, Vec<Address>>,
    },
    OnceFileBackfilled {
        range_start: u64, range_end: u64, contract_name: String,
    },
    Reorg { _common_ancestor: u64, orphaned: Vec<u64> },
    AllComplete,
}
```

- **`LogsReady`** - Sends the filtered logs for a completed range to the decoder
  - `live_mode: false` for historical collection (writes parquet)
  - `live_mode: true` for live WebSocket collection (writes bincode)
  - `has_factory_matchers: bool` indicates if factory matchers exist for this range
- **`EthCallsReady`** - Regular eth_call results ready for decoding (includes `retry_transform_after_decode` for deferred transform execution)
- **`OnceCallsReady`** - One-time eth_call results ready for decoding
- **`EventCallsReady`** - Event-triggered eth_call results ready for decoding
- **`EthCallsBlockComplete`** - Marker indicating all eth_call decode work for a block/range has been queued; when `retry_transform_after_decode` is true, this triggers deferred transform execution
- **`FactoryAddresses`** - Factory-discovered addresses for a range (needed for factory event matching)
- **`OnceFileBackfilled`** - A once-call file was backfilled with new columns for a specific range and contract - decoder should re-check it
- **`Reorg`** - Signals a chain reorganization for cleanup of orphaned blocks
- **`AllComplete`** - Signals that all log collection is finished

## Output

Logs are written to `data/{chain}/historical/raw/logs/` as Parquet files named by block range (or `data/{chain}/live/raw/logs/` for live mode):

```
data/{chain}/historical/raw/logs/
├── logs_0-999.parquet
├── logs_1000-1999.parquet
├── logs_2000-2999.parquet
└── ...
```

## Processing Flow

### Catchup Phase

1. Creates the output directory (`data/{chain}/historical/raw/logs/`) if it doesn't exist
2. Builds the Arrow schema based on configured log fields
3. If `contract_logs_only` is enabled, extracts all configured contract addresses
4. Scans existing parquet files to determine which ranges are already processed
5. Returns `LogsCatchupState` with all pre-computed data

### Current Phase

1. Receives `LogMessage` from the receipt collector via channel
2. For `LogMessage::Logs`: accumulates logs into per-range buckets
3. For `LogMessage::RangeComplete`: signals a range is ready to process
   - Calls `prepare_completed_range` which:
     - Skips the range if the parquet file already exists on disk or in the S3 manifest
     - If `contract_logs_only` is enabled and factories are configured, waits for factory addresses
     - Filters logs to configured contracts and factory-created contracts (if filtering enabled)
     - Sends logs to decoder (if decoder channel provided)
     - Returns a `LogWriteTask` containing all data needed for the write
   - Spawns `execute_log_write(task)` onto a `JoinSet`, allowing the parquet write and S3 upload to proceed concurrently with message processing
4. A dedicated `select!` branch drains completed writes from the `JoinSet` and propagates errors
5. For `LogMessage::AllRangesComplete`: signals collection is finished
   - Sets `all_ranges_complete` flag; only breaks when both `pending_ranges` and the write `JoinSet` are empty
   - Drains remaining in-flight writes after the loop exits
   - Sends `DecoderMessage::AllComplete` to decoder (if configured)

## Contract Logs Only Filtering

When `contract_logs_only: true` is set in the raw_data_collection config, the log collector filters logs to only include those from:

1. **Configured contract addresses** - Any address listed in the contracts config
2. **Factory-created contract addresses** - Addresses discovered by the factory collector

This significantly reduces storage and processing overhead when you're only interested in specific contracts.

### Configuration

```json
{
  "raw_data_collection": {
    "parquet_block_range": 1000,
    "contract_logs_only": true,
    "fields": {
      "log_fields": ["block_number", "timestamp", "address", "topics", "data"]
    }
  }
}
```

### Factory Integration

When factories are configured and `contract_logs_only` is enabled:

1. The log collector receives factory addresses from the factory collector
2. For each block range, it waits for factory address data before filtering and writing
3. Logs from newly discovered factory contracts are included in the output
4. This allows tracking events from dynamically created contracts without knowing their addresses in advance

See [Factory Collection](./FACTORY_COLLECTION.md) for more details on factory address discovery.

## Decoder Integration

When a `decoder_tx` channel is provided, the log collector forwards logs to the decoder for ABI-based event decoding. This enables automatic parsing of event data into human-readable format.

### How It Works

1. After filtering and before writing to parquet, logs are sent to the decoder via `DecoderMessage::LogsReady`
2. The decoder receives the logs along with the block range information
3. When all log collection is complete, `DecoderMessage::AllComplete` is sent to signal the decoder to finish

### Configuration

To enable decoding, provide a decoder channel when calling the current phase of `collect_logs`:

```rust
use doppler_indexer_rs::raw_data::historical::{
    catchup::logs as catchup_logs,
    current::logs as current_logs,
};

let (decoder_tx, decoder_rx) = mpsc::channel(1000);

// Phase 1: Catchup
let catchup_state = catchup_logs::collect_logs(&chain, &config, None, None).await?;

// Phase 2: Current - with decoder channel
current_logs::collect_logs(&chain, log_rx, factory_rx, Some(decoder_tx), catchup_state).await?;
```

See [Decoding](./DECODING.md) for more details on ABI-based event and call decoding.

## Resumability

Collection is resumable through coordination with the receipt collector:

### Catchup Behavior

The logs collector itself doesn't perform independent catchup. Instead, the receipt collector handles this by:

1. Checking if logs parquet files exist for each block range
2. Re-processing ranges where logs files are missing (even if receipt files exist)
3. Sending log data to the logs collector, which then writes the parquet files

This ensures that if the indexer crashes after writing receipts but before logs complete, the logs will be regenerated on the next run.

### Skipping Existing Ranges

During normal operation, the logs collector skips ranges where the parquet file already exists. This check happens before writing, not during accumulation.

### Empty Range Handling

Parquet files are written even for ranges with no logs (either because no events occurred, or because all logs were filtered out by `contract_logs_only`). This marks the range as processed and prevents unnecessary re-processing.

### Manual Re-collection

To re-collect logs for a range, delete the corresponding file from `data/{chain}/historical/raw/logs/`. You may also need to delete the corresponding receipts file to trigger the receipt collector's catchup logic.

### Automatic Recollection (Corrupted Files)

Log files can also be automatically recollected when the factory collector detects corruption during its catchup phase:

1. **Detection**: Factory collector attempts to read a log file and encounters a parse error
2. **Deletion**: The corrupted log file is automatically deleted
3. **Recollection**: Factory collector sends a `RecollectRequest` to the receipt collector
4. **Re-fetching**: Receipt collector re-fetches receipts from RPC and sends logs through channels
5. **Writing**: The logs collector receives the data through its normal channel and writes a new parquet file

This automatic recovery ensures data integrity without manual intervention. See [Factory Collection](./FACTORY_COLLECTION.md#corrupted-file-recovery) and [Receipt Collection](./RECEIPTS_COLLECTION.md#automatic-recollection-corrupted-file-recovery) for more details.

## S3 / Remote Storage Integration

When an `S3Manifest` and `StorageManager` are provided to the catchup phase, the log collector integrates with remote storage:

- **Skipping already-uploaded ranges**: Before writing a parquet file, the collector checks both the local filesystem and the S3 manifest. Ranges that already exist in S3 are skipped even if the local file is absent.
- **Uploading new ranges**: After writing a parquet file locally, the collector uploads it to S3 via the `StorageManager` using the path pattern `raw/logs/{range_start}-{range_end}.parquet`.

Both the `s3_manifest` and `storage_manager` are carried through `LogsCatchupState` so the current phase can use them without additional parameters.

## Field Configuration

### Minimal Schema (when `log_fields` is specified)

Configure specific fields in your config:

```json
{
  "raw_data_collection": {
    "parquet_block_range": 1000,
    "fields": {
      "log_fields": ["block_number", "timestamp", "address", "topics", "data"]
    }
  }
}
```

Available fields:
- `block_number` → `block_number: UInt64`
- `timestamp` → `block_timestamp: UInt64`
- `transaction_hash` → `transaction_hash: FixedSizeBinary(32)`
- `log_index` → `log_index: UInt32`
- `address` → `address: FixedSizeBinary(20)`
- `topics` → `topics: List<FixedSizeBinary(32)>`
- `data` → `data: Binary`

### Full Schema (when `log_fields` is null/omitted)

When no fields are specified, all log fields are stored:

| Column | Type | Nullable |
|--------|------|----------|
| `block_number` | UInt64 | No |
| `block_timestamp` | UInt64 | No |
| `transaction_hash` | FixedSizeBinary(32) | No |
| `log_index` | UInt32 | No |
| `address` | FixedSizeBinary(20) | No |
| `topics` | List\<FixedSizeBinary(32)\> | No |
| `data` | Binary | No |

## Data Flow

### Basic Flow (without factories)

```
┌─────────────┐                              ┌──────────────────┐                              ┌───────────────────────────────────────┐
│   blocks.rs │ ───(block info)────────────▶ │   receipts.rs    │ ───(LogMessage)────────────▶ │            logs collection            │
└─────────────┘                              │                  │                              │                                       │
                                             │  extracts logs   │                              │  ┌───────────────┐  ┌─────────────┐  │
                                             │  from receipts   │                              │  │ catchup/logs  │─▶│current/logs │  │
                                             └──────────────────┘                              │  │ (init state)  │  │(process rx) │  │
                                                                                               │  └───────────────┘  └─────────────┘  │
                                                                                               └───────────────────────────────────────┘
```

### With Factory Filtering (contract_logs_only: true)

```
┌─────────────┐                              ┌──────────────────┐
│   blocks.rs │ ───(block info)────────────▶ │   receipts.rs    │
└─────────────┘                              │                  │
                                             │  extracts logs   │
                                             │  from receipts   │
                                             └────────┬─────────┘
                                                      │
                                     ┌────────────────┼────────────────┐
                                     │                │                │
                                     ▼                ▼                │
                              ┌─────────────┐  ┌─────────────┐         │
                              │   logs.rs   │  │ factories.rs│         │
                              │             │  │             │         │
                              │ waits for   │  │  extracts   │         │
                              │ factory     │◀─│  factory    │         │
                              │ addresses   │  │  addresses  │         │
                              └─────────────┘  └─────────────┘         │
                                     │                                 │
                                     ▼                                 │
                              ┌─────────────┐                          │
                              │  filtered   │                          │
                              │  parquet    │                          │
                              │  output     │                          │
                              └─────────────┘                          │
```

### With Decoder Integration

When a decoder channel is provided, the log collector sends logs for ABI decoding:

```
┌──────────────────┐                              ┌─────────────┐
│   receipts.rs    │ ───(LogMessage)────────────▶ │   logs.rs   │
│                  │                              │             │
│  extracts logs   │                              │ filters &   │
│  from receipts   │                              │ accumulates │
└──────────────────┘                              └──────┬──────┘
                                                         │
                                    ┌────────────────────┼────────────────────┐
                                    │                    │                    │
                                    ▼                    ▼                    ▼
                             ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
                             │   parquet   │      │  decoder.rs │      │  AllComplete│
                             │   output    │      │             │      │   signal    │
                             └─────────────┘      │ decodes via │      └─────────────┘
                                                  │ ABIs        │
                                                  └─────────────┘
```

## Topics Structure

EVM logs have up to 4 topics:
- `topics[0]` - Event signature hash (e.g., `keccak256("Transfer(address,address,uint256)")`)
- `topics[1-3]` - Indexed event parameters

The `topics` column stores these as a list of 32-byte values, preserving the order and allowing efficient filtering.

## Error Handling

| Error | Cause |
|-------|-------|
| `Io` | File system error |
| `Parquet` | Parquet write error |
| `Arrow` | Arrow array construction error |
| `JoinError` | Tokio task join error (from spawn_blocking) |

Note: Log collection does not make RPC calls directly, so there are no RPC-related errors.

## Example Configuration

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

This configuration:
- Writes 10,000 blocks worth of logs per Parquet file
- Stores all common log fields for event decoding
- Coordinates with block and receipt collectors via channels

## Querying Logs

Example using DuckDB to query collected logs:

```sql
-- Find all Transfer events from a specific contract
SELECT
    block_number,
    transaction_hash,
    log_index,
    topics[1] as event_signature,
    topics[2] as from_address,
    topics[3] as to_address,
    data
FROM read_parquet('data/{chain}/historical/raw/logs/*.parquet')
WHERE address = '\x...'  -- contract address
  AND topics[1] = '\xa9059cbb...'  -- Transfer event signature
ORDER BY block_number, log_index;
```

## Complete Pipeline Example

Running all three collectors together with the two-phase log collection:

```rust
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use doppler_indexer_rs::raw_data::historical::{
    catchup::{blocks as catchup_blocks, logs as catchup_logs, receipts as catchup_receipts},
    current::{logs as current_logs, receipts as current_receipts},
};

let client = UnifiedRpcClient::from_url(&rpc_url)?;

// Create channels
let (block_tx, block_rx) = mpsc::channel(1000);
let (log_tx, log_rx) = mpsc::channel(1000);

let mut tasks = JoinSet::new();

// Spawn block collector
tasks.spawn(catchup_blocks::collect_blocks(
    chain.clone(),
    client.clone(),
    config.clone(),
    Some(block_tx),
    None,  // eth_call_tx
));

// Spawn receipt collector (handles both catchup and current phases internally)
tasks.spawn({
    let (chain, client, config) = (chain.clone(), client.clone(), config.clone());
    let log_tx = Some(log_tx);
    async move {
        // Catchup phase
        catchup_receipts::collect_receipts(
            &chain,
            &client,
            &config,
            &log_tx,
            &None,  // factory_log_tx
            &None,  // event_trigger_tx
            &[],    // event_matchers
        ).await?;

        // Current phase
        current_receipts::collect_receipts(
            &chain,
            &client,
            &config,
            block_rx,
            log_tx,
            None,  // factory_log_tx
            None,  // event_trigger_tx
            vec![], // event_matchers
            None,  // recollect_rx
        ).await
    }
});

// Spawn log collector with two-phase approach
tasks.spawn({
    let (chain, config) = (chain.clone(), config.clone());
    async move {
        // Phase 1: Catchup - initialize state
        let catchup_state = catchup_logs::collect_logs(&chain, &config, None, None).await?;

        // Phase 2: Current - process logs from channel
        current_logs::collect_logs(&chain, log_rx, None, None, catchup_state).await
    }
});

// Wait for all to complete
while let Some(result) = tasks.join_next().await {
    result??;
}
```
