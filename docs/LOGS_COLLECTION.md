# Log Collection

The log collection module receives logs extracted from transaction receipts and writes them to Parquet files. Logs are obtained from the receipt collector rather than via separate RPC calls, ensuring consistency and efficiency.

## Usage

```rust
use doppler_indexer_rs::raw_data::historical::logs::collect_logs;
use doppler_indexer_rs::raw_data::historical::receipts::{LogData, LogMessage};
use doppler_indexer_rs::raw_data::historical::factories::FactoryAddressData;
use doppler_indexer_rs::raw_data::decoding::DecoderMessage;
use tokio::sync::mpsc;

// Channel from receipt collector (sends LogMessage)
let (log_tx, log_rx) = mpsc::channel::<LogMessage>(1000);

// Optional factory channel (for contract_logs_only filtering)
let factory_rx: Option<Receiver<FactoryAddressData>> = None;

// Optional decoder channel (for ABI decoding)
let decoder_tx: Option<Sender<DecoderMessage>> = None;

collect_logs(&chain_config, &raw_data_config, log_rx, factory_rx, decoder_tx).await?;
```

## Function Signature

```rust
pub async fn collect_logs(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    log_rx: Receiver<LogMessage>,
    factory_rx: Option<Receiver<FactoryAddressData>>,
    decoder_tx: Option<Sender<DecoderMessage>>,
) -> Result<(), LogCollectionError>
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with name and contracts |
| `raw_data_config` | `&RawDataCollectionConfig` | Parquet range size and field configuration |
| `log_rx` | `Receiver<LogMessage>` | Channel receiving log messages from receipt collector |
| `factory_rx` | `Option<Receiver<FactoryAddressData>>` | Optional channel receiving factory addresses for filtering |
| `decoder_tx` | `Option<Sender<DecoderMessage>>` | Optional channel for sending logs to the decoder for ABI decoding |

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
    LogsReady { range_start: u64, range_end: u64, logs: Vec<LogData> },
    AllComplete,
}
```

- **`LogsReady`** - Sends the filtered logs for a completed range to the decoder
- **`AllComplete`** - Signals that all log collection is finished

## Output

Logs are written to `data/raw/<CHAIN_NAME>/logs/` as Parquet files named by block range:

```
data/raw/ethereum/logs/
├── logs_0-999.parquet
├── logs_1000-1999.parquet
├── logs_2000-2999.parquet
└── ...
```

## Processing Flow

1. Receives `LogMessage` from the receipt collector via channel
2. For `LogMessage::Logs`: accumulates logs into per-range buckets
3. For `LogMessage::RangeComplete`: signals a range is ready to process
   - If `contract_logs_only` is enabled and factories are configured, waits for factory addresses
   - Filters logs to configured contracts and factory-created contracts (if filtering enabled)
   - Sends logs to decoder (if decoder channel provided)
   - Writes range to Parquet file
4. For `LogMessage::AllRangesComplete`: signals collection is finished
   - Waits for any pending ranges that need factory data
   - Sends `DecoderMessage::AllComplete` to decoder (if configured)
   - Exits the collection loop

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

To enable decoding, provide a decoder channel when calling `collect_logs`:

```rust
let (decoder_tx, decoder_rx) = mpsc::channel(1000);
collect_logs(&chain, &config, log_rx, factory_rx, Some(decoder_tx)).await?;
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

To re-collect logs for a range, delete the corresponding file from `data/raw/{chain}/logs/`. You may also need to delete the corresponding receipts file to trigger the receipt collector's catchup logic.

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
┌─────────────┐                              ┌──────────────────┐                              ┌─────────────┐
│   blocks.rs │ ───(block info)────────────▶ │   receipts.rs    │ ───(LogMessage)────────────▶ │   logs.rs   │
└─────────────┘                              │                  │                              │             │
                                             │  extracts logs   │                              │ writes logs │
                                             │  from receipts   │                              │ to parquet  │
                                             └──────────────────┘                              └─────────────┘
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
FROM read_parquet('data/raw/ethereum/logs/*.parquet')
WHERE address = '\x...'  -- contract address
  AND topics[1] = '\xa9059cbb...'  -- Transfer event signature
ORDER BY block_number, log_index;
```

## Complete Pipeline Example

Running all three collectors together:

```rust
use tokio::sync::mpsc;
use tokio::task::JoinSet;

let client = UnifiedRpcClient::from_url(&rpc_url)?;

// Create channels
let (block_tx, block_rx) = mpsc::channel(1000);
let (log_tx, log_rx) = mpsc::channel(1000);

let mut tasks = JoinSet::new();

// Spawn collectors
tasks.spawn(collect_blocks(chain.clone(), client.clone(), config.clone(), Some(block_tx)));
tasks.spawn(collect_receipts(chain.clone(), client.clone(), config.clone(), block_rx, Some(log_tx), None));
tasks.spawn(collect_logs(chain.clone(), config.clone(), log_rx, None, None));

// Wait for all to complete
while let Some(result) = tasks.join_next().await {
    result??;
}
```
