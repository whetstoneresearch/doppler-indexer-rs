# Receipt Collection

The receipt collection module fetches transaction receipts via RPC and writes them to Parquet files. It receives block information from the block collector and extracts logs to forward to the log collector.

## Usage

```rust
use doppler_indexer_rs::raw_data::historical::receipts::collect_receipts;
use doppler_indexer_rs::rpc::UnifiedRpcClient;
use tokio::sync::mpsc;

let client = UnifiedRpcClient::from_url(&rpc_url)?;

// Channel from block collector
let (block_tx, block_rx) = mpsc::channel(1000);
// Channel to log collector
let (log_tx, log_rx) = mpsc::channel(1000);

collect_receipts(&chain_config, &client, &raw_data_config, block_rx, Some(log_tx)).await?;
```

## Function Signature

```rust
pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    block_rx: Receiver<(u64, u64, Vec<B256>)>,
    log_tx: Option<Sender<Vec<LogData>>>,
) -> Result<(), ReceiptCollectionError>
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with name |
| `client` | `&UnifiedRpcClient` | Shared RPC client for fetching receipts |
| `raw_data_config` | `&RawDataCollectionConfig` | Parquet range size and field configuration |
| `block_rx` | `Receiver<(u64, u64, Vec<B256>)>` | Channel receiving block info from block collector |
| `log_tx` | `Option<Sender<Vec<LogData>>>` | Optional channel for forwarding logs |

### Input Channel Message Format

Receives from block collector:
- `u64` - Block number
- `u64` - Block timestamp
- `Vec<B256>` - Transaction hashes in the block

### Output Channel Message Format

When `log_tx` is provided, sends `Vec<LogData>` where each `LogData` contains:
- `block_number: u64`
- `block_timestamp: u64`
- `transaction_hash: B256`
- `log_index: u32`
- `address: [u8; 20]`
- `topics: Vec<[u8; 32]>`
- `data: Vec<u8>`

## Output

Receipts are written to `data/raw/<CHAIN_NAME>/receipts/` as Parquet files named by block range:

```
data/raw/ethereum/receipts/
├── receipts_0-999.parquet
├── receipts_1000-1999.parquet
├── receipts_2000-2999.parquet
└── ...
```

## Processing Flow

1. Receives block info (block number, timestamp, tx hashes) from block collector
2. Accumulates messages until a complete block range is received
3. Fetches all transaction receipts for the range via batch RPC
4. Extracts logs from receipts and forwards to log collector
5. Writes receipt data to Parquet file

## Resumability

Collection is resumable. On each run, existing Parquet files are scanned and their ranges are skipped. To re-collect a range, delete the corresponding file.

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

## Data Flow

```
┌─────────────┐     (block_number, timestamp, tx_hashes)     ┌──────────────────┐
│   blocks.rs │ ──────────────────────────────────────────▶  │   receipts.rs    │
└─────────────┘                                              │                  │
                                                             │  1. Accumulate   │
                                                             │  2. Fetch RPC    │
                                                             │  3. Write parquet│
                                                             │  4. Extract logs │
                                                             └────────┬─────────┘
                                                                      │
                                                              Vec<LogData>
                                                                      │
                                                                      ▼
                                                             ┌──────────────────┐
                                                             │     logs.rs      │
                                                             └──────────────────┘
```

## Error Handling

| Error | Cause |
|-------|-------|
| `Rpc` | RPC request failed |
| `Io` | File system error |
| `Parquet` | Parquet write error |
| `Arrow` | Arrow array construction error |
| `ReceiptNotFound` | RPC returned null for a transaction hash |
| `ChannelSend` | Log receiver dropped the channel |

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
- Writes 10,000 blocks worth of receipts per Parquet file
- Stores only the specified receipt fields (minimal schema)
- Coordinates with block and log collectors via channels
