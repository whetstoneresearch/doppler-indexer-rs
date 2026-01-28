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
3. Fetches transaction receipts (see [Fetching Strategies](#fetching-strategies) below)
4. Extracts logs from receipts and forwards to log collector
5. Writes receipt data to Parquet file

## Fetching Strategies

The receipt collector supports two fetching strategies, configured per-chain via `block_receipts_method`:

### Per-Transaction Fetching (Default)

When `block_receipts_method` is not set, receipts are fetched individually using `eth_getTransactionReceipt`:

- Transactions are batched into chunks (configurable via `rpc_batch_size`, default 100)
- Each batch is fetched concurrently using `futures::join_all`
- Rate limiting is applied per batch

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
- Blocks are processed sequentially (no batching) due to large response sizes
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

Collection is fully resumable with comprehensive catchup logic:

### Catchup Phase

On startup, the receipt collector performs a catchup phase before listening for new blocks:

1. **Scans existing block files** - Reads `data/raw/{chain}/blocks/` to find all available block ranges
2. **Checks downstream files** - For each block range, checks if:
   - Receipt parquet file exists
   - Logs parquet file exists (if log collection is enabled)
   - Factory parquet files exist for all configured collections (if factories are configured)
3. **Re-processes missing ranges** - If any downstream files are missing, reads block info from the existing block parquet file and re-processes that range

This ensures that if the indexer crashes after writing block files but before completing receipt/log/factory collection, those ranges will be automatically re-processed on restart.

### Empty Range Handling

If a block range contains no transactions, the collector writes an empty parquet file to mark the range as processed. This prevents unnecessary re-processing on subsequent runs.

### Manual Re-collection

To re-collect a range, delete the corresponding file from `data/raw/{chain}/receipts/`. Note that you may also need to delete the corresponding logs and factory files if you want those to be regenerated.

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
