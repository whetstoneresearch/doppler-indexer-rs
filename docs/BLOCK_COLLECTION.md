# Block Collection

The block collection module fetches historical Ethereum blocks via RPC and writes them to Parquet files for efficient storage and querying.

## Usage

```rust
use doppler_indexer_rs::raw_data::historical::blocks::collect_blocks;
use doppler_indexer_rs::rpc::UnifiedRpcClient;
use tokio::sync::mpsc;

let client = UnifiedRpcClient::from_url(&rpc_url)?;
let (tx, rx) = mpsc::channel(1000);

// Basic usage with transaction sender only
collect_blocks(&chain_config, &client, &raw_data_config, Some(tx), None).await?;

// With both transaction and eth_call senders
let (eth_call_tx, eth_call_rx) = mpsc::channel(1000);
collect_blocks(&chain_config, &client, &raw_data_config, Some(tx), Some(eth_call_tx)).await?;
```

## Function Signature

```rust
pub async fn collect_blocks(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    tx_sender: Option<Sender<(u64, u64, Vec<B256>)>>,
    eth_call_sender: Option<Sender<(u64, u64)>>,
) -> Result<(), BlockCollectionError>
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with name and start block |
| `client` | `&UnifiedRpcClient` | Shared RPC client (enables rate limit sharing across operations) |
| `raw_data_config` | `&RawDataCollectionConfig` | Parquet range size, RPC batch size, and field configuration |
| `tx_sender` | `Option<Sender<(u64, u64, Vec<B256>)>>` | Optional channel for receipt/log collection |
| `eth_call_sender` | `Option<Sender<(u64, u64)>>` | Optional channel for eth_call data collection |

### Channel Message Format

When `tx_sender` is provided, each block sends a tuple:
- `u64` - Block number
- `u64` - Block timestamp
- `Vec<B256>` - Transaction hashes in the block

When `eth_call_sender` is provided, each block sends a tuple:
- `u64` - Block number
- `u64` - Block timestamp

## Output

Blocks are written to `data/raw/<CHAIN_NAME>/blocks/` as Parquet files named by block range:

```
data/raw/ethereum/blocks/
├── blocks_0-999.parquet
├── blocks_1000-1999.parquet
├── blocks_2000-2999.parquet
└── ...
```

## Block Range Alignment

The start block is aligned to the nearest multiple of the range size (rounded down):

| Config Start Block | Range Size | Actual Start |
|-------------------|------------|--------------|
| 0 | 1000 | 0 |
| 1234 | 1000 | 1000 |
| 5500 | 1000 | 5000 |
| 100 | 500 | 0 |

## Resumability

Collection is resumable. On each run, existing Parquet files are scanned and their ranges are skipped. To re-collect a range, delete the corresponding file.

## Field Configuration

### Minimal Schema (when `block_fields` is specified)

Configure specific fields in your config:

```json
{
  "raw_data_collection": {
    "parquet_block_range": 1000,
    "fields": {
      "block_fields": ["number", "timestamp", "transactions"]
    }
  }
}
```

Available fields:
- `number` → `number: UInt64`
- `timestamp` → `timestamp: UInt64`
- `transactions` → `transaction_count: UInt32` + `transaction_hashes: List<Utf8>`
- `uncles` → `uncle_count: UInt32`

### Full Schema (when `block_fields` is null/omitted)

When no fields are specified, all block header fields are stored:

| Column | Type | Nullable |
|--------|------|----------|
| `number` | UInt64 | No |
| `hash` | FixedSizeBinary(32) | No |
| `parent_hash` | FixedSizeBinary(32) | No |
| `nonce` | FixedSizeBinary(8) | No |
| `ommers_hash` | FixedSizeBinary(32) | No |
| `logs_bloom` | Binary | No |
| `transactions_root` | FixedSizeBinary(32) | No |
| `state_root` | FixedSizeBinary(32) | No |
| `receipts_root` | FixedSizeBinary(32) | No |
| `miner` | FixedSizeBinary(20) | No |
| `difficulty` | Utf8 | No |
| `total_difficulty` | Utf8 | Yes |
| `extra_data` | Binary | No |
| `gas_limit` | UInt64 | No |
| `gas_used` | UInt64 | No |
| `timestamp` | UInt64 | No |
| `mix_hash` | FixedSizeBinary(32) | No |
| `base_fee_per_gas` | UInt64 | Yes |
| `withdrawals_root` | FixedSizeBinary(32) | Yes |
| `blob_gas_used` | UInt64 | Yes |
| `excess_blob_gas` | UInt64 | Yes |
| `parent_beacon_block_root` | FixedSizeBinary(32) | Yes |
| `transaction_count` | UInt32 | No |
| `transaction_hashes` | List\<Utf8\> | No |
| `uncle_count` | UInt32 | No |
| `size` | UInt64 | Yes |

## RPC Client Selection

The `UnifiedRpcClient` automatically selects the appropriate rate limiting strategy:

- **Alchemy URLs** (containing "alchemy"): Uses compute unit rate limiting (330 CU/s default)
- **Other URLs**: Uses request-based rate limiting

Both clients share the same interface, allowing rate limits to be shared across block, receipt, and log collection on the same chain.

## Error Handling

| Error | Cause |
|-------|-------|
| `Rpc` | RPC request failed |
| `Io` | File system error |
| `Parquet` | Parquet write error |
| `Arrow` | Arrow array construction error |
| `BlockNotFound` | RPC returned null for a block number |
| `ChannelSend` | Receiver dropped the channel |
| `JoinError` | Tokio task join failed (e.g., during Parquet write) |

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
    "rpc_batch_size": 100,
    "fields": {
      "block_fields": null
    }
  }
}
```

This configuration:
- Collects blocks starting from 17,000,000 (aligned to 17,000,000 since 17000000 % 10000 == 0)
- Writes 10,000 blocks per Parquet file
- Fetches 100 blocks per RPC batch request (default: 100)
- Stores all block fields (full schema)
- Reads RPC URL from `ETHEREUM_RPC_URL` environment variable

## Helper Functions

### Reading Existing Block Data

Two helper functions are available for reading previously collected block data:

#### `get_existing_block_ranges`

```rust
pub fn get_existing_block_ranges(chain_name: &str) -> Vec<ExistingBlockRange>
```

Scans the output directory and returns all existing Parquet file ranges for a chain. Useful for determining what data has already been collected.

**Returns:** `Vec<ExistingBlockRange>` sorted by start block, where each range contains:
- `start: u64` - First block number (inclusive)
- `end: u64` - Last block number (exclusive)
- `file_path: PathBuf` - Path to the Parquet file

#### `read_block_info_from_parquet`

```rust
pub fn read_block_info_from_parquet(
    file_path: &Path,
) -> Result<Vec<BlockInfoForDownstream>, BlockCollectionError>
```

Reads block information from a Parquet file. Useful for downstream processing that needs block data without re-fetching from RPC.

**Returns:** `Vec<BlockInfoForDownstream>` sorted by block number, where each block contains:
- `block_number: u64` - The block number
- `timestamp: u64` - The block timestamp
- `tx_hashes: Vec<B256>` - Transaction hashes in the block
