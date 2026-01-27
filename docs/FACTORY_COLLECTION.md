# Factory Collection

The factory collection module tracks contracts that are dynamically created by other contracts. This is useful for indexing token deployments, liquidity pool creations, and other patterns where contract addresses aren't known in advance.

## Overview

Factory collection works by:
1. Monitoring logs from configured factory contracts
2. Matching logs against specified event signatures
3. Extracting the created contract address from event parameters
4. Writing discovered addresses to parquet files
5. Notifying the logs and eth_calls collectors about new addresses

## Configuration

Factories are configured within the `factories` array of a contract:

```json
{
    "Airlock": {
        "address": "0x660eAaEdEBc968f8f3694354FA8EC0b4c5Ba8D12",
        "factories": [
            {
                "collection_name": "DERC20",
                "factory_events": {
                    "name": "Create",
                    "topics_signature": "address",
                    "data_signature": "address,address,address"
                },
                "factory_parameters": "data[0]",
                "calls": [
                    {"function": "totalSupply()", "output_type": "uint256"}
                ]
            }
        ]
    }
}
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `collection_name` | string | Yes | Identifier for this group of factory-created contracts |
| `factory_events` | object | Yes | Event signature information for matching |
| `factory_parameters` | string | Yes | Which parameter contains the created contract address |
| `calls` | array | No | eth_call configs to execute on factory-created contracts |

### Factory Events

The `factory_events` object defines how to match factory creation events:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Event name (e.g., "Create", "PairCreated", "PoolCreated") |
| `topics_signature` | string | Comma-separated types of indexed parameters (after topic0) |
| `data_signature` | string | Comma-separated types of non-indexed parameters in event data |

**Event Signature Computation:**

The event topic0 is computed as `keccak256("{name}({topics_signature},{data_signature}")`:

```
Event: Create(address indexed token, address pool, address hook, address asset)
topics_signature: "address"
data_signature: "address,address,address"
topic0: keccak256("Create(address,address,address,address)")
```

### Factory Parameters

The `factory_parameters` field specifies where to extract the created contract address:

| Format | Description |
|--------|-------------|
| `topics[0]` | Event signature (topic0) - rarely useful |
| `topics[1]` | First indexed parameter |
| `topics[2]` | Second indexed parameter |
| `data[0]` | First non-indexed parameter |
| `data[1]` | Second non-indexed parameter |
| etc. | Continue pattern as needed |

**Example:** For a `PairCreated(address indexed token0, address indexed token1, address pair, uint)` event:
- `topics[1]` = token0 address
- `topics[2]` = token1 address
- `data[0]` = pair address (the created contract)

## Function Signature

```rust
pub async fn collect_factories(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    log_rx: Receiver<Vec<LogData>>,
    logs_factory_tx: Option<Sender<FactoryAddressData>>,
    eth_calls_factory_tx: Option<Sender<FactoryAddressData>>,
) -> Result<(), FactoryCollectionError>
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `chain` | `&ChainConfig` | Chain configuration with contracts |
| `raw_data_config` | `&RawDataCollectionConfig` | Parquet range size configuration |
| `log_rx` | `Receiver<Vec<LogData>>` | Channel receiving logs from receipt collector |
| `logs_factory_tx` | `Option<Sender<FactoryAddressData>>` | Channel to send addresses to logs collector |
| `eth_calls_factory_tx` | `Option<Sender<FactoryAddressData>>` | Channel to send addresses to eth_calls collector |

## Output

### Parquet Files

Factory addresses are written to `data/derived/{chain}/factories/` with the naming convention:

```
{collection_name}_{start_block}-{end_block}.parquet
```

Example: `DERC20_0-9999.parquet`

### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block where the contract was created |
| `block_timestamp` | UInt64 | Unix timestamp of the creation block |
| `factory_address` | FixedSizeBinary(20) | Address of the created contract |
| `collection_name` | Utf8 | Name of the factory collection |

### FactoryAddressData Message

Data sent to downstream collectors:

```rust
pub struct FactoryAddressData {
    pub range_start: u64,
    pub range_end: u64,
    pub addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
}
```

The `addresses_by_block` map contains `(timestamp, address, collection_name)` tuples grouped by block number.

## Data Flow

```
┌──────────────────┐                              ┌─────────────────┐
│   receipts.rs    │ ───(Vec<LogData>)──────────▶ │  factories.rs   │
│                  │                              │                 │
│  extracts logs   │                              │ matches factory │
│  from receipts   │                              │ events, extracts│
└──────────────────┘                              │ addresses       │
                                                  └────────┬────────┘
                                                           │
                              ┌─────────────────────────────┼────────────────────────────┐
                              │                             │                            │
                              ▼                             ▼                            ▼
                    ┌─────────────────┐         ┌─────────────────┐         ┌──────────────────┐
                    │    logs.rs      │         │  eth_calls.rs   │         │    parquet       │
                    │                 │         │                 │         │                  │
                    │  filters logs   │         │ executes calls  │         │ data/derived/    │
                    │  to include     │         │ on factory      │         │ {chain}/         │
                    │  factory        │         │ contracts       │         │ factories/       │
                    │  contracts      │         │                 │         │                  │
                    └─────────────────┘         └─────────────────┘         └──────────────────┘
```

## Integration with Other Collectors

### Logs Collector

When `contract_logs_only: true` is configured:
1. The logs collector waits for factory addresses before filtering each range
2. Logs from factory-created contracts are included in the filtered output
3. This allows tracking events from dynamically created contracts

### Eth Calls Collector

When factories have `calls` configured:
1. Regular eth_calls execute immediately (don't wait for factory data)
2. Factory eth_calls execute when factory addresses arrive
3. Factory call results use the collection name in the file name (e.g., `eth_calls_DERC20_totalSupply_0-9999.parquet`)

## Example Use Cases

### Token Factory

Track all tokens deployed by a token factory:

```json
{
    "TokenFactory": {
        "address": "0x...",
        "factories": [
            {
                "collection_name": "FactoryTokens",
                "factory_events": {
                    "name": "TokenCreated",
                    "topics_signature": "",
                    "data_signature": "address,string,string"
                },
                "factory_parameters": "data[0]",
                "calls": [
                    {"function": "totalSupply()", "output_type": "uint256"},
                    {"function": "decimals()", "output_type": "uint8"}
                ]
            }
        ]
    }
}
```

### Uniswap V2 Pairs

Track all pairs created by Uniswap V2 factory:

```json
{
    "UniswapV2Factory": {
        "address": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        "factories": [
            {
                "collection_name": "UniswapV2Pairs",
                "factory_events": {
                    "name": "PairCreated",
                    "topics_signature": "address,address",
                    "data_signature": "address,uint256"
                },
                "factory_parameters": "data[0]",
                "calls": [
                    {"function": "getReserves()", "output_type": "uint256"}
                ]
            }
        ]
    }
}
```

### Multiple Factories Per Contract

A single contract can have multiple factory configurations:

```json
{
    "MultiFactory": {
        "address": "0x...",
        "factories": [
            {
                "collection_name": "TypeA",
                "factory_events": {"name": "TypeACreated", ...},
                "factory_parameters": "data[0]"
            },
            {
                "collection_name": "TypeB",
                "factory_events": {"name": "TypeBCreated", ...},
                "factory_parameters": "topics[1]"
            }
        ]
    }
}
```

## Error Handling

| Error | Cause |
|-------|-------|
| `Io` | File system error when writing parquet |
| `Parquet` | Parquet write error |
| `Arrow` | Arrow array construction error |
| `ChannelSend` | Failed to send data to downstream collector |
| `AbiDecode` | Failed to decode event data (logged as warning, doesn't stop collection) |

ABI decode errors are logged but don't stop collection. This allows the indexer to continue even if some events have unexpected data formats.

## Querying Factory Data

Example using DuckDB to query discovered factory contracts:

```sql
-- Find all tokens created in a time range
SELECT
    block_number,
    block_timestamp,
    encode(factory_address, 'hex') as token_address,
    collection_name
FROM read_parquet('data/derived/base/factories/*.parquet')
WHERE collection_name = 'DERC20'
  AND block_timestamp BETWEEN 1700000000 AND 1700100000
ORDER BY block_number;

-- Count contracts per collection
SELECT
    collection_name,
    COUNT(*) as contract_count
FROM read_parquet('data/derived/base/factories/*.parquet')
GROUP BY collection_name;
```

## Performance Considerations

- Factory matching is O(n × m) where n is logs and m is matchers
- ABI decoding for data extraction adds overhead
- Large numbers of factory contracts increase eth_call RPC costs
- Consider using `parquet_block_range` to balance file count vs. size

## Resumability

Collection is fully resumable with multiple safeguards:

### Factory Parquet Skipping

Existing parquet files are scanned on startup and their ranges are skipped during collection.

### Empty Range Handling

For each block range processed, the factory collector writes a parquet file for **every configured collection**, even if no factory events were found for that collection in the range. This ensures:

1. The receipt collector's catchup logic can verify factory processing is complete
2. Ranges with no factory activity aren't re-processed on subsequent runs
3. Downstream collectors know the range was fully processed

### Downstream Collector Notification

On startup, factory addresses from existing parquet files are read and sent to the logs and eth_calls collectors. This allows those collectors to process ranges that they haven't yet handled, even if the factory collector already processed them in a previous run.

### Catchup Coordination

The receipt collector checks for factory files during its catchup phase. If block files exist but factory files are missing for any configured collection, the receipt collector will re-process that range, which triggers factory collection.

This means if you have:
- Block parquet files for ranges 0-999, 1000-1999, 2000-2999
- Factory parquet files for ranges 0-999, 1000-1999 (missing 2000-2999)

On the next run, the receipt collector will detect the missing factory files and re-process range 2000-2999, ensuring factory collection completes.

### Manual Re-collection

To re-collect a factory range, delete the corresponding file from `data/derived/{chain}/factories/`. Note that you may also need to delete the corresponding receipts file to trigger re-processing.
