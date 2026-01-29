# eth_call Collection

This document describes how the eth_call indexing feature works in the doppler-indexer.

## Overview

The eth_call collector makes `eth_call` RPC requests at historical block heights and stores the raw results in parquet files. This is useful for tracking on-chain state over time (e.g., token balances, total supplies, prices from oracles).

## Configuration

eth_calls are configured per-contract in your chain config:

```json
{
  "contracts": {
    "USDC": {
      "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
      "calls": [
        {
          "function": "totalSupply()",
          "output_type": "uint256"
        }
      ]
    },
    "UniswapV2Pair": {
      "address": [
        "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
        "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"
      ],
      "calls": [
        {
          "function": "getReserves()",
          "output_type": "uint256"
        }
      ]
    }
  }
}
```

### Call Configuration

| Field | Description |
|-------|-------------|
| `function` | The function signature (e.g., `totalSupply()`, `balanceOf(address)`). Used to compute the 4-byte selector. |
| `output_type` | The return type: `uint256` or `int256`. Used by consumers to decode the binary result. |

**Note:** Currently only parameterless functions are supported. Functions with arguments would require additional configuration for the encoded parameters.

## Data Flow

```
blocks collector ──(block_number, timestamp)──> eth_calls collector ──> parquet files
```

1. The blocks collector sends `(block_number, timestamp)` to the eth_calls channel after each RPC batch
2. The eth_calls collector accumulates blocks until a complete parquet range is ready
3. For each block in the range, it builds `eth_call` requests for all configured contracts/functions
4. Calls are executed in batches (controlled by `rpc_batch_size` config)
5. Results are written to parquet, grouped by contract and function

## Output Format

### File Naming

Files are written to `data/raw/{chain}/eth_calls/{contract_name}/{function_name}/` with the naming convention:

```
{start_block}-{end_block}.parquet
```

Example: `data/raw/base/eth_calls/USDC/totalSupply/0-9999.parquet`

### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | The block height at which the call was made |
| `block_timestamp` | UInt64 | Unix timestamp of the block |
| `contract_address` | FixedSizeBinary(20) | The contract address called |
| `value` | Binary | Raw bytes returned by eth_call |

### Decoding Values

The `value` column contains raw bytes from the eth_call response. To decode:

1. Look up the `contract_address` in your config to find the `output_type`
2. Decode the bytes according to the type:
   - `uint256`: 32-byte big-endian unsigned integer
   - `int256`: 32-byte big-endian signed integer (two's complement)

Example in Python:
```python
import pandas as pd

df = pd.read_parquet("eth_calls_USDC_totalSupply_0-9999.parquet")

# Decode uint256
df['decoded_value'] = df['value'].apply(
    lambda x: int.from_bytes(x, byteorder='big') if x else None
)
```

## Error Handling

- If an eth_call fails (e.g., contract doesn't exist at that block), an empty `value` is stored
- Failed calls are logged with a warning but don't stop collection
- Empty values in the output indicate either a failed call or a zero-length response

## Performance Considerations

### Batching

The collector respects the `rpc_batch_size` configuration:

```json
{
  "raw_data_collection": {
    "rpc_batch_size": 100,
    "parquet_block_range": 10000
  }
}
```

- `rpc_batch_size`: Number of eth_calls per RPC batch (default: 100)
- `parquet_block_range`: Number of blocks per parquet file (default: 1000)

### Compute Units

Each eth_call costs compute units on rate-limited providers (e.g., Alchemy). With many contracts and functions configured, costs can add up quickly:

```
total_calls_per_range = num_contracts × num_functions × parquet_block_range
```

For example, 10 contracts with 2 functions each over 10,000 blocks = 200,000 eth_calls per parquet file.

## Benchmarking

Enable the `bench` feature to track timing:

```bash
cargo run --features bench
```

This writes timing data to `data/bench.csv`:

```csv
collector,range_start,range_end,record_count,rpc_ms,process_ms,write_ms
eth_calls_USDC.totalSupply,0,10000,10000,5230,45,12
```

## Factory Contract Calls

The eth_call collector supports executing calls on factory-created contracts. When factories are configured with `calls`, those calls are executed on dynamically discovered contract addresses.

### Configuration

Factory calls are defined within the `factories` array of a contract:

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
                    {"function": "totalSupply()", "output_type": "uint256"},
                    {"function": "name()", "output_type": "string"},
                    {"function": "symbol()", "output_type": "string"}
                ]
            }
        ]
    }
}
```

### Output Files

Factory eth_call results are written to the same directory structure as regular eth_calls, using the collection name in place of the contract name:

```
data/raw/{chain}/eth_calls/{collection}/{function}/{start}-{end}.parquet
```

Example: `data/raw/base/eth_calls/DERC20/totalSupply/0-9999.parquet`

### Processing Flow

1. Regular eth_calls are executed immediately when a block range completes (no waiting)
2. Factory eth_calls are executed when factory addresses arrive from the factory collector
3. Both regular and factory results are written independently
4. Factory calls use the same schema as regular calls

### Performance Considerations

Factory calls scale with the number of discovered contracts:

```
total_factory_calls = num_factory_addresses × num_calls × blocks_per_range
```

For frequently-used factories (e.g., token deployers), this can generate many calls. Consider:
- Limiting the `calls` configuration to essential functions
- Using larger `parquet_block_range` to amortize overhead
- Monitoring RPC compute unit usage

See [Factory Collection](./FACTORY_COLLECTION.md) for details on how factory addresses are discovered.

## Resumability

Collection is fully resumable with catchup logic for regular eth_calls:

### Catchup Phase (Regular Calls)

On startup, the eth_call collector performs a catchup phase for regular (non-factory) calls:

1. **Scans existing block files** - Reads `data/raw/{chain}/blocks/` to find all available block ranges
2. **Checks existing eth_call files** - For each block range, checks if eth_call parquet files exist for all configured contract/function pairs
3. **Re-processes missing ranges** - If any eth_call files are missing, reads block info from the existing block parquet file and executes the calls

### Factory Calls

Factory eth_calls are handled during the normal processing phase (not during catchup). When the factory collector sends addresses, those calls are executed regardless of what catchup has processed. This is because factory addresses may not be known until the factory collector processes the corresponding log data.

### Manual Re-collection

To re-collect eth_calls for a range, delete the corresponding file from `data/raw/{chain}/eth_calls/{contract}/{function}/`.

## Limitations

- No state override support (calls use the actual on-chain state)
- Results are stored as raw bytes; decoding is left to the consumer
- Factory calls require the factory collector to discover addresses first
