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

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `function` | string | Yes | - | The function signature (e.g., `totalSupply()`, `balanceOf(address)`). Used to compute the 4-byte selector. |
| `output_type` | string | Yes | - | The return type. Used by consumers to decode the binary result. |
| `params` | array | No | `[]` | Parameters to pass to the function (see Parameters section below). |
| `frequency` | string/number | No | every block | How often to make the call (see Frequency section below). |

### Supported Types

The `output_type` field (and parameter types) support the following EVM types:

| Type | Description |
|------|-------------|
| `uint8`, `uint16`, `uint32`, `uint64`, `uint80`, `uint128`, `uint256` | Unsigned integers of various sizes |
| `int8`, `int32`, `int64`, `int128`, `int256` | Signed integers of various sizes |
| `address` | 20-byte Ethereum address |
| `bool` | Boolean value |
| `bytes32` | Fixed 32-byte value |
| `bytes` | Dynamic byte array |
| `string` | Dynamic string |

## Parameters

Functions with arguments are supported via the `params` configuration. Each parameter specifies a type and a list of values. The collector generates all combinations of parameter values (cartesian product).

### Parameter Configuration

```json
{
  "function": "balanceOf(address)",
  "output_type": "uint256",
  "params": [
    {
      "type": "address",
      "values": [
        "0x1234567890abcdef1234567890abcdef12345678",
        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
      ]
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | The EVM type of the parameter (see Supported Types above). |
| `values` | array | Yes | List of values to call the function with. |

### Multi-Parameter Example

For functions with multiple parameters, provide multiple param configs. The collector calls the function with every combination:

```json
{
  "function": "allowance(address,address)",
  "output_type": "uint256",
  "params": [
    {
      "type": "address",
      "values": ["0xOwner1...", "0xOwner2..."]
    },
    {
      "type": "address",
      "values": ["0xSpender1...", "0xSpender2..."]
    }
  ]
}
```

This generates 4 calls: `(Owner1, Spender1)`, `(Owner1, Spender2)`, `(Owner2, Spender1)`, `(Owner2, Spender2)`.

### Parameter Value Formats

| Type | Format Examples |
|------|-----------------|
| `address` | `"0x1234..."` (40 hex chars with 0x prefix) |
| `uint*` / `int*` | `"1000000"` (decimal) or `"0xde0b6b3a7640000"` (hex) |
| `bool` | `true`, `false`, `"true"`, `"false"`, `1`, `0` |
| `bytes32` | `"0x..."` (64 hex chars with 0x prefix) |
| `bytes` | `"0x..."` (hex with 0x prefix) |
| `string` | `"any string value"` |

**Note:** Functions with `frequency: "once"` do not support parameters.

## Frequency

The `frequency` field controls how often eth_calls are made. This is useful for optimizing RPC usage when certain data doesn't need to be fetched every block.

### Frequency Options

| Value | Behavior |
|-------|----------|
| *(omitted)* | Call every block (default) |
| `"once"` | Call once per contract address |
| `100` (any positive integer) | Call every N blocks |
| `"5m"`, `"1h"`, `"1d"` | Call at time intervals |

### Duration Format

Duration strings use a number followed by a unit suffix:

| Suffix | Unit |
|--------|------|
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days |

Examples: `"30s"`, `"5m"`, `"1h"`, `"7d"`

### Example Configuration

```json
{
  "contracts": {
    "USDC": {
      "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
      "calls": [
        {
          "function": "totalSupply()",
          "output_type": "uint256"
        },
        {
          "function": "name()",
          "output_type": "string",
          "frequency": "once"
        },
        {
          "function": "decimals()",
          "output_type": "uint8",
          "frequency": "once"
        },
        {
          "function": "balanceOf(address)",
          "output_type": "uint256",
          "frequency": "1h",
          "params": [
            {
              "type": "address",
              "values": [
                "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503",
                "0x0a59649758aa4d66e25f08dd01271e891fe52199"
              ]
            }
          ]
        }
      ]
    },
    "ChainlinkOracle": {
      "address": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
      "calls": [
        {
          "function": "latestAnswer()",
          "output_type": "int256",
          "frequency": "1h"
        },
        {
          "function": "description()",
          "output_type": "string",
          "frequency": "once"
        }
      ]
    }
  }
}
```

### Frequency Behavior

**Every Block (default):**
- Calls are made at every block in the range
- Results stored in per-function parquet files

**Once:**
- For regular contracts: called at the first block in each range
- For factory contracts: called at the block where the address was discovered
- All "once" functions for a contract are combined into a single parquet file
- Useful for immutable data like `name()`, `symbol()`, `decimals()`

**Every N Blocks:**
- Calls are made only when `block_number % N == 0`
- Useful for reducing RPC usage on slowly-changing data

**Duration-based:**
- Calls are made when `current_timestamp >= last_call_timestamp + duration_seconds`
- Uses actual block timestamps, not estimated intervals
- On restart, the last call timestamp is inferred from existing parquet files

### "Once" Storage Format

Functions with `frequency: "once"` are stored differently from regular calls:

**Path:** `data/raw/{chain}/eth_calls/{contract}/once/{start}-{end}.parquet`

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block where the call was made |
| `block_timestamp` | UInt64 | Unix timestamp of the block |
| `address` | FixedSizeBinary(20) | Contract address |
| `{function}_result` | Binary | Raw result for each "once" function |

All "once" functions for a contract/collection are combined into columns in the same file, with one row per unique contract address.

Example for a factory with `name()`, `symbol()`, `decimals()` as "once" calls:
```
data/raw/base/eth_calls/DERC20/once/0-9999.parquet
  - block_number
  - block_timestamp
  - address
  - name_result
  - symbol_result
  - decimals_result
```

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
| `param_0`, `param_1`, ... | Binary | ABI-encoded parameter values (only present if function has parameters) |

### Decoding Values

The `value` column contains raw ABI-encoded bytes from the eth_call response. To decode:

1. Look up the `contract_address` in your config to find the `output_type`
2. Decode the bytes according to the type:
   - `uint*`: 32-byte big-endian unsigned integer (left-padded with zeros)
   - `int*`: 32-byte big-endian signed integer (two's complement)
   - `address`: 32 bytes, last 20 bytes are the address
   - `bool`: 32 bytes, last byte is 0 or 1
   - `bytes32`: 32 bytes, direct value
   - `string`/`bytes`: ABI-encoded with offset + length + data

Example in Python:
```python
import pandas as pd

df = pd.read_parquet("data/raw/base/eth_calls/USDC/totalSupply/0-9999.parquet")

# Decode uint256
df['decoded_value'] = df['value'].apply(
    lambda x: int.from_bytes(x, byteorder='big') if x else None
)

# For parameterized calls, decode param columns too
if 'param_0' in df.columns:
    # param_0 contains ABI-encoded parameter (e.g., address is 32 bytes, last 20 are the address)
    df['param_0_address'] = df['param_0'].apply(
        lambda x: '0x' + x[-20:].hex() if x else None
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
total_calls_per_range = num_contracts × num_functions × num_param_combinations × blocks_per_range
```

For example:
- 10 contracts with 2 functions each over 10,000 blocks = 200,000 eth_calls per parquet file
- 1 contract with `balanceOf(address)` and 100 tracked addresses over 10,000 blocks = 1,000,000 eth_calls

Use `frequency` settings to reduce call volume for slowly-changing data.

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
- Functions with `frequency: "once"` do not support parameters (use parameterless view functions like `name()`, `symbol()`, `decimals()`)
