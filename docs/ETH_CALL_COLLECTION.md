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
| `target` | string | No | *(contract address)* | Override the target address for this call (see Target Override section below). |

### Supported Types

The `output_type` field (and parameter types) support the following EVM types:

| Type | Description |
|------|-------------|
| `uint8`, `uint16`, `uint24`, `uint32`, `uint64`, `uint80`, `uint96`, `uint128`, `uint160`, `uint256` | Unsigned integers of various sizes |
| `int8`, `int16`, `int24`, `int32`, `int64`, `int128`, `int256` | Signed integers of various sizes |
| `address` | 20-byte Ethereum address |
| `bool` | Boolean value |
| `bytes32` | Fixed 32-byte value |
| `bytes` | Dynamic byte array |
| `string` | Dynamic string |

### Named Output Types

Output types can include names to create descriptive column names in the decoded parquet output:

**Named Single Value:**
```json
{
  "function": "latestAnswer()",
  "output_type": "int256 latestAnswer"
}
```
This creates a column named `latestAnswer` instead of the default `decoded_value`.

**Named Tuple (for functions returning multiple values):**
```json
{
  "function": "slot0()",
  "output_type": "(uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)"
}
```
This creates separate columns: `sqrtPriceX96`, `tick`, `observationIndex`, `observationCardinality`, `observationCardinalityNext`, `feeProtocol`, `unlocked`.

The format is `type name` for each field, with tuples enclosed in parentheses and fields separated by commas.

**Named tuples work with all frequency settings**, including `"once"`. For once calls, the decoded column naming pattern is `{function}.{field_name}` (e.g., `slot0.sqrtPriceX96`).

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

### Self-Address Parameters

For `frequency: "once"` calls, you can use `source: "self"` to pass the contract's own address as a parameter. This is useful when calling functions that need to know which contract to query:

```json
{
  "function": "getAssetData(address)",
  "output_type": "(address numeraire, uint256 amount)",
  "frequency": "once",
  "params": [
    {"type": "address", "source": "self"}
  ]
}
```

The `source: "self"` parameter will be replaced with the contract address being called. This works for both regular contracts and factory-discovered contracts.

**Note:** Static parameter values (via `values`) are also supported for "once" calls, but `from_event` parameters are not supported since "once" calls are not triggered by events.

## Target Override

By default, eth_calls are made to the contract they are configured under. The `target` field allows you to override this, directing the call to a different address instead.

### Usage

The `target` field accepts either:

- **A hex address** — call this address directly
- **A contract name** — look up the address from the chain's contract configuration

```json
{
  "MyProtocol": {
    "address": "0xAAA...",
    "calls": [
      {
        "function": "latestAnswer()",
        "output_type": "int256",
        "target": "0xBBB..."
      },
      {
        "function": "getPrice()",
        "output_type": "uint256",
        "target": "ChainlinkEthOracle"
      }
    ]
  }
}
```

In this example:
- `latestAnswer()` is called on `0xBBB...` instead of `0xAAA...`
- `getPrice()` is called on whatever address is configured for `ChainlinkEthOracle` in the same chain's contracts

### Name Resolution

When `target` is a contract name, the address is resolved from the chain's contracts configuration at startup. If the contract has multiple addresses, all of them are used. If the name cannot be found, a warning is logged and the call is skipped.

### Compatibility

The `target` field works with all frequency settings (`"once"`, every-N-blocks, duration-based, and `on_events`) and with both regular contracts and factory collections.

**Factory calls with target:**
- For factory event-triggered calls, the resolved target overrides the default behavior of using the event emitter address
- For factory `"once"` calls, the target address is called once per discovered factory instance (at the block where each instance was discovered). The `source: "self"` parameter still refers to the discovered factory address, allowing you to query a central contract about each discovered instance.

**Example:** Call a registry contract to get metadata about each discovered factory instance:
```json
{
    "Airlock": {
        "address": "0x...",
        "factories": [{
            "collection_name": "DERC20",
            "factory_events": { ... },
            "calls": [{
                "function": "getAssetData(address)",
                "output_type": "(address numeraire, uint256 supply)",
                "frequency": "once",
                "target": "Airlock",
                "params": [{"type": "address", "source": "self"}]
            }]
        }]
    }
}
```
This calls `Airlock.getAssetData(factoryAddress)` for each discovered DERC20, storing results keyed by the factory instance.

Results are still written under the original contract/collection name in the output directory, regardless of which address was actually called.

## Frequency

The `frequency` field controls how often eth_calls are made. This is useful for optimizing RPC usage when certain data doesn't need to be fetched every block.

### Frequency Options

| Value | Behavior |
|-------|----------|
| *(omitted)* | Call every block (default) |
| `"once"` | Call once per contract address |
| `100` (any positive integer) | Call every N blocks |
| `"5m"`, `"1h"`, `"1d"` | Call at time intervals |
| `{"on_events": {...}}` | Call when specific events are emitted (see [On-Events ETH Calls](./ON_EVENT_CALLS.md)) |
| `{"on_events": [...]}` | Call when ANY of multiple events are emitted |

### Duration Format

Duration strings use a number followed by a unit suffix:

| Suffix | Unit |
|--------|------|
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days |

Examples: `"30s"`, `"5m"`, `"1h"`, `"7d"`

### Event-Triggered Calls

Use `on_events` to trigger eth_calls when specific blockchain events are emitted. This supports both single and multiple event triggers:

**Single event:**
```json
{
  "frequency": {
    "on_events": {"source": "V3Pool", "event": "Swap(address,address,int256,int256,uint160,uint128,int24)"}
  }
}
```

**Multiple events (call on ANY of these events):**
```json
{
  "frequency": {
    "on_events": [
      {"source": "V3Pool", "event": "Swap(address,address,int256,int256,uint160,uint128,int24)"},
      {"source": "V3Pool", "event": "Mint(address,int24,int24,uint128,uint256,uint256)"},
      {"source": "V3Pool", "event": "Burn(address,int24,int24,uint128,uint256,uint256)"}
    ]
  }
}
```

Event-triggered calls support extracting parameters from event data:
- `"topics[N]"` - Indexed event parameters
- `"data[N]"` - Non-indexed event parameters (32-byte words)
- `"address"` - The event emitter's address

**Processing Flow:**

1. During **live collection**, the receipts collector extracts event triggers from logs and sends them via channel to the eth_calls collector
2. During **catchup**, the eth_calls collector reads logs from existing parquet files and extracts triggers directly
3. For factory collections, only events from known factory addresses are processed (unknown addresses are skipped and caught up later)
4. Empty parquet files are written for configured call pairs that had no matching events in a range (prevents re-processing during catchup)

For full documentation, see [On-Events ETH Calls](./ON_EVENT_CALLS.md).

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

### Column Index Sidecar

Each `once/` directory contains a `column_index.json` sidecar file that tracks which function result columns exist in each parquet file:

**Path:** `data/raw/{chain}/eth_calls/{contract}/once/column_index.json`

**Format:**
```json
{
  "0-9999.parquet": ["name", "symbol", "decimals"],
  "10000-19999.parquet": ["name", "symbol", "decimals"]
}
```

This index enables **incremental column addition**: when you add a new `frequency: "once"` call to an existing contract configuration, the collector detects which parquet files are missing the new column and:

1. Executes only the newly added call(s)
2. Reads the existing parquet file
3. Merges the new result column(s) into the existing data
4. Rewrites the parquet file with all columns
5. Updates the column index

This avoids re-executing calls that were already collected, significantly reducing RPC usage when adding new "once" calls to an existing configuration.

## Data Flow

The eth_call collector operates in two phases: **catchup** (processing historical data) and **live** (processing new blocks).

### Catchup Phase

```
Existing block parquet files ──> eth_calls collector ──> parquet files
Existing log parquet files ──> [Event Trigger Extraction] ──> event-triggered eth_calls ──> parquet files
Factory parquet files ──> [Load Addresses] ──> factory eth_calls
```

1. **Regular calls catchup**: Scans `data/raw/{chain}/blocks/` for existing block ranges, checks if eth_call parquet files exist for all configured contract/function pairs, and re-processes missing ranges
2. **Once calls catchup**: Column-aware - checks `column_index.json` to detect newly added "once" functions and merges new columns into existing parquet files
3. **Factory once calls catchup**: Waits for factory catchup to complete, then processes "once" calls for factory-created contracts
4. **Event-triggered calls catchup**: Reads from existing log parquet files in `data/raw/{chain}/logs/`, extracts event triggers, and processes them through the same pipeline as live mode

### Live Phase

```
Blocks → Receipts Collector → [Event Trigger Extraction] → Channel → ETH Calls Collector → RPC Calls → Parquet + Decoder
                ↓
           (logs extracted)

Blocks → (block_number, timestamp) → ETH Calls Collector → RPC Calls → Parquet
```

1. The blocks collector sends `(block_number, timestamp)` to the eth_calls channel after each RPC batch
2. The eth_calls collector accumulates blocks until a complete parquet range is ready
3. For each block in the range, it builds `eth_call` requests for all configured contracts/functions
4. Calls are executed in batches (controlled by `rpc_batch_size` config)
5. Results are written to parquet, grouped by contract and function
6. Event triggers arrive via a separate channel from the receipts collector and are processed independently

## Output Format

### File Naming

Files are written to `data/raw/{chain}/eth_calls/{contract_name}/{function_name}/` with the naming convention:

```
{start_block}-{end_block}.parquet
```

Example: `data/raw/base/eth_calls/USDC/totalSupply/0-9999.parquet`

### Event-Triggered Calls Output

Event-triggered call results are written to a separate `on_events/` subdirectory:

```
data/raw/{chain}/eth_calls/{contract_name}/{function_name}/on_events/{start_block}-{end_block}.parquet
```

Example: `data/raw/base/eth_calls/V3Pool/slot0/on_events/0-9999.parquet`

**Empty files:** When no events match the configured triggers for a range, an empty parquet file is written. This prevents the catchup phase from re-processing the range on subsequent runs.

### Parquet Schema

**Regular/Factory/Token Calls:**

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | The block height at which the call was made |
| `block_timestamp` | UInt64 | Unix timestamp of the block |
| `contract_address` | FixedSizeBinary(20) | The contract address called |
| `value` | Binary | Raw bytes returned by eth_call |
| `param_0`, `param_1`, ... | Binary | ABI-encoded parameter values (only present if function has parameters) |

**Event-Triggered Calls:**

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block number where the triggering event occurred |
| `block_timestamp` | UInt64 | Block timestamp |
| `log_index` | UInt32 | Index of the triggering log within the block (for correlation with event data) |
| `target_address` | FixedSizeBinary(20) | Address the eth_call was made to |
| `value` | Binary | Raw ABI-encoded return value |
| `param_0`, `param_1`, ... | Binary | ABI-encoded parameter values (if any) |

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

### Multicall3 Optimization

When a `Multicall3` contract is configured, the eth_call collector automatically batches all calls for the same block into a single `aggregate3` RPC call. This significantly reduces the number of RPC requests and improves throughput.

**Configuration:**

Add the Multicall3 contract to your contracts configuration:

```json
{
  "Multicall3": {
    "address": "0xcA11bde05977b3631167028862bE2a173976CA11"
  }
}
```

The Multicall3 address is the same on most EVM chains. See [multicall3.eth](https://www.multicall3.com/) for deployment addresses.

**How it works:**

1. When Multicall3 is configured, all eth_calls targeting the same block are grouped together
2. Instead of N individual `eth_call` RPCs, a single `aggregate3` call is made to the Multicall3 contract
3. The Multicall3 contract executes all sub-calls and returns results in a single response
4. Results are unpacked and distributed back to their respective output files

**Supported call types:**
- Regular calls (`process_range_multicall`)
- Factory calls (`process_factory_range_multicall`)
- Event-triggered calls (`process_event_triggers_multicall`)
- Token pool calls (`process_token_range_multicall`)
- Once calls (`process_once_calls_multicall`)
- Factory once calls (`process_factory_once_calls_multicall`)

**Error handling:**
- Uses `allowFailure=true` so individual call failures don't abort the entire batch
- Failed sub-calls return empty bytes (same as direct call failures)
- If the multicall RPC itself fails, all sub-calls for that block are treated as failed

**Fallback behavior:**

When Multicall3 is not configured, the collector falls back to making individual `eth_call` RPCs. The output format is identical in both modes — downstream consumers don't need to know which method was used.

**Performance example:**

For 100 contracts × 10 functions × 1000 blocks:
- Without multicall: 1,000,000 individual RPC calls
- With multicall: 1,000 aggregate3 calls (one per block)

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

- `rpc_batch_size`: Number of eth_calls per RPC batch (default: 100). When using Multicall3, this controls how many multicalls are batched into a single RPC request.
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

## Token Pool Calls

The eth_call collector also supports making calls on token pool contracts. This is configured in the token configuration file (`config/tokens/{chain}.json`) rather than the contracts configuration.

### Configuration

Token pool calls are defined within the `pool` configuration of a token:

```json
{
  "Fxh": {
    "address": "0x5fc2843838e65eb0b5d33654628f446d54602791",
    "pool": {
      "type": "v3",
      "address": "0xC3e7433ae4d929092F8dFf62F7E2f15f23bC3E63",
      "quote_token": "Weth",
      "calls": [
        {
          "function": "slot0()",
          "output_type": "(uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)"
        }
      ]
    }
  }
}
```

### Pool Types and Target Addresses

The target address for eth_calls depends on the pool type:

| Pool Type | Target Address | Notes |
|-----------|----------------|-------|
| `v2` | Pool address (from `pool.address`) | Direct call to the pool contract |
| `v3` | Pool address (from `pool.address`) | Direct call to the pool contract |
| `v4` | `UniswapV4StateView` contract | Pool ID (bytes32) passed as first parameter |

For v4 pools, the `pool.address` field should contain the pool ID (bytes32), and the system automatically looks up the `UniswapV4StateView` contract address from the contracts configuration and passes the pool ID as a parameter.

**V4 Pool Example:**
```json
{
  "Eurc": {
    "address": "0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42",
    "pool": {
      "type": "v4",
      "address": "0xb18fad93e3c5a5f932d901f0c22c5639a832d6f29a4392fff3393fb734dd0720",
      "quote_token": "Usdc",
      "calls": [
        {
          "function": "getSlot0(bytes32)",
          "output_type": "(uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)"
        }
      ]
    }
  }
}
```

### Output Files

Token pool eth_call results are written to:

```
data/raw/{chain}/eth_calls/{token_name}_pool/{function_name}/{start}-{end}.parquet
```

Example: `data/raw/base/eth_calls/Fxh_pool/slot0/0-9999.parquet`

### Decoded Output Schema

When using named tuple output types, the decoded parquet files will have named columns:

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block height |
| `block_timestamp` | UInt64 | Unix timestamp |
| `contract_address` | FixedSizeBinary(20) | Target contract address |
| `sqrtPriceX96` | Utf8 | First tuple field (stored as string for large integers) |
| `tick` | Int32 | Second tuple field |
| `observationIndex` | UInt32 | Third tuple field |
| ... | ... | Additional fields from the tuple |

## Resumability

Collection is fully resumable with catchup logic for all eth_call types. The catchup phase runs at startup before live collection begins.

### Catchup Ordering

The catchup phases are executed in a specific order to ensure dependencies are satisfied:

1. **Regular, token, and once calls** - Process independently using existing block parquet files
2. **Factory catchup wait** - Wait for factory collector to complete its catchup (required for factory address discovery)
3. **Factory once calls** - Process "once" calls for factory-created contracts using discovered addresses
4. **Event-triggered calls** - Load historical factory addresses, then process event triggers from log parquet files

### Catchup Phase (Regular Calls)

On startup, the eth_call collector performs a catchup phase for regular (non-factory) calls:

1. **Scans existing block files** - Reads `data/raw/{chain}/blocks/` to find all available block ranges
2. **Checks existing eth_call files** - For each block range, checks if eth_call parquet files exist for all configured contract/function pairs
3. **Re-processes missing ranges** - If any eth_call files are missing, reads block info from the existing block parquet file and executes the calls

### "Once" Call Catchup

For `frequency: "once"` calls, the catchup logic is column-aware:

1. **Checks the column index** - Reads `column_index.json` from each `once/` directory
2. **Detects missing columns** - Compares configured function names against the index to find newly added calls
3. **Executes only missing calls** - If the parquet file exists but is missing columns for new functions, only those new calls are executed
4. **Merges columns** - New result columns are merged into existing parquet files by matching on the `address` column
5. **Updates the index** - The column index is updated after each write

This means you can add new `frequency: "once"` calls to an existing configuration without re-running all historical calls — only the new calls are executed and merged into existing files.

### Factory Calls

Factory eth_calls are processed in two ways:

**During catchup:**
- Factory "once" calls wait for factory catchup to complete before processing
- Factory addresses are loaded from existing factory parquet files in `data/derived/{chain}/factories/`

**During live collection:**
- Factory addresses arrive via channel from the factory collector
- Factory calls are processed when both block data and factory addresses are available for a range
- Factory addresses are tracked in memory for event trigger filtering

### Event-Triggered Calls Catchup

For `on_events` frequency calls, the catchup phase:

1. **Loads historical factory addresses** - Reads from `data/derived/{chain}/factories/` parquet files to know which addresses are valid factory-created contracts
2. **Scans log parquet files** - Reads `data/raw/{chain}/logs/` to find existing log ranges
3. **Checks for existing output** - For each log range, checks if output already exists in `{contract}/{function}/on_events/` subdirectory
4. **Extracts event triggers** - Reads logs from parquet and extracts matching event triggers using configured matchers
5. **Filters factory events** - For factory collections, only processes events from known factory addresses
6. **Writes empty files** - For configured call pairs with no matching events, writes empty parquet files to prevent re-processing

**Important:** Historical factory addresses are loaded before processing any event triggers. This ensures proper filtering of factory collection events and prevents calling random addresses.

### Manual Re-collection

**Regular calls:** Delete the corresponding file from `data/raw/{chain}/eth_calls/{contract}/{function}/`.

**Once calls:** Remove the function name from the `column_index.json` for that file, or delete the entire parquet file to recollect all columns.

**Event-triggered calls:** Delete the corresponding file from `data/raw/{chain}/eth_calls/{contract}/{function}/on_events/`.

### Column Merging

When new "once" columns are added to the configuration:

1. The collector detects missing columns by comparing configured functions against the column index
2. Only the missing columns are collected via RPC calls
3. New columns are merged into existing parquet files
4. If a column name already exists in the parquet (but was removed from the index), it is **replaced** with fresh data
5. The column index is updated to reflect the actual parquet schema after the merge

For "once" calls, you can either:
- Delete the parquet file to re-collect all "once" calls for that range
- Edit `column_index.json` to remove specific function names, then restart — only those functions will be re-collected and merged

## Column Index Tracking

For `frequency: "once"` calls, the collector maintains a `column_index.json` sidecar file in each `once/` directory. This tracks which function result columns exist in each parquet file.

### Index File Location

```
data/raw/{chain}/eth_calls/{contract_or_collection}/once/column_index.json
```

### Index Format

```json
{
  "0-9999.parquet": ["name", "symbol", "decimals"],
  "10000-19999.parquet": ["name", "symbol", "decimals", "getAssetData"]
}
```

### Index Behavior

**On startup (catchup phase):**
- If `column_index.json` exists, it is loaded
- If not, the collector scans all parquet files in the directory and builds the index from their schemas
- The rebuilt index is saved to disk

**During collection:**
- When a file is written or merged, the index is updated based on the actual columns in the parquet file (not the configured functions)
- This ensures the index always reflects reality, even if some calls failed

**For incremental collection:**
- The index determines which columns are missing from each file
- Missing columns are collected and merged into existing files
- Existing columns with the same name are replaced (not duplicated)

**Raw vs Decoded Index Updates:**
- **Raw data collection** processes "once" calls sequentially, so indexes are updated immediately after each file is written
- **Decoded data catchup** processes files in parallel via JoinSet, so decoded column indexes are **batch-updated** after all concurrent tasks complete to avoid race conditions (see [Decoding](./DECODING.md#column-index-batch-updates))

### Recollecting a Column

To force recollection of a specific column (e.g., after fixing a bug):

1. Remove the column from `column_index.json` for the affected files
2. Re-run the collector - it will detect the "missing" column and recollect it
3. The new data will replace the old column in the parquet file

Alternatively, use the helper script:
```bash
python scripts/remove_parquet_column.py data/raw/base/eth_calls/DERC20/once getAssetData_result
```

## Limitations

- No state override support (calls use the actual on-chain state)
- Results are stored as raw bytes; decoding is left to the consumer
- Factory calls require the factory collector to discover addresses first
- Functions with `frequency: "once"` support `source: "self"` and static `values` parameters, but not `from_event` parameters
- Event-triggered calls for factory collections may skip events during live processing if factory addresses haven't been discovered yet (these are caught up later)
- Dynamic types (`bytes`, `string`) cannot be extracted from event data directly for `from_event` parameters
