# Decoding

The decoding module transforms raw blockchain data (logs and eth_call results) into decoded, typed parquet files. It runs as a separate async task that receives data through channels and processes it in parallel with data collection.

## Overview

Decoding works in two phases:

1. **Catchup Phase**: Processes existing raw parquet files that haven't been decoded yet
2. **Live Phase**: Processes new data as it arrives through channels

Decoded data is written to `data/derived/{chain}/decoded/` organized by data type, contract, and event/function name.

## Architecture

```
┌─────────────────────┐     DecoderMessage      ┌──────────────────┐
│  Raw Data Collector │ ──────────────────────► │   Log Decoder    │
│  (logs, eth_calls)  │                         │                  │
└─────────────────────┘                         └────────┬─────────┘
                                                         │
                                                         ▼
                                                data/derived/{chain}/
                                                  decoded/logs/
                                                    {contract}/
                                                      {event}/
                                                        {range}.parquet
```

## Decoder Messages

The `DecoderMessage` enum coordinates communication between collectors and decoders:

| Variant | Description |
|---------|-------------|
| `LogsReady` | Raw logs for a block range are ready for decoding |
| `EthCallsReady` | Regular eth_call results ready for decoding |
| `OnceCallsReady` | One-time eth_call results ready for decoding |
| `FactoryAddresses` | Factory-discovered addresses for a range (needed for factory event matching) |
| `AllComplete` | Shutdown signal when all ranges are complete |

## Log Decoding

### Event Signature Parsing

The `event_parsing` module parses Solidity event signatures to extract:

- Event name
- Parameter types, names, and indexed status
- Canonical signature (types only, for topic0 computation)
- topic0 hash (keccak256 of canonical signature)

**Example (simple params):**
```
Input:  "Transfer(address indexed from, address indexed to, uint256 value)"
Output:
  - name: "Transfer"
  - canonical: "Transfer(address,address,uint256)"
  - topic0: 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
  - params: [
      {name: "from", type: address, indexed: true},
      {name: "to", type: address, indexed: true},
      {name: "value", type: uint256, indexed: false}
    ]
  - flattened_fields: ["from", "to", "value"]
```

### Named Tuple Support

The parser supports named tuples in event signatures, which are common in protocols like Uniswap V4. Named tuples are automatically flattened into individual columns with dot-notation field names.

**Example (named tuples):**
```
Input:  "ModifyLiquidity((address currency0, address currency1, uint24 fee) key, (int24 tickLower, int24 tickUpper) params)"
Output:
  - name: "ModifyLiquidity"
  - canonical: "ModifyLiquidity((address,address,uint24),(int24,int24))"
  - flattened_fields: [
      "key.currency0",
      "key.currency1",
      "key.fee",
      "params.tickLower",
      "params.tickUpper"
    ]
```

**Indexed Tuples:**

When a tuple parameter is marked as `indexed`, the EVM stores only its keccak256 hash in the topic. Since the original values cannot be recovered, indexed tuples are stored as a single `bytes32` column with a `.hash` suffix.

```
Input:  "Swap(address indexed sender, (address currency0, address currency1) indexed poolKey, uint256 amount)"
Output:
  - flattened_fields: [
      "sender",           # address - decoded from topic
      "poolKey.hash",     # bytes32 - keccak256 hash of the tuple
      "amount"            # uint256 - decoded from data
    ]
```

### Log Matching

Logs are matched against configured events using:

1. **Contract address** - Must match one of the configured addresses (for regular contracts) or a factory-discovered address (for factory events)
2. **topic0** - Must match the computed topic0 hash for the event

### Log Decoding Process

1. **Indexed parameters** are decoded from topics (topics[1..n])
   - Simple types (address, uint, int, bool, bytes32) are decoded directly
   - Complex types (arrays, strings, tuples) are stored as their keccak256 hash
   - Indexed tuples are stored with a `.hash` suffix (e.g., `poolKey.hash`)

2. **Non-indexed parameters** are ABI-decoded from the log data field as a tuple
   - Named tuples are recursively flattened into individual columns
   - Field names use dot-notation (e.g., `key.currency0`, `params.tickLower`)

### Output Schema

Decoded log files contain:

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block number |
| `block_timestamp` | UInt64 | Block timestamp |
| `transaction_hash` | FixedSizeBinary(32) | Transaction hash |
| `log_index` | UInt32 | Log index within transaction |
| `contract_address` | FixedSizeBinary(20) | Emitting contract address |
| `{param_name}` | varies | One column per event parameter |
| `{tuple_name}.{field_name}` | varies | Flattened tuple fields (e.g., `key.currency0`) |
| `{tuple_name}.hash` | FixedSizeBinary(32) | Hash of indexed tuple (cannot be decoded) |

### Event Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `signature` | string | Yes | Full Solidity event signature |
| `name` | string | No | Custom name for output directory (defaults to event name from signature) |

The optional `name` field allows you to customize the directory name used for decoded parquet files. This is useful when you want a more descriptive name or need to differentiate events with the same signature from different sources.

### Output Location

```
data/derived/{chain}/decoded/logs/{contract_name}/{event_name}/{start}-{end}.parquet
```

Where `{event_name}` is either the custom `name` from config, or the event name parsed from the signature.

**Example with Uniswap V4:**

For a config like:
```json
{
  "UniswapV4PoolManager": {
    "address": "0x498581ff718922c3f8e6a244956af099b2652b2b",
    "events": [
      { "signature": "Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)" }
    ]
  }
}
```

Output:
```
data/derived/base/decoded/logs/UniswapV4PoolManager/Swap/1000000-1000999.parquet
```

**Example with custom name:**

```json
{
  "UniswapV4PoolManager": {
    "address": "0x498581ff718922c3f8e6a244956af099b2652b2b",
    "events": [
      {
        "signature": "Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)",
        "name": "PoolSwap"
      }
    ]
  }
}
```

Output:
```
data/derived/base/decoded/logs/UniswapV4PoolManager/PoolSwap/1000000-1000999.parquet
```

With columns: `block_number`, `block_timestamp`, `transaction_hash`, `log_index`, `contract_address`, `id`, `sender`, `amount0`, `amount1`, `sqrtPriceX96`, `liquidity`, `tick`, `fee`

**Example with named tuples:**

For a config with named tuple parameters:
```json
{
  "signature": "ModifyLiquidity((address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) key, (int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt) params)"
}
```

Output columns: `block_number`, `block_timestamp`, `transaction_hash`, `log_index`, `contract_address`, `key.currency0`, `key.currency1`, `key.fee`, `key.tickSpacing`, `key.hooks`, `params.tickLower`, `params.tickUpper`, `params.liquidityDelta`, `params.salt`

## Eth Call Decoding

### Regular Calls

Regular eth_calls are made at a configured frequency (every N blocks). Each call result is decoded using the configured `output_type`.

### Once Calls

"Once" calls are made only once per newly discovered address (typically for immutable values like `token0()`, `token1()`, `decimals()`). Multiple once-call functions for the same contract are grouped together.

### Output Schema - Regular Calls

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block number |
| `block_timestamp` | UInt64 | Block timestamp |
| `contract_address` | FixedSizeBinary(20) | Contract called |
| `decoded_value` | varies | Decoded return value |

### Output Schema - Once Calls

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block at which call was made |
| `block_timestamp` | UInt64 | Block timestamp |
| `address` | FixedSizeBinary(20) | Contract address |
| `{function}` | varies | One column per function result (simple types) |
| `{function}.{field}` | varies | One column per tuple field (named tuples) |

**Named Tuple Support for Once Calls:**

When a "once" call returns a named tuple, each field is stored as a separate column using dot-notation:

```json
{
  "function": "slot0()",
  "output_type": "(uint160 sqrtPriceX96, int24 tick, uint8 feeProtocol)",
  "frequency": "once"
}
```

Output columns: `slot0.sqrtPriceX96`, `slot0.tick`, `slot0.feeProtocol`

**Self-Address Parameters:**

"Once" calls support `source: "self"` parameters, which use the contract's own address:

```json
{
  "function": "getAssetData(address)",
  "output_type": "(address numeraire, uint256 amount)",
  "frequency": "once",
  "params": [{"type": "address", "source": "self"}]
}
```

This encodes the call with the target contract address as the parameter, useful for functions that query data about the calling contract itself.

### Output Location

```
# Regular calls
data/derived/{chain}/decoded/eth_calls/{contract_name}/{function_name}/{start}-{end}.parquet

# Once calls
data/derived/{chain}/decoded/eth_calls/{contract_name}/once/{start}-{end}.parquet
```

## Supported Types

### Type Mapping

| Solidity Type | Decoded Representation | Arrow Type |
|---------------|----------------------|------------|
| `address` | `[u8; 20]` | FixedSizeBinary(20) |
| `uint8` | `u8` | UInt8 |
| `uint16`-`uint64` | `u64` | UInt64 |
| `uint80`-`uint256` | `U256` (string) | Utf8 |
| `int8` | `i8` | Int8 |
| `int16`-`int64` | `i64` | Int64 |
| `int128`-`int256` | `I256` (string) | Utf8 |
| `bool` | `bool` | Boolean |
| `bytes32` | `[u8; 32]` | FixedSizeBinary(32) |
| `bytes` | `Vec<u8>` | Binary |
| `string` | `String` | Utf8 |

Large integers (> 64 bits) are stored as decimal strings to avoid precision loss.

## Factory Support

For factory-created contracts, the decoder:

1. Receives `FactoryAddresses` messages with newly discovered addresses
2. Stores addresses per block range
3. When processing logs, matches against factory addresses instead of static addresses
4. Supports factory-specific event and call configurations

## Catchup Behavior

On startup, the decoder:

1. Scans `data/derived/{chain}/decoded/` for existing decoded files
2. Scans `data/raw/{chain}/logs/` and `data/raw/{chain}/eth_calls/` for raw files
3. Skips ranges that are already fully decoded
4. For partially decoded ranges, **only decodes missing events** (incremental catchup)
5. Processes any missing ranges **concurrently** (configurable parallelism)

For factory events during catchup, the decoder loads factory addresses from `data/derived/{chain}/factories/` parquet files.

### Incremental Decoding

When a new event is added to the configuration, the catchup phase efficiently processes only what's needed:

- For each block range, the decoder checks which event types have existing decoded output files
- Only events **without** existing decoded files are processed for that range
- Events that were previously decoded are skipped entirely (no re-reading, re-decoding, or re-writing)

This means adding a new event to the config doesn't require re-processing all historical data for existing events—only the new event is decoded across all ranges.

## Error Handling

- Decoding failures for individual logs/calls are logged at debug level and skipped
- Empty files are written for ranges with no matching events (to mark as processed)
- Channel disconnection triggers graceful shutdown

### Corrupted File Recovery

During the catchup phase, if the log decoder encounters a corrupted raw log file, it automatically handles recovery:

1. **Detection**: When reading a raw log parquet file fails with a parse error
2. **Deletion**: The corrupted file is deleted from `data/raw/{chain}/logs/`
3. **Recollection Request**: A `RecollectRequest` is sent to the receipt collector
4. **Re-fetching**: The receipt collector re-fetches receipts from RPC and sends logs through the normal channel flow
5. **Processing**: The logs collector writes a new parquet file, and the log decoder processes it during the live phase

#### Data Flow for Corrupted File Recovery

```
Log Decoder Catchup                          Receipt Collector
     │                                              │
     │ (attempts to read raw log file)              │
     │                                              │
     ▼                                              │
  Parse error detected                              │
     │                                              │
     ▼                                              │
  Delete corrupted file                             │
  (data/raw/{chain}/logs/logs_X-Y.parquet)          │
     │                                              │
     │───────── RecollectRequest ──────────────────►│
     │          {range_start, range_end}            │
     │                                              ▼
     │                                    Read block info
     │                                    Fetch receipts via RPC
     │                                              │
     │                              ┌───────────────┴───────────────┐
     │                              │                               │
     │                              ▼                               ▼
     │                       Logs Collector               Factory Collector
     │                       (writes new file)            (processes factories)
     │                              │
     │                              │ decoder_tx
     │                              ▼
     │◄─────────── DecoderMessage (LogsReady) ──────
     │              (via normal channel)
     ▼
  Process logs in live phase
  Write decoded parquet
```

This automatic recovery ensures data integrity without manual intervention, even when raw log files become corrupted.

## Performance Considerations

- Decoding runs concurrently with data collection
- **Catchup phase processes multiple files in parallel** for faster historical data processing
- Uses Snappy compression for output parquet files
- Processes data in block ranges matching the configured `parquet_block_range`
- Factory addresses are cached per range to avoid repeated lookups

### Column Index Batch Updates

For "once" call decoding, column indexes (which track decoded function columns per file) are updated in a **batch** after all concurrent decoding tasks complete. This prevents race conditions where multiple tasks might simultaneously read, modify, and overwrite the same index file.

The flow during catchup:
1. All "once" call files are processed concurrently via JoinSet
2. Each task returns its column index info (contract name, file name, decoded columns)
3. After all tasks complete, indexes are grouped by contract and batch-written sequentially
4. A single log message confirms the final index state for each contract

This design ensures data integrity without sacrificing parallelism during the catchup phase.

### Concurrency Configuration

The `decoding_concurrency` setting controls how many files are processed in parallel during the catchup phase:

```json
{
  "raw_data_collection": {
    "decoding_concurrency": 8
  }
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `decoding_concurrency` | integer | 4 | Number of concurrent decoding tasks during catchup |

**Tuning recommendations:**
- Set to the number of CPU cores for CPU-bound workloads
- Consider memory usage when increasing (each task loads a full parquet file)
- Monitor with `htop` to verify CPU utilization during catchup
