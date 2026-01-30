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

**Example:**
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
```

### Log Matching

Logs are matched against configured events using:

1. **Contract address** - Must match one of the configured addresses (for regular contracts) or a factory-discovered address (for factory events)
2. **topic0** - Must match the computed topic0 hash for the event

### Log Decoding Process

1. **Indexed parameters** are decoded from topics (topics[1..n])
   - Simple types (address, uint, int, bool, bytes32) are decoded directly
   - Complex types (arrays, strings, structs) are stored as their keccak256 hash

2. **Non-indexed parameters** are ABI-decoded from the log data field as a tuple

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

### Output Location

```
data/derived/{chain}/decoded/logs/{contract_name}/{event_name}/{start}-{end}.parquet
```

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
| `{function}_decoded` | varies | One column per function result |

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
4. Processes any missing ranges

For factory events during catchup, the decoder loads factory addresses from `data/raw/{chain}/factories/` parquet files.

## Error Handling

- Decoding failures for individual logs/calls are logged at debug level and skipped
- Empty files are written for ranges with no matching events (to mark as processed)
- Channel disconnection triggers graceful shutdown

## Performance Considerations

- Decoding runs concurrently with data collection
- Uses Snappy compression for output parquet files
- Processes data in block ranges matching the configured `parquet_block_range`
- Factory addresses are cached per range to avoid repeated lookups
