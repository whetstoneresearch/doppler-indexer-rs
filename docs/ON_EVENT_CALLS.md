# On-Events ETH Call Collection

This document describes the `on_events` frequency feature for eth_call collection, which allows triggering eth_calls when specific blockchain events are emitted.

## Overview

The `on_events` frequency is an alternative to block-based frequencies (`EveryBlock`, `EveryNBlocks`, `Duration`, `Once`). Instead of making calls at regular intervals, calls are triggered when specific events are emitted from configured contracts.

### Use Cases

- Call `slot0()` on a Uniswap V3 pool whenever a `Swap` event occurs
- Call `balanceOf(address)` on a token whenever a `Transfer` event occurs, using the recipient from the event
- Call `getReserves()` on factory-created pairs whenever they emit a `Swap` event
- Call `slot0()` whenever ANY of `Swap`, `Mint`, or `Burn` events occur (multiple event triggers)

## Configuration

### Basic Example

Call `slot0()` every time a Swap event is emitted:

```json
{
  "V3Pool": {
    "address": "0x...",
    "calls": [
      {
        "function": "slot0()",
        "output_type": "(uint160,int24,uint16,uint16,uint16,uint8,bool)",
        "frequency": {
          "on_events": {
            "source": "V3Pool",
            "event": "Swap(address,address,int256,int256,uint160,uint128,int24)"
          }
        }
      }
    ]
  }
}
```

### With Event Data as Parameters

Use event data (topics or data fields) as call parameters:

```json
{
  "Token": {
    "address": "0x...",
    "calls": [
      {
        "function": "balanceOf(address)",
        "output_type": "uint256",
        "frequency": {
          "on_events": {
            "source": "Token",
            "event": "Transfer(address,address,uint256)"
          }
        },
        "params": [
          {"type": "address", "from_event": "topics[2]"}
        ]
      }
    ]
  }
}
```

### Factory Collection Example

Trigger calls when events are emitted from factory-created contracts:

```json
{
  "UniswapV2Factory": {
    "address": "0x...",
    "factories": [
      {
        "collection_name": "UniswapV2Pairs",
        "factory_events": { ... },
        "calls": [
          {
            "function": "getReserves()",
            "output_type": "(uint112,uint112,uint32)",
            "frequency": {
              "on_events": {
                "source": "UniswapV2Pairs",
                "event": "Swap(address,uint256,uint256,uint256,uint256,address)"
              }
            }
          }
        ]
      }
    ]
  }
}
```

### Multiple Event Triggers

Trigger the same eth_call from multiple different events. This is useful when you want to refresh state on any state-changing event:

```json
{
  "V3Pool": {
    "address": "0x...",
    "calls": [
      {
        "function": "slot0()",
        "output_type": "(uint160 sqrtPriceX96, int24 tick, uint16 observationIndex)",
        "frequency": {
          "on_events": [
            {"source": "V3Pool", "event": "Swap(address,address,int256,int256,uint160,uint128,int24)"},
            {"source": "V3Pool", "event": "Mint(address,int24,int24,uint128,uint256,uint256)"},
            {"source": "V3Pool", "event": "Burn(address,int24,int24,uint128,uint256,uint256)"}
          ]
        }
      }
    ]
  }
}
```

Both single-object and array syntax are supported:
- **Single event (object):** `"on_events": {"source": "...", "event": "..."}`
- **Multiple events (array):** `"on_events": [{"source": "...", "event": "..."}, ...]`

## Configuration Reference

### EventTriggerConfig

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Contract name or factory collection name that emits the event |
| `event` | string | Event signature (e.g., `"Transfer(address,address,uint256)"`) |

### EventTriggerConfigs (on_events value)

The `on_events` field accepts either a single config object or an array of configs:

| Format | Example |
|--------|---------|
| Single | `{"source": "Pool", "event": "Swap(...)"}` |
| Multiple | `[{"source": "Pool", "event": "Swap(...)"}, {"source": "Pool", "event": "Mint(...)"}]` |

When using multiple events, the eth_call is triggered when ANY of the configured events occur.

### Parameter Sources

Parameters can be specified using three different sources:

#### 1. Static Values (existing behavior)
```json
{"type": "address", "values": ["0x..."]}
```

#### 2. From Event Data
```json
{"type": "address", "from_event": "topics[1]"}
{"type": "uint256", "from_event": "data[0]"}
{"type": "address", "from_event": "address"}
```

Event field references:
- `"address"` - The address of the contract that emitted the event (emitter address)
- `"topics[0]"` - Event signature hash (topic0)
- `"topics[1]"`, `"topics[2]"`, `"topics[3]"` - Indexed parameters
- `"data[0]"`, `"data[1]"`, etc. - Non-indexed parameters (32-byte words)

Supported types for `from_event`:
- `address` - Ethereum address (20 bytes, right-aligned in 32-byte word)
- `uint256`, `uint128`, `uint80`, `uint64`, `uint32`, `uint24`, `uint16`, `uint8`
- `int256`, `int128`, `int64`, `int32`, `int24`, `int16`, `int8`
- `bool` - Boolean (last byte of 32-byte word)
- `bytes32` - Fixed 32-byte value

**Note:** Dynamic types (`bytes`, `string`) cannot be extracted from event data directly.

#### 3. Self Address (event emitter)
```json
{"type": "address", "source": "self"}
```

Uses the address of the contract that emitted the event. Useful for factory collections where you want to call back to the contract that emitted the event.

**Note:** Both `{"type": "address", "from_event": "address"}` and `{"type": "address", "source": "self"}` achieve the same result - they use the event emitter's address. The `from_event: "address"` syntax provides consistency with other `from_event` patterns.

## Architecture

### Data Flow

The system supports two modes of operation:

#### Live Mode (New Events)
```
Blocks → Receipts Collector → [Event Trigger Extraction] → Channel → ETH Calls Collector → RPC Calls → Parquet + Decoder
                ↓
           (logs extracted)
```

The ETH Calls Collector:
1. Writes raw results to parquet in `on_events/` subdirectory
2. Sends `DecoderMessage::EventCallsReady` to the decoder for decoded output
3. Writes empty files for configured call pairs with no matching events in the range

#### Catchup Mode (Historical Events)
```
Factory Parquet Files → [Load Addresses] ─────────────────────────────────────────┐
                                                                                  ↓
Existing Log Parquet Files → [Read Logs] → [Event Trigger Extraction] → ETH Calls Collector → RPC Calls → Parquet + Decoder
```

**Catchup ordering:** Factory addresses are loaded from `data/derived/{chain}/factories/` before processing event triggers. This ensures factory collection events are properly filtered.

### Processing Steps

1. **Event Trigger Matchers** are built from contract configurations at startup via `build_event_trigger_matchers()`
2. **Receipts Collector** extracts logs and checks for matching events via `extract_event_triggers()`
3. **EventTriggerMessage** is sent via channel to ETH Calls collector (live mode)
4. **ETH Calls Collector** processes triggers via `process_event_triggers()` or `process_event_triggers_multicall()`:
   - Resolves target address (from config or event emitter for factories)
   - Extracts parameters from event data (topics/data fields) via `build_event_call_params()`
   - Makes RPC calls at the event's block number (batched or multicalled)
   - Writes results to parquet via `write_event_call_results_to_parquet()`
   - Sends `DecoderMessage::EventCallsReady` to decoder
5. **Decoder** processes results via `process_event_calls()`:
   - Decodes raw values using configured `output_type`
   - Writes decoded results to parquet
   - Sends to transformation channel if enabled

### Catchup Mode

On startup, the eth_calls collector runs a catchup phase that:

1. **Loads historical factory addresses** from `data/derived/{chain}/factories/` parquet files (for factory collections only) via `load_historical_factory_addresses()`
2. Scans `data/raw/{chain}/logs/` for existing parquet files
3. For each log file, checks if output already exists in the `on_events/` subdirectory
4. If output doesn't exist:
   - Reads logs from the parquet file
   - Extracts event triggers using configured matchers
   - Filters factory collection triggers by known factory addresses
   - Processes triggers through the same pipeline as live mode
   - Writes empty files for configured pairs with no matching events
5. Skips ranges where output already exists to avoid duplicate processing

This ensures that historical data is processed even if the indexer was started after events occurred.

**Important:** For factory collections, historical factory addresses are loaded before processing any event triggers. This ensures that only events from known factory-created contracts are processed, preventing incorrect calls to unrelated addresses.

## Output Format

### Raw File Location
```
data/raw/{chain}/eth_calls/{contract}/{function}/on_events/{start_block}-{end_block}.parquet
```

### Decoded File Location
```
data/derived/{chain}/decoded/eth_calls/{contract}/{function}/on_events/{start_block}-{end_block}.parquet
```

### Raw Schema

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | uint64 | Block number where the triggering event occurred |
| `block_timestamp` | uint64 | Block timestamp |
| `log_index` | uint32 | Index of the triggering log within the block (for correlation) |
| `target_address` | bytes(20) | Address the eth_call was made to |
| `value` | binary | Raw ABI-encoded return value |
| `param_0`, `param_1`, ... | binary | ABI-encoded parameter values (if any, dynamically added) |

### Decoded Schema

The decoded schema includes the same base columns plus decoded value columns based on the `output_type` configuration. For named tuples, each field becomes a separate column. For unnamed tuples, fields are named `0`, `1`, etc.

### Decoder Integration

Event-triggered call results are sent to the decoder via `DecoderMessage::EventCallsReady`:

```rust
DecoderMessage::EventCallsReady {
    range_start: u64,
    range_end: u64,
    contract_name: String,
    function_name: String,
    results: Vec<EventCallResult>,
}
```

Where `EventCallResult` (from `src/raw_data/decoding/types.rs`) contains:
- `block_number: u64`
- `block_timestamp: u64`
- `log_index: u32`
- `target_address: [u8; 20]`
- `value: Vec<u8>` (raw bytes for decoding)

## Key Types

### Configuration Types (`src/types/config/eth_call.rs`)

```rust
/// Configuration for event-triggered eth_calls
pub struct EventTriggerConfig {
    pub source: String,      // Contract/collection name that emits the trigger event
    pub event: String,       // Event signature (e.g., "Transfer(address,address,uint256)")
}

/// Wrapper supporting single or multiple event triggers
pub enum EventTriggerConfigs {
    Single(EventTriggerConfig),
    Multiple(Vec<EventTriggerConfig>),
}

impl EventTriggerConfigs {
    pub fn iter(&self) -> impl Iterator<Item = &EventTriggerConfig>;
    pub fn configs(&self) -> Vec<&EventTriggerConfig>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

/// Parameter configuration for eth_calls
pub enum ParamConfig {
    /// Static values: {"type": "address", "values": ["0x..."]}
    Static { param_type: EvmType, values: Vec<ParamValue> },
    /// Bind from event data: {"type": "address", "from_event": "topics[1]"}
    FromEvent { param_type: EvmType, from_event: String },  // "address", "topics[N]", "data[N]"
    /// Self address (event emitter): {"type": "address", "source": "self"}
    SelfAddress { param_type: EvmType, source: String },
}

/// Frequency enum with OnEvents variant
pub enum Frequency {
    EveryBlock,
    Once,
    EveryNBlocks(u64),
    Duration(u64),
    OnEvents(EventTriggerConfigs),  // Call when specific events are emitted
}
```

### Runtime Types (`src/raw_data/historical/receipts.rs`)

```rust
/// Data from an event that triggered an eth_call
pub struct EventTriggerData {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub emitter_address: [u8; 20],  // Address that emitted the event
    pub source_name: String,        // Matched source name (contract or factory collection)
    pub event_signature: [u8; 32],  // Event signature hash (topic[0])
    pub topics: Vec<[u8; 32]>,      // All topics including topic0
    pub data: Vec<u8>,              // ABI-encoded event data
}

/// Message for event-triggered eth_calls channel
pub enum EventTriggerMessage {
    Triggers(Vec<EventTriggerData>),  // Batch of event triggers from a range
    RangeComplete { range_start: u64, range_end: u64 },
    AllComplete,
}

/// Matcher for detecting events that should trigger eth_calls
pub struct EventTriggerMatcher {
    pub source_name: String,            // Source name (contract or factory collection name)
    pub addresses: HashSet<[u8; 20]>,   // Addresses to match (empty for factory collections)
    pub is_factory: bool,               // Whether this is a factory collection
    pub event_topic0: [u8; 32],         // Event signature hash
}
```

### Processing Types (`src/raw_data/historical/eth_calls.rs`)

```rust
/// Configuration for an event-triggered call
pub(crate) struct EventTriggeredCallConfig {
    pub contract_name: String,
    pub target_address: Option<Address>,  // None for factory collections (use event emitter)
    pub function_name: String,
    pub function_selector: [u8; 4],
    pub params: Vec<ParamConfig>,
    pub is_factory: bool,
}

/// Result from an event-triggered call (internal processing type)
pub(crate) struct EventCallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub value_bytes: Vec<u8>,
    pub param_values: Vec<Vec<u8>>,
}

/// Key for grouping event-triggered calls: (source_name, event_signature_hash)
pub(crate) type EventCallKey = (String, [u8; 32]);
```

### Decoder Types (`src/raw_data/decoding/types.rs`)

```rust
/// Event-triggered eth_call result data for decoding
pub struct EventCallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub value: Vec<u8>,
}
```

### Decoder Configuration (`src/raw_data/decoding/eth_calls.rs`)

```rust
/// Configuration for an event-triggered call to decode
pub(crate) struct EventCallDecodeConfig {
    pub contract_name: String,
    pub function_name: String,
    pub output_type: EvmType,
}

/// Decoded event-triggered call result
pub(crate) struct DecodedEventCallRecord {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub decoded_value: DecodedValue,
}
```

### Transformation Integration

Decoded event-triggered calls are sent to the transformation layer via `DecodedCallsMessage`. The transformation system receives:

```rust
pub struct DecodedCallsMessage {
    pub range_start: u64,
    pub range_end: u64,
    pub source_name: String,      // Contract or factory collection name
    pub function_name: String,    // Function that was called
    pub calls: Vec<DecodedCall>,  // Decoded results with trigger_log_index set
}
```

Event-triggered calls include `trigger_log_index: Some(log_index)` to correlate with the triggering event. Handlers can use this to match calls to their triggering events within the same block.

## Files Modified

| File | Changes |
|------|---------|
| `src/types/config/eth_call.rs` | Added `OnEvents` frequency variant, `EventTriggerConfig`, `EventTriggerConfigs` (single/multiple support), extended `ParamConfig` with `FromEvent` and `SelfAddress` variants; added `Int24`, `Int16`, `Uint24` types; `CallTarget` for target address overrides |
| `src/raw_data/historical/receipts.rs` | Added `EventTriggerData`, `EventTriggerMessage`, `EventTriggerMatcher` types; `build_event_trigger_matchers()`, `extract_event_triggers()` functions with deduplication; channel sending; `send_range_complete()` |
| `src/raw_data/historical/eth_calls.rs` | Added event processing: `build_event_triggered_call_configs()`, `extract_param_from_event()`, `extract_value_from_32_bytes()`, `build_event_call_params()`, `process_event_triggers()`, `process_event_triggers_multicall()`, `write_event_call_results_to_parquet()`, `write_empty_event_call_file()`, `build_event_call_schema()`; catchup: `load_historical_factory_addresses()`, `read_factory_addresses_from_parquet()` |
| `src/raw_data/decoding/types.rs` | Added `EventCallResult` type and `EventCallsReady` variant to `DecoderMessage` |
| `src/raw_data/decoding/eth_calls.rs` | Added `EventCallDecodeConfig`, `DecodedEventCallRecord` types; `process_event_calls()`, `build_decode_configs()` includes event configs; support for all int/uint types in decoding |
| `src/raw_data/decoding/mod.rs` | Exported `EventCallResult` |
| `src/main.rs` | Added channel creation, event matchers building, and wiring |

## Design Decisions

### 1. Trigger Extraction in Receipts Collector

Event triggers are extracted in the receipts collector (where logs are already available) rather than in a separate step. This avoids re-reading log data during live processing.

### 2. Factory Address Filtering

For factory collections, receipts sends all logs matching the event signature. The eth_calls collector filters by known factory addresses. This avoids complex bidirectional communication between collectors.

**Race Condition Handling:** During live processing, event triggers may arrive before factory addresses are discovered (since factory discovery and event processing happen concurrently). To handle this safely:
- If no factory addresses are known for a collection, event triggers are skipped (not processed)
- Skipped triggers will be processed during the next catchup phase when addresses are known
- This prevents making eth_calls to random addresses that aren't actually factory-created contracts

### 3. No Call Chaining

Call chaining (output of call A → input of call B) is explicitly out of scope. This keeps the configuration simple and type-safe. Complex call dependencies should be handled in downstream event handlers in the transformation layer, which provides:
- Full Rust type safety
- IDE support
- Ability to use arbitrary logic
- Conditional calls and error handling

### 4. One Call Per Event

Each matching event triggers a separate eth_call. This ensures data completeness for scenarios where the same call with different parameters might be needed.

**Deduplication:** Duplicate triggers are prevented at the extraction level - if the same log (identified by block number, log index, and source name) would match multiple matchers with the same event signature, only one trigger is created. This prevents redundant calls while still allowing different call configurations to trigger from the same event.

### 5. Separate Output Directory

Event-triggered call results are written to an `on_events/` subdirectory to avoid conflicts with block-based frequency outputs.

### 6. Catchup Mode Idempotency

The catchup mode checks for existing output files before processing. This ensures that:
- Restarting the indexer doesn't cause duplicate processing
- Partially completed runs can be resumed
- New event-triggered call configurations are automatically backfilled

### 7. Empty File Writing

When processing a block range with no matching events for a configured (contract_name, function_name) pair, an empty parquet file is written. This prevents the catchup system from repeatedly retrying ranges that have no relevant events.

### 8. Multicall3 Support

When Multicall3 is configured, event-triggered calls are batched using the `aggregate3` function. This significantly reduces RPC requests by combining multiple eth_calls to the same block into a single multicall. The system groups calls by block number and executes them in batches.

### 9. Target Address Overrides

The `target` field in call configuration allows routing event-triggered calls to a different contract than the one emitting events. This is useful for oracles or shared state contracts. Target can be specified as a hex address or a contract name to look up.

## Known Limitations

1. **Dynamic types not supported in `from_event`:** The `bytes` and `string` types cannot be extracted from event data directly because they require complex ABI decoding with offset handling.

2. **Factory collection race window:** During live processing, there may be a brief window where events from newly-created factory contracts are skipped because the factory address hasn't been discovered yet. These events will be processed during the next catchup phase.

3. **Failed calls stored as empty:** If an eth_call fails (e.g., contract reverted), an empty result is stored with empty `value_bytes`. Failed calls during catchup are not automatically retried on subsequent runs. Decode failures are logged with warnings.

4. **No cross-event parameter passing:** Parameters can only be extracted from the triggering event itself, not from other events in the same block or transaction.

5. **Tuple types not supported for `from_event` parameters:** `NamedTuple`, `UnnamedTuple`, and `Array` types cannot be used as parameter types when extracting from event data.

## Testing

Run all tests:
```bash
cargo test
```

Run config parsing tests specifically:
```bash
cargo test types::config::eth_call
```

Key tests (44 total):
- `test_frequency_deserialize_on_events` - Deserialize single `on_events` frequency
- `test_frequency_deserialize_on_events_array` - Deserialize array of events
- `test_frequency_deserialize_on_events_single_still_works` - Backward compatibility
- `test_frequency_deserialize_on_events_empty_array_fails` - Empty array validation
- `test_eth_call_config_with_multiple_events` - Full config with multiple event triggers
- `test_event_trigger_configs_iter` - Iterator over single and multiple configs
- `test_event_trigger_configs_len_and_is_empty` - Helper method tests
- `test_param_config_from_event` - Parse `from_event` parameter binding
- `test_param_config_from_event_data` - Parse `from_event` with data fields
- `test_param_config_from_event_address` - Parse `from_event: "address"` (emitter address)
- `test_param_config_self_address` - Parse `source: "self"` parameter
- `test_eth_call_config_with_on_events` - Full config with on_events frequency
- `test_eth_call_config_with_from_event_param` - Config with event-bound parameters
- `test_frequency_on_events_helpers` - Helper method tests for OnEvents variant
