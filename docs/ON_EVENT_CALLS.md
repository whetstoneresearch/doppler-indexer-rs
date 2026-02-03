# On-Events ETH Call Collection

This document describes the `on_events` frequency feature for eth_call collection, which allows triggering eth_calls when specific blockchain events are emitted.

## Overview

The `on_events` frequency is an alternative to block-based frequencies (`EveryBlock`, `EveryNBlocks`, `Duration`, `Once`). Instead of making calls at regular intervals, calls are triggered when specific events are emitted from configured contracts.

### Use Cases

- Call `slot0()` on a Uniswap V3 pool whenever a `Swap` event occurs
- Call `balanceOf(address)` on a token whenever a `Transfer` event occurs, using the recipient from the event
- Call `getReserves()` on factory-created pairs whenever they emit a `Swap` event

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

## Configuration Reference

### EventTriggerConfig

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Contract name or factory collection name that emits the event |
| `event` | string | Event signature (e.g., `"Transfer(address,address,uint256)"`) |

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
```

Event field references:
- `topics[0]` - Event signature hash (topic0)
- `topics[1]`, `topics[2]`, `topics[3]` - Indexed parameters
- `data[0]`, `data[1]`, etc. - Non-indexed parameters (32-byte words)

#### 3. Self Address (event emitter)
```json
{"type": "address", "source": "self"}
```

Uses the address of the contract that emitted the event. Useful for factory collections where you want to call back to the contract that emitted the event.

## Architecture

### Data Flow

The system supports two modes of operation:

#### Live Mode (New Events)
```
Blocks → Receipts Collector → [Event Trigger Extraction] → Channel → ETH Calls Collector → RPC Calls → Parquet + Decoder
                ↓
           (logs extracted)
```

#### Catchup Mode (Historical Events)
```
Existing Log Parquet Files → [Read Logs] → [Event Trigger Extraction] → ETH Calls → RPC Calls → Parquet + Decoder
```

### Processing Steps

1. **Event Trigger Matchers** are built from contract configurations at startup
2. **Receipts Collector** extracts logs and checks for matching events
3. **EventTriggerMessage** is sent via channel to ETH Calls collector (live mode)
4. **ETH Calls Collector** processes triggers:
   - Resolves target address (from config or event emitter for factories)
   - Extracts parameters from event data (topics/data fields)
   - Makes RPC calls at the event's block number
   - Writes results to parquet
   - Sends results to decoder for decoded output

### Catchup Mode

On startup, the eth_calls collector runs a catchup phase that:

1. Scans `data/raw/{chain}/logs/` for existing parquet files
2. For each log file, checks if output already exists in the `on_events/` subdirectory
3. If output doesn't exist:
   - Reads logs from the parquet file
   - Extracts event triggers using configured matchers
   - Processes triggers through the same pipeline as live mode
4. Skips ranges where output already exists to avoid duplicate processing

This ensures that historical data is processed even if the indexer was started after events occurred.

## Output Format

### File Location
```
data/raw/{chain}/eth_calls/{contract}/{function}/on_events/{start_block}-{end_block}.parquet
```

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | uint64 | Block number where the triggering event occurred |
| `block_timestamp` | uint64 | Block timestamp |
| `log_index` | uint32 | Index of the triggering log within the block (for correlation) |
| `target_address` | bytes(20) | Address the eth_call was made to |
| `value` | binary | Raw ABI-encoded return value |
| `param_0`, `param_1`, ... | binary | ABI-encoded parameter values (if any) |

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

Where `EventCallResult` contains:
- `block_number`, `block_timestamp`, `log_index`
- `target_address`
- `value` (raw bytes for decoding)

## Key Types

### Configuration Types (`src/types/config/eth_call.rs`)

```rust
pub struct EventTriggerConfig {
    pub source: String,      // Contract/collection name
    pub event: String,       // Event signature
}

pub enum ParamConfig {
    Static { param_type: EvmType, values: Vec<ParamValue> },
    FromEvent { param_type: EvmType, from_event: String },
    SelfAddress { param_type: EvmType, source: String },
}
```

### Runtime Types (`src/raw_data/historical/receipts.rs`)

```rust
pub struct EventTriggerData {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub emitter_address: [u8; 20],
    pub source_name: String,
    pub event_signature: [u8; 32],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

pub enum EventTriggerMessage {
    Triggers(Vec<EventTriggerData>),
    RangeComplete { range_start: u64, range_end: u64 },
    AllComplete,
}

pub struct EventTriggerMatcher {
    pub source_name: String,
    pub addresses: HashSet<[u8; 20]>,
    pub is_factory: bool,
    pub event_topic0: [u8; 32],
}
```

### Processing Types (`src/raw_data/historical/eth_calls.rs`)

```rust
struct EventTriggeredCallConfig {
    contract_name: String,
    target_address: Option<Address>,  // None for factory collections
    function_name: String,
    function_selector: [u8; 4],
    params: Vec<ParamConfig>,
    is_factory: bool,
}

struct EventCallResult {
    block_number: u64,
    block_timestamp: u64,
    log_index: u32,
    target_address: [u8; 20],
    value_bytes: Vec<u8>,
    param_values: Vec<Vec<u8>>,
}
```

## Files Modified

| File | Changes |
|------|---------|
| `src/types/config/eth_call.rs` | Added `OnEvents` frequency variant, `EventTriggerConfig`, extended `ParamConfig` with `FromEvent` and `SelfAddress` variants |
| `src/raw_data/historical/receipts.rs` | Added `EventTriggerData`, `EventTriggerMessage`, `EventTriggerMatcher` types; `build_event_trigger_matchers()`, `extract_event_triggers()` functions; channel sending |
| `src/raw_data/historical/eth_calls.rs` | Added event processing: `build_event_triggered_call_configs()`, `extract_param_from_event()`, `build_event_call_params()`, `process_event_triggers()`, `write_event_call_results_to_parquet()`; catchup: `get_existing_log_ranges()`, `read_logs_from_parquet()`, `event_output_exists()` |
| `src/raw_data/decoding/types.rs` | Added `EventCallResult` type and `EventCallsReady` variant to `DecoderMessage` |
| `src/raw_data/decoding/mod.rs` | Exported `EventCallResult` |
| `src/main.rs` | Added channel creation, event matchers building, and wiring |

## Design Decisions

### 1. Trigger Extraction in Receipts Collector

Event triggers are extracted in the receipts collector (where logs are already available) rather than in a separate step. This avoids re-reading log data during live processing.

### 2. Factory Address Filtering

For factory collections, receipts sends all logs matching the event signature. The eth_calls collector filters by known factory addresses. This avoids complex bidirectional communication between collectors.

### 3. No Call Chaining

Call chaining (output of call A → input of call B) is explicitly out of scope. This keeps the configuration simple and type-safe. Complex call dependencies should be handled in downstream event handlers in the transformation layer, which provides:
- Full Rust type safety
- IDE support
- Ability to use arbitrary logic
- Conditional calls and error handling

### 4. One Call Per Event

Each matching event triggers a separate eth_call. There is no deduplication within a block. This ensures data completeness for scenarios where the same call with different parameters might be needed.

### 5. Separate Output Directory

Event-triggered call results are written to an `on_events/` subdirectory to avoid conflicts with block-based frequency outputs.

### 6. Catchup Mode Idempotency

The catchup mode checks for existing output files before processing. This ensures that:
- Restarting the indexer doesn't cause duplicate processing
- Partially completed runs can be resumed
- New event-triggered call configurations are automatically backfilled

## Testing

Run all tests:
```bash
cargo test
```

Run config parsing tests specifically:
```bash
cargo test types::config::eth_call
```

Key tests (26 total):
- `test_frequency_deserialize_on_events` - Deserialize `on_events` frequency
- `test_param_config_from_event` - Parse `from_event` parameter binding
- `test_param_config_from_event_data` - Parse `from_event` with data fields
- `test_param_config_self_address` - Parse `source: "self"` parameter
- `test_eth_call_config_with_on_events` - Full config with on_events frequency
- `test_eth_call_config_with_from_event_param` - Config with event-bound parameters
- `test_frequency_on_events_helpers` - Helper method tests for OnEvents variant
