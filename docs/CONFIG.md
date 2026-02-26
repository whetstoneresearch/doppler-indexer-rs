# Configuration

The indexer uses a JSON-based configuration system with support for modular file organization. Configuration is loaded from a main config file (typically `config/config.json`) which can reference external files for contracts, tokens, and factory collections.

## File Structure

```
config/
├── config.json              # Main configuration file
├── contracts/
│   └── base/                # Chain-specific contract files
│       ├── v2.json
│       ├── v3.json
│       ├── v4.json
│       └── shared.json
├── tokens/
│   └── base.json            # Chain-specific token definitions
└── factory_collections/
    └── base.json            # Chain-specific factory collection types
```

## Main Configuration

The root configuration file has three top-level sections:

```json
{
    "chains": [...],
    "raw_data_collection": {...},
    "transformations": {...}
}
```

### Chains

An array of chain configurations. Each chain requires:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Human-readable chain identifier |
| `chain_id` | number | Yes | EVM chain ID |
| `rpc_url_env_var` | string | Yes | Environment variable name containing the RPC URL |
| `start_block` | number | No | Block number to start indexing from |
| `contracts` | object \| string | Yes | Inline contracts object or path to contracts file/directory |
| `tokens` | object \| string | Yes | Inline tokens object or path to tokens file/directory |
| `factory_collections` | object \| string | No | Inline factory collection types or path to factory collections file/directory |
| `block_receipts_method` | string | No | RPC method for fetching all receipts in a block (e.g., `eth_getBlockReceipts`) |

Example:
```json
{
    "name": "base",
    "chain_id": 8453,
    "rpc_url_env_var": "BASE_RPC_URL",
    "start_block": 26602741,
    "contracts": "contracts/base",
    "tokens": "tokens/base.json",
    "factory_collections": "factory_collections/base.json",
    "block_receipts_method": "eth_getBlockReceipts"
}
```

#### Block Receipts Method

Some RPC providers support fetching all transaction receipts for a block in a single call (e.g., `eth_getBlockReceipts`). This is more efficient than fetching receipts individually, especially for blocks with many transactions.

```json
{
    "name": "optimism",
    "chain_id": 10,
    "rpc_url_env_var": "OPTIMISM_RPC_URL",
    "block_receipts_method": "eth_getBlockReceipts",
    "contracts": "contracts/optimism",
    "tokens": "tokens/optimism.json"
}
```

When configured:
- Receipts are fetched one block at a time using the specified method
- Individual receipts that fail to deserialize (e.g., L2 deposit transactions) are skipped gracefully
- Rate limiting is applied per block request

When omitted, the default per-transaction batching is used (`eth_getTransactionReceipt` for each transaction).

### Raw Data Collection

Controls how raw blockchain data is collected and stored:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `parquet_block_range` | number | No | Number of blocks per parquet file |
| `rpc_batch_size` | number | No | Batch size for RPC requests |
| `fields` | object | Yes | Specifies which fields to collect |
| `contract_logs_only` | boolean | No | If true, only collect logs from configured contracts |
| `channel_capacity` | number | No | Capacity for main channels (blocks, logs, eth_calls). Default: 1000 |
| `factory_channel_capacity` | number | No | Capacity for factory-related channels. Default: 1000 |
| `block_receipt_concurrency` | number | No | Number of blocks to fetch receipts for concurrently when using `block_receipts_method`. Default: 10 |
| `decoding_concurrency` | number | No | Number of concurrent decoding tasks for log and eth_call decoding. Default: 4 |
| `factory_concurrency` | number | No | Number of concurrent tasks for factory collection catchup. Default: 4 |

#### Fields Configuration

The `fields` object specifies which data to extract:

```json
{
    "fields": {
        "block_fields": ["number", "timestamp", "transactions", "uncles"],
        "receipt_fields": ["block_number", "timestamp", "transaction_hash", "from", "to"],
        "log_fields": ["block_number", "timestamp", "transaction_hash", "log_index", "address", "topics", "data"]
    }
}
```

**Available Block Fields:**
- `number` - Block number
- `timestamp` - Block timestamp
- `transactions` - Transaction list
- `uncles` - Uncle blocks

**Available Receipt Fields:**
- `block_number` - Block number
- `block_timestamp` / `timestamp` - Block timestamp
- `transaction_hash` - Transaction hash
- `from` - Sender address
- `to` - Recipient address
- `logs` - Transaction logs

**Available Log Fields:**
- `block_number` - Block number
- `block_timestamp` / `timestamp` - Block timestamp
- `transaction_hash` - Transaction hash
- `log_index` - Log index within block
- `address` - Emitting contract address
- `topics` - Indexed event parameters
- `data` - Non-indexed event data

## Contracts Configuration

Contracts can be defined inline or loaded from external JSON files. When using a path, it can point to either a single file or a directory of JSON files (which are merged alphabetically).

Each contract entry is keyed by a human-readable name:

```json
{
    "UniswapV4PoolManager": {
        "address": "0x498581ff718922c3f8e6a244956af099b2652b2b"
    }
}
```

### Contract Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `address` | string \| string[] | Yes | Single address or array of addresses |
| `start_block` | number | No | Override chain-level start block for this contract |
| `calls` | array | No | eth_call configuration for reading contract state |
| `factories` | array | No | Factory configurations for tracking dynamically created contracts |
| `events` | array | No | Event signatures to decode from this contract's logs |

### Multicall3 Optimization

Adding a `Multicall3` contract entry enables automatic batching of all eth_calls for the same block into a single `aggregate3` RPC call, significantly reducing RPC requests:

```json
{
    "Multicall3": {
        "address": "0xcA11bde05977b3631167028862bE2a173976CA11"
    }
}
```

The Multicall3 address is the same on most EVM chains. See [eth_call Collection](./ETH_CALL_COLLECTION.md#multicall3-optimization) for details.

### Event Configuration

Events specify which contract events to decode:

```json
{
    "UniswapV4PoolManager": {
        "address": "0x498581ff718922c3f8e6a244956af099b2652b2b",
        "events": [
            {"signature": "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1)"},
            {"signature": "Transfer(address indexed from, address indexed to, uint256 value)"}
        ]
    }
}
```

**Event Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `signature` | string | Yes | Full ABI signature string including parameter names and types |

### Multiple Addresses

A contract can track multiple addresses:

```json
{
    "UniswapV4Initializer": {
        "address": [
            "0x8AF018e28c273826e6b2d5a99e81c8fB63729b07",
            "0x77EbfBAE15AD200758E9E2E61597c0B07d731254"
        ]
    }
}
```

### eth_call Configuration

Contracts can include `calls` to read on-chain state:

```json
{
    "ChainlinkEthOracle": {
        "address": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
        "calls": [
            {
                "function": "latestAnswer()",
                "output_type": "int256"
            },
            {
                "function": "description()",
                "output_type": "string",
                "frequency": "once"
            }
        ]
    }
}
```

**Call Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `function` | string | Yes | - | Function signature (e.g., `totalSupply()`) |
| `output_type` | string | Yes | - | Return type for decoding (see Output Types below) |
| `params` | array | No | - | Parameters to pass to the function call (see Parameter Configuration below) |
| `frequency` | string/number/object | No | every block | How often to make the call (see Frequency Options below) |
| `target` | string | No | *(contract address)* | Override target: a hex address or a contract name to look up |

The `target` field allows calling a different contract than the one the config is nested under. This is useful for calling view functions on helper contracts like Multicall3 or StateView contracts:

```json
{
    "MyContract": {
        "address": "0x...",
        "calls": [
            {
                "function": "getSlot0(bytes32)",
                "output_type": "(uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)",
                "target": "UniswapV4StateView",
                "params": [{"type": "bytes32", "values": ["0x..."]}]
            }
        ]
    }
}
```

**Parameter Configuration:**

For functions that require parameters, use the `params` field. There are three types of parameter configurations:

**1. Static Values:**

Each value generates a separate call:

```json
{
    "function": "balanceOf(address)",
    "output_type": "uint256",
    "params": [
        {
            "type": "address",
            "values": [
                "0x1234567890abcdef1234567890abcdef12345678",
                "0xabcdef1234567890abcdef1234567890abcdef12"
            ]
        }
    ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | EVM type (address, uint256, bool, etc.) |
| `values` | array | Array of values - each value generates a separate call |

**2. Event Data Binding (for `on_events` frequency):**

Extract parameter values from the triggering event:

```json
{
    "function": "getState(address)",
    "output_type": "(address numeraire, uint8 status)",
    "frequency": {
        "on_events": {
            "source": "UniswapV4Initializer",
            "event": "Create(address,address,address)"
        }
    },
    "params": [
        { "type": "address", "from_event": "topics[2]" }
    ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | EVM type for the parameter |
| `from_event` | string | Event field reference: `topics[N]` or `data[N]` |

**3. Self Address (for factory collections):**

Use the factory-created contract's own address as a parameter:

```json
{
    "function": "balanceOf(address)",
    "output_type": "uint256",
    "params": [
        { "type": "address", "source": "self" }
    ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | EVM type (typically `address`) |
| `source` | string | Must be `"self"` - uses the event emitter's address |

**Output Types:**

Simple types:
- `int256`, `int128`, `int64`, `int32`, `int24`, `int16`, `int8` - Signed integers
- `uint256`, `uint160`, `uint128`, `uint96`, `uint80`, `uint64`, `uint32`, `uint24`, `uint16`, `uint8` - Unsigned integers
- `address` - 20-byte address
- `bool` - Boolean
- `bytes32` - Fixed 32-byte value
- `bytes` - Variable-length bytes
- `string` - UTF-8 string

Named types (for custom column names):
- `"int256 latestAnswer"` - Creates a column named "latestAnswer"

Named tuples (struct return values):
- `"(uint160 sqrtPriceX96, int24 tick)"` - Creates columns "sqrtPriceX96" and "tick"
- `"(address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks)"` - Nested struct with named fields

Unnamed tuples:
- `"(address, uint96)"` - Creates columns "0" and "1"

Arrays:
- `"address[]"` - Array of addresses
- `"(address, uint96)[]"` - Array of tuples
- `"(address beneficiary, uint96 shares)[]"` - Array of named tuples

**Frequency Options:**

| Value | Description |
|-------|-------------|
| *(omitted)* | Call every block (default behavior) |
| `"once"` | Call once per contract address |
| `100` | Call every 100 blocks (any positive integer) |
| `"5m"` | Call every 5 minutes |
| `"1h"` | Call every 1 hour |
| `"1d"` | Call every 1 day |
| `{"on_events": {...}}` | Call when specific events are emitted |

Duration suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days)

**Event-Triggered Calls:**

The `on_events` frequency triggers eth_calls when specific events are emitted. This is useful for reading contract state immediately after state changes.

Single event trigger:
```json
{
    "frequency": {
        "on_events": {
            "source": "UniswapV4Initializer",
            "event": "Create(address,address,address)"
        }
    }
}
```

Multiple event triggers:
```json
{
    "frequency": {
        "on_events": [
            { "source": "ContractA", "event": "EventA(address)" },
            { "source": "ContractB", "event": "EventB(uint256)" }
        ]
    }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Contract name or factory collection name that emits the event |
| `event` | string | Event signature (types only, no parameter names) |

See [eth_call Collection](./ETH_CALL_COLLECTION.md) for detailed documentation on frequency behavior.

### Factory Configuration

Factories allow tracking contracts that are dynamically created by other contracts (e.g., token deployments, liquidity pool creations). The indexer monitors specified events and extracts the addresses of newly created contracts.

```json
{
    "Airlock": {
        "address": "0x660eAaEdEBc968f8f3694354FA8EC0b4c5Ba8D12",
        "factories": [
            {
                "collection": "DERC20",
                "factory_events": {
                    "name": "Create",
                    "topics_signature": "address",
                    "data_signature": "address,address,address",
                    "factory_parameters": "data[0]"
                },
                "calls": [
                    {"function": "totalSupply()", "output_type": "uint256"}
                ]
            }
        ]
    }
}
```

**Factory Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `collection` | string | Yes | Identifier for this group of factory-created contracts (alias: `collection_name`) |
| `factory_events` | object \| array | Yes | Event signature(s) for matching factory creation events |
| `calls` | array | No | eth_call configs to execute on factory-created contracts (merged with collection type calls) |
| `events` | array | No | Event signatures to decode from factory-created contract logs (merged with collection type events) |

**Factory Event Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Event name (e.g., "Create", "PairCreated") |
| `topics_signature` | string | Yes | Comma-separated types of indexed parameters |
| `data_signature` | string | No | Comma-separated types of non-indexed parameters |
| `factory_parameters` | string | Yes | Which parameter contains the created contract address |

**Multiple Factory Events:**

A factory can track multiple creation events by providing an array:

```json
{
    "factory_events": [
        {
            "name": "PoolCreated",
            "topics_signature": "address,address",
            "data_signature": "address",
            "factory_parameters": "data[0]"
        },
        {
            "name": "PairCreated",
            "topics_signature": "address,address",
            "data_signature": "address,uint256",
            "factory_parameters": "data[0]"
        }
    ]
}
```

**Factory Parameters Format:**

The `factory_parameters` field specifies where to extract the created contract address:
- `topics[1]` - First indexed parameter (after event signature)
- `topics[2]` - Second indexed parameter
- `data[0]` - First non-indexed parameter
- `data[1]` - Second non-indexed parameter

Only address-type parameters can be used for extraction.

**Factory Integration with `contract_logs_only`:**

When `contract_logs_only` is `true`, logs from factory-created contracts are automatically included in the filtered output. This allows tracking events from dynamically created contracts without explicitly listing their addresses.

### Factory Collection Types

Factory collection types define shared configurations (calls and events) that are applied to all factories with the same collection name. This reduces duplication when multiple factory contracts create the same type of contract.

Collection types are defined at the chain level using the `factory_collections` field:

```json
{
    "name": "base",
    "chain_id": 8453,
    "factory_collections": "factory_collections/base.json"
}
```

Example `factory_collections/base.json`:
```json
{
    "DERC20": {
        "calls": [
            {"function": "totalSupply()", "output_type": "uint256", "frequency": "once"},
            {"function": "name()", "output_type": "string", "frequency": "once"},
            {"function": "symbol()", "output_type": "string", "frequency": "once"}
        ],
        "events": [
            {"signature": "Transfer(address indexed from, address indexed to, uint256 value)"}
        ]
    },
    "DopplerV4Hook": {
        "calls": [
            {"function": "poolKey()", "output_type": "(address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks)", "frequency": "once"}
        ],
        "events": [
            {"signature": "Swap(int24 currentTick, uint256 totalProceeds, uint256 totalTokensSold)"}
        ]
    }
}
```

**Merging Behavior:**

When a factory config references a collection type, the calls and events are merged:
- Collection type calls/events are applied first
- Inline factory calls/events are appended (not replaced)
- This allows factory-specific overrides while sharing common configurations

See [Factory Collection](./FACTORY_COLLECTION.md) for detailed documentation.

## Tokens Configuration

Tokens define assets to track, optionally with associated liquidity pools for price discovery.

```json
{
    "Weth": {
        "address": "0x4200000000000000000000000000000000000006"
    },
    "Eurc": {
        "address": "0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42",
        "pool": {
            "type": "v4",
            "address": "0xb18fad93e3c5a5f932d901f0c22c5639a832d6f29a4392fff3393fb734dd0720",
            "quote_token": "Usdc"
        }
    }
}
```

### Token Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `address` | string | Yes | Token contract address |
| `pool` | object | No | Associated liquidity pool for pricing |

### Pool Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Pool type: `v2`, `v3`, or `v4` |
| `address` | string | Yes | Pool address (or pool ID for v4) |
| `quote_token` | string | Yes | Reference to another token key for price quotes |

For Uniswap V4 pools, the `address` field accepts a 32-byte pool ID instead of a standard address.

## Path Resolution

All paths in the configuration are resolved relative to the config file's directory. For example, if your config is at `config/config.json`:

- `"contracts": "contracts/base"` resolves to `config/contracts/base/`
- `"tokens": "tokens/base.json"` resolves to `config/tokens/base.json`

## Directory Loading

When a path points to a directory:
1. All `.json` files in that directory are loaded
2. Files are processed in alphabetical order
3. Contract/token definitions are merged into a single map
4. Duplicate keys across files cause a panic

This allows organizing contracts by category (e.g., `v2.json`, `v3.json`, `shared.json`) while maintaining a flat namespace.

## Transformations Configuration

The optional `transformations` section configures the transformation system that processes decoded data and writes to PostgreSQL.

```json
{
    "transformations": {
        "database_url_env_var": "DATABASE_URL",
        "mode": {
            "batch_for_catchup": true,
            "catchup_batch_size": 10000
        },
        "handler_concurrency": 4,
        "max_batch_size": 1000
    }
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `database_url_env_var` | string | No | `"DATABASE_URL"` | Environment variable containing PostgreSQL connection string |
| `mode` | object | No | *(see below)* | Execution mode configuration |
| `handler_concurrency` | number | No | `4` | Number of concurrent handler executions |
| `max_batch_size` | number | No | `1000` | Maximum operations per transaction batch |

### Mode Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `batch_for_catchup` | boolean | No | `true` | Use batch mode for historical catchup (larger batches, more memory) |
| `catchup_batch_size` | number | No | `10000` | Batch size in blocks for batch mode |

When `batch_for_catchup` is enabled, the transformation system processes historical data in larger batches for improved throughput. This uses more memory but significantly speeds up initial synchronization.

Transformations are enabled when this config section is present AND there are registered handlers in the transformation registry.
