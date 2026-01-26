# Configuration

The indexer uses a JSON-based configuration system with support for modular file organization. Configuration is loaded from a main config file (typically `config/config.json`) which can reference external files for contracts and tokens.

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
└── tokens/
    └── base.json            # Chain-specific token definitions
```

## Main Configuration

The root configuration file has two top-level sections:

```json
{
    "chains": [...],
    "raw_data_collection": {...}
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

Example:
```json
{
    "name": "base",
    "chain_id": 8453,
    "rpc_url_env_var": "BASE_RPC_URL",
    "start_block": 26602741,
    "contracts": "contracts/base",
    "tokens": "tokens/base.json"
}
```

### Raw Data Collection

Controls how raw blockchain data is collected and stored:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `parquet_block_range` | number | No | Number of blocks per parquet file |
| `fields` | object | Yes | Specifies which fields to collect |
| `contract_logs_only` | boolean | No | If true, only collect logs from configured contracts |

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
| `calls` | object | No | eth_call configuration for reading contract state |

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
            }
        ]
    }
}
```

**Output Types:**
- `int256` - Signed 256-bit integer
- `uint256` - Unsigned 256-bit integer

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
