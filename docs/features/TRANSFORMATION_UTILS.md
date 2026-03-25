# Transformation Utilities

Shared helpers for metadata extraction, address validation, and database operations. All modules live under `src/transformations/util/`.

## Modules

| Module | Purpose |
|--------|---------|
| `mod.rs` | Module declarations (`db`, `metadata`, `sanitize`) |
| `sanitize.rs` | Address validation helpers (precompile detection) |
| `metadata.rs` | Token metadata extraction from decoded calls |
| `db/` | Database operation builders for common entities |

---

## `sanitize.rs` — Address Validation

### `is_precompile_address`

```rust
fn is_precompile_address(addr: Address) -> bool
```

Checks if an address is an Ethereum precompile contract (addresses 0x01-0x11). Precompiles include:

| Address | Precompile |
|---------|------------|
| 0x01 | ecrecover |
| 0x02 | SHA2-256 |
| 0x03 | RIPEMD-160 |
| 0x04 | identity |
| 0x05 | modexp |
| 0x06-0x08 | bn256 operations |
| 0x09 | blake2f |
| 0x0a-0x11 | BLS12-381 operations |

Used to filter out invalid token addresses in handlers.

---

## `metadata.rs` — Token Metadata Extraction

### `get_metadata`

```rust
fn get_metadata(
    asset: &[u8; 20],
    numeraire: &[u8; 20],
    event: &DecodedEvent,
    ctx: &TransformationContext,
) -> Result<(AssetTokenMetadata, TokenMetadata), TransformationError>
```

Extracts token metadata from decoded `once` calls for asset/numeraire token pairs. Looks up `calls_of_type("DERC20", "once")` for the asset and `calls_of_type("Numeraires", "once")` for the numeraire.

Returns:
- `AssetTokenMetadata`: Full Doppler asset metadata (name, symbol, decimals, tokenURI, totalSupply, governance, integrator, initializer, migrator, migration_pool)
- `TokenMetadata`: Basic token metadata (name, symbol, decimals)

Errors:
- `IncludesPrecompileError`: If either address is a precompile
- `MissingData`: If no `once` call found for the address
- `TypeConversion`: If a field has an unexpected type

For native ETH (zero address numeraire), returns hardcoded metadata: "Native Ether" / "ETH" / 18 decimals.

---

## `db/` — Database Operation Builders

Helper functions that construct `DbOperation` values for common database operations.

### `db/users.rs`

#### `upsert_user`

```rust
fn upsert_user(
    address: &[u8; 20],
    block_timestamp: &u64,
    ctx: &TransformationContext,
) -> DbOperation
```

Upserts a user record with first_seen/last_seen timestamps. On conflict, only updates `last_seen`.

### `db/transfers.rs`

#### `insert_transfer`

```rust
fn insert_transfer(
    block_number: u64,
    block_timestamp: u64,
    token: &[u8; 20],
    from: &[u8; 20],
    to: &[u8; 20],
    value: &U256,
    ctx: &TransformationContext,
) -> DbOperation
```

Inserts a token transfer record.

### `db/token.rs`

#### `insert_token`

```rust
fn insert_token(
    block_number: u64,
    block_timestamp: u64,
    tx_hash: &[u8; 32],
    creator_address: Option<&[u8; 20]>,
    integrator: Option<&[u8; 20]>,
    token_address: &[u8; 20],
    pool: Option<&PoolAddressOrPoolId>,
    name: &str,
    symbol: &str,
    decimals: u8,
    total_supply: Option<&U256>,
    token_uri: Option<&str>,
    is_derc20: bool,
    is_creator_coin: bool,
    is_content_coin: bool,
    creator_coin_pool: Option<&[u8; 32]>,
    governance: Option<&[u8; 20]>,
    ctx: &TransformationContext,
) -> DbOperation
```

Upserts a token record with full metadata. On conflict (chain_id, address), does not update existing records.

### `db/pool.rs`

#### `insert_pool`

```rust
fn insert_pool(
    block_number: u64,
    block_timestamp: u64,
    address: PoolAddressOrPoolId,
    base_token: &[u8; 20],
    quote_token: &[u8; 20],
    is_token_0: bool,
    pool_type: &str,
    integrator: [u8; 20],
    initializer: [u8; 20],
    fee: u32,
    min_threshold: U256,
    max_threshold: U256,
    migrator: [u8; 20],
    migrated_at: Option<u64>,
    migration_pool: PoolAddressOrPoolId,
    migration_type: &str,
    lock_duration: Option<u32>,
    beneficiaries: Option<BeneficiariesData>,
    pool_key: PoolKey,
    starting_time: u64,
    ending_time: u64,
    ctx: &TransformationContext,
) -> DbOperation
```

Upserts a pool record with full configuration. Supports both address-based (V2/V3) and pool_id-based (V4) pool identifiers.

#### `BeneficiariesData`

```rust
pub struct Beneficiary {
    beneficiary: [u8; 20],  // serialized as hex string
    shares: u64,
}
pub type BeneficiariesData = Vec<Beneficiary>;
```

### `db/v4_pool_configs.rs`

#### `insert_pool_config`

```rust
fn insert_pool_config(
    pool_id: [u8; 32],
    hook_address: [u8; 20],
    num_tokens_to_sell: U256,
    min_proceeds: U256,
    max_proceeds: U256,
    starting_time: u64,
    ending_time: u64,
    starting_tick: i32,
    ending_tick: i32,
    epoch_length: U256,
    gamma: u32,
    is_token_0: bool,
    num_pd_slugs: U256,
    ctx: &TransformationContext,
) -> DbOperation
```

Upserts a V4 pool configuration record with Doppler-specific parameters.

---

## TransformationContext

The `TransformationContext` struct (in `src/transformations/context.rs`) provides handlers with all data and utilities needed for transformations.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `chain_name` | `String` | Chain identifier (e.g., "base", "monad") |
| `chain_id` | `u64` | Numeric chain ID |
| `blockrange_start` | `u64` | Start of current processing range (inclusive) |
| `blockrange_end` | `u64` | End of current processing range (exclusive) |
| `events` | `Arc<Vec<DecodedEvent>>` | All decoded events in current range |
| `calls` | `Arc<Vec<DecodedCall>>` | All decoded eth_call results in current range |

### Filtering Helpers

All filtering helpers automatically exclude events/calls before a contract's configured `start_block`.

```rust
// Get events by source and event name
ctx.events_of_type("UniswapV4", "Swap") -> impl Iterator<Item = &DecodedEvent>

// Get all events for a contract address
ctx.events_for_address(address) -> impl Iterator<Item = &DecodedEvent>

// Get calls by source and function name
ctx.calls_of_type("DopplerToken", "slot0") -> impl Iterator<Item = &DecodedCall>

// Get all calls for a contract address
ctx.calls_for_address(address) -> impl Iterator<Item = &DecodedCall>
```

### Contract Configuration Helpers

```rust
// Get the start_block for a contract or collection by name
ctx.get_contract_start_block("UniswapV4") -> Option<u64>

// Look up contract name by address
ctx.get_contract_name_by_address(address) -> Option<&str>

// Check if address matches any of the specified contracts
ctx.match_contract_address(address, &["DopplerHookV4", "DopplerHookV3"]) -> Option<&'static str>
```

### Transaction Address Lookup

```rust
// Get the from_address for a transaction
ctx.tx_from(&tx_hash) -> Option<&[u8; 20]>

// Get the to_address for a transaction
ctx.tx_to(&tx_hash) -> Option<&[u8; 20]>
```

### Historical Data Access

```rust
// Query historical events from parquet files
ctx.query_events(HistoricalEventQuery {
    source: Some("UniswapV4".to_string()),
    event_name: Some("Swap".to_string()),
    contract_address: None,
    from_block: 1000,
    to_block: 2000,
    limit: Some(100),
}).await -> Result<Vec<DecodedEvent>>

// Query historical eth_call results
ctx.query_calls(HistoricalCallQuery {
    source: Some("DopplerToken".to_string()),
    function_name: Some("slot0".to_string()),
    contract_address: None,
    from_block: 1000,
    to_block: 2000,
    limit: Some(100),
}).await -> Result<Vec<DecodedCall>>
```

Note: `to_block` cannot exceed `blockrange_end` (returns `FutureBlockAccess` error).

### Ad-hoc RPC Calls

```rust
// Single eth_call
ctx.eth_call(
    contract_address,
    "balanceOf(address)(uint256)",
    vec![DynSolValue::Address(owner)],
    block_number,
).await -> Result<DynSolValue>

// Batch eth_calls (concurrent)
ctx.eth_call_batch(vec![
    EthCallRequest {
        contract_address,
        function_signature: "slot0()((uint160,int24,uint16,uint16,uint16,uint8,bool))".to_string(),
        params: vec![],
        block_number,
    },
    // ...more requests
]).await -> Result<Vec<Result<DynSolValue>>>
```

---

## DecodedValue

Enum representing decoded values from events or eth_calls. Defined in `src/types/decoded.rs` and re-exported from `src/transformations/context.rs`.

### Variants

| Variant | Rust Type | Description |
|---------|-----------|-------------|
| `Address` | `[u8; 20]` | Ethereum address |
| `Uint256` | `U256` | Unsigned 256-bit integer |
| `Int256` | `I256` | Signed 256-bit integer |
| `Uint128` | `u128` | Unsigned 128-bit integer |
| `Int128` | `i128` | Signed 128-bit integer |
| `Uint64` | `u64` | Unsigned 64-bit integer |
| `Int64` | `i64` | Signed 64-bit integer |
| `Uint32` | `u32` | Unsigned 32-bit integer |
| `Int32` | `i32` | Signed 32-bit integer |
| `Uint8` | `u8` | Unsigned 8-bit integer |
| `Int8` | `i8` | Signed 8-bit integer |
| `Bool` | `bool` | Boolean |
| `Bytes32` | `[u8; 32]` | Fixed 32-byte array |
| `Bytes` | `Vec<u8>` | Dynamic byte array |
| `String` | `String` | UTF-8 string |
| `NamedTuple` | `Vec<(String, DecodedValue)>` | Tuple with named fields |
| `UnnamedTuple` | `Vec<DecodedValue>` | Tuple without field names |
| `Array` | `Vec<DecodedValue>` | Array of values |

### Accessor Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `as_address()` | `Option<[u8; 20]>` | Extract as address |
| `as_bytes32()` | `Option<[u8; 32]>` | Extract as bytes32 |
| `as_uint256()` | `Option<U256>` | Extract as U256 (converts smaller uints and numeric strings) |
| `as_int256()` | `Option<I256>` | Extract as I256 (converts smaller ints and numeric strings) |
| `as_u64()` | `Option<u64>` | Extract as u64 |
| `as_i64()` | `Option<i64>` | Extract as i64 |
| `as_i32()` | `Option<i32>` | Extract as i32 (for ticks) |
| `as_u32()` | `Option<u32>` | Extract as u32 (for fees) |
| `as_u8()` | `Option<u8>` | Extract as u8 |
| `as_bool()` | `Option<bool>` | Extract as bool |
| `as_string()` | `Option<&str>` | Extract as string reference |
| `as_bytes()` | `Option<&[u8]>` | Extract as byte slice |
| `get_field(name)` | `Option<&DecodedValue>` | Get field from NamedTuple |
| `to_numeric_string()` | `Option<String>` | Convert to numeric string for DB storage |

---

## FieldExtractor Trait

The `FieldExtractor` trait provides ergonomic typed field extraction from decoded events and calls. Implemented by both `DecodedEvent` and `DecodedCall`, it reduces boilerplate when extracting fields.

### Why Use FieldExtractor

**Before (verbose):**
```rust
let asset = event.params.get("asset")
    .ok_or_else(|| TransformationError::MissingField("asset".to_string()))?
    .as_address()
    .ok_or_else(|| TransformationError::TypeConversion("asset is not address".to_string()))?;
```

**After (concise):**
```rust
let asset = event.extract_address("asset")?;
```

### Required Trait Methods

| Method | Description |
|--------|-------------|
| `field_values()` | Returns the underlying `HashMap<String, DecodedValue>` |
| `context_info()` | Returns contextual info for error messages (e.g., event name, block number) |

### Extraction Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `get_field(name)` | `Result<&DecodedValue, TransformationError>` | Get field by name, error if missing |
| `try_get_field(name)` | `Option<&DecodedValue>` | Get field by name, None if missing |
| `extract_address(name)` | `Result<[u8; 20], TransformationError>` | Extract as address |
| `extract_uint256(name)` | `Result<U256, TransformationError>` | Extract as U256 |
| `extract_int256(name)` | `Result<I256, TransformationError>` | Extract as I256 |
| `extract_u64(name)` | `Result<u64, TransformationError>` | Extract as u64 |
| `extract_i64(name)` | `Result<i64, TransformationError>` | Extract as i64 |
| `extract_u32(name)` | `Result<u32, TransformationError>` | Extract as u32 |
| `extract_i32(name)` | `Result<i32, TransformationError>` | Extract as i32 |
| `extract_u8(name)` | `Result<u8, TransformationError>` | Extract as u8 |
| `extract_bool(name)` | `Result<bool, TransformationError>` | Extract as bool |
| `extract_string(name)` | `Result<&str, TransformationError>` | Extract as string reference |
| `extract_bytes32(name)` | `Result<[u8; 32], TransformationError>` | Extract as bytes32 |
| `extract_bytes(name)` | `Result<&[u8], TransformationError>` | Extract as byte slice |
| `extract_u64_flexible(name)` | `Result<u64, TransformationError>` | Extract u64 with flexible parsing (handles i64, u64, or numeric strings) |
| `extract_i32_flexible(name)` | `Result<i32, TransformationError>` | Extract i32 with flexible parsing (handles i32, u32, or numeric strings) |
| `extract_u32_flexible(name)` | `Result<u32, TransformationError>` | Extract u32 with flexible parsing (handles u32, i32, or numeric strings) |

### Error Messages

All extraction methods provide context-rich error messages:

```rust
// MissingField error includes context
TransformationError::MissingField("asset in Swap event at block 12345")

// TypeConversion error includes field name and context
TransformationError::TypeConversion("'amount' is not a uint256 in Swap event at block 12345")
```

---

## DecodedEvent

Represents a decoded blockchain event.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `block_number` | `u64` | Block number |
| `block_timestamp` | `u64` | Block timestamp (Unix seconds) |
| `transaction_hash` | `[u8; 32]` | Transaction hash |
| `log_index` | `u32` | Log index within block |
| `contract_address` | `[u8; 20]` | Emitting contract address |
| `source_name` | `String` | Contract/collection name from config |
| `event_name` | `String` | Event name (e.g., "Swap") |
| `event_signature` | `String` | Full event signature |
| `params` | `HashMap<String, DecodedValue>` | Decoded parameters by field name |

### Methods

```rust
// Get parameter, error if missing
event.get("amount0") -> Result<&DecodedValue, TransformationError>

// Try to get parameter
event.try_get("amount0") -> Option<&DecodedValue>
```

Note: Nested tuple fields use flattened names like `"key.currency0"`.

---

## DecodedCall

Represents a decoded eth_call result.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `block_number` | `u64` | Block number |
| `block_timestamp` | `u64` | Block timestamp |
| `contract_address` | `[u8; 20]` | Target contract address |
| `source_name` | `String` | Contract/collection name from config |
| `function_name` | `String` | Function name (e.g., "slot0") |
| `trigger_log_index` | `Option<u32>` | Log index of triggering event (for event-triggered calls) |
| `result` | `HashMap<String, DecodedValue>` | Decoded return values by field name |

### Methods

```rust
// Get result field, error if missing
call.get("sqrtPriceX96") -> Result<&DecodedValue, TransformationError>

// Try to get result field
call.try_get("sqrtPriceX96") -> Option<&DecodedValue>
```
