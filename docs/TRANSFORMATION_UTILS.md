# Transformation Utilities

Shared helpers for price computation, market metrics, price fetching, quote token resolution, metadata extraction, and database operations. All modules live under `src/transformations/util/`.

## Modules

| Module | Purpose |
|--------|---------|
| `mod.rs` | Byte/address/hex conversion helpers |
| `constants.rs` | Numeric constants (Q192, WAD, time intervals) |
| `price.rs` | Price computation from sqrtPriceX96 and reserves |
| `market.rs` | Market cap, liquidity, and volume calculations |
| `price_fetch.rs` | `PriceFetcher` — reads token prices from parquet files |
| `quote_info.rs` | `QuoteResolver` — identifies quote tokens and fetches USD prices |
| `sanitize.rs` | Address validation helpers (precompile detection) |
| `metadata.rs` | Token metadata extraction from decoded calls |
| `db/` | Database operation builders for common entities |

---

## `mod.rs` — Conversion Helpers

Small functions for converting between alloy primitives and raw bytes/hex strings.

| Function | Signature | Description |
|----------|-----------|-------------|
| `bytes_to_address` | `[u8; 20] -> Address` | Wrap raw bytes as an alloy `Address` |
| `address_to_bytes` | `Address -> [u8; 20]` | Extract raw bytes from an `Address` |
| `bytes_to_b256` | `[u8; 32] -> B256` | Wrap raw bytes as alloy `B256` |
| `b256_to_bytes` | `B256 -> [u8; 32]` | Extract raw bytes from a `B256` |
| `format_address` | `[u8; 20] -> String` | Hex-encode with `0x` prefix |
| `format_bytes32` | `[u8; 32] -> String` | Hex-encode with `0x` prefix |
| `parse_hex` | `&str -> Result<Vec<u8>>` | Decode hex string (with or without `0x`) |

---

## `constants.rs`

| Constant | Value | Usage |
|----------|-------|-------|
| `Q192` | 2^192 | Denominator for sqrtPriceX96 → price conversion |
| `WAD` | 10^18 | Standard 18-decimal precision unit |
| `CHAINLINK_ETH_DECIMALS` | 10^8 | Chainlink ETH/USD feed precision |
| `SECONDS_IN_15_MINUTES` | 900 | Time bucketing |
| `SECONDS_IN_30_MINUTES` | 1800 | Time bucketing |
| `SECONDS_IN_HOUR` | 3600 | Time bucketing |
| `SECONDS_IN_DAY` | 86400 | Time bucketing |

---

## `price.rs` — Price Computation

### `compute_price_from_sqrt_price_x96`

```rust
fn compute_price_from_sqrt_price_x96(
    sqrt_price_x96: U256,
    is_token0: bool,
    decimals: u8,
    quote_decimals: u8,
) -> U256
```

Converts a Uniswap V3/V4 `sqrtPriceX96` value to a human-readable price with WAD (18-decimal) precision.

- `is_token0`: `true` when the asset being priced is token0 in the pool (price = ratio_x192 / Q192), `false` for token1 (price = Q192 / ratio_x192).
- `decimals` / `quote_decimals`: token decimal counts, used to scale the result so that 1 unit of the asset = the returned WAD-denominated value in quote terms.

### `compute_price_from_reserves`

```rust
fn compute_price_from_reserves(
    asset_balance: U256,
    quote_balance: U256,
    asset_decimals: u8,
    quote_decimals: u8,
) -> U256
```

Computes price from V2-style constant-product reserves: `price = quote_balance / asset_balance`, normalized to WAD. Panics if `asset_balance` is zero.

Handles differing decimal counts between asset and quote tokens by scaling before division.

---

## `market.rs` — Market Metrics

All functions return values with 18 decimals of precision (WAD).

### `calculate_market_cap`

```rust
fn calculate_market_cap(
    price: U256,           // WAD-denominated asset price
    total_supply: U256,    // raw total supply (asset_decimals precision)
    quote_price_usd: U256, // USD price of quote currency
    asset_decimals: Option<u8>,  // default 18
    decimals: Option<u8>,        // quote price feed decimals, default 8
) -> U256
```

Formula: `(price * total_supply / 10^asset_decimals) * quote_price_usd / 10^decimals`

### `calculate_liquidity`

```rust
fn calculate_liquidity(
    asset_balance: U256,
    quote_balance: U256,
    price: U256,            // WAD-denominated asset price in quote terms
    quote_price_usd: U256,
    is_quote_usd: bool,     // true if quote is already USD-denominated
    decimals: Option<u8>,        // quote price feed decimals, default 8
    asset_decimals: Option<u8>,  // default 18
    quote_decimals: Option<u8>,  // default 18
) -> U256
```

Computes total pool liquidity in USD: converts the asset side to quote units via `price`, then converts both sides to USD via `quote_price_usd`. When `is_quote_usd` is true, skips the USD conversion and returns the sum directly.

### `calculate_volume`

```rust
fn calculate_volume(
    amount_in: U256,
    amount_out: U256,
    quote_price_usd: U256,
    is_quote_usd: bool,
    quote_decimals: Option<u8>,  // default 18
    decimals: Option<u8>,        // quote price feed decimals, default 8
) -> U256
```

Uses `amount_in` if non-zero, otherwise `amount_out`. Returns zero when both are zero. Converts the swap amount to USD via `quote_price_usd` unless `is_quote_usd` is true.

---

## `price_fetch.rs` — PriceFetcher

Reads token prices from decoded `eth_call` parquet files at `data/derived/{chain}/decoded/eth_calls/`.

### Token enum

Supported tokens and their data sources:

| Token | Source | Method | Notes |
|-------|--------|--------|-------|
| `Eth` | `ChainlinkEthOracle` | `latestAnswer` | Raw Chainlink value (8 decimals) |
| `Zora` | `Zora_pool` (Base) | `slot0` | sqrtPriceX96, token0, 18/6 dec |
| `Fxh` | `Fxh_pool` (Base) | `slot0` | sqrtPriceX96, token1, 18/18 dec |
| `Noice` | `Noice_pool` (Base) | `slot0` | sqrtPriceX96, token1, 18/18 dec |
| `Monad` | `Mon_pool` (Monad) | `slot0` | sqrtPriceX96, token0, 18/6 dec; defaults to 0.02 USD |
| `Eurc` | `Eurc_pool` (Base) | `getSlot0` | sqrtPriceX96, token0, 6/6 dec; defaults to ~$1.15 |
| `Usdc` | — | — | Hardcoded: 10^8 (i.e. $1.00 at 8 decimals) |
| `Usdt` | — | — | Hardcoded: 10^8 |

### Usage

```rust
let fetcher = PriceFetcher::new("base")?;
let eth_price = fetcher.fetch_price(Token::Eth, block_number)?;
```

### How it works

1. **Index**: On construction, scans `data/derived/{chain}/decoded/eth_calls/{source}/{function}/` for parquet files. File names follow `decoded_{start}-{end}.parquet`.
2. **Lookup**: For a given block, finds the parquet file whose range contains the block, reads the target column, and returns the value at or before the requested block via binary search.
3. **Cache**: Caches one file's entries per (source, function) pair. Sequential block processing means lookups cluster in the same file range, so a single-entry cache is effective.
4. **Chain gating**: Tokens tied to a specific chain (e.g. Zora on Base) return a default value when the fetcher is initialized for a different chain.
5. **Lookback**: Searches up to 10 prior parquet files if the target file doesn't contain a value at or before the requested block.

---

## `quote_info.rs` — QuoteResolver

Identifies the quote token for a pool and resolves its USD price.

### QuoteToken enum

```
Eth, Zora, Fxh, Noice, Mon, Usdc, Usdt, Eurc, CreatorCoin, Unknown
```

### QuoteInfo

```rust
pub struct QuoteInfo {
    pub quote_token: QuoteToken,
    pub quote_price: Option<U256>,   // USD price (None if block_number not provided)
    pub quote_decimals: u8,          // token decimals (6 for stables, 18 otherwise)
    pub quote_price_decimals: u8,    // price feed decimals (8 for Chainlink, 18 for EURC)
}
```

### Usage

```rust
let resolver = QuoteResolver::new(&chain_config)?;
let info = resolver.resolve(token_address, Some(block_number), &price_fetcher)?;
```

### How it works

1. **Token identification**: Matches the address against known token addresses from `ChainConfig`. If no match, checks if the address is a creator coin (loaded from `ZoraCreatorCoinV4` factory parquets). Falls back to ETH for `Address::ZERO` or WETH, or `Unknown` otherwise.
2. **Price resolution**: For composite-priced tokens (Fxh, Noice), multiplies the token-in-ETH price by the ETH/USD price. CreatorCoin and Unknown return `U256::ZERO`.
3. **Decimals**: Stablecoins (USDC, USDT, EURC) use 6 decimals. Price feed decimals are 8 for Chainlink-sourced prices, 18 for sqrtPriceX96-derived prices.

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

Extracts token metadata from decoded `once` calls for asset/numeraire token pairs.

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
| `block_range_start` | `u64` | Start of current processing range (inclusive) |
| `block_range_end` | `u64` | End of current processing range (exclusive) |
| `events` | `Arc<Vec<DecodedEvent>>` | All decoded events in current range |
| `calls` | `Arc<Vec<DecodedCall>>` | All decoded eth_call results in current range |

### Filtering Helpers

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

Note: `to_block` cannot exceed `block_range_end` (returns `FutureBlockAccess` error).

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

Enum representing decoded values from events or eth_calls. Located in `src/transformations/context.rs`.

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
| `as_uint256()` | `Option<U256>` | Extract as U256 (converts smaller uints) |
| `as_int256()` | `Option<I256>` | Extract as I256 (converts smaller ints) |
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
