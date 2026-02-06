# Transformation Utilities

Shared helpers for price computation, market metrics, price fetching, and quote token resolution. All modules live under `src/transformations/util/`.

## Modules

| Module | Purpose |
|--------|---------|
| `mod.rs` | Byte/address/hex conversion helpers |
| `constants.rs` | Numeric constants (Q192, WAD, time intervals) |
| `price.rs` | Price computation from sqrtPriceX96 and reserves |
| `market.rs` | Market cap, liquidity, and volume calculations |
| `price_fetch.rs` | `PriceFetcher` — reads token prices from parquet files |
| `quote_info.rs` | `QuoteResolver` — identifies quote tokens and fetches USD prices |

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
