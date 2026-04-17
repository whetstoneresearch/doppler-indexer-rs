# Quote Token Pricing

## Scope

Resolves any pool's quote token (numeraire) to a USD price so that metrics like `volume_usd`, `tvl_usd`, and `market_cap_usd` can be computed.

### In scope
- Dynamic discovery of anchor pool configs from `config/contracts/<chain>/tokens.json`
- Transitive multi-hop price resolution (e.g., Bankr -> WETH -> USD)
- Chainlink oracle ETH/USD price extraction
- Stablecoin (USDC, USDT) identity pricing (1.0)
- Zero-address (native ETH) treated as WETH
- Graph-based frontier expansion for unconfigured tokens (Phase 2)

### Not in scope
- Non-Doppler pool price feeds
- Real-time price oracles beyond Chainlink ETH/USD

## Data Flow

```
tokens.json Pool configs (e.g., BankrWethPool)
    |
    v
PriceHandler (eth_call, reads slot0/getSlot0)
    |
    v
prices table: (token, quote_token, price)
    |
    v
build_usd_price_context_with_paths() [per metrics handler invocation]
    |
    +-- Phase 1: Config-driven resolution
    |   +-- Seed stablecoins: USDC=1.0, USDT=1.0
    |   +-- Extract ETH/USD from ChainlinkEthOracle calls
    |   +-- Set WETH + zero-address = ETH/USD
    |   +-- Query all latest prices from prices table
    |   +-- Transitive resolution loop
    |
    +-- Phase 2: Graph-based path resolution
    |   +-- Collect unresolved quote tokens from metadata cache
    |   +-- For each: check token_price_paths cache
    |   +-- If stale/missing: BFS frontier expansion (max 5 hops, $1k min liq)
    |   +-- Derive price by walking path with current pool_state prices
    |   +-- Cache path in token_price_paths (1 week staleness)
    |
    v
UsdPriceContext { prices: HashMap<address, usd_price> }
    |
    v
quote_to_usd_multiplier(quote_token) -> Option<BigDecimal>
```

## Related Files

| File | Role |
|------|------|
| `src/transformations/eth_call/price.rs` | PriceHandler: discovers pool configs from contracts, reads sqrtPriceX96, writes to prices table |
| `src/transformations/eth_call/mod.rs` | Passes Contracts to PriceHandler registration |
| `src/transformations/util/usd_price.rs` | OraclePriceCache (shared, DB-backed), UsdPriceContext (per-invocation), transitive + path resolution |
| `src/transformations/util/price_path.rs` | Frontier expansion BFS, path caching, price derivation from cached paths |
| `src/transformations/util/pool_metadata.rs` | KNOWN_QUOTE_TOKENS for decimal resolution, unique_quote_tokens() for Phase 2 |
| `config/contracts/<chain>/tokens.json` | Token addresses and anchor pool configs |
| `migrations/tables/prices.sql` | Price data storage schema |
| `migrations/tables/token_price_paths.sql` | Cached price paths for Phase 2 |
| `migrations/tables/pools_token_indexes.sql` | Indexes on base_token/quote_token for frontier queries |

## Key Exports

- `OraclePriceCache::with_contracts(contracts)` - shared cache construction
- `build_usd_price_context_with_paths(ctx, cache, db, chain_id, contracts, metadata_cache)` - full builder with path resolution
- `build_usd_price_context(ctx, cache, db, chain_id, contracts)` - Phase 1 only builder
- `UsdPriceContext::quote_volume_to_usd(volume, token, decimals)` - volume conversion
- `UsdPriceContext::quote_decimal_amount_to_usd(amount, token)` - TVL conversion
- `UsdPriceContext::resolve_path_prices(db, chain_id, block, tokens)` - Phase 2 path resolution
- `check_or_resolve_path(db, chain_id, token, priceable, block)` - single token path resolution
- `derive_price_from_path(db, chain_id, path, token, anchor_usd)` - price from cached path

## Invariants

- Stablecoins (USDC, USDT) are always priced at 1.0 USD
- The zero address is always equivalent to WETH for pricing
- Transitive resolution terminates when no new tokens can be resolved (handles cycles)
- PriceHandler version (1) is unchanged; new pool configs produce additive rows
- Pool name convention `<Base><Quote>Pool` is used for automatic discovery
- Path resolution caches results for 1 week (~302,400 blocks on Base)
- Unpriceable tokens (no path within 5 hops at $1k+ liquidity) are cached as is_priceable=false
- Price derivation walks the path at query time using pool_snapshots (historically complete, not latest-only pool_state)

## Config Convention

Anchor pools in `tokens.json` follow the naming pattern `<BaseToken><QuoteToken>Pool`:
- `BankrWethPool`: prices Bankr in terms of Weth
- `ZoraUsdcPool`: prices Zora in terms of Usdc
- `EurcUsdcPool`: prices Eurc in terms of Usdc

Both the base token and quote token must also be configured as standalone entries with their address. The pool entry must have a `calls` array with a `slot0()` or `getSlot0()` function.

## Phase 2: Frontier Expansion Algorithm

For tokens not covered by configured anchor pools, BFS expands outward from the target token through indexed pools:

1. Start with frontier = {target_token}
2. At each hop (max 5), query pools touching frontier tokens with active_liquidity_usd >= $1,000
3. Track parent pointers and bottleneck (min-along-path) liquidity for each discovered token
4. If a priceable token is reached, trace the path back and return it
5. After all hops, return the path with the highest bottleneck liquidity (or None)

The path is cached in `token_price_paths` with a 1-week staleness threshold. On subsequent lookups, the cached path is used and the USD price is re-derived from current `pool_state.price` values.
