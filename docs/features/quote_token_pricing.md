# Quote Token Pricing

## Scope

Resolves any pool's quote token (numeraire) to a USD price so that metrics like `volume_usd`, `tvl_usd`, and `market_cap_usd` can be computed.

### In scope
- Dynamic discovery of anchor pool configs from `config/contracts/<chain>/tokens.json`
- Transitive multi-hop price resolution (e.g., Bankr -> WETH -> USD)
- Chainlink oracle ETH/USD price extraction
- Stablecoin (USDC, USDT) identity pricing (1.0)
- Zero-address (native ETH) treated as WETH
- Phase 2 (planned): graph-based frontier expansion for unconfigured tokens

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
build_usd_price_context() [per metrics handler invocation]
    |
    +-- Seed stablecoins: USDC=1.0, USDT=1.0
    +-- Extract ETH/USD from ChainlinkEthOracle calls
    +-- Set WETH + zero-address = ETH/USD
    +-- Query all latest prices from prices table
    +-- Transitive resolution loop:
    |     for each (token, quote_token, price):
    |       if quote_token already resolved:
    |         token_usd = price * quote_token_usd
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
| `src/transformations/util/usd_price.rs` | OraclePriceCache (shared, DB-backed), UsdPriceContext (per-invocation), transitive resolution |
| `src/transformations/util/pool_metadata.rs` | KNOWN_QUOTE_TOKENS for decimal resolution |
| `config/contracts/<chain>/tokens.json` | Token addresses and anchor pool configs |
| `migrations/tables/prices.sql` | Price data storage schema |

## Key Exports

- `OraclePriceCache::with_contracts(contracts)` - shared cache construction
- `build_usd_price_context(ctx, cache, db, chain_id, contracts)` - per-invocation builder
- `UsdPriceContext::quote_volume_to_usd(volume, token, decimals)` - volume conversion
- `UsdPriceContext::quote_decimal_amount_to_usd(amount, token)` - TVL conversion

## Invariants

- Stablecoins (USDC, USDT) are always priced at 1.0 USD
- The zero address is always equivalent to WETH for pricing
- Transitive resolution terminates when no new tokens can be resolved (handles cycles)
- PriceHandler version (1) is unchanged; new pool configs produce additive rows
- Pool name convention `<Base><Quote>Pool` is used for automatic discovery

## Config Convention

Anchor pools in `tokens.json` follow the naming pattern `<BaseToken><QuoteToken>Pool`:
- `BankrWethPool`: prices Bankr in terms of Weth
- `ZoraUsdcPool`: prices Zora in terms of Usdc
- `EurcUsdcPool`: prices Eurc in terms of Usdc

Both the base token and quote token must also be configured as standalone entries with their address. The pool entry must have a `calls` array with a `slot0()` or `getSlot0()` function.
