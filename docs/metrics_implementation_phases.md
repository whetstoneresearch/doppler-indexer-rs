# Pool Metrics Handlers — Implementation Roadmap

## Context

The old TypeScript indexer maintained only point-in-time pool state (reserves, price, market cap, etc.), updated on every event. The new Rust indexer produces **per-block time-series snapshots** alongside a hot-query **pool_state** table, enabling OHLC candles and historical protocol analytics at query time.

This roadmap covers metrics handlers for all 9 pool type variants, shared abstractions, SQL migrations, and contract config changes. It is designed to be implemented across multiple sessions.

---

## Tables

### pool_state — hot query target for dashboards

Keyed by `(chain_id, pool_id, source, source_version)`. Contains latest tick, sqrt_price_x96, price, active_liquidity. Conditional upsert: only update if new `block_number > existing`. TVL and rolling 24h fields added in later phases.

### pool_snapshots — per-block time series

Keyed by `(chain_id, pool_id, block_number, source, source_version)`. OHLC prices, active_liquidity, volume0/1, swap_count. One row per (pool, block) with activity. INSERT ON CONFLICT updates all columns (idempotent).

### liquidity_deltas — append-only recovery log

Keyed by `(chain_id, pool_id, block_number, log_index)`. tick_lower, tick_upper, liquidity_delta. Simple INSERT. Used to rebuild in-memory tick maps for future TVL computation.

---

## Handler Matrix

| Handler | Source | Swap Event | Liquidity Event | sqrtPriceX96 Source | Pool ID Source | Special |
|---------|--------|-----------|-----------------|--------------------|----|---------|
| V3MetricsHandler | DopplerV3Pool | V3 Swap (has sqrtPriceX96) | Mint/Burn | From event | contract_address (20 bytes) | — |
| LockableV3MetricsHandler | DopplerLockableV3Pool | V3 Swap (has sqrtPriceX96) | Mint/Burn | From event | contract_address (20 bytes) | — |
| V4BaseMetricsHandler | DopplerV4Hook | `Swap(int24, uint256, uint256)` | None (liquidity changes on swap) | Derived from tick via `tick_to_sqrt_price_x96()` | hook_address → pool_id via `v4_pool_configs` table | **Sequential**: in-memory proceeds tracker |
| MulticurveMetricsHandler | UniswapV4MulticurveInitializerHook | V4 hook Swap (int128 amounts) | ModifyLiquidity (tuple PoolKey format) | getSlot0 on_event call | topics[3] (poolId) | — |
| DecayMulticurveMetricsHandler | DecayMulticurveHook | V4 hook Swap (int128 amounts) | ModifyLiquidity (flat format, id in topics[1]) | getSlot0 on_event call | topics[3] (poolId) | — |
| ScheduledMulticurveMetricsHandler | UniswapV4ScheduledMulticurveInitializerHook | V4 hook Swap (int128 amounts) | ModifyLiquidity (flat format, id in topics[1]) | getSlot0 on_event call | topics[3] (poolId) | — |
| DhookMetricsHandler | DopplerHookInitializer | V4 hook Swap (int128 amounts) | ModifyLiquidity (tuple PoolKey format) | getSlot0 on_event call | topics[3] (poolId) | Initializer itself emits events |
| MigrationPoolMetricsHandler | UniswapV4PoolManager + UniswapV4MigratorHook | PoolManager Swap (has sqrtPriceX96) | MigratorHook ModifyLiquidity | From event | topics[1] (id) | In-memory migration pool ID set for filtering |

---

## Event Signatures Reference

**V3 Swap**: `Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)`

**V3 Mint**: `Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)`

**V3 Burn**: `Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)`

**V4 Base Swap**: `Swap(int24 currentTick, uint256 totalProceeds, uint256 totalTokensSold)`

**V4 Hook Swap** (multicurve, decay, scheduled, dhook): `Swap(address indexed sender, (address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) indexed poolKey, bytes32 indexed poolId, (bool zeroForOne, int256 amountSpecified, uint160 sqrtPriceLimitX96) params, int128 amount0, int128 amount1, bytes hookData)`

**ModifyLiquidity (tuple format)** (multicurve, dhook): `ModifyLiquidity((address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) key, (int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt) params)`

**ModifyLiquidity (flat format)** (decay, scheduled): `ModifyLiquidity(bytes32 indexed id, address indexed sender, int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt)`

**PoolManager Swap**: `Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)`

**MigratorHook ModifyLiquidity**: `ModifyLiquidity((address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) key, (int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt) params)`

---

## Processing Model

| What | Ordering | Reason |
|------|----------|--------|
| Swap → pool_snapshots | Parallel OK | Per-block, idempotent upsert on conflict |
| Swap → pool_state | Parallel OK | Conditional upsert (only if newer block) |
| Mint/Burn/ModifyLiquidity → liquidity_deltas | Parallel OK | Append-only INSERT with PK |
| V4 base Swap (proceeds) | Sequential | In-memory cumulative state |
| TVL computation | Deferred | Separate future pass over liquidity_deltas |

---

## Shared Abstractions

### File structure

```
src/transformations/
├── util/
│   ├── tick_math.rs             # NEW — tick_to_sqrt_price_x96(), port of TickMath.getSqrtRatioAtTick
│   ├── pool_metadata.rs         # NEW — PoolMetadataCache (RwLock<HashMap>), quote_token_decimals()
│   └── db/pool_metrics.rs       # NEW — upsert_pool_state(), insert_pool_snapshot(), insert_liquidity_delta()
├── event/
│   ├── metrics/                 # NEW — shared metrics types
│   │   ├── mod.rs
│   │   ├── accumulator.rs       # BlockAccumulator — per-pool per-block OHLC + volume aggregation
│   │   └── swap_data.rs         # SwapInput, LiquidityInput, process_swaps(), process_liquidity_deltas()
│   ├── v3/metrics.rs            # NEW
│   ├── v4/metrics.rs            # NEW
│   ├── multicurve/metrics.rs    # NEW
│   ├── decay_multicurve/metrics.rs  # NEW
│   ├── scheduled_multicurve/metrics.rs  # NEW
│   ├── dhook/metrics.rs         # NEW
│   └── migration_pool/          # NEW module
│       ├── mod.rs
│       └── metrics.rs
```

### Core types

- **SwapInput**: Normalized swap data all pool types produce. Fields: pool_id, block_number, block_timestamp, log_index, amount0 (I256), amount1 (I256), sqrt_price_x96 (U256), tick (i32), liquidity (U256).
- **LiquidityInput**: Normalized liquidity delta. Fields: pool_id, block_number, log_index, tick_lower, tick_upper, liquidity_delta (I256).
- **BlockAccumulator**: Aggregates multiple swaps in one block into OHLC + volume. Has `record_swap()` method.
- **PoolMetadataCache**: Thread-safe cache loaded from `pools` table on init. Maps pool_id → (base_token, quote_token, is_token_0, decimals).
- **process_swaps()**: Takes `Vec<SwapInput>` → groups by (pool_id, block_number) → builds accumulators → emits `Vec<DbOperation>` for pool_snapshots + pool_state.
- **process_liquidity_deltas()**: Takes `Vec<LiquidityInput>` → emits `Vec<DbOperation>` for liquidity_deltas INSERT.

### Each handler's job

Extract events from its source → normalize to SwapInput/LiquidityInput → call shared process functions. The per-handler code is thin: just event-specific field extraction.

---

## Config Changes Required

### Add getSlot0 on_event calls (4 files)

Target: `UniswapV4StateView`. Triggered by Swap events. Pool ID from topics[3].

| Config File | Contract | Swap Event Source |
|---|---|---|
| `config/contracts/base/multicurve.json` | UniswapV4MulticurveInitializerHook | topics[3] |
| `config/contracts/base/decay_multicurve.json` | DecayMulticurveHook | topics[3] |
| `config/contracts/base/scheduled_multicurve.json` | UniswapV4ScheduledMulticurveInitializerHook | topics[3] |
| `config/contracts/base/dhook.json` | DopplerHookInitializer | topics[3] |

On_event call config pattern:
```json
{
    "function": "getSlot0(bytes32)",
    "output_type": "(uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)",
    "frequency": { "on_events": {
        "source": "<contract_name>",
        "event": "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)"
    }},
    "target": "UniswapV4StateView",
    "params": [{ "type": "bytes32", "from_event": "topics[3]" }]
}
```

### Add DopplerHookInitializer events (dhook.json)

Add to `DopplerHookInitializer.events[]`:
```json
{ "signature": "Swap(address indexed sender, (address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) indexed poolKey, bytes32 indexed poolId, (bool zeroForOne, int256 amountSpecified, uint160 sqrtPriceLimitX96) params, int128 amount0, int128 amount1, bytes hookData)" },
{ "signature": "ModifyLiquidity((address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) key, (int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt) params)" }
```

### V4 base — no config change needed

sqrtPriceX96 derived from currentTick via `tick_to_sqrt_price_x96()` in handler code.

---

## Phase 1: Foundation

**Goal**: Build all shared infrastructure. No handlers yet.

### Tasks
1. Write SQL migrations: `pool_state.sql`, `pool_snapshots.sql`, populate `liquidity_deltas.sql`
2. Implement `src/transformations/util/tick_math.rs` — port of Uniswap TickMath.getSqrtRatioAtTick()
3. Implement `src/transformations/util/pool_metadata.rs` — PoolMetadataCache, quote_token_decimals()
4. Implement `src/transformations/util/db/pool_metrics.rs` — DB operation builders
5. Implement `src/transformations/event/metrics/` — accumulator.rs, swap_data.rs, mod.rs
6. Wire new modules in util/mod.rs, util/db/mod.rs, event/mod.rs
7. Unit tests for tick_math, accumulator, process_swaps

### Key design decisions
- `pool_state` uses `RawSql` for conditional upsert (`WHERE block_number < EXCLUDED.block_number`). Handler must include source/source_version manually since RawSql skips auto-injection.
- `pool_snapshots` uses standard `Upsert` with all columns as update_columns (idempotent).
- `liquidity_deltas` uses standard `Insert`.
- Quote token decimals hardcoded: WETH=18, USDC=6, USDT=6, EURC=6. Extracted to a shared function from existing price handler pattern.

---

## Phase 2: V3 Handlers

**Goal**: Validate the full pipeline end-to-end with the simplest pool type.

### Tasks
1. Implement `src/transformations/event/v3/metrics.rs` — V3MetricsHandler + LockableV3MetricsHandler
2. Register in `v3/mod.rs` and `event/mod.rs`
3. Test: run against a block range with known V3 swap/mint/burn events, verify pool_state + pool_snapshots + liquidity_deltas

### Handler specifics
- Source: `DopplerV3Pool` / `DopplerLockableV3Pool`
- Pool ID = `event.contract_address` (20 bytes, stored as BYTEA)
- Swap: sqrtPriceX96, tick, liquidity all in event params
- Mint → LiquidityInput with positive liquidity_delta
- Burn → LiquidityInput with negative liquidity_delta (negate the `amount`)
- call_dependencies: none (all data in events)

---

## Phase 3: V4 Hook Handlers

**Goal**: Add metrics for multicurve, decay, scheduled, and dhook pools.

### Tasks
1. Config changes: add getSlot0 on_event calls to multicurve.json, decay_multicurve.json, scheduled_multicurve.json, dhook.json
2. Config change: add Swap + ModifyLiquidity events to DopplerHookInitializer in dhook.json
3. Implement `multicurve/metrics.rs` — MulticurveMetricsHandler
4. Implement `decay_multicurve/metrics.rs` — DecayMulticurveMetricsHandler
5. Implement `scheduled_multicurve/metrics.rs` — ScheduledMulticurveMetricsHandler
6. Implement `dhook/metrics.rs` — DhookMetricsHandler
7. Register all handlers

### Handler specifics
- Swap event: `int128 amount0, amount1` (need I256 conversion), poolId from topics[3]
- sqrtPriceX96 from getSlot0 call matched by `trigger_log_index == event.log_index`
- ModifyLiquidity: two formats (tuple vs flat) — extract tick_lower/upper/liquidityDelta accordingly
- Tuple format (multicurve, dhook): compute poolId from PoolKey via keccak hash
- Flat format (decay, scheduled): poolId from topics[1] (`id`)
- call_dependencies: `("UniswapV4StateView", "getSlot0")`

### Verify on_event call format
Test with one hook (e.g., DecayMulticurveHook) first. The `event` field in `frequency.on_events` is keccak-hashed and matched against log topic0. The ABI-canonical form (types only) is: `Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)`

---

## Phase 4: V4 Base Handler

**Goal**: Handle the unique cumulative-counter Swap event on DopplerV4Hook.

### Tasks
1. Implement `src/transformations/event/v4/metrics.rs` — V4BaseMetricsHandler
2. Register in `v4/mod.rs`
3. Test sequential processing with cumulative proceeds tracking

### Handler specifics
- Source: `DopplerV4Hook` (factory collection)
- Swap event: `Swap(int24 currentTick, uint256 totalProceeds, uint256 totalTokensSold)`
- **No poolId in event**: map hook contract_address → pool_id via `v4_pool_configs` table (loaded on init into `HashMap<[u8;20], [u8;32]>`)
- **No sqrtPriceX96 in event**: derive from currentTick via `tick_to_sqrt_price_x96()`
- **Cumulative counters**: in-memory `RwLock<HashMap<Vec<u8>, (U256, U256)>>` for (last_totalProceeds, last_totalTokensSold). Compute per-swap deltas. Rebuild from pool_snapshots or a dedicated column on init.
- **Sequential processing required**: this handler cannot be parallelized across block ranges
- No liquidity events (liquidity only changes via swaps or post-migration MigratorHook events)
- amount0/amount1 derived from proceeds/tokens_sold deltas using is_token_0 orientation

---

## Phase 5: Migration Pool Handler

**Goal**: Handle swaps and liquidity on graduated (migrated) V4 pools.

### Tasks
1. Create `src/transformations/event/migration_pool/` module
2. Implement `migration_pool/metrics.rs` — MigrationPoolMetricsHandler
3. Register in `event/mod.rs`

### Handler specifics
- Swap source: `UniswapV4PoolManager` — Swap event has sqrtPriceX96/tick/liquidity directly
- Liquidity source: `UniswapV4MigratorHook` — ModifyLiquidity (tuple format)
- **Must filter PoolManager Swaps**: PoolManager emits Swap for ALL V4 pools. Use in-memory `RwLock<HashSet<Vec<u8>>>` of migration pool IDs. Rebuild on init from `pools WHERE migrated_from IS NOT NULL`.
- Also scan for Migrate events in current context to add newly graduated pools inline
- call_dependencies: none (all data in events)

---

## Phase 6: USD Pricing & Rolling Metrics

**Goal**: Add USD-denominated values to snapshots and rolling metrics to pool_state.

### Tasks
1. Read ChainlinkEthOracle latestAnswer from transformation context (calls_of_type)
2. Read reference pool prices from `prices` table (written by existing PriceHandler)
3. Resolve quote token → USD price (WETH via ETH/USD, USDC/USDT directly, others via reference pools)
4. Add volume_usd to pool_snapshots
5. Add rolling aggregation queries for pool_state: volume_24h_usd, price_change_1h, price_change_24h, swap_count_24h

### Design notes
- Price resolution: for each swap, look up quote token USD price. If quote is WETH, multiply by latest ETH/USD. If quote is USDC/USDT, use 1.0. If quote is another token, chain through reference pool prices.
- Rolling metrics: query pool_snapshots for the relevant time window. This can be done as part of pool_state upsert or as a separate periodic handler.
- Leave architecture open for future price graph traversal.

---

## Phase 7: TVL Computation (future)

**Goal**: Compute TVL from in-memory tick maps and update pool_snapshots + pool_state.

### Tasks
1. Implement tick map store: `RwLock<HashMap<pool_id, BTreeMap<(tick_lower, tick_upper), i128>>>`
2. Rebuild on startup from liquidity_deltas table (one GROUP BY query)
3. Sequential pass: process liquidity_deltas in order, update tick maps, compute TVL per block
4. TVL formula: iterate tick map positions, compute token amounts based on position vs current tick
5. Add amount0, amount1, tvl_usd, market_cap_usd columns to pool_state and pool_snapshots
6. Market cap = token price × total supply (total_supply from tokens table)

### Design notes
- This pass is sequential by nature (tick maps are cumulative state)
- Could be a separate handler or a background task
- Consider materialized views for protocol-wide TVL aggregation

---

## Resolved Design Decisions

- **Separate handlers per pool type** with shared `process_swaps()`/`process_liquidity_deltas()` functions
- **Per-block granularity** snapshots, candles built at query time
- **Out-of-order safe**: pool_state conditional upsert, pool_snapshots idempotent, liquidity_deltas append-only
- **V4 base sqrtPriceX96**: derived from tick in code, no extra RPC call
- **V4 base pool_id**: hook_address → pool_id via `v4_pool_configs` table (has `hook_address` and `pool_id` columns from create handlers)
- **DopplerHookInitializer events**: emitted by the initializer contract itself (not a separate hook)
- **DopplerHookInitializer ModifyLiquidity**: tuple format `ModifyLiquidity((PoolKey) key, (ModifyLiquidityParams) params)`, same as multicurve
- **Quote token decimals**: hardcoded lookup (WETH=18, USDC=6, USDT=6, EURC=6), extracted to shared util
- **Indexed poolKey in Swap**: keccak hash of PoolKey = pool_id, so topics[2] = topics[3]
- **getSlot0 on_event calls**: configured on hook contracts with `target: UniswapV4StateView`
- **TVL deferred**: liquidity_deltas written in parallel, TVL computed in a separate sequential pass (Phase 7)
