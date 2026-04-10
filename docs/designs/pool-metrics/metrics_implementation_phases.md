# Pool Metrics Handlers — Implementation Roadmap

## Context

The old TypeScript indexer maintained only point-in-time pool state (reserves, price, market cap, etc.), updated on every event. The new Rust indexer produces **per-block time-series snapshots** alongside a hot-query **pool_state** table, enabling OHLC candles and historical protocol analytics at query time.

This roadmap covers metrics handlers for all 9 pool type variants, shared abstractions, SQL migrations, and contract config changes. It is designed to be implemented across multiple sessions.

---

## Tables

### pool_state — hot query target for dashboards

Keyed by `(chain_id, pool_id, source, source_version)`. Contains latest tick, sqrt_price_x96, price, active_liquidity (raw L), and rolling 24h fields (volume_24h_usd, price_change_1h/24h, swap_count_24h — Phase 6). Phase 7 adds `amount0`, `amount1`, `tvl_usd`, `active_liquidity_usd`, `market_cap_usd`, and `total_supply`. Conditional upsert: only update if new `block_number > existing`.

### pool_snapshots — per-block time series

Keyed by `(chain_id, pool_id, block_number, source, source_version)`. OHLC prices, active_liquidity (raw L), volume0/1, swap_count, volume_usd (Phase 6). Phase 7 adds `amount0`, `amount1`, `tvl_usd`, `active_liquidity_usd`, `market_cap_usd`. One row per (pool, block) with activity. INSERT ON CONFLICT updates all non-TVL columns (idempotent); TVL columns are populated by a second pass via the TVL handler's RawSql UPDATE.

### liquidity_deltas — append-only recovery log

Keyed by `(chain_id, pool_id, block_number, log_index)`. tick_lower, tick_upper, liquidity_delta. Simple INSERT. Used by Phase 7's TVL handler to rebuild per-range tick maps (stateless re-query, not an in-memory cache).

---

## Handler Matrix

| Handler | Source | Event | sqrtPriceX96 Source | Pool ID Source | Phase | Special |
|---------|--------|-------|--------------------|----|-------|---------|
| V3SwapMetricsHandler | DopplerV3Pool | V3 Swap | From event | contract_address | 2 ✅ | — |
| V3LiquidityMetricsHandler | DopplerV3Pool | Mint/Burn | — | contract_address | 2 ✅ | Insert-only |
| LockableV3SwapMetricsHandler | DopplerLockableV3Pool | V3 Swap | From event | contract_address | 2 ✅ | — |
| LockableV3LiquidityMetricsHandler | DopplerLockableV3Pool | Mint/Burn | — | contract_address | 2 ✅ | Insert-only |
| MulticurveSwapMetricsHandler | UniswapV4MulticurveInitializerHook | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 3 ✅ | — |
| MulticurveLiquidityMetricsHandler | UniswapV4MulticurveInitializerHook | ModifyLiquidity (tuple) | — | PoolKey::pool_id() | 3 ✅ | — |
| DecayMulticurveSwapMetricsHandler | DecayMulticurveHook | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 3 ✅ | — |
| DecayMulticurveLiquidityMetricsHandler | DecayMulticurveHook | ModifyLiquidity (flat) | — | extract_bytes32("id") | 3 ✅ | — |
| ScheduledMulticurveSwapMetricsHandler | UniswapV4ScheduledMulticurveInitializerHook | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 3 ✅ | — |
| ScheduledMulticurveLiquidityMetricsHandler | UniswapV4ScheduledMulticurveInitializerHook | ModifyLiquidity (flat) | — | extract_bytes32("id") | 3 ✅ | — |
| DhookSwapMetricsHandler | DopplerHookInitializer | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 3 ✅ | Initializer emits events |
| DhookLiquidityMetricsHandler | DopplerHookInitializer | ModifyLiquidity (tuple) | — | PoolKey::pool_id() | 3 ✅ | — |
| V4BaseMetricsHandler | DopplerV4Hook | `Swap(int24, uint256, uint256)` | tick_to_sqrt_price_x96() | hook_address → v4_pool_configs | 4 ✅ | **Sequential** proceeds tracker |
| MigrationPoolMetricsHandler | UniswapV4PoolManager + MigratorHook | PoolManager Swap + ModifyLiquidity | From event | topics[1] (id) | 5 | Migration pool ID filter |
| V3TvlMetricsHandler | DopplerV3Pool | V3 Swap | From event | contract_address | 7 | Stateless tick-map UPDATE |
| LockableV3TvlMetricsHandler | DopplerLockableV3Pool | V3 Swap | From event | contract_address | 7 | Stateless tick-map UPDATE |
| MulticurveTvlMetricsHandler | UniswapV4MulticurveInitializerHook | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 7 | Stateless tick-map UPDATE |
| DecayMulticurveTvlMetricsHandler | DecayMulticurveHook | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 7 | Stateless tick-map UPDATE |
| ScheduledMulticurveTvlMetricsHandler | UniswapV4ScheduledMulticurveInitializerHook | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 7 | Stateless tick-map UPDATE |
| DhookTvlMetricsHandler | DopplerHookInitializer | V4 hook Swap | getSlot0 on_event call | extract_bytes32("poolId") | 7 | Stateless tick-map UPDATE |
| MigrationPoolTvlMetricsHandler | UniswapV4MigratorHook | PoolManager Swap | From event | topics[1] (id) | 7 | Stateless tick-map UPDATE |

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
| V4 base Swap (proceeds) | **Sequential** (`requires_sequential=true`) | Cumulative totals require ordered per-pool deltas; enforced via capacity-1 FIFO semaphore in catchup |
| TVL + active liquidity USD → pool_snapshots/pool_state | Parallel OK | Stateless per-range: re-queries `liquidity_deltas` each call. Ordering with respect to swap/liquidity handlers provided by `contiguous_handler_dependencies`, not `requires_sequential`. |

---

## Shared Abstractions

### File structure

```
src/transformations/
├── util/
│   ├── tick_math.rs                    # tick_to_sqrt_price_x96() (Phase 1)
│   ├── pool_metadata.rs               # PoolMetadataCache (Phase 1)
│   └── db/pool_metrics.rs             # upsert_pool_state(), insert_pool_snapshot(), insert_liquidity_delta() (Phase 1)
├── event/
│   ├── metrics/                        # Shared metrics types
│   │   ├── mod.rs
│   │   ├── accumulator.rs             # BlockAccumulator (Phase 1)
│   │   ├── swap_data.rs              # SwapInput, LiquidityInput, process_swaps(), process_liquidity_deltas(), refresh_cache_if_needed() (Phase 1, refactored Phase 3)
│   │   └── v4_hook_extract.rs        # extract_v4_hook_swaps(), extract_tuple_modify_liquidity(), extract_flat_modify_liquidity() (Phase 3)
│   ├── v3/metrics.rs                  # V3 swap + liquidity handlers (Phase 2)
│   ├── multicurve/metrics.rs          # Multicurve swap + liquidity handlers (Phase 3)
│   ├── decay_multicurve/metrics.rs    # Decay multicurve swap + liquidity handlers (Phase 3)
│   ├── scheduled_multicurve/metrics.rs # Scheduled multicurve swap + liquidity handlers (Phase 3)
│   ├── dhook/metrics.rs               # Dhook swap + liquidity handlers (Phase 3)
│   ├── v4/metrics.rs                  # V4 base (DopplerV4Hook) handler (Phase 4)
│   └── migration_pool/metrics.rs      # Migration pool handler (Phase 5 — future)
```

### Core types

- **SwapInput**: Normalized swap data all pool types produce. Fields: pool_id, block_number, block_timestamp, log_index, amount0 (I256), amount1 (I256), sqrt_price_x96 (U256), tick (i32), liquidity (U256).
- **LiquidityInput**: Normalized liquidity delta. Fields: pool_id, block_number, log_index, tick_lower, tick_upper, liquidity_delta (I256).
- **BlockAccumulator**: Aggregates multiple swaps in one block into OHLC + volume. Has `record_swap()` method.
- **PoolMetadataCache**: Thread-safe cache loaded from `pools` table on init. Maps pool_id → (base_token, quote_token, is_token_0, decimals).
- **process_swaps()**: Takes `Vec<SwapInput>` → groups by (pool_id, block_number) → builds accumulators → emits `Vec<DbOperation>` for pool_snapshots + pool_state.
- **process_liquidity_deltas()**: Takes `Vec<LiquidityInput>` → emits `Vec<DbOperation>` for liquidity_deltas INSERT.

### Shared extraction functions

- **V3**: `extract_v3_swaps()`, `extract_v3_liquidity()` in `v3/metrics.rs` — extract from Swap/Mint/Burn events using `contract_address` as pool_id
- **V4 hooks**: `extract_v4_hook_swaps()`, `extract_tuple_modify_liquidity()`, `extract_flat_modify_liquidity()` in `metrics/v4_hook_extract.rs` — extract from V4 hook events using `poolId` bytes32 field or computed from PoolKey
- **refresh_cache_if_needed()**: in `metrics/swap_data.rs` — re-queries DB if swap pools are absent from cache; shared by all swap handlers

### Each handler's job

Extract events from its source → normalize to SwapInput/LiquidityInput → call shared process functions. The per-handler code is thin: just event-specific field extraction.

---

## Config Changes Required

### getSlot0 on_event calls ✅ (Phase 3)

Added to all 4 hook contracts across all chain directories (15 JSON files). Each call targets `UniswapV4StateView`, triggered by the hook's Swap event, with pool ID from `topics[3]`.

| Config | Hook Contract | Chains |
|---|---|---|
| `multicurve.json` | UniswapV4MulticurveInitializerHook | base, baseSepolia |
| `decay_multicurve.json` | DecayMulticurveHook | base, baseSepolia, sepolia |
| `scheduled_multicurve.json` | UniswapV4ScheduledMulticurveInitializerHook | base, baseSepolia, mainnet, monad, sepolia |
| `dhook.json` | DopplerHookInitializer | base, baseSepolia, mainnet, monad, sepolia |

### DopplerHookInitializer events ✅ (Phase 3)

Swap and ModifyLiquidity (tuple format) events added to `DopplerHookInitializer.events[]` across all dhook.json chain files.

### V4 base — no config change needed

sqrtPriceX96 derived from currentTick via `tick_to_sqrt_price_x96()` in handler code.

---

## Phase 1: Foundation ✅

**Status**: Complete — PR #79

**Delivered**:
- SQL migrations: `pool_state.sql`, `pool_snapshots.sql`, `liquidity_deltas.sql`
- `src/transformations/util/tick_math.rs` — `tick_to_sqrt_price_x96()` with 8 passing tests
- `src/transformations/util/pool_metadata.rs` — `PoolMetadataCache` + `quote_token_decimals()`
- `src/transformations/util/db/pool_metrics.rs` — `upsert_pool_state()`, `insert_pool_snapshot()`, `insert_liquidity_delta()`
- `src/transformations/event/metrics/accumulator.rs` — `BlockAccumulator` with 2 passing tests
- `src/transformations/event/metrics/swap_data.rs` — `SwapInput`, `LiquidityInput`, `process_swaps()`, `process_liquidity_deltas()`
- All modules wired in `util/mod.rs`, `util/db/mod.rs`, `event/mod.rs`

### Key design decisions (for reference by later phases)
- `pool_state` uses `DbOperation::Upsert` with `update_condition: Some("EXCLUDED.block_number > pool_state.block_number")`. The conditional update ensures stale retried blocks are no-ops (`affected_rows = 0`), which also prevents bogus rollback snapshots from being recorded.
- `pool_snapshots` uses `DbOperation::Upsert` with all non-key columns as `update_columns` and no `update_condition` (unconditional, idempotent). source/source_version are auto-injected by the executor.
- `liquidity_deltas` uses `DbOperation::Upsert` with `update_columns: vec![]` (ON CONFLICT DO NOTHING). source/source_version are auto-injected.
- Quote token decimals: WETH=18, USDC=6, USDT=6, EURC=6. Resolved lazily on first `handle()` via `resolve_quote_decimals()` using `std::sync::Once` for thread safety.

---

## Phase 2: V3 Handlers ✅

**Status**: Complete — PR #96

**Delivered**:
- `src/transformations/event/v3/metrics.rs` — `V3SwapMetricsHandler`, `V3LiquidityMetricsHandler`, `LockableV3SwapMetricsHandler`, `LockableV3LiquidityMetricsHandler`. Split swap/liquidity into separate handlers to avoid multi-trigger snapshot capture issues (see issue #95)
- `PoolMetadataCache` additions: `load_into()` for shared cache instances, `resolve_quote_decimals()`, `refresh(contracts)` for DB re-query on cache miss with immediate decimal resolution under the same write lock
- `liquidity_deltas` changed from INSERT to Upsert with `update_columns: vec![]` (ON CONFLICT DO NOTHING) — prevents duplicate key errors on re-runs
- DDL: all three tables now use UNIQUE constraints including `source`/`source_version` instead of PRIMARY KEY; added `idx_*_reorg` indexes on `(chain_id, block_number)` for cleanup queries
- `handler_dependencies()`: V3SwapMetricsHandler and V3LiquidityMetricsHandler depend on V3CreateHandler; Lockable variants depend on LockableV3CreateHandler
- `PoolMetadataCache` crash recovery via `load_into()` in `initialize()` — no separate persistence needed

**Correctness fixes applied during review (PRs #93, #94)**:
- Removed pre-commit `insert_if_absent` from create handlers — cache was populated before DB commit, poisoning it on write failures
- `refresh_cache_if_needed` now returns `Result` and propagates errors — missing pool metadata causes handler retry instead of silent data loss
- `std::sync::Once` replaces `AtomicBool + Ordering::Relaxed` for decimal resolution — prevents concurrent catchup tasks from reading `quote_decimals = 18` before resolution completes
- `refresh()` resolves quote decimals under the same write lock that inserts new entries — closes a two-lock race window
- Live event paths (`process_events_message`, `try_process_pending_events`) now use `WithSnapshotCapture` for single-trigger handlers — swap handlers previously wrote `pool_state` with no rollback snapshots
- Snapshot recording gated on `affected_rows > 0` — no-op conditional upserts no longer produce bogus rollback snapshots
- Per-block fallback DELETE in `cleanup_reorg_tables` — was skipping entire tables when any orphaned block had a snapshot; now only skips the specific covered blocks per table
- Retry loop checks completed ranges before executing — prevents duplicate snapshots from double-writing a handler that already committed

### Established patterns (reference for all future handlers)

**Handler struct pattern** (single-trigger swap handler):
```rust
pub struct MySwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,  // shared with sibling handlers
    decimals_init: std::sync::Once,          // guards one-time resolve in handle()
    chain_id: u64,
    db_pool: OnceLock<Pool>,                 // set in initialize(), used by refresh_cache_if_needed
}
```

**Lifecycle**:
1. `register_handlers()`: create shared `Arc<PoolMetadataCache>`, construct handlers, pass `Arc::clone` to each
2. `initialize(&self, db_pool)`: call `self.metadata_cache.load_into(db_pool, chain_id)` — loads pools from DB, is a no-op if sibling handler already loaded. Store `db_pool.inner().clone()` in `self.db_pool`.
3. First `handle()`: call `self.decimals_init.call_once(|| self.metadata_cache.resolve_quote_decimals(&ctx.contracts))` — `call_once` blocks all concurrent callers until done, resolves WETH=18, USDC=6, USDT=6, EURC=6
4. Every `handle()`: call `refresh_cache_if_needed(...).await?` — re-queries DB if any swap pool is absent from cache. Returns `Err` on DB failure or if pools still missing after refresh; this causes the handler to fail and be retried.
5. Delegate to `process_swaps()` / `process_liquidity_deltas()`

**Extraction helpers**: defined as module-level `fn` to share between handler structs:
```rust
fn extract_v3_swaps(ctx: &TransformationContext, source: &str) -> Result<Vec<SwapInput>, TransformationError>
fn extract_v3_liquidity(ctx: &TransformationContext, source: &str) -> Result<Vec<LiquidityInput>, TransformationError>
```

**Reorg tables declaration**:
- Swap handlers: `reorg_tables() = ["pool_state", "pool_snapshots"]` — snapshot capture required; single-trigger only
- Liquidity handlers: `reorg_tables() = ["liquidity_deltas"]` — append-only; fallback DELETE is correct; safe for multi-trigger

**Registration pattern** in `event/mod.rs`:
```rust
v3::metrics::register_handlers(registry, chain_id, Arc::clone(&metadata_cache));
```

---

## Phase 3: V4 Hook Handlers ✅

**Status**: Complete

**Goal**: Add metrics for multicurve, decay, scheduled, and dhook pools.

**Delivered**:
- Config changes: getSlot0 on_event calls added to all 4 hook contract configs across all chain directories (15 JSON files total)
- Config change: Swap + ModifyLiquidity events added to DopplerHookInitializer in all dhook.json files
- `src/transformations/event/metrics/v4_hook_extract.rs` — shared extraction functions: `extract_v4_hook_swaps()`, `extract_tuple_modify_liquidity()`, `extract_flat_modify_liquidity()`
- `src/transformations/event/multicurve/metrics.rs` — MulticurveSwapMetricsHandler, MulticurveLiquidityMetricsHandler
- `src/transformations/event/decay_multicurve/metrics.rs` — DecayMulticurveSwapMetricsHandler, DecayMulticurveLiquidityMetricsHandler
- `src/transformations/event/scheduled_multicurve/metrics.rs` — ScheduledMulticurveSwapMetricsHandler, ScheduledMulticurveLiquidityMetricsHandler
- `src/transformations/event/dhook/metrics.rs` — DhookSwapMetricsHandler, DhookLiquidityMetricsHandler
- `refresh_cache_if_needed()` moved from `v3/metrics.rs` to shared `metrics/swap_data.rs`
- All 8 handlers registered in their respective `mod.rs` + `event/mod.rs`

**Key design decisions**:
- Split into swap + liquidity handlers per pool type (same rationale as Phase 2: avoids multi-trigger snapshot capture issues)
- Each pool type gets its own `Arc<PoolMetadataCache>` (not shared with create handlers since create handlers don't use the cache)
- V4 hook swap handlers declare `call_dependencies` on `getSlot0` to ensure the on_event call results are available before handle() runs
- Shared extraction functions in `metrics/v4_hook_extract.rs` avoid code duplication across 4 handler files
- Two ModifyLiquidity extraction variants: tuple format (multicurve, dhook) computes pool_id from PoolKey, flat format (decay, scheduled) reads pool_id from event topics

### Established patterns (reference for all future V4 hook handlers)

**V4 hook extraction** — all 4 handlers use shared functions from `metrics/v4_hook_extract.rs`:
- `extract_v4_hook_swaps(ctx, event_source, call_source)` — matches getSlot0 calls by `trigger_log_index`; pool_id from `extract_bytes32("poolId")`; sqrtPriceX96/tick from getSlot0 result; reverted calls are skipped with a warning
- `extract_tuple_modify_liquidity(ctx, source)` — for multicurve/dhook; builds `PoolKey` from `key.*` fields, computes `pool_id = pool_key.pool_id()` (keccak256)
- `extract_flat_modify_liquidity(ctx, source)` — for decay/scheduled; pool_id from `extract_bytes32("id")`

**call_dependencies**: Swap handlers declare `vec![(hook_contract_name, "getSlot0")]`. The `source_name` on decoded calls is set from `EventTriggeredCallConfig.contract_name` (the hook contract where the call is configured, NOT the target `UniswapV4StateView`).

**handler_dependencies**: Both swap and liquidity handlers depend on their respective Create handler (e.g., `V4MulticurveCreateHandler`).

**Registration** in `event/mod.rs` — each pool type creates its own `Arc<PoolMetadataCache>`:
```rust
let multicurve_cache = Arc::new(PoolMetadataCache::new());
multicurve::metrics::register_handlers(registry, chain_id, multicurve_cache);
```

**Trigger signatures** (ABI-canonical, types only):
- Swap: `Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)`
- ModifyLiquidity (tuple): `ModifyLiquidity((address,address,uint24,int24,address),(int24,int24,int256,bytes32))`
- ModifyLiquidity (flat): `ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)`

---

## Phase 4: V4 Base Handler ✅

**Status**: Complete

**Goal**: Handle the unique cumulative-counter Swap event on DopplerV4Hook.

**Delivered**:
- `migrations/tables/v4_base_proceeds_state.sql` — checkpoint table for cumulative proceeds per pool
- `src/transformations/event/v4/metrics.rs` — V4BaseMetricsHandler (9 passing tests)
- `src/transformations/traits.rs` — `requires_sequential()` method added to TransformationHandler
- `src/transformations/engine.rs` — capacity-1 FIFO semaphore for sequential handlers in catchup

**Key design decisions**:
- **Sequential enforcement**: `requires_sequential() = true` causes the catchup engine to create a capacity-1 semaphore for this handler. Tokio's FIFO `acquire_owned()` guarantee ensures ranges execute in ascending order, preventing delta corruption.
- **In-memory cache + durable checkpoint**: cumulative `(totalProceeds, totalTokensSold)` per pool is kept in a `RwLock<HashMap>` loaded from `v4_base_proceeds_state` at init and refilled on miss (for just the missing pool IDs, using `pool_id = ANY($4)`). Pools with no checkpoint get cached as `(0, 0)` so misses aren't re-queried. `handle()` reads from the cache, returns upserts to `v4_base_proceeds_state` inside the same transaction as the snapshot writes, and updates the cache optimistically. On restart the cache rehydrates from the last-committed checkpoint.
- **hook_address → pool_id lookup**: loaded from `v4_pool_configs` at init, refreshed on cache miss. `handler_dependencies = ["V4CreateHandler"]` ensures the config exists.
- **sqrtPriceX96**: derived from `currentTick` via `tick_to_sqrt_price_x96()` (no extra RPC call).
- **amount0/amount1**: `proceeds_delta` = quote inflow (positive for pool), `tokens_delta` = base outflow (negative for pool). Signs flipped via `is_token_0` from metadata cache.
- **liquidity**: set to `U256::ZERO` — not available from the V4 base Swap event.
- **No liquidity handler**: DopplerV4Hook has no ModifyLiquidity events (the pool only sells tokens; migration liquidity is handled by Phase 5's MigratorHook handler).

---

## Phase 5: Migration Pool Handler ✅

**Status**: Complete

**Goal**: Handle swaps and liquidity on graduated (migrated) V4 pools.

**Delivered**:
- `src/transformations/event/migration_pool/mod.rs`
- `src/transformations/event/migration_pool/create.rs` — MigrationPoolCreateHandler
- `src/transformations/event/migration_pool/metrics.rs` — MigrationPoolSwapMetricsHandler, MigrationPoolLiquidityMetricsHandler
- `src/transformations/util/db/pool.rs` — `MigrationPoolData` + `insert_migration_pool()`
- All 3 handlers registered in `event/mod.rs`

**Key design decisions**:
- **Create handler required**: no existing handler inserted migration pool rows into `pools`. `MigrationPoolCreateHandler` handles `UniswapV4Migrator.Migrate`, queries the original Doppler pool via `migration_pool = poolId`, and inserts a new row with `migrated_from` set. This enables `PoolMetadataCache` to work normally for migration pools.
- **Swap filtering**: `MigrationPoolSwapMetricsHandler` holds `RwLock<HashSet<Vec<u8>>>` of migration pool IDs. Seeded at init from `pools WHERE migrated_from IS NOT NULL`. Inline scan of `Migrate` events in the current context handles same-range Migrate+Swap edge cases.
- **Liquidity**: `MigrationPoolLiquidityMetricsHandler` triggers on `UniswapV4MigratorHook.ModifyLiquidity` (tuple format). No filter needed — MigratorHook only handles migration pools. Uses `extract_tuple_modify_liquidity()`.
- **Dependencies**: both metrics handlers depend on `MigrationPoolCreateHandler` so the pool row and metadata exist before metrics handlers run.
- **Chains**: 5 chains have Migrator + MigratorHook (base, baseSepolia, mainnet, unichain, sepolia). ink and monad have PoolManager but no migration infrastructure — handlers simply find no events on those chains.

---

## Phase 6: USD Pricing & Rolling Metrics ✅

**Status**: Complete

**Goal**: Add USD-denominated values to snapshots and rolling metrics to pool_state.

### Implementation
- **New module**: `src/transformations/util/usd_price.rs` — `OraclePriceCache` (shared, DB-backed) + `UsdPriceContext` (per-invocation)
- **Migration**: `pool_snapshots_add_volume_usd.sql` adds nullable `volume_usd` column
- **USD resolution**: WETH via ChainlinkEthOracle `latestAnswer` (8 decimals), USDC/USDT at $1, EURC via prices table (EURC/USDC from PriceHandler)
- **Volume**: Single-side (quote-side only), standard DeFi convention
- **Rolling metrics**: `DbOperation::RawSql` UPDATE with backward-looking LATERAL subqueries on `pool_snapshots`, executed in-transaction after snapshot inserts
- **Price change**: Backward-looking — compares current `price_close` to last known `price_close` at or before the 1h/24h mark
- **Oracle persistence**: ETH/USD written to `prices` table (source = "chainlink"). `OraclePriceCache` shared across all 8 swap handlers, seeded from DB on startup
- **All 8 swap handlers** updated with shared `Arc<OraclePriceCache>`, new `process_swaps()` signature

---

## Phase 7: TVL + Active Liquidity USD

**Goal**: Add dollar-denominated TVL, active-liquidity-in-USD, and market cap to `pool_snapshots` and `pool_state`, computed from per-range tick maps rebuilt out of the `liquidity_deltas` table.

### Handler shape

- **Per-pool-type TVL handler**, 7 total: `V3TvlMetricsHandler`, `LockableV3TvlMetricsHandler`, `MulticurveTvlMetricsHandler`, `DecayMulticurveTvlMetricsHandler`, `ScheduledMulticurveTvlMetricsHandler`, `DhookTvlMetricsHandler`, `MigrationPoolTvlMetricsHandler`. Mirrors the existing `*SwapMetricsHandler` / `*LiquidityMetricsHandler` split.
- **Stateless** — each `handle()` call re-queries `liquidity_deltas` per target block. No in-memory tick map cache, no checkpoint table, no `requires_sequential = true`. The cost is O(pools-in-range) extra queries per range; the benefit is simple reorg behaviour (nothing to invalidate) and continued parallelism across TVL handler ranges.
- **Ordering via `contiguous_handler_dependencies = [swap_handler, liquidity_handler]`** — the DAG scheduler waits until both dependencies have committed contiguously through the TVL handler's `range_start` before it runs, so the `liquidity_deltas` query sees a complete tick map and the `pool_snapshots` rows it UPDATEs already exist. No new scheduler logic.
- **Cross-handler writes via `DbOperation::RawSql` + `DbSnapshot`** — the TVL handler owns a different `source` than the swap handler but UPDATEs rows keyed by the swap handler's source. Same pattern Phase 6 uses for rolling metrics (`swap_data.rs::build_rolling_metrics_update`). `DbSnapshot` captures the pre-update column values so reorgs roll them back.
- **Source version coupled to swap handler**: the TVL handler's own `source_version` is used in the UPDATE WHERE clause. Version bumps are kept in lockstep so TVL-v1 rows always UPDATE swap-v1 rows.

### New columns

Two migration files add nullable columns (so existing rows remain valid):

**`pool_snapshots`** (via `pool_snapshots_add_tvl.sql` and a Phase 7 follow-on migration for active liquidity):
- `amount0 NUMERIC` — total token0 locked across all positions at this block
- `amount1 NUMERIC` — total token1 locked across all positions at this block
- `tvl_usd NUMERIC` — dollar value of `(amount0, amount1)`
- `market_cap_usd NUMERIC` — `price_close × total_supply × usd_per_quote`
- `active_liquidity_usd NUMERIC` — dollar value of positions that straddle the current tick only (Uniswap v3 convention: `tick_lower ≤ current_tick < tick_upper`). The raw `L` lives in the existing `active_liquidity` column; this adds the dollar version.

**`pool_state`** — same four USD-column set plus `total_supply NUMERIC` for dashboard convenience.

### Computation (shared module: `src/transformations/event/metrics/tvl.rs`)

- `TickMapPosition { tick_lower: i32, tick_upper: i32, liquidity: u128 }` — one active position.
- `query_tick_map(db, chain_id, pool_id, target_block, liquidity_source, liquidity_source_version)` — `SUM(liquidity_delta)` grouped by `(tick_lower, tick_upper)` with `HAVING SUM > 0`. Casts via `::text` to preserve NUMERIC precision past rust_decimal's 28-digit range; errors rather than silently clamps if the aggregate exceeds `u128`.
- `compute_position_amounts(positions, current_sqrt_price_x96)` — iterates **all** positions, using the tick_math helpers to split each position's liquidity into raw `(amount0, amount1)` contributions based on whether it sits below / above / straddling the current tick. Sums with saturating U256 addition.
- `compute_active_liquidity_amounts(positions, current_tick, current_sqrt_price_x96)` — same walk with a filter to **only** positions where `tick_lower ≤ current_tick < tick_upper`. These represent liquidity immediately available to absorb a swap at the current price.
- `compute_tvl_usd(amount0_raw, amount1_raw, meta, price_close, usd_ctx)` — generic raw-amounts → USD. Used for both total TVL and active liquidity USD (the conversion is identical; only the input amounts differ).
- `compute_market_cap_usd(meta, price_close, usd_ctx)` — `price_close × total_supply × usd_per_quote`. Needs `meta.total_supply`, which is loaded via the LEFT JOIN on `tokens` added in the Phase 7 foundations PR.
- `build_snapshot_tvl_update(...)` / `build_state_tvl_update(...)` — `DbOperation::RawSql` UPDATEs that populate all five TVL/active-liquidity columns on a row owned by the `target_source` handler. Include `DbSnapshot` with the full key set for reorg rollback.

### Out of scope

- **Backfilling raw `active_liquidity` for V4 hook snapshots** (currently zero because V4 hook Swap events don't carry `liquidity`). Trivial follow-up: walk the tick map for positions straddling `current_tick` and sum their `u128` liquidities. Not required for the dollar-valued metric since `active_liquidity_usd` is computed independently from the tick map.
- **Protocol-wide TVL materialized view** — leave for a follow-up.
- **In-memory tick map cache + checkpoint table** — can be added later if profiling shows the per-range `liquidity_deltas` queries are a bottleneck. Would require flipping the handler to `requires_sequential = true`.
- **Liquidity-only block snapshots** — Phase 7 only updates blocks that already have a `pool_snapshots` row (i.e. swap blocks). Liquidity-only blocks get their TVL captured on the next swap in that pool.

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
- **getSlot0 on_event calls**: configured on hook contracts with `target: UniswapV4StateView`; `source_name` on decoded calls = hook contract name (not target)
- **TVL stateless**: Phase 7 re-queries `liquidity_deltas` per range instead of maintaining an in-memory tick map cache. Avoids checkpoint tables, reorg invalidation, and `requires_sequential`. Ordering is provided by `contiguous_handler_dependencies = [swap_handler, liquidity_handler]` which gates the TVL handler on both siblings committing contiguously through its `range_start`.
- **Per-pool-type TVL handlers**: 7 handlers mirroring the swap/liquidity split, each with its own Swap-event trigger. Keeps swap handlers parallel/unchanged and preserves the Phases 2–6 parallelism model.
- **Cross-handler TVL writes**: TVL handler UPDATEs rows owned by the swap handler via `DbOperation::RawSql` with the swap handler's name hardcoded in the WHERE clause. Matches Phase 6's rolling metrics pattern. `DbSnapshot` captures the full key set for reorg rollback.
- **Active liquidity USD**: dollar-valued "liquidity at current tick" is computed from the same tick map as TVL by filtering to straddling positions (`tick_lower ≤ current_tick < tick_upper`) and running the same raw-amount → USD conversion. The pre-existing `active_liquidity` column continues to hold the Uniswap v3 `L` parameter (not a dollar amount); `active_liquidity_usd` is the new dollar-valued column.
- **Total supply in metadata cache**: `PoolMetadata.total_supply: Option<U256>`, loaded via LEFT JOIN on `tokens` using `::text` to preserve NUMERIC precision beyond rust_decimal's 28-digit range. `refresh()` opportunistically backfills it when an earlier load saw `None` due to the race between the create-handler commit and the tokens-row commit.
- **V4 hook swap/liquidity split**: same rationale as V3 — avoids multi-trigger snapshot capture issues
- **V4 hook metadata caches**: each pool type gets its own `Arc<PoolMetadataCache>`, not shared with create handlers (create handlers don't use the cache)
- **Shared V4 extraction functions**: `metrics/v4_hook_extract.rs` avoids duplication across 4 handler files
- **`refresh_cache_if_needed` shared**: moved from `v3/metrics.rs` to `metrics/swap_data.rs` for reuse by all swap handlers
- **Sequential handlers**: `TransformationHandler::requires_sequential()` method gates the catchup semaphore at capacity 1, with Tokio's FIFO permit ordering guaranteeing ascending-range execution. Per-handler, not global.
- **V4 base proceeds state**: in-memory `RwLock<HashMap<[u8;32], (U256, U256)>>` cache + durable `v4_base_proceeds_state` checkpoint table. Loaded at init, refilled only for missing pool IDs via `pool_id = ANY($4)`. Optimistic update on commit; restart rehydrates from checkpoint. Minimizes DB reads while staying durable across process restarts.
- **V4 base amount signs**: `proceeds_delta` is quote inflow, `tokens_delta` is base outflow. `is_token_0=true` → `amount0 = -tokens_delta`, `amount1 = proceeds_delta`; reversed for `is_token_0=false`.
- **V4 base liquidity**: `U256::ZERO` — the V4 base Swap event doesn't carry liquidity, and Doppler auction "active liquidity" isn't meaningful in the same way as a v3 pool.
