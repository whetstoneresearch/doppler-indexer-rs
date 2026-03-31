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
- `pool_state` uses `RawSql` for conditional upsert (`WHERE block_number < EXCLUDED.block_number`). Handler must include source/source_version manually since RawSql skips auto-injection.
- `pool_snapshots` uses standard `Upsert` with all columns as update_columns (idempotent). source/source_version are auto-injected by the executor.
- `liquidity_deltas` uses standard `Insert`. source/source_version are auto-injected.
- Quote token decimals hardcoded: WETH=18, USDC=6, USDT=6, EURC=6. Implemented in `pool_metadata::quote_token_decimals()`.

---

## Phase 2: V3 Handlers ✅

**Status**: Complete — `feat/pool-metrics-phase2` branch

**Delivered**:
- `src/transformations/event/v3/metrics.rs` — V3MetricsHandler + LockableV3MetricsHandler
- Fixed `PoolMetadataCache` API: `load_from_db()` no longer requires contracts, added `load_into()` for shared caches, added `resolve_quote_decimals()` for lazy decimal resolution
- Fixed `liquidity_deltas` from INSERT to Upsert with empty update_columns (ON CONFLICT DO NOTHING) to prevent duplicate key errors on re-runs
- Updated DDL: `liquidity_deltas` changed from PRIMARY KEY to UNIQUE constraint including source/source_version
- Registered in `v3/mod.rs` and `event/mod.rs`

### Established patterns (reference for all future handlers)

**Handler struct pattern**:
```rust
pub struct MyMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,   // shared across related handlers
    decimals_resolved: AtomicBool,            // guards one-time resolve in handle()
}
```

**Lifecycle**:
1. `register_handlers()`: create shared `Arc<PoolMetadataCache>`, construct handlers
2. `initialize(&self, db_pool)`: call `self.metadata_cache.load_into(db_pool, 8453)` — loads pools from DB, skips if already loaded by sibling handler
3. First `handle()`: call `self.metadata_cache.resolve_quote_decimals(&ctx.contracts)` guarded by `AtomicBool` — resolves WETH=18, USDC=6, USDT=6, EURC=6
4. Every `handle()`: extract events → normalize to `SwapInput`/`LiquidityInput` → delegate to `process_swaps()`/`process_liquidity_deltas()`

**Extraction helpers**: defined as module-level `fn` (not methods) to share between two handler structs:
```rust
fn extract_v3_swaps(ctx: &TransformationContext, source: &str) -> Result<Vec<SwapInput>, TransformationError>
fn extract_v3_liquidity(ctx: &TransformationContext, source: &str) -> Result<Vec<LiquidityInput>, TransformationError>
```

**Registration pattern** in `event/mod.rs`:
```rust
v3::metrics::register_handlers(registry);
```

---

## Phase 3: V4 Hook Handlers

**Goal**: Add metrics for multicurve, decay, scheduled, and dhook pools.

### Tasks
1. Config changes: add getSlot0 on_event calls to 4 config files
2. Config change: add Swap + ModifyLiquidity events to DopplerHookInitializer in dhook.json
3. Implement `multicurve/metrics.rs` — MulticurveMetricsHandler
4. Implement `decay_multicurve/metrics.rs` — DecayMulticurveMetricsHandler
5. Implement `scheduled_multicurve/metrics.rs` — ScheduledMulticurveMetricsHandler
6. Implement `dhook/metrics.rs` — DhookMetricsHandler
7. Register all handlers in their respective `mod.rs` + `event/mod.rs`
8. Test: `cargo build`, `cargo test`, verify on_event call configs work

### Config changes needed

**Add getSlot0 on_event call** to each hook contract config. The call targets `UniswapV4StateView` and passes the pool ID from the Swap event's topics[3]. Add a `"calls"` array to contracts that currently only have `"events"`.

**`config/contracts/base/multicurve.json`** — add to `UniswapV4MulticurveInitializerHook`:
```json
"calls": [{
    "function": "getSlot0(bytes32)",
    "output_type": "(uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)",
    "frequency": { "on_events": {
        "source": "UniswapV4MulticurveInitializerHook",
        "event": "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)"
    }},
    "target": "UniswapV4StateView",
    "params": [{ "type": "bytes32", "from_event": "topics[3]" }]
}]
```

**`config/contracts/base/decay_multicurve.json`** — add to `DecayMulticurveHook`:
Same call config but `"source": "DecayMulticurveHook"`.

**`config/contracts/base/scheduled_multicurve.json`** — add to `UniswapV4ScheduledMulticurveInitializerHook`:
Same call config but `"source": "UniswapV4ScheduledMulticurveInitializerHook"`.

**`config/contracts/base/dhook.json`** — add events AND call to `DopplerHookInitializer`:
```json
"events": [
    { "signature": "Create(address indexed poolOrHook, address indexed asset, address indexed numeraire)" },
    { "signature": "Swap(address indexed sender, (address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) indexed poolKey, bytes32 indexed poolId, (bool zeroForOne, int256 amountSpecified, uint160 sqrtPriceLimitX96) params, int128 amount0, int128 amount1, bytes hookData)" },
    { "signature": "ModifyLiquidity((address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) key, (int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt) params)" }
],
"calls": [
    // ... existing getState, getBeneficiaries calls ...
    {
        "function": "getSlot0(bytes32)",
        "output_type": "(uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)",
        "frequency": { "on_events": {
            "source": "DopplerHookInitializer",
            "event": "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)"
        }},
        "target": "UniswapV4StateView",
        "params": [{ "type": "bytes32", "from_event": "topics[3]" }]
    }
]
```

**Important**: The `"event"` field in `frequency.on_events` is keccak-hashed by `compute_event_signature_hash()` in `src/raw_data/historical/eth_calls/event_triggers.rs` and matched against log topic0. It must be the ABI-canonical form (types only, no names, no `indexed`). Test with one hook first (e.g., DecayMulticurveHook) to verify the format works before applying to all 4 configs.

### How to implement V4 hook handlers using Phase 2 patterns

Each handler follows the same struct + lifecycle pattern from Phase 2 (see `src/transformations/event/v3/metrics.rs`). The key differences are in event extraction.

**Handler struct** (identical to V3):
```rust
pub struct MulticurveMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_resolved: AtomicBool,
}
```

**Swap extraction** — V4 hooks emit a different Swap signature. The key differences from V3:
- `amount0`/`amount1` are `int128` (decoded as I256 by the decoder — use `extract_int256`)
- No sqrtPriceX96/tick/liquidity in the event itself — get from the getSlot0 on_event call
- Pool ID from the `poolId` param (bytes32), not from `contract_address`

```rust
fn extract_v4_hook_swaps(
    ctx: &TransformationContext,
    source: &str,         // e.g., "DecayMulticurveHook"
    call_source: &str,    // e.g., "DecayMulticurveHook" (where getSlot0 is configured)
) -> Result<Vec<SwapInput>, TransformationError> {
    let mut swaps = Vec::new();
    for event in ctx.events_of_type(source, "Swap") {
        let pool_id = event.extract_bytes32("poolId")?;

        // Find the matching getSlot0 call for this swap event
        let slot0 = ctx.calls_of_type(call_source, "getSlot0")
            .find(|call| call.trigger_log_index == Some(event.log_index))
            .ok_or_else(|| TransformationError::MissingData(format!(
                "No getSlot0 call for swap at block {} log_index {}",
                event.block_number, event.log_index
            )))?;

        if slot0.is_reverted {
            tracing::warn!(
                "getSlot0 reverted for pool {} at block {}, skipping",
                hex::encode(pool_id), event.block_number
            );
            continue;
        }

        let sqrt_price_x96 = slot0.extract_uint256("sqrtPriceX96")?;
        let tick = slot0.extract_i32_flexible("tick")?;

        swaps.push(SwapInput {
            pool_id: pool_id.to_vec(),
            block_number: event.block_number,
            block_timestamp: event.block_timestamp,
            log_index: event.log_index,
            amount0: event.extract_int256("amount0")?,
            amount1: event.extract_int256("amount1")?,
            sqrt_price_x96,
            tick,
            liquidity: U256::ZERO,  // not in event; populated by TVL pass later
        });
    }
    Ok(swaps)
}
```

**Note on `call_source`**: The getSlot0 on_event call is configured on the hook contract (e.g., `DecayMulticurveHook`). But the executor stores the call result under the *target* contract name (`UniswapV4StateView`) with the function name `getSlot0`. So the lookup should be:
```rust
ctx.calls_of_type("UniswapV4StateView", "getSlot0")
```
Wait — this is wrong. On-event calls are stored under the source contract name, not the target. Need to verify. Check `src/raw_data/historical/eth_calls/event_triggers.rs` to see what `source_name` is set to on the decoded call result. The `contract_name` on `EventTriggeredCallConfig` is the contract where the call config is defined (the hook), but the call is executed against the target address. The decoded result's `source_name` is set from the config's contract_name. So the lookup uses the hook's contract name as source, not `UniswapV4StateView`.

**Actually**: look at how the existing on_event calls work. In `DecayMulticurveInitializer`, the getState call has no target override — it calls itself. The source_name on the decoded call matches the contract name. For getSlot0 with `"target": "UniswapV4StateView"`, the source_name is still the contract where the call is configured (e.g., `DecayMulticurveHook`). So the lookup is:
```rust
ctx.calls_of_type("DecayMulticurveHook", "getSlot0")
```

**Verify this** by checking what `source_name` is set to in the on_event call collection code. Look at `EventTriggeredCallConfig.contract_name` and how it flows to `DecodedCall.source_name`.

**ModifyLiquidity extraction** — two different event formats:

**Tuple format** (multicurve, dhook) — `ModifyLiquidity((PoolKey) key, (Params) params)`:
```rust
fn extract_tuple_modify_liquidity(
    ctx: &TransformationContext,
    source: &str,
) -> Result<Vec<LiquidityInput>, TransformationError> {
    let mut deltas = Vec::new();
    for event in ctx.events_of_type(source, "ModifyLiquidity") {
        // Compute pool_id from PoolKey fields
        let pool_key = PoolKey {
            currency0: event.extract_address("key.currency0")?.into(),
            currency1: event.extract_address("key.currency1")?.into(),
            fee: event.extract_u32("key.fee")?,
            tick_spacing: event.extract_i32_flexible("key.tickSpacing")?,
            hooks: event.extract_address("key.hooks")?.into(),
        };
        let pool_id = pool_key.pool_id();

        deltas.push(LiquidityInput {
            pool_id: pool_id.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index,
            tick_lower: event.extract_i32_flexible("params.tickLower")?,
            tick_upper: event.extract_i32_flexible("params.tickUpper")?,
            liquidity_delta: event.extract_int256("params.liquidityDelta")?,
        });
    }
    Ok(deltas)
}
```

**Flat format** (decay, scheduled) — `ModifyLiquidity(bytes32 indexed id, address indexed sender, int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt)`:
```rust
fn extract_flat_modify_liquidity(
    ctx: &TransformationContext,
    source: &str,
) -> Result<Vec<LiquidityInput>, TransformationError> {
    let mut deltas = Vec::new();
    for event in ctx.events_of_type(source, "ModifyLiquidity") {
        let pool_id = event.extract_bytes32("id")?;

        deltas.push(LiquidityInput {
            pool_id: pool_id.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index,
            tick_lower: event.extract_i32_flexible("tickLower")?,
            tick_upper: event.extract_i32_flexible("tickUpper")?,
            liquidity_delta: event.extract_int256("liquidityDelta")?,
        });
    }
    Ok(deltas)
}
```

### Per-handler summary

| Handler | Source | Swap extraction | ModifyLiquidity extraction | getSlot0 call_source |
|---------|--------|----------------|---------------------------|---------------------|
| MulticurveMetricsHandler | UniswapV4MulticurveInitializerHook | `extract_v4_hook_swaps` | `extract_tuple_modify_liquidity` | UniswapV4MulticurveInitializerHook |
| DecayMulticurveMetricsHandler | DecayMulticurveHook | `extract_v4_hook_swaps` | `extract_flat_modify_liquidity` | DecayMulticurveHook |
| ScheduledMulticurveMetricsHandler | UniswapV4ScheduledMulticurveInitializerHook | `extract_v4_hook_swaps` | `extract_flat_modify_liquidity` | UniswapV4ScheduledMulticurveInitializerHook |
| DhookMetricsHandler | DopplerHookInitializer | `extract_v4_hook_swaps` | `extract_tuple_modify_liquidity` | DopplerHookInitializer |

### EventTrigger signatures

Must match what's in the config JSON files. The registry uses `extract_event_name()` which takes the part before `(` as the event name for dispatch.

**Multicurve + DHook (tuple ModifyLiquidity)**:
```rust
fn triggers(&self) -> Vec<EventTrigger> {
    vec![
        EventTrigger::new("UniswapV4MulticurveInitializerHook",
            "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)"),
        EventTrigger::new("UniswapV4MulticurveInitializerHook",
            "ModifyLiquidity((address,address,uint24,int24,address),(int24,int24,int256,bytes32))"),
    ]
}
```

**Decay + Scheduled (flat ModifyLiquidity)**:
```rust
fn triggers(&self) -> Vec<EventTrigger> {
    vec![
        EventTrigger::new("DecayMulticurveHook",
            "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)"),
        EventTrigger::new("DecayMulticurveHook",
            "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)"),
    ]
}
```

### call_dependencies

All 4 handlers declare the getSlot0 dependency so the validation at startup passes:
```rust
fn call_dependencies(&self) -> Vec<(String, String)> {
    vec![("UniswapV4MulticurveInitializerHook".to_string(), "getSlot0".to_string())]
}
```

**Note**: The `call_dependencies` source name must match the contract where the call is configured in the JSON, not the target. Verify this matches what `validate_call_dependencies()` checks against.

### Shared extraction functions location

The V4 hook extraction functions (`extract_v4_hook_swaps`, `extract_tuple_modify_liquidity`, `extract_flat_modify_liquidity`) are shared across 4 handlers. They should live in a shared location. Options:
- `src/transformations/event/metrics/v4_hook.rs` — new file in the shared metrics module
- Or inline in each handler's file (more duplication but simpler)

Recommended: add `src/transformations/event/metrics/v4_hook.rs` with the shared extraction functions, and `pub mod v4_hook;` in `metrics/mod.rs`.

### Registration

In each handler module's `mod.rs` — add `pub mod metrics;`

In `src/transformations/event/mod.rs` — add to `register_handlers()`:
```rust
multicurve::metrics::register_handlers(registry);
decay_multicurve::metrics::register_handlers(registry);
scheduled_multicurve::metrics::register_handlers(registry);
dhook::metrics::register_handlers(registry);
```

Each handler's `register_handlers()` creates its own `Arc<PoolMetadataCache>` (not shared across pool types — each pool type may initialize at different times).

### Key verification steps

1. **on_event call format**: Start with one config (e.g., `decay_multicurve.json`), run the indexer for a small block range, and verify that getSlot0 calls appear in the decoded call parquet files. If the event signature hash doesn't match, no calls will be collected.
2. **call source_name**: After getSlot0 calls are collected, verify the `source_name` on the `DecodedCall` matches what `ctx.calls_of_type(source, "getSlot0")` expects.
3. **PoolKey field paths**: Verify that flattened field names like `"key.currency0"`, `"params.tickLower"` match what the decoder produces. Check existing tuple-format events in other handlers for the exact naming convention.
4. **int128 amounts**: The Swap event has `int128 amount0, int128 amount1`. Verify the decoder produces these as values compatible with `extract_int256()` (the decoder likely widens them to I256).

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
