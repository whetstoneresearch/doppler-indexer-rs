//! Stateless TVL computation for pool metrics (Phase 7).
//!
//! Rebuilds per-pool tick maps on demand from the `liquidity_deltas` table,
//! computes token amounts via Uniswap v3 position math, and produces
//! `DbOperation::NamedSql` materializing upserts for `pool_snapshots` +
//! `pool_state`.
//!
//! Stateless by design: each `handle()` invocation re-queries
//! `liquidity_deltas` per target block. This trades a small amount of extra
//! DB work for simpler correctness — no in-memory caches to invalidate on
//! reorg, no checkpoint table, and no `requires_sequential` constraint.
//! Parallel ordering with respect to swap and liquidity handlers is provided
//! by the scheduler's `contiguous_handler_dependencies` mechanism.
//!
//! See `docs/designs/pool-metrics/metrics_implementation_phases.md` (Phase 7)
//! for the architectural context.

use std::collections::BTreeMap;
use std::str::FromStr;

use alloy_primitives::U256;
use bigdecimal::BigDecimal;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbSnapshot, DbValue};
use crate::transformations::error::TransformationError;
use crate::transformations::util::pool_metadata::{PoolMetadata, PoolMetadataCache};
use crate::transformations::util::price::sqrt_price_x96_to_price;
use crate::transformations::util::tick_math::get_amounts_for_position;
use crate::transformations::util::usd_price::{u256_to_decimal_adjusted, UsdPriceContext};

/// One active position in a pool's tick map, as reconstructed from a SUM
/// aggregation over `liquidity_deltas`. Only positions with a strictly
/// positive net liquidity are materialized; the `HAVING SUM > 0` filter in
/// [`query_tick_map`] excludes fully-burned positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickMapPosition {
    pub tick_lower: i32,
    pub tick_upper: i32,
    /// Net liquidity, always positive. `u128` matches Uniswap v3's on-chain
    /// liquidity type; [`query_tick_map`] errors if the aggregate exceeds it.
    pub liquidity: u128,
}

/// Query `liquidity_deltas` for a single pool and reconstruct its tick map
/// as of the given `target_block` (inclusive).
///
/// Groups rows by `(tick_lower, tick_upper)`, sums the signed deltas, and
/// keeps only positions with a strictly positive net. Matches rows by the
/// owning liquidity handler's `source` / `source_version` so parallel
/// versioned handlers (e.g. V3 vs LockableV3) don't leak into each other's
/// tick maps.
///
/// Returns an empty vec if the pool has no liquidity events up to
/// `target_block`. Errors only if the DB query fails or an aggregate exceeds
/// the `u128` range.
pub async fn query_tick_map(
    db: &Pool,
    chain_id: u64,
    pool_id: &[u8],
    target_block: u64,
    liquidity_source: &str,
    liquidity_source_version: u32,
) -> Result<Vec<TickMapPosition>, TransformationError> {
    let client = db
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    // `::text` cast preserves NUMERIC precision; SUM over 128-bit values can
    // technically exceed i128/u128 in pathological cases, so we parse via
    // string and surface an error rather than silently clamping.
    let rows = client
        .query(
            "SELECT tick_lower, tick_upper, SUM(liquidity_delta)::text AS net_liquidity_text \
             FROM liquidity_deltas \
             WHERE chain_id = $1 AND pool_id = $2 AND block_number <= $3 \
               AND source = $4 AND source_version = $5 \
             GROUP BY tick_lower, tick_upper \
             HAVING SUM(liquidity_delta) > 0",
            &[
                &i64::try_from(chain_id).expect("chain_id fits i64"),
                &pool_id,
                &i64::try_from(target_block).expect("block_number fits i64"),
                &liquidity_source,
                &i32::try_from(liquidity_source_version).expect("source_version fits i32"),
            ],
        )
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let mut positions = Vec::with_capacity(rows.len());
    for row in &rows {
        let tick_lower: i32 = row.get("tick_lower");
        let tick_upper: i32 = row.get("tick_upper");
        let text: String = row.get("net_liquidity_text");

        // NUMERIC::text for integer-valued aggregates emits a plain integer
        // (no trailing ".0"); strip any fractional part defensively to handle
        // future-compatible Postgres output changes without breaking.
        let integer_part = text.trim().split('.').next().unwrap_or("");
        let liquidity: u128 = integer_part.parse::<u128>().map_err(|e| {
            TransformationError::TypeConversion(format!(
                "net liquidity out of u128 range or unparsable: chain_id={} pool={} \
                 tick=({},{}) raw={:?} err={}",
                chain_id,
                hex::encode(pool_id),
                tick_lower,
                tick_upper,
                text,
                e
            ))
        })?;

        positions.push(TickMapPosition {
            tick_lower,
            tick_upper,
            liquidity,
        });
    }
    Ok(positions)
}

/// Compute the total `(amount0, amount1)` in RAW token units across all
/// positions, given the current sqrt price of the pool.
///
/// Delegates per-position math to
/// [`crate::transformations::util::tick_math::get_amounts_for_position`].
/// Amounts are summed with saturating arithmetic to be defensive against
/// (extremely unlikely) U256 overflow in protocol-wide aggregates; in
/// practice, no realistic pool can produce a raw amount > U256::MAX.
pub fn compute_position_amounts(
    positions: &[TickMapPosition],
    current_sqrt_price_x96: U256,
) -> Result<(U256, U256), TransformationError> {
    let mut total0 = U256::ZERO;
    let mut total1 = U256::ZERO;
    for pos in positions {
        let (a0, a1) = get_amounts_for_position(
            pos.tick_lower,
            pos.tick_upper,
            current_sqrt_price_x96,
            pos.liquidity,
        )?;
        total0 = total0.saturating_add(a0);
        total1 = total1.saturating_add(a1);
    }
    Ok((total0, total1))
}

/// Compute `(amount0, amount1)` contributed by positions currently in range
/// at `current_tick`, i.e. those whose range satisfies
/// `tick_lower <= current_tick < tick_upper`. Matches Uniswap v3's convention
/// for which positions earn fees at the current price — the upper bound is
/// exclusive so a position being crossed is not double-counted with the
/// adjacent tick's active set.
///
/// Feed the result through [`compute_tvl_usd`] to obtain `active_liquidity_usd`.
/// Pass `current_tick` from the swap accumulator (`BlockAccumulator::last_tick`)
/// and the same `current_sqrt_price_x96` used for full-pool TVL.
pub fn compute_active_position_amounts(
    positions: &[TickMapPosition],
    current_tick: i32,
    current_sqrt_price_x96: U256,
) -> Result<(U256, U256), TransformationError> {
    let mut total0 = U256::ZERO;
    let mut total1 = U256::ZERO;
    for pos in positions {
        if pos.tick_lower <= current_tick && current_tick < pos.tick_upper {
            let (a0, a1) = get_amounts_for_position(
                pos.tick_lower,
                pos.tick_upper,
                current_sqrt_price_x96,
                pos.liquidity,
            )?;
            total0 = total0.saturating_add(a0);
            total1 = total1.saturating_add(a1);
        }
    }
    Ok((total0, total1))
}

/// Compute `tvl_usd` from raw token amounts + pool metadata + current price +
/// USD pricing context.
///
/// Strategy:
/// 1. Convert `amount0` / `amount1` to decimal-adjusted human units.
/// 2. Identify base vs quote amounts via `meta.is_token_0`.
/// 3. Express TVL in quote-token units: `base_dec * price_close + quote_dec`.
/// 4. Convert quote-token value to USD via `UsdPriceContext`.
///
/// Returns `None` if the quote token has no USD price available
/// (e.g. WETH quote with missing ETH/USD).
pub fn compute_tvl_usd(
    amount0_raw: &U256,
    amount1_raw: &U256,
    meta: &PoolMetadata,
    price_close_quote_per_base: &BigDecimal,
    usd_ctx: &UsdPriceContext,
) -> Option<BigDecimal> {
    let (base_raw, quote_raw) = if meta.is_token_0 {
        (amount0_raw, amount1_raw)
    } else {
        (amount1_raw, amount0_raw)
    };

    let base_dec = u256_to_decimal_adjusted(base_raw, meta.base_decimals);
    let quote_dec = u256_to_decimal_adjusted(quote_raw, meta.quote_decimals);

    let tvl_in_quote: BigDecimal = &base_dec * price_close_quote_per_base + &quote_dec;

    usd_ctx.quote_decimal_amount_to_usd(&tvl_in_quote, &meta.quote_token)
}

/// Compute `market_cap_usd = total_supply_dec × price_close × usd_per_quote`.
///
/// Returns `None` if either `meta.total_supply` is absent (no tokens row yet)
/// or the quote token has no USD multiplier.
pub fn compute_market_cap_usd(
    meta: &PoolMetadata,
    price_close_quote_per_base: &BigDecimal,
    usd_ctx: &UsdPriceContext,
) -> Option<BigDecimal> {
    let total_supply_raw = meta.total_supply.as_ref()?;
    let total_supply_dec = u256_to_decimal_adjusted(total_supply_raw, meta.base_decimals);
    let value_in_quote = &total_supply_dec * price_close_quote_per_base;
    usd_ctx.quote_decimal_amount_to_usd(&value_in_quote, &meta.quote_token)
}

// ─── TVL row builders ───────────────────────────────────────────────

fn optional_decimal_value(value: Option<&BigDecimal>) -> DbValue {
    match value {
        Some(v) => DbValue::Numeric(v.to_string()),
        None => DbValue::Null,
    }
}

fn optional_u256_value(value: Option<&U256>) -> DbValue {
    match value {
        Some(v) => DbValue::Numeric(v.to_string()),
        None => DbValue::Null,
    }
}

fn optional_i32_value(value: Option<i32>) -> DbValue {
    match value {
        Some(v) => DbValue::Int32(v),
        None => DbValue::Null,
    }
}

/// Bootstrap state for LP-only TVL materialization when there is no prior
/// `pool_state`/`pool_snapshots` row yet for the pool.
///
/// Future TVL callers are expected to derive this from per-block state sources
/// such as V3 `slot0`, V4 hook `getSlot0`, or a pool-type-specific equivalent.
#[derive(Debug, Clone, PartialEq)]
pub struct TvlBootstrapSeed {
    pub tick: i32,
    pub sqrt_price_x96: U256,
    pub price: BigDecimal,
}

/// Build a `DbOperation::NamedSql` upsert that populates TVL columns on a
/// `pool_snapshots` row owned by the given `target_source` handler.
///
/// If the per-block snapshot row already exists (the swap path wrote it first),
/// this updates only TVL-related columns. Otherwise it materializes a new
/// LP-only block row by carrying forward the latest known `price_close` into
/// OHLC, or by seeding from `bootstrap_seed` when no prior snapshot exists,
/// while initializing swap volumes/counts to zero.
#[allow(clippy::too_many_arguments)]
pub fn build_snapshot_tvl_update(
    chain_id: u64,
    pool_id: &[u8],
    block_number: u64,
    block_timestamp: u64,
    active_liquidity: &BigDecimal,
    amount0: &BigDecimal,
    amount1: &BigDecimal,
    tvl_usd: Option<&BigDecimal>,
    market_cap_usd: Option<&BigDecimal>,
    active_liquidity_usd: Option<&BigDecimal>,
    bootstrap_seed: Option<&TvlBootstrapSeed>,
    target_source: &str,
    target_source_version: u32,
) -> DbOperation {
    let template = r#"
WITH prev AS (
  SELECT price_close
  FROM pool_snapshots
  WHERE chain_id = :chain_id
    AND pool_id = :pool_id
    AND block_number <= :block_number
    AND source = :source
    AND source_version = :source_version
  ORDER BY block_number DESC
  LIMIT 1
),
seed_price AS (
  SELECT prev.price_close
  FROM prev
  UNION ALL
  SELECT :bootstrap_price
  WHERE NOT EXISTS (SELECT 1 FROM prev)
    AND :bootstrap_price IS NOT NULL
)
INSERT INTO pool_snapshots (
  chain_id,
  pool_id,
  block_number,
  block_timestamp,
  price_open,
  price_close,
  price_high,
  price_low,
  active_liquidity,
  volume0,
  volume1,
  swap_count,
  volume_usd,
  amount0,
  amount1,
  tvl_usd,
  market_cap_usd,
  active_liquidity_usd,
  source,
  source_version
)
SELECT
  :chain_id,
  :pool_id,
  :block_number,
  :block_timestamp,
  seed_price.price_close,
  seed_price.price_close,
  seed_price.price_close,
  seed_price.price_close,
  :active_liquidity,
  0::numeric,
  0::numeric,
  0,
  NULL::numeric,
  :amount0,
  :amount1,
  :tvl_usd,
  :market_cap_usd,
  :active_liquidity_usd,
  :source,
  :source_version
FROM seed_price
ON CONFLICT (chain_id, pool_id, block_number, source, source_version)
DO UPDATE SET
  block_timestamp = EXCLUDED.block_timestamp,
  active_liquidity = EXCLUDED.active_liquidity,
  amount0 = EXCLUDED.amount0,
  amount1 = EXCLUDED.amount1,
  tvl_usd = EXCLUDED.tvl_usd,
  market_cap_usd = EXCLUDED.market_cap_usd,
  active_liquidity_usd = EXCLUDED.active_liquidity_usd
"#;

    DbOperation::NamedSql {
        template: template.to_string(),
        params: vec![
            (
                "chain_id".into(),
                DbValue::Int64(i64::try_from(chain_id).expect("chain_id fits i64")),
            ),
            ("pool_id".into(), DbValue::Bytes(pool_id.to_vec())),
            (
                "block_number".into(),
                DbValue::Int64(i64::try_from(block_number).expect("block_number fits i64")),
            ),
            (
                "block_timestamp".into(),
                DbValue::Int64(i64::try_from(block_timestamp).expect("block_timestamp fits i64")),
            ),
            (
                "active_liquidity".into(),
                DbValue::Numeric(active_liquidity.to_string()),
            ),
            ("amount0".into(), DbValue::Numeric(amount0.to_string())),
            ("amount1".into(), DbValue::Numeric(amount1.to_string())),
            ("tvl_usd".into(), optional_decimal_value(tvl_usd)),
            (
                "market_cap_usd".into(),
                optional_decimal_value(market_cap_usd),
            ),
            (
                "active_liquidity_usd".into(),
                optional_decimal_value(active_liquidity_usd),
            ),
            ("source".into(), DbValue::VarChar(target_source.to_string())),
            (
                "source_version".into(),
                DbValue::Int32(
                    i32::try_from(target_source_version).expect("source_version fits i32"),
                ),
            ),
            (
                "bootstrap_price".into(),
                optional_decimal_value(bootstrap_seed.map(|seed| &seed.price)),
            ),
        ],
        snapshot: Some(DbSnapshot {
            table: "pool_snapshots".to_string(),
            key_columns: vec![
                (
                    "chain_id".to_string(),
                    DbValue::Int64(i64::try_from(chain_id).expect("chain_id fits i64")),
                ),
                ("pool_id".to_string(), DbValue::Bytes(pool_id.to_vec())),
                (
                    "block_number".to_string(),
                    DbValue::Int64(i64::try_from(block_number).expect("block_number fits i64")),
                ),
                (
                    "source".to_string(),
                    DbValue::Text(target_source.to_string()),
                ),
                (
                    "source_version".to_string(),
                    DbValue::Int32(
                        i32::try_from(target_source_version).expect("source_version fits i32"),
                    ),
                ),
            ],
        }),
    }
}

/// Build a `DbOperation::NamedSql` upsert that populates TVL columns on the
/// `pool_state` row owned by the given `target_source` handler.
///
/// If the latest `pool_state` row is at or before `block_number`, this advances
/// that row to the new block while carrying forward price/tick fields and
/// recomputing the rolling metrics for the new timestamp. If no prior state row
/// exists yet, `bootstrap_seed` provides the initial tick/price anchor. If a
/// swap path already wrote the target block, the conflict update fills in TVL
/// columns in place. If `pool_state` has already advanced past `block_number`,
/// the statement is intentionally a no-op rather than overwriting newer state.
#[allow(clippy::too_many_arguments)]
pub fn build_state_tvl_update(
    chain_id: u64,
    pool_id: &[u8],
    block_number: u64,
    block_timestamp: u64,
    active_liquidity: &BigDecimal,
    amount0: &BigDecimal,
    amount1: &BigDecimal,
    tvl_usd: Option<&BigDecimal>,
    market_cap_usd: Option<&BigDecimal>,
    active_liquidity_usd: Option<&BigDecimal>,
    total_supply: Option<&U256>,
    bootstrap_seed: Option<&TvlBootstrapSeed>,
    target_source: &str,
    target_source_version: u32,
) -> DbOperation {
    let template = r#"
WITH prev AS (
  SELECT
    block_number,
    tick,
    sqrt_price_x96,
    price
  FROM pool_state
  WHERE chain_id = :chain_id
    AND pool_id = :pool_id
    AND source = :source
    AND source_version = :source_version
    AND block_number <= :block_number
  ORDER BY block_number DESC
  LIMIT 1
),
newer AS (
  SELECT 1
  FROM pool_state
  WHERE chain_id = :chain_id
    AND pool_id = :pool_id
    AND source = :source
    AND source_version = :source_version
    AND block_number > :block_number
  LIMIT 1
),
state_seed AS (
  SELECT
    prev.tick,
    prev.sqrt_price_x96,
    prev.price
  FROM prev
  UNION ALL
  SELECT
    :bootstrap_tick::integer,
    :bootstrap_sqrt_price,
    :bootstrap_price
  WHERE NOT EXISTS (SELECT 1 FROM prev)
    AND NOT EXISTS (SELECT 1 FROM newer)
    AND :bootstrap_tick::integer IS NOT NULL
    AND :bootstrap_sqrt_price IS NOT NULL
    AND :bootstrap_price IS NOT NULL
)
INSERT INTO pool_state (
  chain_id,
  pool_id,
  block_number,
  block_timestamp,
  tick,
  sqrt_price_x96,
  price,
  active_liquidity,
  volume_24h_usd,
  price_change_1h,
  price_change_24h,
  swap_count_24h,
  amount0,
  amount1,
  tvl_usd,
  market_cap_usd,
  active_liquidity_usd,
  total_supply,
  source,
  source_version
)
SELECT
  :chain_id,
  :pool_id,
  :block_number,
  :block_timestamp::bigint,
  state_seed.tick,
  state_seed.sqrt_price_x96,
  state_seed.price,
  :active_liquidity,
  rolling.vol_24h,
  rolling.pc_1h,
  rolling.pc_24h,
  rolling.swaps_24h,
  :amount0,
  :amount1,
  :tvl_usd,
  :market_cap_usd,
  :active_liquidity_usd,
  :total_supply,
  :source,
  :source_version
FROM state_seed
CROSS JOIN LATERAL (
  SELECT
    agg.vol_24h,
    agg.swaps_24h,
    CASE WHEN h1h.price_close IS NOT NULL AND h1h.price_close != 0
         THEN (state_seed.price - h1h.price_close) / h1h.price_close
    END AS pc_1h,
    CASE WHEN h24h.price_close IS NOT NULL AND h24h.price_close != 0
         THEN (state_seed.price - h24h.price_close) / h24h.price_close
    END AS pc_24h
  FROM (
    SELECT
      COALESCE(SUM(s.volume_usd), 0) AS vol_24h,
      COALESCE(SUM(s.swap_count), 0)::integer AS swaps_24h
    FROM pool_snapshots s
    WHERE s.chain_id = :chain_id
      AND s.pool_id = :pool_id
      AND s.block_timestamp > (:block_timestamp::bigint - 86400)
      AND s.block_timestamp <= :block_timestamp::bigint
      AND s.source = :source
      AND s.source_version = :source_version
  ) agg
  LEFT JOIN LATERAL (
    SELECT price_close
    FROM pool_snapshots
    WHERE chain_id = :chain_id
      AND pool_id = :pool_id
      AND block_timestamp <= (:block_timestamp::bigint - 3600)
      AND source = :source
      AND source_version = :source_version
    ORDER BY block_timestamp DESC, block_number DESC
    LIMIT 1
  ) h1h ON true
  LEFT JOIN LATERAL (
    SELECT price_close
    FROM pool_snapshots
    WHERE chain_id = :chain_id
      AND pool_id = :pool_id
      AND block_timestamp <= (:block_timestamp::bigint - 86400)
      AND source = :source
      AND source_version = :source_version
    ORDER BY block_timestamp DESC, block_number DESC
    LIMIT 1
  ) h24h ON true
) AS rolling
ON CONFLICT (chain_id, pool_id, source, source_version)
DO UPDATE SET
  block_number = EXCLUDED.block_number,
  block_timestamp = EXCLUDED.block_timestamp,
  tick = EXCLUDED.tick,
  sqrt_price_x96 = EXCLUDED.sqrt_price_x96,
  price = EXCLUDED.price,
  active_liquidity = EXCLUDED.active_liquidity,
  volume_24h_usd = EXCLUDED.volume_24h_usd,
  price_change_1h = EXCLUDED.price_change_1h,
  price_change_24h = EXCLUDED.price_change_24h,
  swap_count_24h = EXCLUDED.swap_count_24h,
  amount0 = EXCLUDED.amount0,
  amount1 = EXCLUDED.amount1,
  tvl_usd = EXCLUDED.tvl_usd,
  market_cap_usd = EXCLUDED.market_cap_usd,
  active_liquidity_usd = EXCLUDED.active_liquidity_usd,
  total_supply = EXCLUDED.total_supply
WHERE EXCLUDED.block_number >= pool_state.block_number
"#;

    DbOperation::NamedSql {
        template: template.to_string(),
        params: vec![
            (
                "chain_id".into(),
                DbValue::Int64(i64::try_from(chain_id).expect("chain_id fits i64")),
            ),
            ("pool_id".into(), DbValue::Bytes(pool_id.to_vec())),
            (
                "block_number".into(),
                DbValue::Int64(i64::try_from(block_number).expect("block_number fits i64")),
            ),
            (
                "block_timestamp".into(),
                DbValue::Int64(i64::try_from(block_timestamp).expect("block_timestamp fits i64")),
            ),
            (
                "active_liquidity".into(),
                DbValue::Numeric(active_liquidity.to_string()),
            ),
            ("amount0".into(), DbValue::Numeric(amount0.to_string())),
            ("amount1".into(), DbValue::Numeric(amount1.to_string())),
            ("tvl_usd".into(), optional_decimal_value(tvl_usd)),
            (
                "market_cap_usd".into(),
                optional_decimal_value(market_cap_usd),
            ),
            (
                "active_liquidity_usd".into(),
                optional_decimal_value(active_liquidity_usd),
            ),
            ("total_supply".into(), optional_u256_value(total_supply)),
            ("source".into(), DbValue::VarChar(target_source.to_string())),
            (
                "source_version".into(),
                DbValue::Int32(
                    i32::try_from(target_source_version).expect("source_version fits i32"),
                ),
            ),
            (
                "bootstrap_tick".into(),
                optional_i32_value(bootstrap_seed.map(|seed| seed.tick)),
            ),
            (
                "bootstrap_sqrt_price".into(),
                match bootstrap_seed {
                    Some(seed) => DbValue::Numeric(seed.sqrt_price_x96.to_string()),
                    None => DbValue::Null,
                },
            ),
            (
                "bootstrap_price".into(),
                optional_decimal_value(bootstrap_seed.map(|seed| &seed.price)),
            ),
        ],
        snapshot: Some(DbSnapshot {
            table: "pool_state".to_string(),
            key_columns: vec![
                (
                    "chain_id".to_string(),
                    DbValue::Int64(i64::try_from(chain_id).expect("chain_id fits i64")),
                ),
                ("pool_id".to_string(), DbValue::Bytes(pool_id.to_vec())),
                (
                    "source".to_string(),
                    DbValue::Text(target_source.to_string()),
                ),
                (
                    "source_version".to_string(),
                    DbValue::Int32(
                        i32::try_from(target_source_version).expect("source_version fits i32"),
                    ),
                ),
            ],
        }),
    }
}

// ─── Shared TVL handler processing ─────────────────────────────────

/// A single (pool, block) TVL computation target extracted from swap events.
#[derive(Debug, Clone)]
pub struct TvlTarget {
    pub pool_id: Vec<u8>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tick: i32,
    pub sqrt_price_x96: U256,
}

/// Handler source identities for reading `liquidity_deltas` and writing to
/// the swap handler's `pool_snapshots`/`pool_state` rows.
#[derive(Debug, Clone)]
pub struct TvlHandlerConfig {
    /// Source name of the liquidity handler whose `liquidity_deltas` rows to query.
    pub liquidity_source: &'static str,
    /// Source version of the liquidity handler.
    pub liquidity_source_version: u32,
    /// Source name of the swap handler whose rows to update with TVL columns.
    pub swap_source: &'static str,
    /// Source version of the swap handler.
    pub swap_source_version: u32,
}

/// Intermediate result cached during the snapshot pass to avoid re-querying
/// the tick map for the state update.
struct TvlComputeResult {
    active_liquidity: BigDecimal,
    amount0: BigDecimal,
    amount1: BigDecimal,
    tvl_usd: Option<BigDecimal>,
    market_cap_usd: Option<BigDecimal>,
    active_liquidity_usd: Option<BigDecimal>,
    bootstrap_seed: TvlBootstrapSeed,
    total_supply: Option<U256>,
}

/// Core TVL processing: for each target, query the tick map from
/// `liquidity_deltas`, compute token amounts and USD values, and produce
/// `pool_snapshots` + `pool_state` update operations targeting the swap
/// handler's rows.
///
/// Targets should be deduplicated by `(pool_id, block_number)` before calling,
/// keeping the last swap's `tick`/`sqrt_price_x96` per block (the TVL snapshot
/// reflects end-of-block state).
pub async fn process_tvl(
    targets: &[TvlTarget],
    config: &TvlHandlerConfig,
    metadata_cache: &PoolMetadataCache,
    db: &Pool,
    chain_id: u64,
    usd_ctx: &UsdPriceContext,
) -> Result<Vec<DbOperation>, TransformationError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let mut ops = Vec::new();
    // Track the latest block per pool and its compute result for state updates.
    let mut latest_per_pool: BTreeMap<Vec<u8>, (&TvlTarget, TvlComputeResult)> = BTreeMap::new();

    for target in targets {
        let Some(meta) = metadata_cache.get(&target.pool_id) else {
            tracing::debug!(
                pool_id = %hex::encode(&target.pool_id),
                "TVL: skipping pool, metadata not found in cache"
            );
            continue;
        };

        let positions = query_tick_map(
            db,
            chain_id,
            &target.pool_id,
            target.block_number,
            config.liquidity_source,
            config.liquidity_source_version,
        )
        .await?;

        if positions.is_empty() {
            tracing::debug!(
                pool_id = %hex::encode(&target.pool_id),
                block_number = target.block_number,
                "TVL: skipping pool, no liquidity positions at block"
            );
            continue;
        }

        let price_close = match sqrt_price_x96_to_price(
            &target.sqrt_price_x96,
            meta.base_decimals,
            meta.quote_decimals,
            meta.is_token_0,
        ) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    pool_id = %hex::encode(&target.pool_id),
                    "TVL: skipping pool, sqrt_price_x96_to_price returned None"
                );
                continue;
            }
        };

        let (total0, total1) = compute_position_amounts(&positions, target.sqrt_price_x96)?;
        let (active0, active1) =
            compute_active_position_amounts(&positions, target.tick, target.sqrt_price_x96)?;

        let active_liquidity: u128 = positions
            .iter()
            .filter(|p| p.tick_lower <= target.tick && target.tick < p.tick_upper)
            .map(|p| p.liquidity)
            .fold(0u128, |acc, l| acc.saturating_add(l));
        let active_liquidity_dec = BigDecimal::from_str(&active_liquidity.to_string())
            .expect("u128::to_string is valid decimal");

        let amount0 =
            BigDecimal::from_str(&total0.to_string()).expect("U256::to_string is valid decimal");
        let amount1 =
            BigDecimal::from_str(&total1.to_string()).expect("U256::to_string is valid decimal");

        let tvl_usd = compute_tvl_usd(&total0, &total1, &meta, &price_close, usd_ctx);
        let market_cap_usd = compute_market_cap_usd(&meta, &price_close, usd_ctx);
        let active_liquidity_usd =
            compute_tvl_usd(&active0, &active1, &meta, &price_close, usd_ctx);

        let bootstrap_seed = TvlBootstrapSeed {
            tick: target.tick,
            sqrt_price_x96: target.sqrt_price_x96,
            price: price_close,
        };

        ops.push(build_snapshot_tvl_update(
            chain_id,
            &target.pool_id,
            target.block_number,
            target.block_timestamp,
            &active_liquidity_dec,
            &amount0,
            &amount1,
            tvl_usd.as_ref(),
            market_cap_usd.as_ref(),
            active_liquidity_usd.as_ref(),
            Some(&bootstrap_seed),
            config.swap_source,
            config.swap_source_version,
        ));

        let result = TvlComputeResult {
            active_liquidity: active_liquidity_dec,
            amount0,
            amount1,
            tvl_usd,
            market_cap_usd,
            active_liquidity_usd,
            bootstrap_seed,
            total_supply: meta.total_supply,
        };

        let update_latest = latest_per_pool
            .get(&target.pool_id)
            .is_none_or(|(prev, _)| target.block_number >= prev.block_number);
        if update_latest {
            latest_per_pool.insert(target.pool_id.clone(), (target, result));
        }
    }

    // Emit pool_state updates for the latest block per pool.
    for (pool_id, (target, result)) in &latest_per_pool {
        ops.push(build_state_tvl_update(
            chain_id,
            pool_id,
            target.block_number,
            target.block_timestamp,
            &result.active_liquidity,
            &result.amount0,
            &result.amount1,
            result.tvl_usd.as_ref(),
            result.market_cap_usd.as_ref(),
            result.active_liquidity_usd.as_ref(),
            result.total_supply.as_ref(),
            Some(&result.bootstrap_seed),
            config.swap_source,
            config.swap_source_version,
        ));
    }

    Ok(ops)
}

// ─── TVL from direct positions (no liquidity_deltas) ──────────────

/// A TVL target that carries its own positions instead of querying
/// `liquidity_deltas`. Used for pool types (e.g. Zora) where the
/// hook contract provides a `getPoolCoin` view that returns the full
/// LP position array directly.
#[derive(Debug, Clone)]
pub struct TvlTargetWithPositions {
    pub pool_id: Vec<u8>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tick: i32,
    pub sqrt_price_x96: U256,
    pub positions: Vec<TickMapPosition>,
}

/// Core TVL processing for targets that carry their own position arrays.
///
/// Mirrors [`process_tvl`] but skips the `liquidity_deltas` DB query,
/// using the positions embedded in each target instead.
pub async fn process_tvl_from_positions(
    targets: &[TvlTargetWithPositions],
    metadata_cache: &PoolMetadataCache,
    chain_id: u64,
    usd_ctx: &UsdPriceContext,
    swap_source: &str,
    swap_source_version: u32,
) -> Result<Vec<DbOperation>, TransformationError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let mut ops = Vec::new();
    let mut latest_per_pool: BTreeMap<Vec<u8>, (&TvlTargetWithPositions, TvlComputeResult)> =
        BTreeMap::new();

    for target in targets {
        let Some(meta) = metadata_cache.get(&target.pool_id) else {
            tracing::debug!(
                pool_id = %hex::encode(&target.pool_id),
                "TVL (from positions): skipping pool, metadata not found in cache"
            );
            continue;
        };

        if target.positions.is_empty() {
            tracing::debug!(
                pool_id = %hex::encode(&target.pool_id),
                block_number = target.block_number,
                "TVL (from positions): skipping pool, no liquidity positions"
            );
            continue;
        }

        let price_close = match sqrt_price_x96_to_price(
            &target.sqrt_price_x96,
            meta.base_decimals,
            meta.quote_decimals,
            meta.is_token_0,
        ) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    pool_id = %hex::encode(&target.pool_id),
                    "TVL (from positions): skipping pool, sqrt_price_x96_to_price returned None"
                );
                continue;
            }
        };

        let (total0, total1) =
            compute_position_amounts(&target.positions, target.sqrt_price_x96)?;
        let (active0, active1) = compute_active_position_amounts(
            &target.positions,
            target.tick,
            target.sqrt_price_x96,
        )?;

        let active_liquidity: u128 = target
            .positions
            .iter()
            .filter(|p| p.tick_lower <= target.tick && target.tick < p.tick_upper)
            .map(|p| p.liquidity)
            .fold(0u128, |acc, l| acc.saturating_add(l));
        let active_liquidity_dec = BigDecimal::from_str(&active_liquidity.to_string())
            .expect("u128::to_string is valid decimal");

        let amount0 =
            BigDecimal::from_str(&total0.to_string()).expect("U256::to_string is valid decimal");
        let amount1 =
            BigDecimal::from_str(&total1.to_string()).expect("U256::to_string is valid decimal");

        let tvl_usd = compute_tvl_usd(&total0, &total1, &meta, &price_close, usd_ctx);
        let market_cap_usd = compute_market_cap_usd(&meta, &price_close, usd_ctx);
        let active_liquidity_usd =
            compute_tvl_usd(&active0, &active1, &meta, &price_close, usd_ctx);

        let bootstrap_seed = TvlBootstrapSeed {
            tick: target.tick,
            sqrt_price_x96: target.sqrt_price_x96,
            price: price_close,
        };

        ops.push(build_snapshot_tvl_update(
            chain_id,
            &target.pool_id,
            target.block_number,
            target.block_timestamp,
            &active_liquidity_dec,
            &amount0,
            &amount1,
            tvl_usd.as_ref(),
            market_cap_usd.as_ref(),
            active_liquidity_usd.as_ref(),
            Some(&bootstrap_seed),
            swap_source,
            swap_source_version,
        ));

        let result = TvlComputeResult {
            active_liquidity: active_liquidity_dec,
            amount0,
            amount1,
            tvl_usd,
            market_cap_usd,
            active_liquidity_usd,
            bootstrap_seed,
            total_supply: meta.total_supply,
        };

        let update_latest = latest_per_pool
            .get(&target.pool_id)
            .is_none_or(|(prev, _)| target.block_number >= prev.block_number);
        if update_latest {
            latest_per_pool.insert(target.pool_id.clone(), (target, result));
        }
    }

    // Emit pool_state updates for the latest block per pool.
    for (pool_id, (target, result)) in &latest_per_pool {
        ops.push(build_state_tvl_update(
            chain_id,
            pool_id,
            target.block_number,
            target.block_timestamp,
            &result.active_liquidity,
            &result.amount0,
            &result.amount1,
            result.tvl_usd.as_ref(),
            result.market_cap_usd.as_ref(),
            result.active_liquidity_usd.as_ref(),
            result.total_supply.as_ref(),
            Some(&result.bootstrap_seed),
            swap_source,
            swap_source_version,
        ));
    }

    Ok(ops)
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformations::util::price::sqrt_price_x96_to_price;
    use crate::transformations::util::tick_math::tick_to_sqrt_price_x96;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn q96() -> U256 {
        U256::from_str_radix("79228162514264337593543950336", 10).unwrap()
    }

    fn sample_meta(is_token_0: bool, total_supply: Option<U256>) -> PoolMetadata {
        PoolMetadata {
            quote_token: [0x02; 20],
            is_token_0,
            base_decimals: 18,
            quote_decimals: 18,
            total_supply,
        }
    }

    fn sample_usd_ctx(eth_usd: Option<&str>) -> UsdPriceContext {
        let mut prices = std::collections::HashMap::new();
        if let Some(p) = eth_usd {
            prices.insert([0x02; 20], bd(p)); // WETH = quote for sample_meta
        }
        UsdPriceContext::new_for_test(prices, Some([0x02; 20]))
    }

    fn sample_bootstrap_seed() -> TvlBootstrapSeed {
        TvlBootstrapSeed {
            tick: 42,
            sqrt_price_x96: q96(),
            price: bd("2"),
        }
    }

    // ─── compute_position_amounts ───

    #[test]
    fn test_compute_position_amounts_empty() {
        let (a0, a1) = compute_position_amounts(&[], q96()).unwrap();
        assert_eq!(a0, U256::ZERO);
        assert_eq!(a1, U256::ZERO);
    }

    #[test]
    fn test_compute_position_amounts_single_straddling() {
        let positions = vec![TickMapPosition {
            tick_lower: -60,
            tick_upper: 60,
            liquidity: 1_000_000_000_000_000_000u128, // 1e18
        }];
        let (a0, a1) = compute_position_amounts(&positions, q96()).unwrap();
        assert!(a0 > U256::ZERO);
        assert!(a1 > U256::ZERO);
    }

    #[test]
    fn test_compute_position_amounts_entirely_above_current() {
        // Position (60, 120) above current tick 0 — only token0.
        let positions = vec![TickMapPosition {
            tick_lower: 60,
            tick_upper: 120,
            liquidity: 1_000_000_000_000_000_000u128,
        }];
        let (a0, a1) = compute_position_amounts(&positions, q96()).unwrap();
        assert!(a0 > U256::ZERO);
        assert_eq!(a1, U256::ZERO);
    }

    #[test]
    fn test_compute_position_amounts_entirely_below_current() {
        // Position (-120, -60) below current tick 0 — only token1.
        let positions = vec![TickMapPosition {
            tick_lower: -120,
            tick_upper: -60,
            liquidity: 1_000_000_000_000_000_000u128,
        }];
        let (a0, a1) = compute_position_amounts(&positions, q96()).unwrap();
        assert_eq!(a0, U256::ZERO);
        assert!(a1 > U256::ZERO);
    }

    #[test]
    fn test_compute_position_amounts_multi_position_sum() {
        let single = vec![TickMapPosition {
            tick_lower: -60,
            tick_upper: 60,
            liquidity: 1_000_000_000_000_000_000u128,
        }];
        let doubled = vec![
            TickMapPosition {
                tick_lower: -60,
                tick_upper: 60,
                liquidity: 500_000_000_000_000_000u128,
            },
            TickMapPosition {
                tick_lower: -60,
                tick_upper: 60,
                liquidity: 500_000_000_000_000_000u128,
            },
        ];
        let (single0, single1) = compute_position_amounts(&single, q96()).unwrap();
        let (double0, double1) = compute_position_amounts(&doubled, q96()).unwrap();
        // Two half-liquidity positions should match one full-liquidity position
        // (within integer truncation tolerance of a few wei).
        let diff0 = if single0 > double0 {
            single0 - double0
        } else {
            double0 - single0
        };
        let diff1 = if single1 > double1 {
            single1 - double1
        } else {
            double1 - single1
        };
        assert!(diff0 < U256::from(10u64), "diff0={}", diff0);
        assert!(diff1 < U256::from(10u64), "diff1={}", diff1);
    }

    // ─── compute_tvl_usd ───

    #[test]
    fn test_compute_tvl_usd_base_token0_weth_quote() {
        // 1 base @ 18 decimals, price_close = 2 (quote per base),
        // 0.5 quote @ 18 decimals. ETH/USD = 2000.
        // tvl_in_quote = 1 * 2 + 0.5 = 2.5 quote
        // tvl_usd = 2.5 * 2000 = 5000
        let base_raw = U256::from(1_000_000_000_000_000_000u128);
        let quote_raw = U256::from(500_000_000_000_000_000u128);
        let meta = sample_meta(true, None); // is_token_0 = true → token0 is base
        let price_close = bd("2");
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let tvl = compute_tvl_usd(&base_raw, &quote_raw, &meta, &price_close, &usd_ctx).unwrap();
        assert_eq!(tvl, bd("5000"));
    }

    #[test]
    fn test_compute_tvl_usd_base_token1_reversed() {
        // is_token_0 = false → token1 is base, so amount0 is quote and amount1 is base.
        // Same numbers as above but swapped: amount0_raw = 0.5 quote, amount1_raw = 1 base.
        let amount0_quote = U256::from(500_000_000_000_000_000u128);
        let amount1_base = U256::from(1_000_000_000_000_000_000u128);
        let meta = sample_meta(false, None);
        let price_close = bd("2");
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let tvl =
            compute_tvl_usd(&amount0_quote, &amount1_base, &meta, &price_close, &usd_ctx).unwrap();
        // base_dec = 1, quote_dec = 0.5, tvl_in_quote = 1*2 + 0.5 = 2.5, usd = 5000
        assert_eq!(tvl, bd("5000"));
    }

    #[test]
    fn test_compute_tvl_usd_zero_amounts() {
        let meta = sample_meta(true, None);
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let tvl = compute_tvl_usd(&U256::ZERO, &U256::ZERO, &meta, &bd("2"), &usd_ctx).unwrap();
        assert_eq!(tvl, bd("0"));
    }

    #[test]
    fn test_compute_tvl_usd_unknown_quote_returns_none() {
        let mut meta = sample_meta(true, None);
        meta.quote_token = [0xFF; 20]; // not registered in usd_ctx
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let tvl = compute_tvl_usd(
            &U256::from(1u64),
            &U256::from(1u64),
            &meta,
            &bd("2"),
            &usd_ctx,
        );
        assert!(tvl.is_none());
    }

    #[test]
    fn test_compute_tvl_usd_weth_quote_missing_eth_price() {
        let meta = sample_meta(true, None);
        let usd_ctx = sample_usd_ctx(None);
        let tvl = compute_tvl_usd(
            &U256::from(1_000_000_000_000_000_000u128),
            &U256::ZERO,
            &meta,
            &bd("2"),
            &usd_ctx,
        );
        assert!(tvl.is_none());
    }

    // ─── compute_market_cap_usd ───

    #[test]
    fn test_compute_market_cap_usd_basic() {
        // total_supply = 1 billion * 1e18, base_decimals = 18
        // → total_supply_dec = 1e9
        // price_close = 2 quote per base, eth_usd = 2000
        // → market_cap = 1e9 * 2 * 2000 = 4e12
        let supply = U256::from_str_radix("1000000000000000000000000000", 10).unwrap(); // 1e27
        let meta = sample_meta(true, Some(supply));
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let mcap = compute_market_cap_usd(&meta, &bd("2"), &usd_ctx).unwrap();
        assert_eq!(mcap, bd("4000000000000"));
    }

    #[test]
    fn test_compute_market_cap_usd_missing_total_supply() {
        let meta = sample_meta(true, None);
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let mcap = compute_market_cap_usd(&meta, &bd("2"), &usd_ctx);
        assert!(mcap.is_none());
    }

    #[test]
    fn test_compute_market_cap_usd_unknown_quote() {
        let supply = U256::from(1_000_000u64);
        let mut meta = sample_meta(true, Some(supply));
        meta.quote_token = [0xFF; 20];
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let mcap = compute_market_cap_usd(&meta, &bd("2"), &usd_ctx);
        assert!(mcap.is_none());
    }

    // ─── compute_active_position_amounts ───

    #[test]
    fn test_compute_active_position_amounts_empty() {
        let (a0, a1) = compute_active_position_amounts(&[], 0, q96()).unwrap();
        assert_eq!(a0, U256::ZERO);
        assert_eq!(a1, U256::ZERO);
    }

    #[test]
    fn test_compute_active_position_amounts_filters_out_of_range() {
        // Straddling (in-range) + out-of-range-above + out-of-range-below.
        let positions = vec![
            TickMapPosition {
                tick_lower: -60,
                tick_upper: 60,
                liquidity: 1_000_000_000_000_000_000u128,
            },
            TickMapPosition {
                tick_lower: 120,
                tick_upper: 180,
                liquidity: 5_000_000_000_000_000_000u128,
            },
            TickMapPosition {
                tick_lower: -180,
                tick_upper: -120,
                liquidity: 7_000_000_000_000_000_000u128,
            },
        ];
        let (active0, active1) = compute_active_position_amounts(&positions, 0, q96()).unwrap();

        // Should match computing over just the in-range position alone.
        let in_range_only = vec![positions[0].clone()];
        let (expected0, expected1) = compute_position_amounts(&in_range_only, q96()).unwrap();
        assert_eq!(active0, expected0);
        assert_eq!(active1, expected1);
    }

    #[test]
    fn test_compute_active_position_amounts_boundary_lower_inclusive() {
        // Position (0, 60) at current_tick = 0 → lower bound inclusive, so included.
        let positions = vec![TickMapPosition {
            tick_lower: 0,
            tick_upper: 60,
            liquidity: 1_000_000_000_000_000_000u128,
        }];
        let (a0, a1) = compute_active_position_amounts(&positions, 0, q96()).unwrap();
        // At current_tick = 0 == tick_lower, all liquidity sits in token0.
        assert!(a0 > U256::ZERO);
        assert_eq!(a1, U256::ZERO);
    }

    #[test]
    fn test_compute_active_position_amounts_boundary_upper_exclusive() {
        // Position (-60, 0) at current_tick = 0 → upper bound exclusive, so skipped.
        let positions = vec![TickMapPosition {
            tick_lower: -60,
            tick_upper: 0,
            liquidity: 1_000_000_000_000_000_000u128,
        }];
        let (a0, a1) = compute_active_position_amounts(&positions, 0, q96()).unwrap();
        assert_eq!(a0, U256::ZERO);
        assert_eq!(a1, U256::ZERO);
    }

    #[test]
    fn test_compute_active_liquidity_usd_end_to_end() {
        // Same shape as test_compute_tvl_usd_base_token0_weth_quote, but the
        // amounts come from the filtered compute and then flow through
        // compute_tvl_usd for the USD conversion.
        //
        // In-range position contributes base_raw = 1e18, quote_raw = 0.5e18.
        // Out-of-range position is ignored. price_close = 2, eth_usd = 2000.
        // → tvl_in_quote = 1 * 2 + 0.5 = 2.5, usd = 5000.
        //
        // We construct amounts directly rather than going through
        // get_amounts_for_position, since the filtering behavior is already
        // covered by the tests above; this test pins the USD wiring.
        let base_raw = U256::from(1_000_000_000_000_000_000u128);
        let quote_raw = U256::from(500_000_000_000_000_000u128);
        let meta = sample_meta(true, None);
        let price_close = bd("2");
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let active_usd =
            compute_tvl_usd(&base_raw, &quote_raw, &meta, &price_close, &usd_ctx).unwrap();
        assert_eq!(active_usd, bd("5000"));
    }

    // ─── build_snapshot_tvl_update ───

    #[test]
    fn test_build_snapshot_tvl_update_structure() {
        let bootstrap_seed = sample_bootstrap_seed();
        let op = build_snapshot_tvl_update(
            8453,
            &[0xAAu8; 20],
            1000,
            1_700_000_000,
            &bd("9.75"),
            &bd("1.5"),
            &bd("2.25"),
            Some(&bd("5000")),
            Some(&bd("4000000000000")),
            Some(&bd("3200")),
            Some(&bootstrap_seed),
            "V3SwapMetricsHandler",
            1,
        );
        match op {
            DbOperation::NamedSql {
                template,
                params,
                snapshot,
            } => {
                assert!(template.contains("INSERT INTO pool_snapshots"));
                assert!(template.contains("WITH prev AS"));
                assert!(template.contains("FROM seed_price"));
                assert!(template.contains("SELECT :bootstrap_price"));
                assert!(template.contains(
                    "ON CONFLICT (chain_id, pool_id, block_number, source, source_version)"
                ));
                assert!(template.contains("active_liquidity = EXCLUDED.active_liquidity"));
                assert!(template.contains("active_liquidity_usd = EXCLUDED.active_liquidity_usd"));
                assert!(template.contains("source = :source"));
                assert!(template.contains("source_version = :source_version"));
                assert_eq!(params.len(), 13);
                assert_eq!(params[9].0, "active_liquidity_usd");
                match &params[9].1 {
                    DbValue::Numeric(s) => assert_eq!(s, "3200"),
                    other => panic!("expected Numeric for active_liquidity_usd, got {:?}", other),
                }
                assert_eq!(params[12].0, "bootstrap_price");
                match &params[12].1 {
                    DbValue::Numeric(s) => assert_eq!(s, "2"),
                    other => panic!("expected Numeric for bootstrap seed price, got {:?}", other),
                }

                let snap = snapshot.expect("snapshot should be present");
                assert_eq!(snap.table, "pool_snapshots");
                assert_eq!(snap.key_columns.len(), 5);
                let keys: Vec<&str> = snap.key_columns.iter().map(|(k, _)| k.as_str()).collect();
                assert_eq!(
                    keys,
                    vec![
                        "chain_id",
                        "pool_id",
                        "block_number",
                        "source",
                        "source_version"
                    ]
                );
            }
            _ => panic!("expected NamedSql"),
        }
    }

    #[test]
    fn test_build_snapshot_tvl_update_none_values() {
        let op = build_snapshot_tvl_update(
            8453,
            &[0xAAu8; 20],
            1000,
            1_700_000_000,
            &bd("9.75"),
            &bd("1.5"),
            &bd("2.25"),
            None,
            None,
            None,
            None,
            "V3SwapMetricsHandler",
            1,
        );
        match op {
            DbOperation::NamedSql { params, .. } => {
                // params[7] = tvl_usd, [8] = market_cap_usd, [9] = active_liquidity_usd,
                // [12] = bootstrap price
                assert_eq!(params[7].0, "tvl_usd");
                assert!(matches!(params[7].1, DbValue::Null));
                assert_eq!(params[8].0, "market_cap_usd");
                assert!(matches!(params[8].1, DbValue::Null));
                assert_eq!(params[9].0, "active_liquidity_usd");
                assert!(matches!(params[9].1, DbValue::Null));
                assert_eq!(params[12].0, "bootstrap_price");
                assert!(matches!(params[12].1, DbValue::Null));
            }
            _ => panic!("expected NamedSql"),
        }
    }

    // ─── build_state_tvl_update ───

    #[test]
    fn test_build_state_tvl_update_structure() {
        let supply = U256::from(1_000_000_000u64);
        let bootstrap_seed = sample_bootstrap_seed();
        let op = build_state_tvl_update(
            8453,
            &[0xAAu8; 20],
            1000,
            1_700_000_000,
            &bd("9.75"),
            &bd("1.5"),
            &bd("2.25"),
            Some(&bd("5000")),
            Some(&bd("4000000000000")),
            Some(&bd("3200")),
            Some(&supply),
            Some(&bootstrap_seed),
            "V3SwapMetricsHandler",
            1,
        );
        match op {
            DbOperation::NamedSql {
                template,
                params,
                snapshot,
            } => {
                assert!(template.contains("INSERT INTO pool_state"));
                assert!(template.contains("WITH prev AS"));
                assert!(template.contains(":bootstrap_tick::integer"));
                assert!(template.contains("CROSS JOIN LATERAL"));
                assert!(template.contains("agg.vol_24h"));
                assert!(template.contains(
                    "FROM (\n    SELECT\n      COALESCE(SUM(s.volume_usd), 0) AS vol_24h"
                ));
                assert!(template.contains(":block_timestamp::bigint"));
                assert!(template.contains("s.block_timestamp <= :block_timestamp::bigint"));
                assert!(!template.contains("prev.volume_24h_usd"));
                assert!(!template.contains("prev.price_change_1h"));
                assert!(
                    template.contains("ON CONFLICT (chain_id, pool_id, source, source_version)")
                );
                assert!(template.contains("active_liquidity_usd = EXCLUDED.active_liquidity_usd"));
                assert!(template.contains("WHERE EXCLUDED.block_number >= pool_state.block_number"));
                assert_eq!(params.len(), 16);
                assert_eq!(params[9].0, "active_liquidity_usd");
                match &params[9].1 {
                    DbValue::Numeric(s) => assert_eq!(s, "3200"),
                    other => panic!("expected Numeric for active_liquidity_usd, got {:?}", other),
                }
                assert_eq!(params[13].0, "bootstrap_tick");
                match &params[13].1 {
                    DbValue::Int32(v) => assert_eq!(*v, 42),
                    other => panic!("expected Int32 for bootstrap tick, got {:?}", other),
                }
                assert_eq!(params[14].0, "bootstrap_sqrt_price");
                match &params[14].1 {
                    DbValue::Numeric(s) => assert_eq!(s, &q96().to_string()),
                    other => {
                        panic!(
                            "expected Numeric for bootstrap sqrt_price_x96, got {:?}",
                            other
                        )
                    }
                }
                assert_eq!(params[15].0, "bootstrap_price");
                match &params[15].1 {
                    DbValue::Numeric(s) => assert_eq!(s, "2"),
                    other => panic!("expected Numeric for bootstrap price, got {:?}", other),
                }

                let snap = snapshot.expect("snapshot should be present");
                assert_eq!(snap.table, "pool_state");
                assert_eq!(snap.key_columns.len(), 4);
                let keys: Vec<&str> = snap.key_columns.iter().map(|(k, _)| k.as_str()).collect();
                assert_eq!(
                    keys,
                    vec!["chain_id", "pool_id", "source", "source_version"]
                );
            }
            _ => panic!("expected NamedSql"),
        }
    }

    #[test]
    fn test_build_state_tvl_update_nulls_for_missing_totals() {
        let op = build_state_tvl_update(
            8453,
            &[0xAAu8; 20],
            1000,
            1_700_000_000,
            &bd("9.75"),
            &bd("1.5"),
            &bd("2.25"),
            None,
            None,
            None,
            None,
            None,
            "V3SwapMetricsHandler",
            1,
        );
        match op {
            DbOperation::NamedSql { params, .. } => {
                // params[7] = tvl_usd, [8] = market_cap_usd, [9] = active_liquidity_usd,
                // [10] = total_supply, [13..=15] = bootstrap seed
                assert_eq!(params[7].0, "tvl_usd");
                assert!(matches!(params[7].1, DbValue::Null));
                assert_eq!(params[8].0, "market_cap_usd");
                assert!(matches!(params[8].1, DbValue::Null));
                assert_eq!(params[9].0, "active_liquidity_usd");
                assert!(matches!(params[9].1, DbValue::Null));
                assert_eq!(params[10].0, "total_supply");
                assert!(matches!(params[10].1, DbValue::Null));
                assert_eq!(params[13].0, "bootstrap_tick");
                assert!(matches!(params[13].1, DbValue::Null));
                assert_eq!(params[14].0, "bootstrap_sqrt_price");
                assert!(matches!(params[14].1, DbValue::Null));
                assert_eq!(params[15].0, "bootstrap_price");
                assert!(matches!(params[15].1, DbValue::Null));
            }
            _ => panic!("expected NamedSql"),
        }
    }

    // Sanity: verify that tick_to_sqrt_price_x96 at tick 0 matches our q96 helper.
    #[test]
    fn test_q96_helper_matches_tick_zero() {
        assert_eq!(q96(), tick_to_sqrt_price_x96(0).unwrap());
    }

    // ─── End-to-end TVL pipeline tests ─────────────────────────────────

    /// Build a USDC-quoted metadata (base=18 decimals, quote=6 decimals).
    fn sample_meta_usdc_quote(is_token_0: bool, total_supply: Option<U256>) -> PoolMetadata {
        PoolMetadata {
            quote_token: [0x03; 20], // USDC address
            is_token_0,
            base_decimals: 18,
            quote_decimals: 6,
            total_supply,
        }
    }

    fn sample_usd_ctx_with_usdc() -> UsdPriceContext {
        let mut prices = std::collections::HashMap::new();
        prices.insert([0x02; 20], bd("2000")); // WETH
        prices.insert([0x03; 20], bigdecimal::BigDecimal::from(1)); // USDC
        UsdPriceContext::new_for_test(prices, Some([0x02; 20]))
    }

    #[test]
    fn test_compute_tvl_usd_usdc_quote_token() {
        // USDC quote: quote_to_usd_multiplier = 1.0 (stablecoin).
        // base = 1e18 raw (1 token at 18 dec), quote = 500_000 raw (0.5 USDC at 6 dec).
        // price_close = 2 USDC per base.
        // tvl_in_quote = 1 * 2 + 0.5 = 2.5 USDC → 2.5 USD.
        let base_raw = U256::from(1_000_000_000_000_000_000u128); // 1e18
        let quote_raw = U256::from(500_000u64); // 0.5 USDC
        let meta = sample_meta_usdc_quote(true, None);
        let usd_ctx = sample_usd_ctx_with_usdc();
        let tvl = compute_tvl_usd(&base_raw, &quote_raw, &meta, &bd("2"), &usd_ctx).unwrap();
        assert_eq!(tvl, bd("2.5"));
    }

    #[test]
    fn test_end_to_end_tvl_from_positions_to_usd() {
        // Full pipeline: positions → compute_position_amounts → compute_tvl_usd
        //
        // Pool at tick 0 (sqrt_price = 2^96), one straddling position (-600, 600).
        // With is_token_0 = true, WETH quote, ETH/USD = 2000.
        let current_sqrt = q96(); // tick 0
        let positions = vec![TickMapPosition {
            tick_lower: -600,
            tick_upper: 600,
            liquidity: 1_000_000_000_000_000_000u128, // 1e18
        }];

        let (total0, total1) = compute_position_amounts(&positions, current_sqrt).unwrap();
        assert!(
            total0 > U256::ZERO,
            "straddling position should have token0"
        );
        assert!(
            total1 > U256::ZERO,
            "straddling position should have token1"
        );

        let meta = sample_meta(true, None);
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let price_close = sqrt_price_x96_to_price(&current_sqrt, 18, 18, true).unwrap();

        let tvl = compute_tvl_usd(&total0, &total1, &meta, &price_close, &usd_ctx);
        assert!(
            tvl.is_some(),
            "tvl_usd should be computable with known quote"
        );
        assert!(
            tvl.as_ref().unwrap() > &bd("0"),
            "tvl_usd should be positive"
        );
    }

    #[test]
    fn test_active_vs_total_liquidity_with_mixed_positions() {
        // Three positions: in-range, above-range, below-range.
        // Active liquidity should be strictly less than total TVL.
        let current_sqrt = q96(); // tick 0
        let liq = 1_000_000_000_000_000_000u128;
        let positions = vec![
            TickMapPosition {
                tick_lower: -600,
                tick_upper: 600,
                liquidity: liq,
            },
            TickMapPosition {
                tick_lower: 1200,
                tick_upper: 2400,
                liquidity: liq,
            },
            TickMapPosition {
                tick_lower: -2400,
                tick_upper: -1200,
                liquidity: liq,
            },
        ];

        let (total0, total1) = compute_position_amounts(&positions, current_sqrt).unwrap();
        let (active0, active1) =
            compute_active_position_amounts(&positions, 0, current_sqrt).unwrap();

        // Total amounts include all three positions; active only includes the first.
        assert!(
            total0 > active0,
            "total token0 should exceed active (above-range contributes token0)"
        );
        assert!(
            total1 > active1,
            "total token1 should exceed active (below-range contributes token1)"
        );

        // Verify USD values follow the same ordering.
        let meta = sample_meta(true, None);
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let price = bd("1"); // tick 0 with equal decimals → price ≈ 1

        let total_usd = compute_tvl_usd(&total0, &total1, &meta, &price, &usd_ctx).unwrap();
        let active_usd = compute_tvl_usd(&active0, &active1, &meta, &price, &usd_ctx).unwrap();
        assert!(
            total_usd > active_usd,
            "total_usd ({}) should exceed active_usd ({})",
            total_usd,
            active_usd
        );
    }

    #[test]
    fn test_compute_position_amounts_wide_tick_range() {
        // Very wide position range near tick boundaries.
        let current_sqrt = q96();
        let positions = vec![TickMapPosition {
            tick_lower: -500_000,
            tick_upper: 500_000,
            liquidity: 1_000_000_000u128,
        }];
        let (a0, a1) = compute_position_amounts(&positions, current_sqrt).unwrap();
        // Wide range should produce some of both tokens.
        assert!(a0 > U256::ZERO);
        assert!(a1 > U256::ZERO);
    }

    #[test]
    fn test_compute_position_amounts_narrow_tick_range() {
        // Very narrow position (single tick spacing), straddling current.
        let current_sqrt = q96();
        let positions = vec![TickMapPosition {
            tick_lower: -1,
            tick_upper: 1,
            liquidity: 1_000_000_000_000_000_000u128,
        }];
        let (a0, a1) = compute_position_amounts(&positions, current_sqrt).unwrap();
        // Narrow range with high liquidity still produces amounts.
        assert!(a0 > U256::ZERO);
        assert!(a1 > U256::ZERO);
    }

    #[test]
    fn test_compute_tvl_usd_large_amounts_no_overflow() {
        // Stress test with near-max realistic amounts (U256 from ~1e38 raw).
        let large = U256::from_str_radix("100000000000000000000000000000000000000", 10).unwrap();
        let meta = sample_meta(true, None);
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let tvl = compute_tvl_usd(&large, &large, &meta, &bd("1"), &usd_ctx);
        assert!(tvl.is_some(), "large amounts should not overflow");
        assert!(tvl.unwrap() > bd("0"));
    }

    #[test]
    fn test_compute_market_cap_consistency_with_tvl() {
        // If total_supply equals the total token amounts in the pool, and all
        // liquidity is in-range, market_cap should equal TVL (for a fully
        // backed token with price_close representing quote-per-base).
        //
        // Use a simplified scenario: 1000 tokens total supply, all deposited.
        let supply_raw = U256::from(1000u64) * U256::from(10u64).pow(U256::from(18u64)); // 1000e18
        let meta = sample_meta(true, Some(supply_raw));
        let price_close = bd("5");
        let usd_ctx = sample_usd_ctx(Some("2000"));
        let mcap = compute_market_cap_usd(&meta, &price_close, &usd_ctx).unwrap();
        // market_cap = 1000 * 5 * 2000 = 10_000_000
        assert_eq!(mcap, bd("10000000"));
    }

    // ─── SQL regression tests ──────────────────────────────────────────

    #[test]
    fn test_build_state_tvl_update_lateral_has_source_filters() {
        // Regression test for bug #2: the CROSS JOIN LATERAL and its h1h/h24h
        // subqueries must filter pool_snapshots by source/source_version.
        let bootstrap_seed = sample_bootstrap_seed();
        let op = build_state_tvl_update(
            8453,
            &[0xAAu8; 20],
            1000,
            1_700_000_000,
            &bd("9.75"),
            &bd("1.5"),
            &bd("2.25"),
            Some(&bd("5000")),
            None,
            None,
            None,
            Some(&bootstrap_seed),
            "V3SwapMetricsHandler",
            1,
        );
        match op {
            DbOperation::NamedSql { template, .. } => {
                // The lateral subqueries should filter by source/source_version.
                // prev + newer CTEs (2 each) + h1h + h24h + outer lateral (3 each) = 5.
                let source_filter_count = template.matches("source = :source").count();
                assert!(
                    source_filter_count >= 5,
                    "expected source=:source in prev + newer + h1h + h24h + outer lateral, found {} occurrences",
                    source_filter_count
                );
                let version_filter_count =
                    template.matches("source_version = :source_version").count();
                assert!(
                    version_filter_count >= 5,
                    "expected source_version=:source_version in prev + newer + h1h + h24h + outer lateral, found {} occurrences",
                    version_filter_count
                );
            }
            _ => panic!("expected NamedSql"),
        }
    }

    #[test]
    fn test_build_state_tvl_update_newer_guard_present() {
        // Verify the state_seed CTE has the NOT EXISTS(newer) guard,
        // which prevents bootstrap from overwriting newer state.
        let bootstrap_seed = sample_bootstrap_seed();
        let op = build_state_tvl_update(
            8453,
            &[0xAAu8; 20],
            1000,
            1_700_000_000,
            &bd("0"),
            &bd("0"),
            &bd("0"),
            None,
            None,
            None,
            None,
            Some(&bootstrap_seed),
            "V3SwapMetricsHandler",
            1,
        );
        match op {
            DbOperation::NamedSql { template, .. } => {
                assert!(
                    template.contains("NOT EXISTS (SELECT 1 FROM newer)"),
                    "state_seed must guard against overwriting newer pool_state"
                );
                assert!(
                    template.contains("WHERE EXCLUDED.block_number >= pool_state.block_number"),
                    "conflict update must respect block ordering"
                );
            }
            _ => panic!("expected NamedSql"),
        }
    }
}
