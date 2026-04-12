//! Stateless TVL computation for pool metrics (Phase 7).
//!
//! Rebuilds per-pool tick maps on demand from the `liquidity_deltas` table,
//! computes token amounts via Uniswap v3 position math, and produces
//! `DbOperation::RawSql` materializing upserts for `pool_snapshots` +
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

use alloy_primitives::U256;
use bigdecimal::BigDecimal;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbSnapshot, DbValue};
use crate::transformations::error::TransformationError;
use crate::transformations::util::pool_metadata::PoolMetadata;
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
                &(chain_id as i64),
                &pool_id,
                &(target_block as i64),
                &liquidity_source,
                &(liquidity_source_version as i32),
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

/// Build a `DbOperation::RawSql` upsert that populates TVL columns on a
/// `pool_snapshots` row owned by the given `target_source` handler.
///
/// If the per-block snapshot row already exists (the swap path wrote it first),
/// this updates only TVL-related columns. Otherwise it materializes a new
/// LP-only block row by carrying forward the latest known `price_close` into
/// OHLC, or by seeding from `bootstrap_seed` when no prior snapshot exists,
/// while initializing swap volumes/counts to zero.
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
    let query = r#"
WITH prev AS (
  SELECT price_close
  FROM pool_snapshots
  WHERE chain_id = $1
    AND pool_id = $2
    AND block_number <= $3
    AND source = $11
    AND source_version = $12
  ORDER BY block_number DESC
  LIMIT 1
),
seed_price AS (
  SELECT prev.price_close
  FROM prev
  UNION ALL
  SELECT $13::numeric
  WHERE NOT EXISTS (SELECT 1 FROM prev)
    AND $13::numeric IS NOT NULL
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
  $1,
  $2,
  $3,
  $4,
  seed_price.price_close,
  seed_price.price_close,
  seed_price.price_close,
  seed_price.price_close,
  $5,
  0::numeric,
  0::numeric,
  0,
  NULL::numeric,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  $12
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

    DbOperation::RawSql {
        query: query.to_string(),
        params: vec![
            DbValue::Int64(chain_id as i64),
            DbValue::Bytes(pool_id.to_vec()),
            DbValue::Int64(block_number as i64),
            DbValue::Int64(block_timestamp as i64),
            DbValue::Numeric(active_liquidity.to_string()),
            DbValue::Numeric(amount0.to_string()),
            DbValue::Numeric(amount1.to_string()),
            optional_decimal_value(tvl_usd),
            optional_decimal_value(market_cap_usd),
            optional_decimal_value(active_liquidity_usd),
            DbValue::VarChar(target_source.to_string()),
            DbValue::Int32(target_source_version as i32),
            optional_decimal_value(bootstrap_seed.map(|seed| &seed.price)),
        ],
        snapshot: Some(DbSnapshot {
            table: "pool_snapshots".to_string(),
            key_columns: vec![
                ("chain_id".to_string(), DbValue::Int64(chain_id as i64)),
                ("pool_id".to_string(), DbValue::Bytes(pool_id.to_vec())),
                (
                    "block_number".to_string(),
                    DbValue::Int64(block_number as i64),
                ),
                (
                    "source".to_string(),
                    DbValue::Text(target_source.to_string()),
                ),
                (
                    "source_version".to_string(),
                    DbValue::Int32(target_source_version as i32),
                ),
            ],
        }),
    }
}

/// Build a `DbOperation::RawSql` upsert that populates TVL columns on the
/// `pool_state` row owned by the given `target_source` handler.
///
/// If the latest `pool_state` row is at or before `block_number`, this advances
/// that row to the new block while carrying forward price/tick fields and
/// recomputing the rolling metrics for the new timestamp. If no prior state row
/// exists yet, `bootstrap_seed` provides the initial tick/price anchor. If a
/// swap path already wrote the target block, the conflict update fills in TVL
/// columns in place. If `pool_state` has already advanced past `block_number`,
/// the statement is intentionally a no-op rather than overwriting newer state.
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
    let query = r#"
WITH prev AS (
  SELECT
    block_number,
    tick,
    sqrt_price_x96,
    price
  FROM pool_state
  WHERE chain_id = $1
    AND pool_id = $2
    AND source = $12
    AND source_version = $13
    AND block_number <= $3
  ORDER BY block_number DESC
  LIMIT 1
),
newer AS (
  SELECT 1
  FROM pool_state
  WHERE chain_id = $1
    AND pool_id = $2
    AND source = $12
    AND source_version = $13
    AND block_number > $3
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
    $14::integer,
    $15::numeric,
    $16::numeric
  WHERE NOT EXISTS (SELECT 1 FROM prev)
    AND NOT EXISTS (SELECT 1 FROM newer)
    AND $14::integer IS NOT NULL
    AND $15::numeric IS NOT NULL
    AND $16::numeric IS NOT NULL
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
  $1,
  $2,
  $3,
  $4,
  state_seed.tick,
  state_seed.sqrt_price_x96,
  state_seed.price,
  $5,
  rolling.vol_24h,
  rolling.pc_1h,
  rolling.pc_24h,
  rolling.swaps_24h,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  $12,
  $13
FROM state_seed
CROSS JOIN LATERAL (
  SELECT
    COALESCE(SUM(s.volume_usd), 0) AS vol_24h,
    COALESCE(SUM(s.swap_count), 0)::integer AS swaps_24h,
    CASE WHEN h1h.price_close IS NOT NULL AND h1h.price_close != 0
         THEN (state_seed.price - h1h.price_close) / h1h.price_close
    END AS pc_1h,
    CASE WHEN h24h.price_close IS NOT NULL AND h24h.price_close != 0
         THEN (state_seed.price - h24h.price_close) / h24h.price_close
    END AS pc_24h
  FROM pool_snapshots s
  LEFT JOIN LATERAL (
    SELECT price_close
    FROM pool_snapshots
    WHERE chain_id = $1
      AND pool_id = $2
      AND block_timestamp <= ($4 - 3600)
    ORDER BY block_timestamp DESC, block_number DESC
    LIMIT 1
  ) h1h ON true
  LEFT JOIN LATERAL (
    SELECT price_close
    FROM pool_snapshots
    WHERE chain_id = $1
      AND pool_id = $2
      AND block_timestamp <= ($4 - 86400)
    ORDER BY block_timestamp DESC, block_number DESC
    LIMIT 1
  ) h24h ON true
  WHERE s.chain_id = $1
    AND s.pool_id = $2
    AND s.block_timestamp > ($4 - 86400)
    AND s.block_timestamp <= $4
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

    DbOperation::RawSql {
        query: query.to_string(),
        params: vec![
            DbValue::Int64(chain_id as i64),
            DbValue::Bytes(pool_id.to_vec()),
            DbValue::Int64(block_number as i64),
            DbValue::Int64(block_timestamp as i64),
            DbValue::Numeric(active_liquidity.to_string()),
            DbValue::Numeric(amount0.to_string()),
            DbValue::Numeric(amount1.to_string()),
            optional_decimal_value(tvl_usd),
            optional_decimal_value(market_cap_usd),
            optional_decimal_value(active_liquidity_usd),
            optional_u256_value(total_supply),
            DbValue::VarChar(target_source.to_string()),
            DbValue::Int32(target_source_version as i32),
            optional_i32_value(bootstrap_seed.map(|seed| seed.tick)),
            match bootstrap_seed {
                Some(seed) => DbValue::Numeric(seed.sqrt_price_x96.to_string()),
                None => DbValue::Null,
            },
            optional_decimal_value(bootstrap_seed.map(|seed| &seed.price)),
        ],
        snapshot: Some(DbSnapshot {
            table: "pool_state".to_string(),
            key_columns: vec![
                ("chain_id".to_string(), DbValue::Int64(chain_id as i64)),
                ("pool_id".to_string(), DbValue::Bytes(pool_id.to_vec())),
                (
                    "source".to_string(),
                    DbValue::Text(target_source.to_string()),
                ),
                (
                    "source_version".to_string(),
                    DbValue::Int32(target_source_version as i32),
                ),
            ],
        }),
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformations::util::tick_math::tick_to_sqrt_price_x96;
    use std::str::FromStr;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    fn q96() -> U256 {
        U256::from_str_radix("79228162514264337593543950336", 10).unwrap()
    }

    fn sample_meta(is_token_0: bool, total_supply: Option<U256>) -> PoolMetadata {
        PoolMetadata {
            pool_id: vec![0xAAu8; 20],
            base_token: [0x01; 20],
            quote_token: [0x02; 20],
            is_token_0,
            pool_type: "v3".to_string(),
            base_decimals: 18,
            quote_decimals: 18,
            total_supply,
        }
    }

    fn sample_usd_ctx(eth_usd: Option<&str>) -> UsdPriceContext {
        UsdPriceContext::new_for_test(
            eth_usd.map(bd),
            None,
            Some([0x02; 20]), // WETH = quote for sample_meta
            None,
            None,
            None,
        )
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
            DbOperation::RawSql {
                query,
                params,
                snapshot,
            } => {
                assert!(query.contains("INSERT INTO pool_snapshots"));
                assert!(query.contains("WITH prev AS"));
                assert!(query.contains("FROM seed_price"));
                assert!(query.contains("SELECT $13::numeric"));
                assert!(query.contains(
                    "ON CONFLICT (chain_id, pool_id, block_number, source, source_version)"
                ));
                assert!(query.contains("active_liquidity = EXCLUDED.active_liquidity"));
                assert!(query.contains("active_liquidity_usd = EXCLUDED.active_liquidity_usd"));
                assert!(query.contains("source = $11"));
                assert!(query.contains("source_version = $12"));
                assert_eq!(params.len(), 13);
                match &params[9] {
                    DbValue::Numeric(s) => assert_eq!(s, "3200"),
                    other => panic!("expected Numeric for active_liquidity_usd, got {:?}", other),
                }
                match &params[12] {
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
            _ => panic!("expected RawSql"),
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
            DbOperation::RawSql { params, .. } => {
                // params[7] = tvl_usd, [8] = market_cap_usd, [9] = active_liquidity_usd,
                // [12] = bootstrap price
                assert!(matches!(params[7], DbValue::Null));
                assert!(matches!(params[8], DbValue::Null));
                assert!(matches!(params[9], DbValue::Null));
                assert!(matches!(params[12], DbValue::Null));
            }
            _ => panic!("expected RawSql"),
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
            DbOperation::RawSql {
                query,
                params,
                snapshot,
            } => {
                assert!(query.contains("INSERT INTO pool_state"));
                assert!(query.contains("WITH prev AS"));
                assert!(query.contains("$14::integer"));
                assert!(query.contains("CROSS JOIN LATERAL"));
                assert!(query.contains("COALESCE(SUM(s.volume_usd), 0) AS vol_24h"));
                assert!(query.contains("s.block_timestamp <= $4"));
                assert!(!query.contains("prev.volume_24h_usd"));
                assert!(!query.contains("prev.price_change_1h"));
                assert!(query.contains("ON CONFLICT (chain_id, pool_id, source, source_version)"));
                assert!(query.contains("active_liquidity_usd = EXCLUDED.active_liquidity_usd"));
                assert!(query.contains("WHERE EXCLUDED.block_number >= pool_state.block_number"));
                assert_eq!(params.len(), 16);
                match &params[9] {
                    DbValue::Numeric(s) => assert_eq!(s, "3200"),
                    other => panic!("expected Numeric for active_liquidity_usd, got {:?}", other),
                }
                match &params[13] {
                    DbValue::Int32(v) => assert_eq!(*v, 42),
                    other => panic!("expected Int32 for bootstrap tick, got {:?}", other),
                }
                match &params[14] {
                    DbValue::Numeric(s) => assert_eq!(s, &q96().to_string()),
                    other => {
                        panic!(
                            "expected Numeric for bootstrap sqrt_price_x96, got {:?}",
                            other
                        )
                    }
                }
                match &params[15] {
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
            _ => panic!("expected RawSql"),
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
            DbOperation::RawSql { params, .. } => {
                // params[7] = tvl_usd, [8] = market_cap_usd, [9] = active_liquidity_usd,
                // [10] = total_supply, [13..=15] = bootstrap seed
                assert!(matches!(params[7], DbValue::Null));
                assert!(matches!(params[8], DbValue::Null));
                assert!(matches!(params[9], DbValue::Null));
                assert!(matches!(params[10], DbValue::Null));
                assert!(matches!(params[13], DbValue::Null));
                assert!(matches!(params[14], DbValue::Null));
                assert!(matches!(params[15], DbValue::Null));
            }
            _ => panic!("expected RawSql"),
        }
    }

    // Sanity: verify that tick_to_sqrt_price_x96 at tick 0 matches our q96 helper.
    #[test]
    fn test_q96_helper_matches_tick_zero() {
        assert_eq!(q96(), tick_to_sqrt_price_x96(0).unwrap());
    }
}
