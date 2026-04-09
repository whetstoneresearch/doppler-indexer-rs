//! Normalized swap and liquidity types with shared processing logic.
//!
//! All pool type handlers normalize their events into SwapInput/LiquidityInput,
//! then call process_swaps()/process_liquidity_deltas() to produce DbOperations.

use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;

use alloy_primitives::{I256, U256};
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbValue};
use crate::transformations::error::TransformationError;
use crate::transformations::util::db::pool_metrics::{
    insert_liquidity_delta, insert_pool_snapshot, upsert_pool_state, LiquidityDeltaData,
    PoolStateData, SnapshotData,
};
use crate::transformations::util::pool_metadata::PoolMetadataCache;
use crate::transformations::util::price::sqrt_price_x96_to_price;
use crate::transformations::util::usd_price::UsdPriceContext;

use super::accumulator::BlockAccumulator;

/// Normalized swap input produced by all pool type handlers.
#[derive(Debug, Clone)]
pub struct SwapInput {
    pub pool_id: Vec<u8>,
    pub transaction_hash: [u8; 32],
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub amount0: I256,
    pub amount1: I256,
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: U256,
}

/// Normalized liquidity delta from Mint/Burn/ModifyLiquidity events.
#[derive(Debug, Clone)]
pub struct LiquidityInput {
    pub pool_id: Vec<u8>,
    pub block_number: u64,
    pub log_index: u32,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub liquidity_delta: I256,
}

/// Process a batch of swap inputs into pool_snapshots + pool_state DbOperations.
///
/// Groups swaps by (pool_id, block_number), builds a BlockAccumulator for each group,
/// then emits one snapshot row per group and one pool_state upsert per pool (latest block).
/// Groups where no valid price could be computed (e.g. all swaps had zero sqrtPriceX96)
/// are silently skipped — no ghost snapshot rows are emitted.
///
/// When `usd_ctx` is provided, computes `volume_usd` for each snapshot and emits
/// `RawSql` UPDATE operations for rolling metrics on `pool_state`.
pub fn process_swaps(
    swaps: &[SwapInput],
    metadata_cache: &PoolMetadataCache,
    chain_id: u64,
    handler_name: &str,
    source_name: &str,
    usd_ctx: Option<&UsdPriceContext>,
    source_version: u32,
) -> Vec<DbOperation> {
    if swaps.is_empty() {
        return Vec::new();
    }

    // Group by (pool_id, block_number), preserving log_index order within each group.
    // BTreeMap ensures deterministic ordering by block_number.
    let mut grouped: BTreeMap<(Vec<u8>, u64), Vec<&SwapInput>> = BTreeMap::new();
    for swap in swaps {
        grouped
            .entry((swap.pool_id.clone(), swap.block_number))
            .or_default()
            .push(swap);
    }

    let mut ops = Vec::new();
    // Track the latest block per pool for pool_state upserts
    let mut latest_per_pool: BTreeMap<Vec<u8>, (u64, &BlockAccumulator)> = BTreeMap::new();

    // We need to collect accumulators first, then reference them
    let mut accumulators: Vec<(Vec<u8>, u64, BlockAccumulator)> = Vec::new();

    // Track skipped swaps for a single summary log at the end.
    // Collect a few (pool, block, tx_hash) samples for debugging.
    let mut skipped_swap_count = 0u64;
    let mut skipped_pool_count = 0u64;
    let mut skipped_samples: Vec<(String, u64, String)> = Vec::new();

    for ((pool_id, block_number), swap_group) in &grouped {
        let meta = match metadata_cache.get(pool_id) {
            Some(m) => m,
            None => {
                skipped_swap_count += swap_group.len() as u64;
                skipped_pool_count += 1;
                if skipped_samples.len() < 5 {
                    let tx = swap_group
                        .first()
                        .map(|s| hex::encode(s.transaction_hash))
                        .unwrap_or_default();
                    skipped_samples.push((hex::encode(pool_id), *block_number, tx));
                }
                continue;
            }
        };

        let block_timestamp = swap_group[0].block_timestamp;
        let mut acc = BlockAccumulator::new(block_timestamp);

        // Sort by log_index within the block
        let mut sorted_swaps: Vec<&&SwapInput> = swap_group.iter().collect();
        sorted_swaps.sort_by_key(|s| s.log_index);

        for swap in sorted_swaps {
            let Some(price) = sqrt_price_x96_to_price(
                &swap.sqrt_price_x96,
                meta.base_decimals,
                meta.quote_decimals,
                meta.is_token_0,
            ) else {
                continue;
            };
            acc.record_swap(
                price,
                swap.amount0,
                swap.amount1,
                swap.tick,
                swap.sqrt_price_x96,
                swap.liquidity,
            );
        }

        // Skip pools where no valid price was computed (all swaps had invalid sqrtPriceX96).
        if acc.swap_count == 0 {
            continue;
        }

        accumulators.push((pool_id.clone(), *block_number, acc));
    }

    if skipped_swap_count > 0 {
        let samples_str: Vec<String> = skipped_samples
            .iter()
            .map(|(pool, block, tx)| format!("pool={} block={} tx={}", pool, block, tx))
            .collect();
        tracing::warn!(
            handler = handler_name,
            source = source_name,
            "Skipped {} swap(s) across {} pool(s) due to missing metadata. Samples: [{}]",
            skipped_swap_count,
            skipped_pool_count,
            samples_str.join("; "),
        );
    }

    for (pool_id, block_number, acc) in &accumulators {
        // price_open/close/high/low are guaranteed Some because swap_count > 0.
        let price_open = acc.price_open.as_ref().unwrap().to_string();
        let price_close = acc.price_close.as_ref().unwrap().to_string();
        let price_high = acc.price_high.as_ref().unwrap().to_string();
        let price_low = acc.price_low.as_ref().unwrap().to_string();

        // Compute volume_usd from quote-side volume if USD context is available.
        let volume_usd = usd_ctx.and_then(|ctx| {
            let meta = metadata_cache.get(pool_id)?;
            // Quote-side volume: volume1 if base is token0, volume0 otherwise.
            let quote_volume = if meta.is_token_0 {
                &acc.volume1
            } else {
                &acc.volume0
            };
            ctx.quote_volume_to_usd(quote_volume, &meta.quote_token, meta.quote_decimals)
        });

        ops.push(insert_pool_snapshot(&SnapshotData {
            chain_id,
            pool_id: pool_id.clone(),
            block_number: *block_number,
            block_timestamp: acc.block_timestamp,
            price_open,
            price_close,
            price_high,
            price_low,
            active_liquidity: acc.last_liquidity.to_string(),
            volume0: acc.volume0.to_string(),
            volume1: acc.volume1.to_string(),
            swap_count: i32::try_from(acc.swap_count).unwrap_or(i32::MAX),
            volume_usd: volume_usd.map(|v| v.to_string()),
        }));

        // Track latest block for pool_state.
        // BTreeMap iterates in ascending order by (pool_id, block_number), so the
        // last insert for each pool_id always has the highest block_number.
        latest_per_pool.insert(pool_id.clone(), (*block_number, acc));
    }

    // Emit pool_state upserts for the latest block per pool
    for (pool_id, (block_number, acc)) in &latest_per_pool {
        let price_close = acc.price_close.as_ref().unwrap().to_string();

        ops.push(upsert_pool_state(&PoolStateData {
            chain_id,
            pool_id: pool_id.clone(),
            block_number: *block_number,
            block_timestamp: acc.block_timestamp,
            tick: acc.last_tick,
            sqrt_price_x96: acc.last_sqrt_price_x96.to_string(),
            price: price_close.clone(),
            active_liquidity: acc.last_liquidity.to_string(),
        }));

        // Emit RawSql to update rolling metrics on pool_state.
        // This executes after the snapshot upserts (RawSql sorts last in the
        // transaction), so the LATERAL subqueries see the just-inserted data.
        if usd_ctx.is_some() {
            ops.push(build_rolling_metrics_update(
                chain_id,
                pool_id,
                &price_close,
                acc.block_timestamp,
                handler_name,
                source_version,
            ));
        }
    }

    ops
}

/// Build a RawSql UPDATE for rolling metrics on pool_state.
///
/// Uses backward-looking LATERAL subqueries: "what was the price N hours ago?"
/// finds the last known price_close at or before the target timestamp.
fn build_rolling_metrics_update(
    chain_id: u64,
    pool_id: &[u8],
    price_close: &str,
    block_timestamp: u64,
    handler_name: &str,
    source_version: u32,
) -> DbOperation {
    let query = r#"
UPDATE pool_state SET
  volume_24h_usd = sub.vol_24h,
  swap_count_24h = sub.swaps_24h,
  price_change_1h = sub.pc_1h,
  price_change_24h = sub.pc_24h
FROM (
  SELECT
    COALESCE(SUM(s.volume_usd), 0) AS vol_24h,
    COALESCE(SUM(s.swap_count), 0)::integer AS swaps_24h,
    CASE WHEN h1h.price_close IS NOT NULL AND h1h.price_close != 0
         THEN ($3::numeric - h1h.price_close) / h1h.price_close
    END AS pc_1h,
    CASE WHEN h24h.price_close IS NOT NULL AND h24h.price_close != 0
         THEN ($3::numeric - h24h.price_close) / h24h.price_close
    END AS pc_24h
  FROM pool_snapshots s
  LEFT JOIN LATERAL (
    SELECT price_close FROM pool_snapshots
    WHERE chain_id = $1 AND pool_id = $2
      AND block_timestamp <= ($4 - 3600)
    ORDER BY block_timestamp DESC, block_number DESC LIMIT 1
  ) h1h ON true
  LEFT JOIN LATERAL (
    SELECT price_close FROM pool_snapshots
    WHERE chain_id = $1 AND pool_id = $2
      AND block_timestamp <= ($4 - 86400)
    ORDER BY block_timestamp DESC, block_number DESC LIMIT 1
  ) h24h ON true
  WHERE s.chain_id = $1 AND s.pool_id = $2
    AND s.block_timestamp > ($4 - 86400)
) sub
WHERE pool_state.chain_id = $1
  AND pool_state.pool_id = $2
  AND pool_state.source = $5
  AND pool_state.source_version = $6
"#;

    DbOperation::RawSql {
        query: query.to_string(),
        params: vec![
            DbValue::Int64(chain_id as i64),
            DbValue::Bytes(pool_id.to_vec()),
            DbValue::Numeric(price_close.to_string()),
            DbValue::Int64(block_timestamp as i64),
            DbValue::VarChar(handler_name.to_string()),
            DbValue::Int32(source_version as i32),
        ],
    }
}

/// Process a batch of liquidity inputs into liquidity_deltas INSERT operations.
pub fn process_liquidity_deltas(deltas: &[LiquidityInput], chain_id: u64) -> Vec<DbOperation> {
    deltas
        .iter()
        .map(|d| {
            insert_liquidity_delta(&LiquidityDeltaData {
                chain_id,
                pool_id: d.pool_id.clone(),
                block_number: d.block_number,
                log_index: d.log_index,
                tick_lower: d.tick_lower,
                tick_upper: d.tick_upper,
                liquidity_delta: d.liquidity_delta.to_string(),
            })
        })
        .collect()
}

/// Refresh the metadata cache from DB if any pool IDs in the batch are missing.
///
/// Returns `Err` only if the DB refresh itself fails. Pools that remain absent
/// after the refresh are logged and silently skipped — `process_swaps` already
/// handles missing metadata gracefully by skipping the affected pools.
pub async fn refresh_cache_if_needed(
    pool_ids: impl Iterator<Item = &Vec<u8>>,
    cache: &PoolMetadataCache,
    db_pool: &OnceLock<Pool>,
    chain_id: u64,
    contracts: &crate::types::config::contract::Contracts,
    handler_name: &str,
    source_name: &str,
) -> Result<(), TransformationError> {
    let missing: Vec<_> = {
        let unique: HashSet<&Vec<u8>> = pool_ids.collect();
        unique
            .into_iter()
            .filter(|id| cache.get(id).is_none())
            .collect()
    };
    if missing.is_empty() {
        return Ok(());
    }

    let Some(pool) = db_pool.get() else {
        return Ok(());
    };

    match cache.refresh(pool, chain_id, contracts).await {
        Ok(new_count) if new_count > 0 => {
            tracing::info!(
                handler = handler_name,
                source = source_name,
                "Metadata cache refreshed: {} new pool(s) discovered ({} were missing)",
                new_count,
                missing.len()
            );
        }
        Ok(_) => {}
        Err(e) => {
            return Err(e);
        }
    }

    let still_missing: Vec<_> = missing
        .iter()
        .filter(|id| cache.get(id).is_none())
        .collect();

    if !still_missing.is_empty() {
        let sample: Vec<String> = still_missing
            .iter()
            .take(10)
            .map(|id| hex::encode(id))
            .collect();
        tracing::warn!(
            handler = handler_name,
            source = source_name,
            "{} pool(s) not in pools table — metrics will be skipped (sample: {})",
            still_missing.len(),
            sample.join(", "),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformations::util::pool_metadata::{PoolMetadata, PoolMetadataCache};

    fn make_cache_with_pool(pool_id: Vec<u8>) -> PoolMetadataCache {
        let cache = PoolMetadataCache::new();
        cache.insert_if_absent(
            pool_id.clone(),
            PoolMetadata {
                pool_id,
                base_token: [0u8; 20],
                quote_token: [1u8; 20],
                is_token_0: true,
                pool_type: "v3".to_string(),
                base_decimals: 18,
                quote_decimals: 18,
            },
        );
        cache
    }

    fn q96() -> U256 {
        // 2^96
        U256::from_str_radix("79228162514264337593543950336", 10).unwrap()
    }

    #[test]
    fn test_ghost_snapshot_zero_sqrt_price() {
        let pool_id = vec![0u8; 20];
        let cache = make_cache_with_pool(pool_id.clone());

        let swaps = vec![SwapInput {
            pool_id: pool_id.clone(),
            transaction_hash: [0u8; 32],
            block_number: 100,
            block_timestamp: 1000,
            log_index: 0,
            amount0: I256::try_from(100i64).unwrap(),
            amount1: I256::try_from(-100i64).unwrap(),
            sqrt_price_x96: U256::ZERO, // invalid — zero sqrtPrice
            tick: 0,
            liquidity: U256::from(1000u64),
        }];

        let ops = process_swaps(&swaps, &cache, 8453, "test", "test", None, 1);
        assert!(ops.is_empty(), "zero sqrtPrice should produce no ops");
    }

    #[test]
    fn test_all_invalid_prices_produce_no_snapshot() {
        let pool_id = vec![0u8; 20];
        let cache = make_cache_with_pool(pool_id.clone());

        // Two swaps, both with zero sqrtPrice
        let swaps = vec![
            SwapInput {
                pool_id: pool_id.clone(),
                transaction_hash: [0u8; 32],
                block_number: 100,
                block_timestamp: 1000,
                log_index: 0,
                amount0: I256::try_from(50i64).unwrap(),
                amount1: I256::try_from(-50i64).unwrap(),
                sqrt_price_x96: U256::ZERO,
                tick: 0,
                liquidity: U256::from(1000u64),
            },
            SwapInput {
                pool_id: pool_id.clone(),
                transaction_hash: [0u8; 32],
                block_number: 100,
                block_timestamp: 1000,
                log_index: 1,
                amount0: I256::try_from(50i64).unwrap(),
                amount1: I256::try_from(-50i64).unwrap(),
                sqrt_price_x96: U256::ZERO,
                tick: 0,
                liquidity: U256::from(1000u64),
            },
        ];

        let ops = process_swaps(&swaps, &cache, 8453, "test", "test", None, 1);
        assert!(ops.is_empty(), "all-invalid swaps should produce no ops");
    }

    #[test]
    fn test_valid_swap_produces_snapshot_and_state() {
        let pool_id = vec![0u8; 20];
        let cache = make_cache_with_pool(pool_id.clone());

        let swaps = vec![SwapInput {
            pool_id: pool_id.clone(),
            transaction_hash: [0u8; 32],
            block_number: 100,
            block_timestamp: 1000,
            log_index: 0,
            amount0: I256::try_from(100i64).unwrap(),
            amount1: I256::try_from(-100i64).unwrap(),
            sqrt_price_x96: q96(), // tick 0, price = 1
            tick: 0,
            liquidity: U256::from(1000u64),
        }];

        let ops = process_swaps(&swaps, &cache, 8453, "test", "test", None, 1);
        // Expect one pool_snapshots upsert + one pool_state upsert
        assert_eq!(ops.len(), 2, "valid swap should emit snapshot + state");
    }

    #[test]
    fn test_valid_swap_with_usd_ctx_emits_rolling_metrics() {
        let pool_id = vec![0u8; 20];
        let cache = make_cache_with_pool(pool_id.clone());

        let usd_ctx = UsdPriceContext::new_for_test(
            Some(bigdecimal::BigDecimal::from(2000)),
            None,
            Some([1u8; 20]), // quote_token matches cache's quote_token
            None,
            None,
            None,
        );

        let swaps = vec![SwapInput {
            pool_id: pool_id.clone(),
            transaction_hash: [0u8; 32],
            block_number: 100,
            block_timestamp: 1000,
            log_index: 0,
            amount0: I256::try_from(100i64).unwrap(),
            amount1: I256::try_from(-100i64).unwrap(),
            sqrt_price_x96: q96(),
            tick: 0,
            liquidity: U256::from(1000u64),
        }];

        let ops = process_swaps(&swaps, &cache, 8453, "test", "test", Some(&usd_ctx), 1);
        // snapshot + pool_state + rolling_metrics RawSql
        assert_eq!(ops.len(), 3, "USD context should add rolling metrics RawSql");

        // Verify the third op is a RawSql
        match &ops[2] {
            DbOperation::RawSql { query, params } => {
                assert!(query.contains("volume_24h_usd"));
                assert!(query.contains("price_change_1h"));
                assert_eq!(params.len(), 6);
            }
            _ => panic!("Expected RawSql for rolling metrics"),
        }
    }

    #[test]
    fn test_no_rolling_metrics_without_usd_ctx() {
        let pool_id = vec![0u8; 20];
        let cache = make_cache_with_pool(pool_id.clone());

        let swaps = vec![SwapInput {
            pool_id: pool_id.clone(),
            transaction_hash: [0u8; 32],
            block_number: 100,
            block_timestamp: 1000,
            log_index: 0,
            amount0: I256::try_from(100i64).unwrap(),
            amount1: I256::try_from(-100i64).unwrap(),
            sqrt_price_x96: q96(),
            tick: 0,
            liquidity: U256::from(1000u64),
        }];

        let ops = process_swaps(&swaps, &cache, 8453, "test", "test", None, 1);
        // snapshot + pool_state only — no RawSql
        assert_eq!(ops.len(), 2);
        for op in &ops {
            assert!(
                !matches!(op, DbOperation::RawSql { .. }),
                "Should not emit RawSql without USD context"
            );
        }
    }
}
