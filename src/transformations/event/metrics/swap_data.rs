//! Normalized swap and liquidity types with shared processing logic.
//!
//! All pool type handlers normalize their events into SwapInput/LiquidityInput,
//! then call process_swaps()/process_liquidity_deltas() to produce DbOperations.

use std::collections::BTreeMap;

use alloy_primitives::{I256, U256};

use crate::db::DbOperation;
use crate::transformations::util::db::pool_metrics::{
    insert_liquidity_delta, insert_pool_snapshot, upsert_pool_state, LiquidityDeltaData,
    PoolStateData, SnapshotData,
};
use crate::transformations::util::pool_metadata::PoolMetadataCache;
use crate::transformations::util::price::sqrt_price_x96_to_price;

use super::accumulator::BlockAccumulator;

/// Normalized swap input produced by all pool type handlers.
#[derive(Debug, Clone)]
pub struct SwapInput {
    pub pool_id: Vec<u8>,
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
pub fn process_swaps(
    swaps: &[SwapInput],
    metadata_cache: &PoolMetadataCache,
    chain_id: u64,
    handler_name: &str,
    handler_version: u32,
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

    for ((pool_id, block_number), swap_group) in &grouped {
        let meta = match metadata_cache.get(pool_id) {
            Some(m) => m,
            None => {
                tracing::warn!(
                    "No metadata for pool {} at block {}, skipping {} swaps",
                    hex::encode(pool_id),
                    block_number,
                    swap_group.len()
                );
                continue;
            }
        };

        let block_timestamp = swap_group[0].block_timestamp;
        let mut acc = BlockAccumulator::new(block_timestamp);

        // Sort by log_index within the block
        let mut sorted_swaps: Vec<&&SwapInput> = swap_group.iter().collect();
        sorted_swaps.sort_by_key(|s| s.log_index);

        for swap in sorted_swaps {
            let price = sqrt_price_x96_to_price(
                &swap.sqrt_price_x96,
                meta.base_decimals,
                meta.quote_decimals,
                meta.is_token_0,
            );
            acc.record_swap(
                price,
                swap.amount0,
                swap.amount1,
                swap.tick,
                swap.sqrt_price_x96,
                swap.liquidity,
            );
        }

        accumulators.push((pool_id.clone(), *block_number, acc));
    }

    for (pool_id, block_number, acc) in &accumulators {
        // Emit snapshot
        let price_open = acc.price_open.unwrap_or(0.0);
        let price_close = acc.price_close.unwrap_or(0.0);
        let price_high = acc.price_high.unwrap_or(0.0);
        let price_low = acc.price_low.unwrap_or(0.0);

        ops.push(insert_pool_snapshot(&SnapshotData {
            chain_id,
            pool_id: pool_id.clone(),
            block_number: *block_number,
            block_timestamp: acc.block_timestamp,
            price_open: format!("{:.18}", price_open),
            price_close: format!("{:.18}", price_close),
            price_high: format!("{:.18}", price_high),
            price_low: format!("{:.18}", price_low),
            active_liquidity: acc.last_liquidity.to_string(),
            volume0: acc.volume0.to_string(),
            volume1: acc.volume1.to_string(),
            swap_count: acc.swap_count as i32,
        }));

        // Track latest block for pool_state
        match latest_per_pool.get(pool_id) {
            Some((prev_block, _)) if *prev_block >= *block_number => {}
            _ => {
                latest_per_pool.insert(pool_id.clone(), (*block_number, acc));
            }
        }
    }

    // Emit pool_state upserts for the latest block per pool
    for (pool_id, (block_number, acc)) in &latest_per_pool {
        let price_close = acc.price_close.unwrap_or(0.0);

        ops.push(upsert_pool_state(
            &PoolStateData {
                chain_id,
                pool_id: pool_id.clone(),
                block_number: *block_number,
                block_timestamp: acc.block_timestamp,
                tick: acc.last_tick,
                sqrt_price_x96: acc.last_sqrt_price_x96.to_string(),
                price: format!("{:.18}", price_close),
                active_liquidity: acc.last_liquidity.to_string(),
            },
            handler_name,
            handler_version,
        ));
    }

    ops
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
