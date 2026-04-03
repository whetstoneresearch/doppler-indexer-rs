//! V3 pool metrics handlers.
//!
//! Processes Swap, Mint, and Burn events from DopplerV3Pool and
//! DopplerLockableV3Pool factory collections into pool_state,
//! pool_snapshots, and liquidity_deltas tables.
//!
//! Split into separate handlers for swap metrics (pool_state/pool_snapshots)
//! and liquidity metrics (liquidity_deltas) so that each handler has a
//! single primary trigger, avoiding duplicate live executions and snapshot
//! capture issues with multi-trigger handlers.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use alloy_primitives::I256;
use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_liquidity_deltas, process_swaps, LiquidityInput, SwapInput,
};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::pool_metadata::PoolMetadataCache;

// --- Shared extraction functions ---

fn extract_v3_swaps(
    ctx: &TransformationContext,
    source: &str,
) -> Result<Vec<SwapInput>, TransformationError> {
    let mut swaps = Vec::new();
    for event in ctx.events_of_type(source, "Swap") {
        swaps.push(SwapInput {
            pool_id: event.contract_address.to_vec(),
            block_number: event.block_number,
            block_timestamp: event.block_timestamp,
            log_index: event.log_index,
            amount0: event.extract_int256("amount0")?,
            amount1: event.extract_int256("amount1")?,
            sqrt_price_x96: event.extract_uint256("sqrtPriceX96")?,
            tick: event.extract_i32_flexible("tick")?,
            liquidity: event.extract_uint256("liquidity")?,
        });
    }
    Ok(swaps)
}

fn extract_v3_liquidity(
    ctx: &TransformationContext,
    source: &str,
) -> Result<Vec<LiquidityInput>, TransformationError> {
    let mut deltas = Vec::new();

    for event in ctx.events_of_type(source, "Mint") {
        let amount = event.extract_uint256("amount")?;
        let delta = I256::try_from(amount).map_err(|_| {
            TransformationError::TypeConversion(format!(
                "Mint amount {} overflows I256 (pool {}, block {})",
                amount,
                hex::encode(&event.contract_address),
                event.block_number,
            ))
        })?;
        deltas.push(LiquidityInput {
            pool_id: event.contract_address.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index,
            tick_lower: event.extract_i32_flexible("tickLower")?,
            tick_upper: event.extract_i32_flexible("tickUpper")?,
            liquidity_delta: delta,
        });
    }

    for event in ctx.events_of_type(source, "Burn") {
        let amount = event.extract_uint256("amount")?;
        let delta = I256::try_from(amount).map_err(|_| {
            TransformationError::TypeConversion(format!(
                "Burn amount {} overflows I256 (pool {}, block {})",
                amount,
                hex::encode(&event.contract_address),
                event.block_number,
            ))
        })?;
        deltas.push(LiquidityInput {
            pool_id: event.contract_address.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index,
            tick_lower: event.extract_i32_flexible("tickLower")?,
            tick_upper: event.extract_i32_flexible("tickUpper")?,
            liquidity_delta: -delta,
        });
    }

    Ok(deltas)
}

/// Refresh the metadata cache from DB if any pool IDs in the batch are missing.
async fn refresh_cache_if_needed(
    pool_ids: impl Iterator<Item = &Vec<u8>>,
    cache: &PoolMetadataCache,
    db_pool: &OnceLock<Pool>,
    chain_id: u64,
    contracts: &crate::types::config::contract::Contracts,
) {
    let missing: Vec<_> = {
        let unique: HashSet<&Vec<u8>> = pool_ids.collect();
        unique
            .into_iter()
            .filter(|id| cache.get(id).is_none())
            .collect()
    };
    if missing.is_empty() {
        return;
    }

    let Some(pool) = db_pool.get() else {
        return;
    };

    match cache.refresh(pool, chain_id).await {
        Ok(new_count) if new_count > 0 => {
            cache.resolve_quote_decimals(contracts);
            tracing::info!(
                "Metadata cache refreshed: {} new pool(s) discovered ({} were missing)",
                new_count,
                missing.len()
            );
        }
        Ok(_) => {
            tracing::debug!(
                "{} pool(s) still missing from DB after refresh",
                missing.len()
            );
        }
        Err(e) => {
            tracing::warn!("Failed to refresh metadata cache: {}", e);
        }
    }
}

// --- V3SwapMetricsHandler ---

pub struct V3SwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_resolved: AtomicBool,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for V3SwapMetricsHandler {
    fn name(&self) -> &'static str {
        "V3SwapMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_state.sql",
            "migrations/tables/pool_snapshots.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pool_state", "pool_snapshots"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        if !self.decimals_resolved.swap(true, Ordering::Relaxed) {
            self.metadata_cache
                .resolve_quote_decimals(&ctx.contracts);
        }

        let swaps = extract_v3_swaps(ctx, "DopplerV3Pool")?;

        refresh_cache_if_needed(
            swaps.iter().map(|s| &s.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
        )
        .await;

        Ok(process_swaps(&swaps, &self.metadata_cache, ctx.chain_id))
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        self.metadata_cache
            .load_into(db_pool, self.chain_id)
            .await?;
        tracing::info!("V3SwapMetricsHandler initialized");
        Ok(())
    }
}

impl EventHandler for V3SwapMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "DopplerV3Pool",
            "Swap(address,address,int256,int256,uint160,uint128,int24)",
        )]
    }

    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V3CreateHandler"]
    }
}

// --- V3LiquidityMetricsHandler ---

pub struct V3LiquidityMetricsHandler {
    chain_id: u64,
}

#[async_trait]
impl TransformationHandler for V3LiquidityMetricsHandler {
    fn name(&self) -> &'static str {
        "V3LiquidityMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/liquidity_deltas.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["liquidity_deltas"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let deltas = extract_v3_liquidity(ctx, "DopplerV3Pool")?;
        Ok(process_liquidity_deltas(&deltas, self.chain_id))
    }
}

impl EventHandler for V3LiquidityMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new(
                "DopplerV3Pool",
                "Mint(address,address,int24,int24,uint128,uint256,uint256)",
            ),
            EventTrigger::new(
                "DopplerV3Pool",
                "Burn(address,int24,int24,uint128,uint256,uint256)",
            ),
        ]
    }

    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec![]
    }
}

// --- LockableV3SwapMetricsHandler ---

pub struct LockableV3SwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_resolved: AtomicBool,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for LockableV3SwapMetricsHandler {
    fn name(&self) -> &'static str {
        "LockableV3SwapMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_state.sql",
            "migrations/tables/pool_snapshots.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pool_state", "pool_snapshots"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        if !self.decimals_resolved.swap(true, Ordering::Relaxed) {
            self.metadata_cache
                .resolve_quote_decimals(&ctx.contracts);
        }

        let swaps = extract_v3_swaps(ctx, "DopplerLockableV3Pool")?;

        refresh_cache_if_needed(
            swaps.iter().map(|s| &s.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
        )
        .await;

        Ok(process_swaps(&swaps, &self.metadata_cache, ctx.chain_id))
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        self.metadata_cache
            .load_into(db_pool, self.chain_id)
            .await?;
        tracing::info!("LockableV3SwapMetricsHandler initialized");
        Ok(())
    }
}

impl EventHandler for LockableV3SwapMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "DopplerLockableV3Pool",
            "Swap(address,address,int256,int256,uint160,uint128,int24)",
        )]
    }

    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec!["LockableV3CreateHandler"]
    }
}

// --- LockableV3LiquidityMetricsHandler ---

pub struct LockableV3LiquidityMetricsHandler {
    chain_id: u64,
}

#[async_trait]
impl TransformationHandler for LockableV3LiquidityMetricsHandler {
    fn name(&self) -> &'static str {
        "LockableV3LiquidityMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/liquidity_deltas.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["liquidity_deltas"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let deltas = extract_v3_liquidity(ctx, "DopplerLockableV3Pool")?;
        Ok(process_liquidity_deltas(&deltas, self.chain_id))
    }
}

impl EventHandler for LockableV3LiquidityMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new(
                "DopplerLockableV3Pool",
                "Mint(address,address,int24,int24,uint128,uint256,uint256)",
            ),
            EventTrigger::new(
                "DopplerLockableV3Pool",
                "Burn(address,int24,int24,uint128,uint256,uint256)",
            ),
        ]
    }

    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec!["LockableV3CreateHandler"]
    }
}

// --- Registration ---

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
) {
    // Swap metrics: pool_state + pool_snapshots (single-trigger, captures snapshots)
    registry.register_event_handler(V3SwapMetricsHandler {
        metadata_cache: cache.clone(),
        decimals_resolved: AtomicBool::new(false),
        chain_id,
        db_pool: OnceLock::new(),
    });
    registry.register_event_handler(LockableV3SwapMetricsHandler {
        metadata_cache: cache,
        decimals_resolved: AtomicBool::new(false),
        chain_id,
        db_pool: OnceLock::new(),
    });

    // Liquidity metrics: liquidity_deltas (insert-only, no snapshot concerns)
    registry.register_event_handler(V3LiquidityMetricsHandler { chain_id });
    registry.register_event_handler(LockableV3LiquidityMetricsHandler { chain_id });
}
