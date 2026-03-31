//! V3 pool metrics handlers.
//!
//! Processes Swap, Mint, and Burn events from DopplerV3Pool and
//! DopplerLockableV3Pool factory collections into pool_state,
//! pool_snapshots, and liquidity_deltas tables.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use alloy_primitives::I256;
use async_trait::async_trait;

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
        deltas.push(LiquidityInput {
            pool_id: event.contract_address.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index,
            tick_lower: event.extract_i32_flexible("tickLower")?,
            tick_upper: event.extract_i32_flexible("tickUpper")?,
            liquidity_delta: I256::try_from(amount).unwrap_or(I256::MAX),
        });
    }

    for event in ctx.events_of_type(source, "Burn") {
        let amount = event.extract_uint256("amount")?;
        deltas.push(LiquidityInput {
            pool_id: event.contract_address.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index,
            tick_lower: event.extract_i32_flexible("tickLower")?,
            tick_upper: event.extract_i32_flexible("tickUpper")?,
            liquidity_delta: -I256::try_from(amount).unwrap_or(I256::MAX),
        });
    }

    Ok(deltas)
}

// --- V3MetricsHandler ---

pub struct V3MetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_resolved: AtomicBool,
}

#[async_trait]
impl TransformationHandler for V3MetricsHandler {
    fn name(&self) -> &'static str {
        "V3MetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_state.sql",
            "migrations/tables/pool_snapshots.sql",
            "migrations/tables/liquidity_deltas.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pool_state", "pool_snapshots", "liquidity_deltas"]
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
        let deltas = extract_v3_liquidity(ctx, "DopplerV3Pool")?;

        let mut ops = process_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            self.version(),
        );
        ops.extend(process_liquidity_deltas(&deltas, ctx.chain_id));
        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        // Load pool metadata; chain_id 8453 = Base (only chain for now)
        // TODO: parameterize chain_id when multi-chain support is added
        self.metadata_cache.load_into(db_pool, 8453).await?;
        tracing::info!("V3MetricsHandler initialized");
        Ok(())
    }
}

impl EventHandler for V3MetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new(
                "DopplerV3Pool",
                "Swap(address,address,int256,int256,uint160,uint128,int24)",
            ),
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
}

// --- LockableV3MetricsHandler ---

pub struct LockableV3MetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_resolved: AtomicBool,
}

#[async_trait]
impl TransformationHandler for LockableV3MetricsHandler {
    fn name(&self) -> &'static str {
        "LockableV3MetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_state.sql",
            "migrations/tables/pool_snapshots.sql",
            "migrations/tables/liquidity_deltas.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pool_state", "pool_snapshots", "liquidity_deltas"]
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
        let deltas = extract_v3_liquidity(ctx, "DopplerLockableV3Pool")?;

        let mut ops = process_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            self.version(),
        );
        ops.extend(process_liquidity_deltas(&deltas, ctx.chain_id));
        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.metadata_cache.load_into(db_pool, 8453).await?;
        tracing::info!("LockableV3MetricsHandler initialized");
        Ok(())
    }
}

impl EventHandler for LockableV3MetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new(
                "DopplerLockableV3Pool",
                "Swap(address,address,int256,int256,uint160,uint128,int24)",
            ),
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
}

// --- Registration ---

pub fn register_handlers(registry: &mut TransformationRegistry) {
    let cache = Arc::new(PoolMetadataCache::new());
    registry.register_event_handler(V3MetricsHandler {
        metadata_cache: cache.clone(),
        decimals_resolved: AtomicBool::new(false),
    });
    registry.register_event_handler(LockableV3MetricsHandler {
        metadata_cache: cache,
        decimals_resolved: AtomicBool::new(false),
    });
}
