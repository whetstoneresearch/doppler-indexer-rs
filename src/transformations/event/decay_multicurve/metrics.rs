//! Decay multicurve pool metrics handlers.
//!
//! Processes Swap and ModifyLiquidity events from DecayMulticurveHook
//! into pool_state, pool_snapshots, and liquidity_deltas tables.

use std::sync::{Arc, Once, OnceLock};

use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::TransformationContext;
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_liquidity_deltas, process_swaps, refresh_cache_if_needed,
};
use crate::transformations::event::metrics::v4_hook_extract::{
    extract_flat_modify_liquidity, extract_v4_hook_swaps,
};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::pool_metadata::PoolMetadataCache;

const SOURCE: &str = "DecayMulticurveHook";

// --- DecayMulticurveSwapMetricsHandler ---

pub struct DecayMulticurveSwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_init: Once,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for DecayMulticurveSwapMetricsHandler {
    fn name(&self) -> &'static str {
        "DecayMulticurveSwapMetricsHandler"
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
        self.decimals_init.call_once(|| {
            self.metadata_cache.resolve_quote_decimals(&ctx.contracts);
        });

        let swaps = extract_v4_hook_swaps(ctx, SOURCE, SOURCE)?;

        refresh_cache_if_needed(
            swaps.iter().map(|s| &s.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            self.name(),
            SOURCE,
        )
        .await?;

        Ok(process_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            SOURCE,
        ))
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        self.metadata_cache
            .load_into(db_pool, self.chain_id)
            .await?;
        tracing::info!("DecayMulticurveSwapMetricsHandler initialized");
        Ok(())
    }
}

impl EventHandler for DecayMulticurveSwapMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            SOURCE,
            "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)",
        )]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![(SOURCE.to_string(), "getSlot0".to_string())]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V4DecayMulticurveCreateHandler"]
    }
}

// --- DecayMulticurveLiquidityMetricsHandler ---

pub struct DecayMulticurveLiquidityMetricsHandler {
    chain_id: u64,
}

#[async_trait]
impl TransformationHandler for DecayMulticurveLiquidityMetricsHandler {
    fn name(&self) -> &'static str {
        "DecayMulticurveLiquidityMetricsHandler"
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
        let deltas = extract_flat_modify_liquidity(ctx, SOURCE)?;
        Ok(process_liquidity_deltas(&deltas, self.chain_id))
    }
}

impl EventHandler for DecayMulticurveLiquidityMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            SOURCE,
            "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)",
        )]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V4DecayMulticurveCreateHandler"]
    }
}

// --- Registration ---

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
) {
    registry.register_event_handler(DecayMulticurveSwapMetricsHandler {
        metadata_cache: cache,
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
    });
    registry.register_event_handler(DecayMulticurveLiquidityMetricsHandler { chain_id });
}
