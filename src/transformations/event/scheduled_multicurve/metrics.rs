//! Scheduled multicurve pool metrics handlers.
//!
//! Processes Swap and ModifyLiquidity events from UniswapV4ScheduledMulticurveInitializerHook
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
use crate::transformations::util::usd_price::{
    build_usd_price_context, chainlink_latest_answer_dependency, OraclePriceCache,
};

const SOURCE: &str = "UniswapV4ScheduledMulticurveInitializerHook";

// --- ScheduledMulticurveSwapMetricsHandler ---

pub struct ScheduledMulticurveSwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
    decimals_init: Once,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for ScheduledMulticurveSwapMetricsHandler {
    fn name(&self) -> &'static str {
        "ScheduledMulticurveSwapMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_state.sql",
            "migrations/tables/pool_snapshots.sql",
            "migrations/tables/pool_snapshots_add_volume_usd.sql",
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

        let (usd_ctx, price_ops) = build_usd_price_context(
            ctx,
            &self.oracle_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
        )
        .await;

        let mut ops = process_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            SOURCE,
            Some(&usd_ctx),
            self.version(),
        );
        ops.extend(price_ops);
        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        self.oracle_cache
            .load_from_db_once(db_pool.inner(), self.chain_id)
            .await?;
        self.metadata_cache
            .load_into(db_pool, self.chain_id)
            .await?;
        tracing::info!("ScheduledMulticurveSwapMetricsHandler initialized");
        Ok(())
    }
}

impl EventHandler for ScheduledMulticurveSwapMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            SOURCE,
            "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)",
        )]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![
            (SOURCE.to_string(), "getSlot0".to_string()),
            chainlink_latest_answer_dependency(),
        ]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V4ScheduledMulticurveCreateHandler"]
    }
}

// --- ScheduledMulticurveLiquidityMetricsHandler ---

pub struct ScheduledMulticurveLiquidityMetricsHandler {
    chain_id: u64,
}

#[async_trait]
impl TransformationHandler for ScheduledMulticurveLiquidityMetricsHandler {
    fn name(&self) -> &'static str {
        "ScheduledMulticurveLiquidityMetricsHandler"
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

impl EventHandler for ScheduledMulticurveLiquidityMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            SOURCE,
            "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)",
        )]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V4ScheduledMulticurveCreateHandler"]
    }
}

// --- Registration ---

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
) {
    registry.register_event_handler(ScheduledMulticurveSwapMetricsHandler {
        metadata_cache: cache,
        oracle_cache,
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
    });
    registry.register_event_handler(ScheduledMulticurveLiquidityMetricsHandler { chain_id });
}
