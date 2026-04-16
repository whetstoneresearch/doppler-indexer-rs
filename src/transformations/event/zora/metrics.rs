use std::sync::{Arc, Once, OnceLock};

use alloy_primitives::U256;
use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_swaps, refresh_cache_if_needed, SwapInput,
};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::pool_metadata::PoolMetadataCache;
use crate::transformations::util::tick_math::sqrt_price_x96_to_tick;
use crate::transformations::util::usd_price::{
    build_usd_price_context_with_paths, chainlink_latest_answer_dependency, OraclePriceCache,
};

// The singleton `CreatorCoinHook` / `ContentCoinHook` addresses are subsets
// of the factory-spawned collections `ZoraCreatorCoinHook` /
// `ZoraContentCoinHook`, so triggering on the factory collection names
// captures the singletons as well as every instance spawned by ZoraFactory.
const CREATOR_HOOK_SOURCE: &str = "ZoraCreatorCoinHook";
const CONTENT_HOOK_SOURCE: &str = "ZoraContentCoinHook";

pub struct ZoraSwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
    decimals_init: Once,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for ZoraSwapMetricsHandler {
    fn name(&self) -> &'static str {
        "ZoraSwapMetricsHandler"
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

        let swaps = extract_zora_swaps(ctx)?;
        if swaps.is_empty() {
            return Ok(Vec::new());
        }

        refresh_cache_if_needed(
            swaps.iter().map(|s| &s.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            self.name(),
            CREATOR_HOOK_SOURCE,
        )
        .await?;

        let (usd_ctx, price_ops) = build_usd_price_context_with_paths(
            ctx,
            &self.oracle_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            &self.metadata_cache,
        )
        .await;

        let mut ops = process_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            CREATOR_HOOK_SOURCE,
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
        tracing::info!("ZoraSwapMetricsHandler initialized");
        Ok(())
    }
}

impl EventHandler for ZoraSwapMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new(
                CREATOR_HOOK_SOURCE,
                "Swapped(address,address,bool,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bool,bytes,uint160)",
            ),
            EventTrigger::new(
                CONTENT_HOOK_SOURCE,
                "Swapped(address,address,bool,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bool,bytes,uint160)",
            ),
        ]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![chainlink_latest_answer_dependency()]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["ZoraCreateHandler"]
    }
}

fn extract_zora_swaps(ctx: &TransformationContext) -> Result<Vec<SwapInput>, TransformationError> {
    let mut swaps = Vec::new();

    for source in [CREATOR_HOOK_SOURCE, CONTENT_HOOK_SOURCE] {
        for event in ctx.events_of_type(source, "Swapped") {
            let sqrt_price_x96 = event.extract_uint256("sqrtPriceX96")?;
            let tick = match sqrt_price_x96_to_tick(&sqrt_price_x96) {
                Ok(tick) => tick,
                Err(err) => {
                    tracing::warn!(
                        source,
                        block = event.block_number,
                        log_index = event.log_index,
                        sqrt_price_x96 = %sqrt_price_x96,
                        "Skipping Zora swap with invalid sqrtPriceX96: {}",
                        err
                    );
                    continue;
                }
            };

            swaps.push(SwapInput {
                pool_id: event.extract_bytes32("poolKeyHash")?.to_vec(),
                transaction_hash: event.transaction_hash,
                block_number: event.block_number,
                block_timestamp: event.block_timestamp,
                log_index: event.log_index,
                amount0: event.extract_int256("amount0")?,
                amount1: event.extract_int256("amount1")?,
                sqrt_price_x96,
                tick,
                liquidity: U256::ZERO,
            });
        }
    }

    Ok(swaps)
}

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
) {
    registry.register_event_handler(ZoraSwapMetricsHandler {
        metadata_cache: cache,
        oracle_cache,
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
    });
}
