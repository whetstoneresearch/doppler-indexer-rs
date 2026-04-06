//! Migration pool metrics handlers.
//!
//! Processes swaps and liquidity changes on graduated (migrated) Doppler V4 pools.
//! After migration, swaps flow through `UniswapV4PoolManager` and initial liquidity
//! through `UniswapV4MigratorHook`.
//!
//! ## Swap filtering
//!
//! `UniswapV4PoolManager` emits `Swap` for every V4 pool on the chain. The swap
//! handler maintains an in-memory `HashSet` of known migration pool IDs loaded at
//! init from `pools WHERE migrated_from IS NOT NULL`. `Migrate` events observed
//! in the current context are also added inline so that a pool's first swap in
//! the same range as its graduation is not missed.
//!
//! ## Liquidity
//!
//! `UniswapV4MigratorHook` only emits `ModifyLiquidity` for migration pools, so
//! no filtering is needed. Pool ID is computed from the tuple PoolKey via keccak256,
//! matching the same approach used by V4 hook liquidity handlers.

use std::collections::HashSet;
use std::sync::{Arc, Once, OnceLock, RwLock};

use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_liquidity_deltas, process_swaps, refresh_cache_if_needed, SwapInput,
};
use crate::transformations::event::metrics::v4_hook_extract::extract_tuple_modify_liquidity;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::pool_metadata::PoolMetadataCache;

const POOL_MANAGER_SOURCE: &str = "UniswapV4PoolManager";
const MIGRATOR_HOOK_SOURCE: &str = "UniswapV4MigratorHook";
const MIGRATOR_SOURCE: &str = "UniswapV4Migrator";

// ─── Swap handler ─────────────────────────────────────────────────────────────

pub struct MigrationPoolSwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_init: Once,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
    /// bytes32 pool IDs of known migration pools.
    /// Loaded at init from `pools WHERE migrated_from IS NOT NULL`;
    /// augmented inline per-handle() from Migrate events in the current context.
    migration_pool_ids: RwLock<HashSet<Vec<u8>>>,
}

#[async_trait]
impl TransformationHandler for MigrationPoolSwapMetricsHandler {
    fn name(&self) -> &'static str {
        "MigrationPoolSwapMetricsHandler"
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

        // Add any newly graduated pools from Migrate events in this context so that
        // a pool's first swap in the same range as its graduation is not missed.
        {
            let migrate_events: Vec<_> =
                ctx.events_of_type(MIGRATOR_SOURCE, "Migrate").collect();
            if !migrate_events.is_empty() {
                let mut ids = self.migration_pool_ids.write().unwrap();
                for event in migrate_events {
                    match event.extract_bytes32("poolId") {
                        Ok(pool_id) => {
                            ids.insert(pool_id.to_vec());
                        }
                        Err(e) => {
                            tracing::warn!(
                                block = event.block_number,
                                "failed to extract poolId from Migrate event: {e}"
                            );
                        }
                    }
                }
            }
        }

        // Extract PoolManager Swap events, filtering to known migration pool IDs.
        let mut swaps: Vec<SwapInput> = Vec::new();
        for event in ctx.events_of_type(POOL_MANAGER_SOURCE, "Swap") {
            let pool_id = event.extract_bytes32("id")?;

            let known = {
                let ids = self.migration_pool_ids.read().unwrap();
                ids.contains(pool_id.as_slice())
            };
            if !known {
                continue;
            }

            swaps.push(SwapInput {
                pool_id: pool_id.to_vec(),
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

        if swaps.is_empty() {
            return Ok(Vec::new());
        }

        refresh_cache_if_needed(
            swaps.iter().map(|s| &s.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
        )
        .await?;

        Ok(process_swaps(&swaps, &self.metadata_cache, ctx.chain_id))
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        self.metadata_cache
            .load_into(db_pool, self.chain_id)
            .await?;

        // Seed the migration pool ID set from pools that have already graduated.
        let client = db_pool.inner().get().await?;
        let rows = client
            .query(
                "SELECT p.address \
                 FROM pools p \
                 JOIN active_versions av \
                   ON p.source = av.source AND p.source_version = av.active_version \
                 WHERE p.chain_id = $1 AND p.migrated_from IS NOT NULL",
                &[&(self.chain_id as i64)],
            )
            .await?;

        let mut ids = self.migration_pool_ids.write().unwrap();
        for row in &rows {
            let address: Vec<u8> = row.get("address");
            ids.insert(address);
        }

        tracing::info!(
            count = ids.len(),
            chain_id = self.chain_id,
            "MigrationPoolSwapMetricsHandler initialized"
        );
        Ok(())
    }
}

impl EventHandler for MigrationPoolSwapMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            POOL_MANAGER_SOURCE,
            "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)",
        )]
    }

    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec!["MigrationPoolCreateHandler"]
    }
}

// ─── Liquidity handler ─────────────────────────────────────────────────────────

pub struct MigrationPoolLiquidityMetricsHandler {
    chain_id: u64,
}

#[async_trait]
impl TransformationHandler for MigrationPoolLiquidityMetricsHandler {
    fn name(&self) -> &'static str {
        "MigrationPoolLiquidityMetricsHandler"
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
        let deltas = extract_tuple_modify_liquidity(ctx, MIGRATOR_HOOK_SOURCE)?;
        Ok(process_liquidity_deltas(&deltas, self.chain_id))
    }
}

impl EventHandler for MigrationPoolLiquidityMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            MIGRATOR_HOOK_SOURCE,
            "ModifyLiquidity((address,address,uint24,int24,address),(int24,int24,int256,bytes32))",
        )]
    }

    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec!["MigrationPoolCreateHandler"]
    }
}

// ─── Registration ──────────────────────────────────────────────────────────────

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
) {
    registry.register_event_handler(MigrationPoolSwapMetricsHandler {
        metadata_cache: cache,
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
        migration_pool_ids: RwLock::new(HashSet::new()),
    });
    registry.register_event_handler(MigrationPoolLiquidityMetricsHandler { chain_id });
}
