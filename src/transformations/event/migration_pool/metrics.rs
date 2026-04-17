//! Migration pool metrics handlers.
//!
//! Processes swaps and liquidity changes on graduated (migrated) Doppler V4 pools.
//! After migration, swaps flow through `UniswapV4PoolManager` and initial liquidity
//! through `UniswapV4MigratorHook`.
//!
//! ## Swap filtering
//!
//! `UniswapV4PoolManager` emits `Swap` for every V4 pool on the chain. The swap
//! handler maintains an in-memory `HashSet` of known migration pool IDs seeded at
//! init from `pools WHERE migrated_from IS NOT NULL` and refreshed from the DB on
//! each `handle()` invocation to pick up pools that graduated since init (e.g.
//! during catchup processing).
//!
//! ## Liquidity
//!
//! `UniswapV4MigratorHook` only emits `ModifyLiquidity` for migration pools, so
//! no filtering is needed. Pool ID is computed from the tuple PoolKey via keccak256,
//! matching the same approach used by V4 hook liquidity handlers.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Once, OnceLock, RwLock};

use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_liquidity_deltas, process_swaps, refresh_cache_if_needed, SwapInput,
};
use crate::transformations::event::metrics::tvl::{process_tvl, TvlHandlerConfig, TvlTarget};
use crate::transformations::event::metrics::v4_hook_extract::extract_tuple_modify_liquidity;
use crate::transformations::event::migration_pool::create::{
    MIGRATION_POOL_CREATE_HANDLER_NAME, MIGRATION_POOL_CREATE_HANDLER_VERSION,
};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::pool_metadata::PoolMetadataCache;
use crate::transformations::util::usd_price::{
    build_usd_price_context_with_paths, chainlink_latest_answer_dependency, OraclePriceCache,
};

const POOL_MANAGER_SOURCE: &str = "UniswapV4PoolManager";
const MIGRATOR_HOOK_SOURCE: &str = "UniswapV4MigratorHook";

// ─── Swap handler ─────────────────────────────────────────────────────────────

pub struct MigrationPoolSwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
    decimals_init: Once,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
    /// bytes32 pool IDs of known migration pools.
    /// Seeded at init from `pools WHERE migrated_from IS NOT NULL`;
    /// refreshed from DB on each `handle()` to discover newly graduated pools.
    migration_pool_ids: RwLock<HashSet<Vec<u8>>>,
}

impl MigrationPoolSwapMetricsHandler {
    /// Re-query the DB for migration pool IDs to pick up pools that graduated
    /// since initialization (e.g. committed by MigrationPoolCreateHandler during
    /// catchup).
    async fn refresh_migration_pool_ids(&self) -> Result<(), TransformationError> {
        let Some(pool) = self.db_pool.get() else {
            return Ok(());
        };
        let client = pool.get().await?;
        let rows = client
            .query(
                "SELECT p.address \
                 FROM pools p \
                 WHERE p.chain_id = $1 \
                   AND p.migrated_from IS NOT NULL \
                   AND p.source = $2 \
                   AND p.source_version = $3",
                &[
                    &(self.chain_id as i64),
                    &MIGRATION_POOL_CREATE_HANDLER_NAME,
                    &(MIGRATION_POOL_CREATE_HANDLER_VERSION as i32),
                ],
            )
            .await?;

        let mut ids = self.migration_pool_ids.write().unwrap();
        let before = ids.len();
        for row in &rows {
            let address: Vec<u8> = row.get("address");
            ids.insert(address);
        }
        let added = ids.len() - before;
        if added > 0 {
            tracing::info!(
                added,
                total = ids.len(),
                chain_id = self.chain_id,
                "Refreshed migration pool ID set"
            );
        }
        Ok(())
    }
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
            "migrations/tables/pool_state_add_tvl.sql",
            "migrations/tables/pool_snapshots.sql",
            "migrations/tables/pool_snapshots_add_tvl.sql",
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
            self.metadata_cache
                .resolve_quote_decimals(ctx.contracts_ref());
        });

        // Refresh the migration pool ID set from DB to pick up any pools
        // graduated since init (e.g. by MigrationPoolCreateHandler during catchup).
        self.refresh_migration_pool_ids().await?;

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
                transaction_hash: event.evm_tx_hash(),
                block_number: event.block_number,
                block_timestamp: event.block_timestamp,
                log_index: event.log_index(),
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
            ctx.contracts_ref(),
            self.name(),
            POOL_MANAGER_SOURCE,
            Some((ctx.blockrange_start, ctx.blockrange_end)),
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
            POOL_MANAGER_SOURCE,
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
        // Seed the migration pool ID set from pools that have already graduated.
        let client = db_pool.inner().get().await?;
        let rows = client
            .query(
                "SELECT p.address \
                 FROM pools p \
                 WHERE p.chain_id = $1 \
                   AND p.migrated_from IS NOT NULL \
                   AND p.source = $2 \
                   AND p.source_version = $3",
                &[
                    &(self.chain_id as i64),
                    &MIGRATION_POOL_CREATE_HANDLER_NAME,
                    &(MIGRATION_POOL_CREATE_HANDLER_VERSION as i32),
                ],
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

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["MigrationPoolCreateHandler"]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![chainlink_latest_answer_dependency()]
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

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["MigrationPoolCreateHandler"]
    }
}

// ─── TVL handler ──────────────────────────────────────────────────────────────

pub struct MigrationPoolTvlMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
    tvl_config: TvlHandlerConfig,
    /// bytes32 pool IDs of known migration pools (same filter as swap handler).
    migration_pool_ids: RwLock<HashSet<Vec<u8>>>,
}

impl MigrationPoolTvlMetricsHandler {
    async fn refresh_migration_pool_ids(&self) -> Result<(), TransformationError> {
        let Some(pool) = self.db_pool.get() else {
            return Ok(());
        };
        let client = pool.get().await?;
        let rows = client
            .query(
                "SELECT p.address \
                 FROM pools p \
                 WHERE p.chain_id = $1 \
                   AND p.migrated_from IS NOT NULL \
                   AND p.source = $2 \
                   AND p.source_version = $3",
                &[
                    &(self.chain_id as i64),
                    &MIGRATION_POOL_CREATE_HANDLER_NAME,
                    &(MIGRATION_POOL_CREATE_HANDLER_VERSION as i32),
                ],
            )
            .await?;

        let mut ids = self.migration_pool_ids.write().unwrap();
        let before = ids.len();
        for row in &rows {
            let address: Vec<u8> = row.get("address");
            ids.insert(address);
        }
        let added = ids.len() - before;
        if added > 0 {
            tracing::info!(
                added,
                total = ids.len(),
                chain_id = self.chain_id,
                "TVL handler refreshed migration pool ID set"
            );
        }
        Ok(())
    }
}

#[async_trait]
impl TransformationHandler for MigrationPoolTvlMetricsHandler {
    fn name(&self) -> &'static str {
        "MigrationPoolTvlMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_state_add_tvl.sql",
            "migrations/tables/pool_state_add_active_liquidity_usd.sql",
            "migrations/tables/pool_snapshots_add_tvl.sql",
            "migrations/tables/pool_snapshots_add_active_liquidity_usd.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pool_state", "pool_snapshots"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        self.refresh_migration_pool_ids().await?;

        // Extract PoolManager Swap events, filtering to known migration pool IDs,
        // deduplicated by (pool_id, block_number).
        let mut by_pool_block: BTreeMap<(Vec<u8>, u64), TvlTarget> = BTreeMap::new();
        for event in ctx.events_of_type(POOL_MANAGER_SOURCE, "Swap") {
            let pool_id = event.extract_bytes32("id")?;

            let known = {
                let ids = self.migration_pool_ids.read().unwrap();
                ids.contains(pool_id.as_slice())
            };
            if !known {
                continue;
            }

            let sqrt_price_x96 = event.extract_uint256("sqrtPriceX96")?;
            let tick = event.extract_i32_flexible("tick")?;

            let key = (pool_id.to_vec(), event.block_number);
            by_pool_block.insert(
                key,
                TvlTarget {
                    pool_id: pool_id.to_vec(),
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    tick,
                    sqrt_price_x96,
                },
            );
        }

        let targets: Vec<TvlTarget> = by_pool_block.into_values().collect();
        if targets.is_empty() {
            return Ok(Vec::new());
        }

        refresh_cache_if_needed(
            targets.iter().map(|t| &t.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            self.name(),
            POOL_MANAGER_SOURCE,
            Some((ctx.blockrange_start, ctx.blockrange_end)),
        )
        .await?;

        let Some(pool) = self.db_pool.get() else {
            return Ok(Vec::new());
        };

        let (usd_ctx, price_ops) = build_usd_price_context_with_paths(
            ctx,
            &self.oracle_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            &self.metadata_cache,
        )
        .await;

        let mut ops = process_tvl(
            &targets,
            &self.tvl_config,
            &self.metadata_cache,
            pool,
            self.chain_id,
            &usd_ctx,
        )
        .await?;
        ops.extend(price_ops);
        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        self.oracle_cache
            .load_from_db_once(db_pool.inner(), self.chain_id)
            .await?;
        let client = db_pool.inner().get().await?;
        let rows = client
            .query(
                "SELECT p.address \
                 FROM pools p \
                 WHERE p.chain_id = $1 \
                   AND p.migrated_from IS NOT NULL \
                   AND p.source = $2 \
                   AND p.source_version = $3",
                &[
                    &(self.chain_id as i64),
                    &MIGRATION_POOL_CREATE_HANDLER_NAME,
                    &(MIGRATION_POOL_CREATE_HANDLER_VERSION as i32),
                ],
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
            "MigrationPoolTvlMetricsHandler initialized"
        );
        Ok(())
    }
}

impl EventHandler for MigrationPoolTvlMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            POOL_MANAGER_SOURCE,
            "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)",
        )]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec![
            "MigrationPoolCreateHandler",
            "MigrationPoolLiquidityMetricsHandler",
        ]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![chainlink_latest_answer_dependency()]
    }
}

// ─── Registration ──────────────────────────────────────────────────────────────

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
) {
    registry.register_event_handler(MigrationPoolSwapMetricsHandler {
        metadata_cache: cache.clone(),
        oracle_cache: Arc::clone(&oracle_cache),
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
        migration_pool_ids: RwLock::new(HashSet::new()),
    });
    registry.register_event_handler(MigrationPoolLiquidityMetricsHandler { chain_id });
    registry.register_event_handler(MigrationPoolTvlMetricsHandler {
        metadata_cache: cache,
        oracle_cache,
        chain_id,
        db_pool: OnceLock::new(),
        tvl_config: TvlHandlerConfig {
            liquidity_source: "MigrationPoolLiquidityMetricsHandler",
            liquidity_source_version: 1,
            swap_source: "MigrationPoolSwapMetricsHandler",
            swap_source_version: 1,
        },
        migration_pool_ids: RwLock::new(HashSet::new()),
    });
}
