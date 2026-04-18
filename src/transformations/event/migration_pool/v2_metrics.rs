//! V2 migration pool swap metrics handler.
//!
//! Processes UniswapV2 Swap events emitted by graduated V2 migration pair
//! contracts. The `MigrationPool` factory collection in shared.json already
//! spawns per-pair Swap listeners from Airlock.Migrate events, so these events
//! arrive in the context with source="MigrationPool".
//!
//! Price is computed from execution amounts rather than sqrtPriceX96 (which V2
//! does not have). tick and liquidity are stored as 0 in the DB.

use std::collections::HashSet;
use std::sync::{Arc, Once, OnceLock, RwLock};

use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_v2_swaps, refresh_cache_if_needed, V2SwapInput,
};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::pool_metadata::PoolMetadataCache;
use crate::transformations::util::usd_price::{
    build_usd_price_context_with_paths, chainlink_latest_answer_dependency, OraclePriceCache,
};

const MIGRATION_POOL_SOURCE: &str = "MigrationPool";

pub struct V2MigrationSwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
    decimals_init: Once,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
    /// 20-byte pair addresses of known V2 migration pools.
    /// Seeded at init and refreshed per handle() to pick up newly graduated pools.
    v2_pair_addresses: RwLock<HashSet<Vec<u8>>>,
}

impl V2MigrationSwapMetricsHandler {
    async fn refresh_v2_pair_addresses(&self) -> Result<(), TransformationError> {
        let Some(pool) = self.db_pool.get() else {
            return Ok(());
        };
        let client = pool.get().await?;
        let rows = client
            .query(
                "SELECT p.address \
                 FROM pools p \
                 WHERE p.chain_id = $1 \
                   AND p.migration_type = 'v2' \
                   AND p.migrated_from IS NOT NULL",
                &[&(self.chain_id as i64)],
            )
            .await?;

        let mut addrs = self.v2_pair_addresses.write().unwrap();
        let before = addrs.len();
        for row in &rows {
            let address: Vec<u8> = row.get("address");
            addrs.insert(address);
        }
        let added = addrs.len() - before;
        if added > 0 {
            tracing::info!(
                added,
                total = addrs.len(),
                chain_id = self.chain_id,
                "V2MigrationSwapMetricsHandler refreshed pair address set"
            );
        }
        Ok(())
    }
}

#[async_trait]
impl TransformationHandler for V2MigrationSwapMetricsHandler {
    fn name(&self) -> &'static str {
        "V2MigrationSwapMetricsHandler"
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
            self.metadata_cache.resolve_quote_decimals(&ctx.contracts);
        });

        self.refresh_v2_pair_addresses().await?;

        let mut swaps: Vec<V2SwapInput> = Vec::new();
        for event in ctx.events_of_type(
            MIGRATION_POOL_SOURCE,
            "Swap(address,uint256,uint256,uint256,uint256,address)",
        ) {
            let pair_address = event.contract_address;

            let known = {
                let addrs = self.v2_pair_addresses.read().unwrap();
                addrs.contains(pair_address.as_slice())
            };
            if !known {
                continue;
            }

            swaps.push(V2SwapInput {
                pool_id: pair_address.to_vec(),
                transaction_hash: event.transaction_hash,
                block_number: event.block_number,
                block_timestamp: event.block_timestamp,
                log_index: event.log_index,
                amount0_in: event.extract_uint256("amount0In")?,
                amount1_in: event.extract_uint256("amount1In")?,
                amount0_out: event.extract_uint256("amount0Out")?,
                amount1_out: event.extract_uint256("amount1Out")?,
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
            self.name(),
            MIGRATION_POOL_SOURCE,
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

        let mut ops = process_v2_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            MIGRATION_POOL_SOURCE,
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

        let client = db_pool.inner().get().await?;
        let rows = client
            .query(
                "SELECT p.address \
                 FROM pools p \
                 WHERE p.chain_id = $1 \
                   AND p.migration_type = 'v2' \
                   AND p.migrated_from IS NOT NULL",
                &[&(self.chain_id as i64)],
            )
            .await?;

        let mut addrs = self.v2_pair_addresses.write().unwrap();
        for row in &rows {
            let address: Vec<u8> = row.get("address");
            addrs.insert(address);
        }

        tracing::info!(
            count = addrs.len(),
            chain_id = self.chain_id,
            "V2MigrationSwapMetricsHandler initialized"
        );
        Ok(())
    }
}

impl EventHandler for V2MigrationSwapMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            MIGRATION_POOL_SOURCE,
            "Swap(address,uint256,uint256,uint256,uint256,address)",
        )]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V2MigrationPoolCreateHandler"]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![chainlink_latest_answer_dependency()]
    }
}

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
) {
    registry.register_event_handler(V2MigrationSwapMetricsHandler {
        metadata_cache: cache,
        oracle_cache,
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
        v2_pair_addresses: RwLock::new(HashSet::new()),
    });
}
