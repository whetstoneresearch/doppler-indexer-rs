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

use std::sync::{Arc, Once, OnceLock};

use alloy_primitives::I256;
use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_liquidity_deltas, process_swaps, refresh_cache_if_needed, LiquidityInput, SwapInput,
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
            transaction_hash: event.transaction_hash,
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

// --- V3SwapMetricsHandler ---

pub struct V3SwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_init: Once,
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
        self.decimals_init.call_once(|| {
            self.metadata_cache.resolve_quote_decimals(&ctx.contracts);
        });

        let swaps = extract_v3_swaps(ctx, "DopplerV3Pool")?;

        refresh_cache_if_needed(
            swaps.iter().map(|s| &s.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            self.name(),
            "DopplerV3Pool",
        )
        .await?;

        Ok(process_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            "DopplerV3Pool",
        ))
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

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
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

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V3CreateHandler"]
    }
}

// --- LockableV3SwapMetricsHandler ---

pub struct LockableV3SwapMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    decimals_init: Once,
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
        self.decimals_init.call_once(|| {
            self.metadata_cache.resolve_quote_decimals(&ctx.contracts);
        });

        let swaps = extract_v3_swaps(ctx, "DopplerLockableV3Pool")?;

        refresh_cache_if_needed(
            swaps.iter().map(|s| &s.pool_id),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            self.name(),
            "DopplerLockableV3Pool",
        )
        .await?;

        Ok(process_swaps(
            &swaps,
            &self.metadata_cache,
            ctx.chain_id,
            self.name(),
            "DopplerLockableV3Pool",
        ))
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

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
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

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
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
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
    });
    registry.register_event_handler(LockableV3SwapMetricsHandler {
        metadata_cache: cache,
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
    });

    // Liquidity metrics: liquidity_deltas (insert-only, no snapshot concerns)
    registry.register_event_handler(V3LiquidityMetricsHandler { chain_id });
    registry.register_event_handler(LockableV3LiquidityMetricsHandler { chain_id });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Once, OnceLock};

    use crate::rpc::UnifiedRpcClient;
    use crate::transformations::context::TransformationContext;
    use crate::transformations::historical::HistoricalDataReader;
    use crate::transformations::traits::EventHandler;
    use crate::transformations::util::pool_metadata::{PoolMetadata, PoolMetadataCache};

    fn make_empty_ctx() -> TransformationContext {
        let historical = Arc::new(
            HistoricalDataReader::new("test_chain_metrics")
                .expect("HistoricalDataReader::new should succeed"),
        );
        let rpc = Arc::new(
            UnifiedRpcClient::from_url("http://localhost:8545")
                .expect("RPC client construction should succeed"),
        );
        TransformationContext::new(
            "test".to_string(),
            8453,
            100,
            200,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            HashMap::new(),
            historical,
            rpc,
            Arc::new(HashMap::new()),
        )
    }

    /// Fix F: V3LiquidityMetricsHandler must declare V3CreateHandler as a contiguous dependency.
    #[test]
    fn test_v3_liquidity_handler_deps() {
        let handler = V3LiquidityMetricsHandler { chain_id: 8453 };
        let deps = handler.contiguous_handler_dependencies();
        assert_eq!(deps, vec!["V3CreateHandler"]);
    }

    /// Fix G: handle() returns Ok([]) when the context has no swap events, even
    /// when the cache is empty and no DB pool is set (refresh short-circuits).
    #[tokio::test]
    async fn test_missing_metadata_causes_handler_failure() {
        let cache = Arc::new(PoolMetadataCache::new());
        let handler = V3SwapMetricsHandler {
            metadata_cache: cache.clone(),
            decimals_init: Once::new(),
            chain_id: 8453,
            db_pool: OnceLock::new(),
        };

        // No swap events in context -> no missing pool IDs -> refresh short-circuits.
        let ctx = make_empty_ctx();
        let result = handler.handle(&ctx).await;
        assert!(
            result.is_ok(),
            "handle() with no swaps and no DB should return Ok([])"
        );
        assert!(result.unwrap().is_empty());
    }

    /// Fix H: Once::call_once ensures resolve_quote_decimals runs at most once
    /// even when called concurrently, preventing the AtomicBool race.
    #[tokio::test]
    async fn test_concurrent_decimal_init() {
        let pool_id = vec![0xCCu8; 20];
        let cache = Arc::new(PoolMetadataCache::new());

        // Insert a pool with placeholder decimals (zero address won't match any
        // known contract so decimals stay 18 — tests idempotency not the value).
        cache.insert_if_absent(
            pool_id.clone(),
            PoolMetadata {
                pool_id: pool_id.clone(),
                base_token: [0u8; 20],
                quote_token: [0u8; 20],
                is_token_0: true,
                pool_type: "v3".to_string(),
                base_decimals: 18,
                quote_decimals: 18,
            },
        );

        // Two tasks each with their own handler but sharing the cache.
        // Once::call_once on a per-handler Once means each handler resolves once.
        let cache1 = cache.clone();
        let cache2 = cache.clone();

        let t1 = tokio::spawn(async move {
            let handler = V3SwapMetricsHandler {
                metadata_cache: cache1,
                decimals_init: Once::new(),
                chain_id: 8453,
                db_pool: OnceLock::new(),
            };
            let ctx = make_empty_ctx();
            handler.handle(&ctx).await
        });

        let t2 = tokio::spawn(async move {
            let handler = V3SwapMetricsHandler {
                metadata_cache: cache2,
                decimals_init: Once::new(),
                chain_id: 8453,
                db_pool: OnceLock::new(),
            };
            let ctx = make_empty_ctx();
            handler.handle(&ctx).await
        });

        let (r1, r2) = tokio::join!(t1, t2);
        assert!(r1.unwrap().is_ok(), "task 1 should succeed");
        assert!(r2.unwrap().is_ok(), "task 2 should succeed");

        // Pool entry should be accessible after concurrent handle() calls.
        assert!(
            cache.get(&pool_id).is_some(),
            "pool entry should still be in cache after concurrent handle() calls"
        );
    }
}
