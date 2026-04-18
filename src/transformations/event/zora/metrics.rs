use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Once, OnceLock};

use alloy_primitives::U256;
use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{DecodedValue, FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_swaps, refresh_cache_if_needed, SwapInput,
};
use crate::transformations::event::metrics::tvl::{
    process_tvl_from_positions, TickMapPosition, TvlTargetWithPositions,
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
        vec!["ZoraCreateHandler", "ZoraMigrateHandler"]
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

// --- ZoraTvlMetricsHandler ---
//
// Parameterized by source so each hook gets its own handler instance with
// an independent getPoolCoin call dependency. This avoids blocking the
// handler during retry/replay when only one hook type has events.

const SWAPPED_SIG: &str = "Swapped(address,address,bool,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bool,bytes,uint160)";

pub struct ZoraTvlMetricsHandler {
    source: &'static str,
    metadata_cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for ZoraTvlMetricsHandler {
    fn name(&self) -> &'static str {
        match self.source {
            CREATOR_HOOK_SOURCE => "ZoraCreatorTvlMetricsHandler",
            CONTENT_HOOK_SOURCE => "ZoraContentTvlMetricsHandler",
            _ => unreachable!(),
        }
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
        let targets = extract_zora_tvl_targets_for_source(ctx, self.source)?;
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
            self.source,
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

        let mut ops = process_tvl_from_positions(
            &targets,
            &self.metadata_cache,
            self.chain_id,
            &usd_ctx,
            "ZoraSwapMetricsHandler",
            1,
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
        tracing::info!("{} initialized", self.name());
        Ok(())
    }
}

impl EventHandler for ZoraTvlMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(self.source, SWAPPED_SIG)]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![
            (self.source.to_string(), "getPoolCoin".to_string()),
            chainlink_latest_answer_dependency(),
        ]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["ZoraCreateHandler", "ZoraSwapMetricsHandler"]
    }
}

// --- TVL extraction ---

/// Extract positions from a getPoolCoin call result.
///
/// The return type is `(address coin, (int24 tickLower, int24 tickUpper, uint128 liquidity)[] positions)`.
/// We extract the `positions` array field and convert each element to a `TickMapPosition`.
fn extract_positions_from_call(
    call: &crate::transformations::context::DecodedCall,
) -> Result<Vec<TickMapPosition>, TransformationError> {
    let positions_value = call.result.get("positions").ok_or_else(|| {
        TransformationError::MissingData(format!(
            "getPoolCoin result missing 'positions' field at block {} log_index {:?}",
            call.block_number, call.trigger_log_index
        ))
    })?;

    match positions_value {
        DecodedValue::Array(elements) => {
            let mut positions = Vec::with_capacity(elements.len());
            for elem in elements {
                let tick_lower = elem
                    .get_field("tickLower")
                    .and_then(|v| v.as_i32())
                    .ok_or_else(|| {
                        TransformationError::TypeConversion(format!(
                            "position tickLower missing or not i32 at block {} log_index {:?}",
                            call.block_number, call.trigger_log_index
                        ))
                    })?;
                let tick_upper = elem
                    .get_field("tickUpper")
                    .and_then(|v| v.as_i32())
                    .ok_or_else(|| {
                        TransformationError::TypeConversion(format!(
                            "position tickUpper missing or not i32 at block {} log_index {:?}",
                            call.block_number, call.trigger_log_index
                        ))
                    })?;
                let liquidity = elem
                    .get_field("liquidity")
                    .and_then(|v| match v {
                        DecodedValue::Uint128(val) => Some(*val),
                        _ => v.as_uint256().and_then(|u| u.try_into().ok()),
                    })
                    .ok_or_else(|| {
                        TransformationError::TypeConversion(format!(
                            "position liquidity missing or not u128 at block {} log_index {:?}",
                            call.block_number, call.trigger_log_index
                        ))
                    })?;

                if liquidity > 0 {
                    positions.push(TickMapPosition {
                        tick_lower,
                        tick_upper,
                        liquidity,
                    });
                }
            }
            Ok(positions)
        }
        _ => Err(TransformationError::MissingData(format!(
            "getPoolCoin 'positions' is not an array at block {} log_index {:?}",
            call.block_number, call.trigger_log_index
        ))),
    }
}

/// Extract TVL targets with embedded positions from Zora Swapped events
/// and their corresponding getPoolCoin call results for a single source.
///
/// Multiple Swapped events for the same pool in the same block produce
/// identical getPoolCoin results (same view function, same block state).
/// We deduplicate by (pool_id, block_number) first and only look up one
/// call per group, using the latest event's log_index as the representative.
fn extract_zora_tvl_targets_for_source(
    ctx: &TransformationContext,
    source: &str,
) -> Result<Vec<TvlTargetWithPositions>, TransformationError> {
    struct EventSnapshot {
        log_index: u32,
        block_timestamp: u64,
        tick: i32,
        sqrt_price_x96: U256,
    }

    let mut best_per_pool_block: BTreeMap<(Vec<u8>, u64), EventSnapshot> = BTreeMap::new();

    for event in ctx.events_of_type(source, "Swapped") {
        let sqrt_price_x96 = event.extract_uint256("sqrtPriceX96")?;
        let tick = match sqrt_price_x96_to_tick(&sqrt_price_x96) {
            Ok(t) => t,
            Err(err) => {
                tracing::warn!(
                    source,
                    block = event.block_number,
                    log_index = event.log_index,
                    "Skipping Zora TVL with invalid sqrtPriceX96: {}",
                    err
                );
                continue;
            }
        };

        let pool_id = event.extract_bytes32("poolKeyHash")?.to_vec();
        let key = (pool_id, event.block_number);
        best_per_pool_block.insert(
            key,
            EventSnapshot {
                log_index: event.log_index,
                block_timestamp: event.block_timestamp,
                tick,
                sqrt_price_x96,
            },
        );
    }

    let calls_by_key: HashMap<(u64, u32), _> = ctx
        .calls_of_type(source, "getPoolCoin")
        .filter_map(|call| {
            call.trigger_log_index
                .map(|idx| ((call.block_number, idx), call))
        })
        .collect();

    let mut targets = Vec::with_capacity(best_per_pool_block.len());

    for ((pool_id, block_number), snap) in best_per_pool_block {
        let call = calls_by_key
            .get(&(block_number, snap.log_index))
            .ok_or_else(|| {
                TransformationError::MissingData(format!(
                    "No getPoolCoin call for Zora TVL target at block {} log_index {} (source: {})",
                    block_number, snap.log_index, source
                ))
            })?;

        if call.is_reverted {
            tracing::warn!(
                source,
                block = block_number,
                log_index = snap.log_index,
                "getPoolCoin reverted, skipping"
            );
            continue;
        }

        let positions = extract_positions_from_call(call)?;

        targets.push(TvlTargetWithPositions {
            pool_id,
            block_number,
            block_timestamp: snap.block_timestamp,
            tick: snap.tick,
            sqrt_price_x96: snap.sqrt_price_x96,
            positions,
        });
    }

    Ok(targets)
}

// --- Registration ---

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
) {
    registry.register_event_handler(ZoraSwapMetricsHandler {
        metadata_cache: cache.clone(),
        oracle_cache: Arc::clone(&oracle_cache),
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
    });
    registry.register_event_handler(ZoraTvlMetricsHandler {
        source: CREATOR_HOOK_SOURCE,
        metadata_cache: cache.clone(),
        oracle_cache: Arc::clone(&oracle_cache),
        chain_id,
        db_pool: OnceLock::new(),
    });
    registry.register_event_handler(ZoraTvlMetricsHandler {
        source: CONTENT_HOOK_SOURCE,
        metadata_cache: cache,
        oracle_cache,
        chain_id,
        db_pool: OnceLock::new(),
    });
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy_primitives::U256;

    use crate::rpc::UnifiedRpcClient;
    use crate::transformations::context::{DecodedCall, DecodedEvent, DecodedValue};
    use crate::transformations::historical::HistoricalDataReader;
    use crate::transformations::traits::EventHandler;
    use crate::transformations::util::pool_metadata::PoolMetadata;
    use crate::transformations::util::usd_price::UsdPriceContext;
    use crate::{db::DbOperation, transformations::traits::TransformationHandler};

    use super::*;

    fn make_test_ctx(events: Vec<DecodedEvent>, calls: Vec<DecodedCall>) -> TransformationContext {
        let historical = Arc::new(
            HistoricalDataReader::new("test_zora_metrics")
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
            Arc::new(events),
            Arc::new(calls),
            Arc::new(HashMap::new()),
            historical,
            rpc,
            Arc::new(HashMap::new()),
        )
    }

    fn q96() -> U256 {
        U256::from(1u64) << 96
    }

    fn swapped_event(source: &str, block_number: u64, log_index: u32, pool_id: [u8; 32]) -> DecodedEvent {
        let mut params = HashMap::new();
        params.insert(Arc::from("poolKeyHash"), DecodedValue::Bytes32(pool_id));
        params.insert(Arc::from("sqrtPriceX96"), DecodedValue::Uint256(q96()));

        DecodedEvent {
            block_number,
            block_timestamp: 1_700_000_000 + block_number,
            transaction_hash: [0u8; 32],
            log_index,
            contract_address: [0u8; 20],
            source_name: source.to_string(),
            event_name: "Swapped".to_string(),
            event_signature: SWAPPED_SIG.to_string(),
            params,
        }
    }

    fn position(tick_lower: i32, tick_upper: i32, liquidity: u128) -> DecodedValue {
        DecodedValue::NamedTuple(vec![
            (Arc::from("tickLower"), DecodedValue::Int32(tick_lower)),
            (Arc::from("tickUpper"), DecodedValue::Int32(tick_upper)),
            (Arc::from("liquidity"), DecodedValue::Uint128(liquidity)),
        ])
    }

    fn get_pool_coin_call(
        source: &str,
        block_number: u64,
        trigger_log_index: u32,
        positions: Vec<DecodedValue>,
        is_reverted: bool,
    ) -> DecodedCall {
        let mut result = HashMap::new();
        result.insert(Arc::from("positions"), DecodedValue::Array(positions));

        DecodedCall {
            block_number,
            block_timestamp: 1_700_000_000 + block_number,
            contract_address: [0u8; 20],
            source_name: source.to_string(),
            function_name: "getPoolCoin".to_string(),
            trigger_log_index: Some(trigger_log_index),
            result,
            is_reverted,
            revert_reason: None,
        }
    }

    #[test]
    fn test_zora_tvl_handler_dependencies_are_source_scoped() {
        let creator_handler = ZoraTvlMetricsHandler {
            source: CREATOR_HOOK_SOURCE,
            metadata_cache: Arc::new(PoolMetadataCache::new()),
            oracle_cache: Arc::new(OraclePriceCache::new()),
            chain_id: 8453,
            db_pool: OnceLock::new(),
        };
        let content_handler = ZoraTvlMetricsHandler {
            source: CONTENT_HOOK_SOURCE,
            metadata_cache: Arc::new(PoolMetadataCache::new()),
            oracle_cache: Arc::new(OraclePriceCache::new()),
            chain_id: 8453,
            db_pool: OnceLock::new(),
        };

        assert_eq!(creator_handler.name(), "ZoraCreatorTvlMetricsHandler");
        assert_eq!(content_handler.name(), "ZoraContentTvlMetricsHandler");
        assert_eq!(
            creator_handler.call_dependencies(),
            vec![
                (CREATOR_HOOK_SOURCE.to_string(), "getPoolCoin".to_string()),
                chainlink_latest_answer_dependency(),
            ]
        );
        assert_eq!(
            content_handler.call_dependencies(),
            vec![
                (CONTENT_HOOK_SOURCE.to_string(), "getPoolCoin".to_string()),
                chainlink_latest_answer_dependency(),
            ]
        );
        let creator_triggers = creator_handler.triggers();
        let content_triggers = content_handler.triggers();
        assert_eq!(creator_triggers.len(), 1);
        assert_eq!(creator_triggers[0].source, CREATOR_HOOK_SOURCE);
        assert_eq!(creator_triggers[0].event_signature, SWAPPED_SIG);
        assert_eq!(content_triggers.len(), 1);
        assert_eq!(content_triggers[0].source, CONTENT_HOOK_SOURCE);
        assert_eq!(content_triggers[0].event_signature, SWAPPED_SIG);
    }

    #[test]
    fn test_extract_positions_from_call_filters_zero_liquidity() {
        let call = get_pool_coin_call(
            CREATOR_HOOK_SOURCE,
            100,
            0,
            vec![position(-60, 60, 0), position(-120, 120, 42)],
            false,
        );

        let positions = extract_positions_from_call(&call).expect("positions should decode");
        assert_eq!(
            positions,
            vec![TickMapPosition {
                tick_lower: -120,
                tick_upper: 120,
                liquidity: 42,
            }]
        );
    }

    #[test]
    fn test_extract_zora_tvl_targets_missing_call_returns_err() {
        let pool_id = [0x11; 32];
        let ctx = make_test_ctx(
            vec![swapped_event(CREATOR_HOOK_SOURCE, 100, 0, pool_id)],
            vec![],
        );

        let result = extract_zora_tvl_targets_for_source(&ctx, CREATOR_HOOK_SOURCE);
        let err = result.expect_err("missing getPoolCoin should fail");
        assert!(
            err.to_string().contains("No getPoolCoin call"),
            "error should mention missing getPoolCoin: {err}"
        );
    }

    #[test]
    fn test_extract_zora_tvl_targets_filters_calls_by_source() {
        let pool_id = [0x22; 32];
        let ctx = make_test_ctx(
            vec![swapped_event(CREATOR_HOOK_SOURCE, 100, 0, pool_id)],
            vec![get_pool_coin_call(
                CONTENT_HOOK_SOURCE,
                100,
                0,
                vec![position(-60, 60, 1)],
                false,
            )],
        );

        let result = extract_zora_tvl_targets_for_source(&ctx, CREATOR_HOOK_SOURCE);
        let err = result.expect_err("wrong-source call should not satisfy creator event");
        assert!(
            err.to_string().contains(CREATOR_HOOK_SOURCE),
            "error should identify creator source: {err}"
        );
    }

    #[test]
    fn test_extract_zora_tvl_targets_reverted_call_is_skipped() {
        let pool_id = [0x33; 32];
        let ctx = make_test_ctx(
            vec![swapped_event(CREATOR_HOOK_SOURCE, 100, 0, pool_id)],
            vec![get_pool_coin_call(
                CREATOR_HOOK_SOURCE,
                100,
                0,
                vec![position(-60, 60, 1)],
                true,
            )],
        );

        let targets = extract_zora_tvl_targets_for_source(&ctx, CREATOR_HOOK_SOURCE)
            .expect("reverted call should be skipped cleanly");
        assert!(targets.is_empty());
    }

    #[test]
    fn test_extract_zora_tvl_targets_matches_on_block_and_log_index() {
        let pool_a = [0x44; 32];
        let pool_b = [0x55; 32];
        let ctx = make_test_ctx(
            vec![
                swapped_event(CREATOR_HOOK_SOURCE, 100, 0, pool_a),
                swapped_event(CREATOR_HOOK_SOURCE, 101, 0, pool_b),
            ],
            vec![
                get_pool_coin_call(
                    CREATOR_HOOK_SOURCE,
                    100,
                    0,
                    vec![position(-60, 60, 10)],
                    false,
                ),
                get_pool_coin_call(
                    CREATOR_HOOK_SOURCE,
                    101,
                    0,
                    vec![position(-120, 120, 20)],
                    false,
                ),
            ],
        );

        let targets = extract_zora_tvl_targets_for_source(&ctx, CREATOR_HOOK_SOURCE)
            .expect("matching by block + log index should succeed");
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].pool_id, pool_a.to_vec());
        assert_eq!(targets[0].positions[0].liquidity, 10);
        assert_eq!(targets[1].pool_id, pool_b.to_vec());
        assert_eq!(targets[1].positions[0].liquidity, 20);
    }

    #[test]
    fn test_extract_zora_tvl_targets_dedupes_by_pool_and_block_using_last_swap() {
        let pool_id = [0x66; 32];
        let ctx = make_test_ctx(
            vec![
                swapped_event(CREATOR_HOOK_SOURCE, 100, 0, pool_id),
                swapped_event(CREATOR_HOOK_SOURCE, 100, 1, pool_id),
            ],
            vec![
                get_pool_coin_call(
                    CREATOR_HOOK_SOURCE,
                    100,
                    0,
                    vec![position(-60, 60, 10)],
                    false,
                ),
                get_pool_coin_call(
                    CREATOR_HOOK_SOURCE,
                    100,
                    1,
                    vec![position(-120, 120, 99)],
                    false,
                ),
            ],
        );

        let targets = extract_zora_tvl_targets_for_source(&ctx, CREATOR_HOOK_SOURCE)
            .expect("targets should decode");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].pool_id, pool_id.to_vec());
        assert_eq!(targets[0].block_number, 100);
        assert_eq!(
            targets[0].positions,
            vec![TickMapPosition {
                tick_lower: -120,
                tick_upper: 120,
                liquidity: 99,
            }]
        );
    }

    #[tokio::test]
    async fn test_process_tvl_from_positions_emits_snapshots_and_latest_state() {
        let pool_id = vec![0x77; 32];
        let cache = PoolMetadataCache::new();
        cache.insert_if_absent(
            pool_id.clone(),
            PoolMetadata {
                quote_token: [0x02; 20],
                is_token_0: true,
                base_decimals: 18,
                quote_decimals: 18,
                total_supply: None,
            },
        );

        let mut prices = HashMap::new();
        prices.insert([0x02; 20], bigdecimal::BigDecimal::from(2000));
        let usd_ctx = UsdPriceContext::new_for_test(prices, Some([0x02; 20]));
        let positions = vec![TickMapPosition {
            tick_lower: -60,
            tick_upper: 60,
            liquidity: 1_000_000_000_000_000_000,
        }];

        let ops = process_tvl_from_positions(
            &[
                TvlTargetWithPositions {
                    pool_id: pool_id.clone(),
                    block_number: 100,
                    block_timestamp: 1_700_000_100,
                    tick: 0,
                    sqrt_price_x96: q96(),
                    positions: positions.clone(),
                },
                TvlTargetWithPositions {
                    pool_id: pool_id.clone(),
                    block_number: 101,
                    block_timestamp: 1_700_000_101,
                    tick: 0,
                    sqrt_price_x96: q96(),
                    positions,
                },
            ],
            &cache,
            8453,
            &usd_ctx,
            "ZoraSwapMetricsHandler",
            1,
        )
        .await
        .expect("TVL from positions should succeed");

        let snapshot_blocks: Vec<i64> = ops
            .iter()
            .filter_map(|op| match op {
                DbOperation::NamedSql {
                    params,
                    snapshot: Some(snapshot),
                    ..
                } if snapshot.table == "pool_snapshots" => params.iter().find_map(|(name, value)| {
                    if name == "block_number" {
                        match value {
                            crate::db::DbValue::Int64(v) => Some(*v),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }),
                _ => None,
            })
            .collect();
        let state_blocks: Vec<i64> = ops
            .iter()
            .filter_map(|op| match op {
                DbOperation::NamedSql {
                    params,
                    snapshot: Some(snapshot),
                    ..
                } if snapshot.table == "pool_state" => params.iter().find_map(|(name, value)| {
                    if name == "block_number" {
                        match value {
                            crate::db::DbValue::Int64(v) => Some(*v),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }),
                _ => None,
            })
            .collect();

        assert_eq!(snapshot_blocks, vec![100, 101]);
        assert_eq!(state_blocks, vec![101]);
    }
}
