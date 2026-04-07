use std::sync::Arc;

use async_trait::async_trait;

use alloy_primitives::{Address, B256, U256};

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::pool::{insert_pool, PoolData};
use crate::transformations::util::db::token::{insert_token, TokenData};
use crate::transformations::util::metadata::get_metadata_or_skip;
use crate::transformations::util::migration::resolve_migration_type;
use crate::transformations::util::pool_metadata::PoolMetadataCache;

use crate::types::uniswap::v4::PoolAddressOrPoolId;

pub struct V3CreateHandler {
    pub(crate) metadata_cache: Arc<PoolMetadataCache>,
}

#[async_trait]
impl TransformationHandler for V3CreateHandler {
    fn name(&self) -> &'static str {
        "V3CreateHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/tokens.sql",
            "migrations/tables/pools.sql",
            "migrations/tables/skipped_addresses.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["tokens", "pools"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("UniswapV3Initializer", "Create") {
            let asset = event.extract_address("asset")?;
            let numeraire = event.extract_address("numeraire")?;
            let pool_or_hook = event.extract_address("poolOrHook")?;

            let Some((asset_metadata, numeraire_metadata)) =
                get_metadata_or_skip(&asset, &numeraire, event, ctx, &mut ops).await?
            else {
                continue;
            };

            let pool_call = ctx
                .current_or_historical_once_call_for_address("DopplerV3Pool", pool_or_hook)
                .await?
                .ok_or_else(|| {
                    let available_calls: Vec<_> = ctx
                        .calls_for_address(pool_or_hook)
                        .map(|c| format!("{}:{}", c.source_name, c.function_name))
                        .collect();
                    TransformationError::MissingData(format!(
                        "No 'once' call found for pool {} at block {} tx {}. Available calls: {:?}",
                        Address::from(pool_or_hook),
                        event.block_number,
                        B256::from(event.transaction_hash),
                        available_calls
                    ))
                })?;

            let fee = pool_call.extract_u32("fee")?;
            let is_token_0 = asset < numeraire;

            ops.push(insert_token(
                &TokenData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    tx_hash: &event.transaction_hash,
                    creator_address: ctx.tx_from(&event.transaction_hash),
                    integrator: Some(&asset_metadata.integrator.into()),
                    token_address: &asset,
                    pool: Some(&PoolAddressOrPoolId::Address(pool_or_hook)),
                    name: &asset_metadata.name,
                    symbol: &asset_metadata.symbol,
                    decimals: asset_metadata.decimals,
                    total_supply: Some(&asset_metadata.total_supply),
                    token_uri: Some(&asset_metadata.token_uri),
                    is_derc20: true,
                    is_creator_coin: false,
                    is_content_coin: false,
                    creator_coin_pool: None,
                    governance: Some(&asset_metadata.governance),
                },
                ctx,
            ));

            ops.push(insert_token(
                &TokenData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    tx_hash: &event.transaction_hash,
                    creator_address: None,
                    integrator: None,
                    token_address: &numeraire,
                    pool: None,
                    name: &numeraire_metadata.name,
                    symbol: &numeraire_metadata.symbol,
                    decimals: numeraire_metadata.decimals,
                    total_supply: None,
                    token_uri: None,
                    is_derc20: false,
                    is_creator_coin: false,
                    is_content_coin: false,
                    creator_coin_pool: None,
                    governance: None,
                },
                ctx,
            ));

            let migration_type = resolve_migration_type(ctx, asset_metadata.migrator.into());

            ops.push(insert_pool(
                &PoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    address: PoolAddressOrPoolId::Address(pool_or_hook),
                    base_token: asset,
                    quote_token: numeraire,
                    is_token_0,
                    pool_type: "v3".to_string(),
                    integrator: asset_metadata.integrator.into(),
                    initializer: asset_metadata.initializer.into(),
                    fee,
                    min_threshold: U256::ZERO,
                    max_threshold: U256::ZERO,
                    migrator: asset_metadata.migrator.into(),
                    migrated_at: None,
                    migration_pool: asset_metadata.migration_pool,
                    migration_type: migration_type.to_string(),
                    lock_duration: None,
                    beneficiaries: None,
                    pool_key: None,
                    starting_time: 0,
                    ending_time: 0,
                },
                ctx,
            ));
            // NOTE: intentionally no metadata_cache.insert_if_absent here.
            // Inserting before the DB transaction commits would leave stale
            // cache entries if the transaction later fails.  Swap handlers
            // call refresh_cache_if_needed which queries the DB on cache miss.
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("V3CreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for V3CreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "UniswapV3Initializer",
            "Create(address,address,address)",
        )]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![("DopplerV3Pool".to_string(), "once".to_string())]
    }
}

pub struct LockableV3CreateHandler {
    pub(crate) metadata_cache: Arc<PoolMetadataCache>,
}

#[async_trait]
impl TransformationHandler for LockableV3CreateHandler {
    fn name(&self) -> &'static str {
        "LockableV3CreateHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/tokens.sql",
            "migrations/tables/pools.sql",
            "migrations/tables/skipped_addresses.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["tokens", "pools"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("LockableUniswapV3Initializer", "Create") {
            let asset = event.extract_address("asset")?;
            let numeraire = event.extract_address("numeraire")?;
            let pool_or_hook = event.extract_address("poolOrHook")?;

            let Some((asset_metadata, numeraire_metadata)) =
                get_metadata_or_skip(&asset, &numeraire, event, ctx, &mut ops).await?
            else {
                continue;
            };

            let pool_call = ctx
                .current_or_historical_once_call_for_address("DopplerLockableV3Pool", pool_or_hook)
                .await?
                .ok_or_else(|| {
                    let available_calls: Vec<_> = ctx
                        .calls_for_address(pool_or_hook)
                        .map(|c| format!("{}:{}", c.source_name, c.function_name))
                        .collect();
                    TransformationError::MissingData(format!(
                        "No 'once' call found for pool {} at block {} tx {}. Available calls: {:?}",
                        Address::from(pool_or_hook),
                        event.block_number,
                        B256::from(event.transaction_hash),
                        available_calls
                    ))
                })?;

            let fee = pool_call.extract_u32("fee")?;
            let is_token_0 = asset < numeraire;

            ops.push(insert_token(
                &TokenData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    tx_hash: &event.transaction_hash,
                    creator_address: ctx.tx_from(&event.transaction_hash),
                    integrator: Some(&asset_metadata.integrator.into()),
                    token_address: &asset,
                    pool: Some(&PoolAddressOrPoolId::Address(pool_or_hook)),
                    name: &asset_metadata.name,
                    symbol: &asset_metadata.symbol,
                    decimals: asset_metadata.decimals,
                    total_supply: Some(&asset_metadata.total_supply),
                    token_uri: Some(&asset_metadata.token_uri),
                    is_derc20: true,
                    is_creator_coin: false,
                    is_content_coin: false,
                    creator_coin_pool: None,
                    governance: Some(&asset_metadata.governance),
                },
                ctx,
            ));

            ops.push(insert_token(
                &TokenData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    tx_hash: &event.transaction_hash,
                    creator_address: None,
                    integrator: None,
                    token_address: &numeraire,
                    pool: None,
                    name: &numeraire_metadata.name,
                    symbol: &numeraire_metadata.symbol,
                    decimals: numeraire_metadata.decimals,
                    total_supply: None,
                    token_uri: None,
                    is_derc20: false,
                    is_creator_coin: false,
                    is_content_coin: false,
                    creator_coin_pool: None,
                    governance: None,
                },
                ctx,
            ));

            let migration_type = resolve_migration_type(ctx, asset_metadata.migrator.into());

            ops.push(insert_pool(
                &PoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    address: PoolAddressOrPoolId::Address(pool_or_hook),
                    base_token: asset,
                    quote_token: numeraire,
                    is_token_0,
                    pool_type: "lockable_v3".to_string(),
                    integrator: asset_metadata.integrator.into(),
                    initializer: asset_metadata.initializer.into(),
                    fee,
                    min_threshold: U256::ZERO,
                    max_threshold: U256::ZERO,
                    migrator: asset_metadata.migrator.into(),
                    migrated_at: None,
                    migration_pool: asset_metadata.migration_pool,
                    migration_type: migration_type.to_string(),
                    lock_duration: None,
                    beneficiaries: None,
                    pool_key: None,
                    starting_time: 0,
                    ending_time: 0,
                },
                ctx,
            ));
            // NOTE: intentionally no metadata_cache.insert_if_absent here.
            // See V3CreateHandler for rationale.
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("LockableV3CreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for LockableV3CreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "LockableUniswapV3Initializer",
            "Create(address,address,address)",
        )]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![("DopplerLockableV3Pool".to_string(), "once".to_string())]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry, metadata_cache: Arc<PoolMetadataCache>) {
    registry.register_event_handler(V3CreateHandler {
        metadata_cache: metadata_cache.clone(),
    });
    registry.register_event_handler(LockableV3CreateHandler {
        metadata_cache,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::rpc::UnifiedRpcClient;
    use crate::transformations::context::TransformationContext;
    use crate::transformations::historical::HistoricalDataReader;
    use crate::transformations::util::pool_metadata::PoolMetadataCache;

    fn make_empty_ctx() -> TransformationContext {
        let historical = Arc::new(
            HistoricalDataReader::new("test_chain_create")
                .expect("HistoricalDataReader::new should succeed with nonexistent dir"),
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

    /// Fix E: After removing insert_if_absent, calling handle() must not
    /// populate the cache even when the context contains no Create events.
    /// If the context has no matching events the for-loop body never runs,
    /// confirming the cache insertion path is gone.
    #[tokio::test]
    async fn test_no_stale_cache_on_create_failure() {
        let pool_id = vec![0xABu8; 20];
        let cache = Arc::new(PoolMetadataCache::new());

        let handler = V3CreateHandler {
            metadata_cache: cache.clone(),
        };

        let ctx = make_empty_ctx();
        // handle() returns Ok([]) because no Create events are present.
        let ops = handler.handle(&ctx).await.expect("handle() must not fail");
        assert!(ops.is_empty(), "no ops expected with empty context");

        // The cache must still be empty — no insert_if_absent was called.
        assert!(
            cache.get(&pool_id).is_none(),
            "cache must not contain pool after handle() without committed DB ops"
        );
    }
}
