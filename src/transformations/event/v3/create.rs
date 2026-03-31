use async_trait::async_trait;

use alloy_primitives::{Address, B256, U256};

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::pool::{insert_pool, PoolData};
use crate::transformations::util::db::token::{insert_token, TokenData};
use crate::transformations::util::metadata::get_metadata;
use crate::transformations::util::migration::resolve_migration_type;

use crate::types::uniswap::v4::PoolAddressOrPoolId;

pub struct V3CreateHandler;

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

            let (asset_metadata, numeraire_metadata) =
                get_metadata(&asset, &numeraire, event, ctx)?;

            let pool_call = ctx
                .calls_for_address(pool_or_hook)
                .find(|call| call.function_name == "once")
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
        vec![
            ("DERC20".to_string(), "once".to_string()),
            ("Numeraires".to_string(), "once".to_string()),
            ("DopplerV3Pool".to_string(), "once".to_string()),
        ]
    }
}

pub struct LockableV3CreateHandler;

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

            let (asset_metadata, numeraire_metadata) =
                get_metadata(&asset, &numeraire, event, ctx)?;

            let pool_call = ctx
                .calls_for_address(pool_or_hook)
                .find(|call| call.function_name == "once")
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
        vec![
            ("DERC20".to_string(), "once".to_string()),
            ("Numeraires".to_string(), "once".to_string()),
            ("DopplerLockableV3Pool".to_string(), "once".to_string()),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(V3CreateHandler);
    registry.register_event_handler(LockableV3CreateHandler);
}
