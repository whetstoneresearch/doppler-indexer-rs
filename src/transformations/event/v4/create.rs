use async_trait::async_trait;

use alloy_primitives::{Address, B256};

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::pool::{insert_pool, PoolData};
use crate::transformations::util::db::token::{insert_token, TokenData};
use crate::transformations::util::db::v4_pool_configs::{insert_pool_config, PoolConfigData};
use crate::transformations::util::metadata::get_metadata;
use crate::transformations::util::migration::resolve_migration_type;

use crate::types::uniswap::v4::{PoolAddressOrPoolId, PoolKey, V4PoolConfig};

pub struct V4CreateHandler;

#[async_trait]
impl TransformationHandler for V4CreateHandler {
    fn name(&self) -> &'static str {
        "V4CreateHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/tokens.sql",
            "migrations/tables/pools.sql",
            "migrations/tables/v4_pool_configs.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        // tokens and pools have block_number for rollback
        // v4_pool_configs is immutable config without block_number
        vec!["tokens", "pools"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("UniswapV4Initializer", "Create") {
            let asset = event.extract_address("asset")?;
            let numeraire = event.extract_address("numeraire")?;
            let hook = event.extract_address("poolOrHook")?;

            let metadata_result = get_metadata(&asset, &numeraire, event, ctx);

            let asset_metadata;
            let numeraire_metadata;
            match metadata_result {
                Ok(r) => {
                    asset_metadata = r.0;
                    numeraire_metadata = r.1;
                }
                Err(e) => return Err(e),
            }

            let hook_call = ctx
                .calls_for_address(hook)
                .find(|call| call.function_name == "once")
                .ok_or_else(|| {
                    let available_calls: Vec<_> = ctx
                        .calls_for_address(hook)
                        .map(|c| format!("{}:{}", c.source_name, c.function_name))
                        .collect();
                    TransformationError::MissingData(format!(
                        "No 'once' call found for hook {} at block {} tx {}. Available calls: {:?}",
                        Address::from(hook),
                        event.block_number,
                        B256::from(event.transaction_hash),
                        available_calls
                    ))
                })?;

            let pool_key = PoolKey {
                currency0: hook_call.extract_address("poolKey.currency0")?.into(),
                currency1: hook_call.extract_address("poolKey.currency1")?.into(),
                fee: hook_call.extract_u32("poolKey.fee")?,
                tick_spacing: hook_call.extract_i32("poolKey.tickSpacing")?,
                hooks: hook_call.extract_address("poolKey.hooks")?.into(),
            };

            let pool_id = pool_key.pool_id();

            let pool_config = V4PoolConfig {
                num_tokens_to_sell: hook_call.extract_uint256("numTokensToSell")?,
                min_proceeds: hook_call.extract_uint256("minimumProceeds")?,
                max_proceeds: hook_call.extract_uint256("maximumProceeds")?,
                starting_time: hook_call.extract_u64_flexible("startingTime")?,
                ending_time: hook_call.extract_u64_flexible("endingTime")?,
                starting_tick: hook_call.extract_i32_flexible("startingTick")?,
                ending_tick: hook_call.extract_i32_flexible("endingTick")?,
                epoch_length: hook_call.extract_uint256("epochLength")?,
                gamma: hook_call.extract_u32_flexible("gamma")?,
                is_token_0: hook_call.extract_bool("isToken0")?,
                num_pd_slugs: hook_call.extract_uint256("numPDSlugs")?,
            };

            ops.push(insert_token(
                &TokenData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    tx_hash: &event.transaction_hash,
                    creator_address: ctx.tx_from(&event.transaction_hash),
                    integrator: Some(&asset_metadata.integrator.into()),
                    token_address: &asset,
                    pool: Some(&PoolAddressOrPoolId::PoolId(pool_id.0)),
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

            ops.push(insert_pool_config(
                &PoolConfigData {
                    pool_id: pool_id.into(),
                    hook_address: hook,
                    num_tokens_to_sell: pool_config.num_tokens_to_sell,
                    min_proceeds: pool_config.min_proceeds,
                    max_proceeds: pool_config.max_proceeds,
                    starting_time: pool_config.starting_time,
                    ending_time: pool_config.ending_time,
                    starting_tick: pool_config.starting_tick,
                    ending_tick: pool_config.ending_tick,
                    epoch_length: pool_config.epoch_length,
                    gamma: pool_config.gamma,
                    is_token_0: pool_config.is_token_0,
                    num_pd_slugs: pool_config.num_pd_slugs,
                },
                ctx,
            ));

            let migration_type = resolve_migration_type(ctx, asset_metadata.migrator.into());

            ops.push(insert_pool(
                &PoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    address: PoolAddressOrPoolId::PoolId(pool_id.into()),
                    base_token: asset,
                    quote_token: numeraire,
                    is_token_0: pool_config.is_token_0,
                    pool_type: "v4".to_string(),
                    integrator: asset_metadata.integrator.into(),
                    initializer: asset_metadata.initializer.into(),
                    fee: pool_key.fee,
                    min_threshold: pool_config.min_proceeds,
                    max_threshold: pool_config.max_proceeds,
                    migrator: asset_metadata.migrator.into(),
                    migrated_at: None,
                    migration_pool: asset_metadata.migration_pool,
                    migration_type: migration_type.to_string(),
                    lock_duration: None,
                    beneficiaries: None,
                    pool_key: Some(pool_key),
                    starting_time: pool_config.starting_time,
                    ending_time: pool_config.ending_time,
                },
                ctx,
            ));
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("V4CreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for V4CreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "UniswapV4Initializer",
            "Create(address,address,address)",
        )]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![
            ("DERC20".to_string(), "once".to_string()),
            ("Numeraires".to_string(), "once".to_string()),
            ("DopplerV4Hook".to_string(), "once".to_string()),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(V4CreateHandler);
}
