use async_trait::async_trait;

use alloy_primitives::{Address, B256};

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::pool::insert_pool;
use crate::transformations::util::db::token::insert_token;
use crate::transformations::util::db::v4_pool_configs::insert_pool_config;
use crate::transformations::util::metadata::get_metadata;

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
                .calls_for_address(hook).find(|call| call.function_name == "once")
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
                event.block_number,
                event.block_timestamp,
                &event.transaction_hash,
                ctx.tx_from(&event.transaction_hash),
                Some(&asset_metadata.integrator.into()),
                &asset,
                Some(&PoolAddressOrPoolId::PoolId(pool_id.0)),
                &asset_metadata.name,
                &asset_metadata.symbol,
                asset_metadata.decimals,
                Some(&asset_metadata.total_supply),
                Some(&asset_metadata.token_uri),
                true,
                false,
                false,
                None,
                Some(&asset_metadata.governance),
                ctx,
            ));

            ops.push(insert_token(
                event.block_number,
                event.block_timestamp,
                &event.transaction_hash,
                None,
                None,
                &numeraire,
                None,
                &numeraire_metadata.name,
                &numeraire_metadata.symbol,
                numeraire_metadata.decimals,
                None,
                None,
                false,
                false,
                false,
                None,
                None,
                ctx,
            ));

            ops.push(insert_pool_config(
                pool_id.into(),
                hook,
                pool_config.num_tokens_to_sell,
                pool_config.min_proceeds,
                pool_config.max_proceeds,
                pool_config.starting_time,
                pool_config.ending_time,
                pool_config.starting_tick,
                pool_config.ending_tick,
                pool_config.epoch_length,
                pool_config.gamma,
                pool_config.is_token_0,
                pool_config.num_pd_slugs,
                ctx,
            ));

            let migration_type = ctx
                .match_contract_address(
                    asset_metadata.migrator.into(),
                    &[
                        "UniswapV4Migrator",
                        "UniswapV2Migrator",
                        "NimCustomV2Migrator",
                        "UniswapV3Migrator",
                        "NimCustomV3Migrator",
                    ],
                )
                .map(|contract_name| match contract_name {
                    "UniswapV4Migrator" => "v4",
                    "UniswapV2Migrator" | "NimCustomV2Migrator" => "v2",
                    "UniswapV3Migrator" | "NimCustomV3Migrator" => "v3",
                    _ => "unknown",
                })
                .unwrap_or("unknown");

            ops.push(insert_pool(
                event.block_number,
                event.block_timestamp,
                PoolAddressOrPoolId::PoolId(pool_id.into()),
                &asset,
                &numeraire,
                pool_config.is_token_0,
                "v4",
                asset_metadata.integrator.into(),
                asset_metadata.initializer.into(),
                pool_key.fee,
                pool_config.min_proceeds,
                pool_config.max_proceeds,
                asset_metadata.migrator.into(),
                None,
                asset_metadata.migration_pool,
                migration_type,
                None,
                None,
                pool_key,
                pool_config.starting_time,
                pool_config.ending_time,
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
