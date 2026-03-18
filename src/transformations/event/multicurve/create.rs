use async_trait::async_trait;

use alloy_primitives::{Address, B256, U256};

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::TransformationContext;
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::token::insert_token;
use crate::transformations::util::db::v4_pool_configs::insert_pool_config;
use crate::transformations::util::db::pool::{Beneficiary, BeneficiariesData, insert_pool};
use crate::transformations::util::metadata::get_metadata;
use crate::types::decoded::DecodedValue;
use crate::types::uniswap::v4::{PoolKey, V4PoolConfig, PoolAddressOrPoolId};

pub struct V4MulticurveCreateHandler;

#[async_trait]
impl TransformationHandler for V4MulticurveCreateHandler {
    fn name(&self) -> &'static str {
        "V4MulticurveCreateHandler"
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
    ) -> Result<Vec<DbOperation>, TransformationError>{
        let mut ops = Vec::new();

        for event in ctx.events_of_type("UniswapV4MulticurveInitializer", "Create") {
            let asset = event.get("asset")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("asset is not an address".to_string())
            })?;

            let numeraire = event.get("numeraire")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("numeraire is not an address".to_string())
            })?;
            
            let metadata_result = get_metadata(&asset, &numeraire, event, &ctx);

            let asset_metadata;
            let numeraire_metadata;
            match metadata_result {
                Ok(r) => {
                    asset_metadata = r.0;
                    numeraire_metadata = r.1;
                }
                Err(e) => {
                    return Err(e)
                }
            }

            let get_state_call = ctx.calls_of_type("UniswapV4MulticurveInitializer", "getState")
                .filter(|call| call.trigger_log_index.unwrap() == event.log_index)
                .next()
                .ok_or_else(|| TransformationError::MissingData(format!(
                    "No getState call for asset {} at block {} tx {}",
                    Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                )))?;

            let num_to_sell = ctx.calls_of_type("DERC20", "once")
                .filter(|call| call.contract_address == asset)
                .next()
                .ok_or_else( || TransformationError::MissingData(format!(
                    "No getAssetData call for asset {} at block {} tx {}",
                    Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                )))?
                .get("getAssetData.numTokensToSell")?;

            let pool_key = {
                let field_err = |field: &str, expected: &str| {
                    TransformationError::TypeConversion(format!(
                        "UniswapV4MulticurveInitializer getState for {} field '{}': expected {} but got {:?} at block {} tx {}",
                        Address::from(asset), field, expected, get_state_call.result.get(field),
                        event.block_number, B256::from(event.transaction_hash)
                    ))
                };
                let missing_err = |field: &str| {
                    TransformationError::MissingData(format!(
                        "UniswapV4MulticurveInitializer getState for {} missing field '{}' at block {} tx {}. Available fields: {:?}",
                        Address::from(asset), field, event.block_number, B256::from(event.transaction_hash),
                        get_state_call.result.keys().collect::<Vec<_>>()
                    ))
                };

                PoolKey {
                    currency0: get_state_call.result.get("poolKey.currency0")
                        .ok_or_else(|| missing_err("poolKey.currency0"))?
                        .as_address()
                        .ok_or_else(|| field_err("poolKey.currency0", "address"))?
                        .into(),
                    currency1: get_state_call.result.get("poolKey.currency1")
                        .ok_or_else(|| missing_err("poolKey.currency1"))?
                        .as_address()
                        .ok_or_else(|| field_err("poolKey.currency1", "address"))?
                        .into(),
                    fee: get_state_call.result.get("poolKey.fee")
                        .ok_or_else(|| missing_err("poolKey.fee"))?
                        .as_u32()
                        .ok_or_else(|| field_err("poolKey.fee", "u32"))?,
                    tick_spacing: get_state_call.result.get("poolKey.tickSpacing")
                        .ok_or_else(|| missing_err("poolKey.tickSpacing"))?
                        .as_i32()
                        .ok_or_else(|| field_err("poolKey.tickSpacing", "i32"))?,
                    hooks: get_state_call.result.get("poolKey.hooks")
                        .ok_or_else(|| missing_err("poolKey.hooks"))?
                        .as_address()
                        .ok_or_else(|| field_err("poolKey.hooks", "address"))?
                        .into(),
                }
            };

            let hook: [u8; 20] = pool_key.hooks.into();
            let pool_id = pool_key.pool_id();

            let far_tick = get_state_call.result.get("farTick")
                .ok_or_else(|| TransformationError::MissingData(format!(
                    "No farTick in getState for asset {} at block {} tx {}",
                    Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                )))?
                .as_i32()
                .ok_or_else(|| TransformationError::TypeConversion(format!(
                    "farTick is not an i32 in getState for asset {} at block {} tx {}",
                    Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                )))?;

            let get_positions_call = ctx.calls_of_type("UniswapV4MulticurveInitializer", "getPositions")
                .filter(|call| call.trigger_log_index.unwrap() == event.log_index)
                .next()
                .ok_or_else(|| TransformationError::MissingData(format!(
                    "No getPositions call for asset {} at block {} tx {}",
                    Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                )))?;

            let positions = get_positions_call.result.get("getPositions")
                .ok_or_else(|| TransformationError::MissingData(format!(
                    "No getPositions result for asset {} at block {} tx {}",
                    Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                )))?;

            let (min_tick_lower, max_tick_upper) = match positions {
                DecodedValue::Array(elements) if !elements.is_empty() => {
                    let mut min_lower = i32::MAX;
                    let mut max_upper = i32::MIN;
                    for elem in elements {
                        let tick_lower = elem.get_field("tickLower")
                            .and_then(|v| v.as_i32())
                            .ok_or_else(|| TransformationError::TypeConversion(format!(
                                "position tickLower missing or not i32 for asset {} at block {} tx {}",
                                Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                            )))?;
                        let tick_upper = elem.get_field("tickUpper")
                            .and_then(|v| v.as_i32())
                            .ok_or_else(|| TransformationError::TypeConversion(format!(
                                "position tickUpper missing or not i32 for asset {} at block {} tx {}",
                                Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                            )))?;
                        min_lower = min_lower.min(tick_lower);
                        max_upper = max_upper.max(tick_upper);
                    }
                    (min_lower, max_upper)
                }
                _ => return Err(TransformationError::MissingData(format!(
                    "getPositions is not a non-empty array for asset {} at block {} tx {}",
                    Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                ))),
            };

            let (starting_tick, ending_tick) = if far_tick == min_tick_lower {
                (max_tick_upper, far_tick)
            } else {
                (min_tick_lower, far_tick)
            };

            let is_token_0 = asset < numeraire;

            let pool_config = V4PoolConfig {
                num_tokens_to_sell: num_to_sell
                    .as_uint256()
                    .ok_or_else(|| TransformationError::TypeConversion(format!(
                        "numTokensToSell is not uint256 in getState for asset {} at block {} tx {}",
                        Address::from(asset), event.block_number, B256::from(event.transaction_hash)
                    )))?,
                min_proceeds: U256::ZERO,
                max_proceeds: U256::ZERO,
                starting_time: 0,
                ending_time: 0,
                starting_tick,
                ending_tick,
                epoch_length: U256::ZERO,
                gamma: 0,
                is_token_0,
                num_pd_slugs: U256::ZERO,
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
                ctx
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
                ctx
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
                ctx
            ));

            let beneficiaries: Option<BeneficiariesData> = ctx.calls_of_type("UniswapV4MulticurveInitializer", "getBeneficiaries")
                .filter(|call| call.trigger_log_index.unwrap() == event.log_index)
                .next()
                .and_then(|call| call.result.get("getBeneficiaries"))
                .map(|val| {
                    match val {
                        DecodedValue::Array(elements) => {
                            elements.iter().filter_map(|elem| {
                                if let DecodedValue::UnnamedTuple(fields) = elem {
                                    let address = fields.first()?.as_address()?;
                                    let shares = fields.get(1)?.as_u64()?;
                                    Some(Beneficiary::new(address, shares))
                                } else {
                                    None
                                }
                            }).collect()
                        }
                        _ => Vec::new(),
                    }
                });

            let migration_type = ctx.match_contract_address(
                asset_metadata.migrator.into(),
                &[
                    "UniswapV4Migrator",
                    "UniswapV2Migrator", "NimCustomV2Migrator",
                    "UniswapV3Migrator", "NimCustomV3Migrator",
                ],
            ).map(|contract_name| {
                match contract_name {
                    "UniswapV4Migrator" => "v4",
                    "UniswapV2Migrator" | "NimCustomV2Migrator" => "v2",
                    "UniswapV3Migrator" | "NimCustomV3Migrator" => "v3",
                    _ => "unknown",
                }
            }).unwrap_or("unknown");

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
                beneficiaries,
                pool_key,
                pool_config.starting_time,
                pool_config.ending_time,
                ctx
            ));
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("V4MulticurveCreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for V4MulticurveCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "UniswapV4MulticurveInitializer",
            "Create(address,address,address)"
        )]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![
            ("DERC20".to_string(), "once".to_string()),
            ("Numeraires".to_string(), "once".to_string()),
            ("UniswapV4MulticurveInitializer".to_string(), "getState".to_string()),
            ("UniswapV4MulticurveInitializer".to_string(), "getBeneficiaries".to_string()),
            ("UniswapV4MulticurveInitializer".to_string(), "getPositions".to_string()),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(V4MulticurveCreateHandler);
}