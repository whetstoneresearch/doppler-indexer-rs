use async_trait::async_trait;

use alloy_primitives::{Address, B256, U256};

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::TransformationContext;
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::token::insert_token;
use crate::transformations::util::db::v4_pool_configs::insert_pool_config;
use crate::transformations::util::db::pool::insert_pool;
use crate::transformations::util::sanitize::is_precompile_address;

use crate::types::uniswap::v4::{PoolKey, V4PoolConfig, PoolAddressOrPoolId};
use crate::types::shared::metadata::{TokenMetadata, AssetTokenMetadata};

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

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError>{
        let mut ops = Vec::new();

        for event in ctx.events_of_type("UniswapV4Initializer", "Create") {
            let asset = event.get("asset")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("asset is not an address".to_string())
            })?;

            let numeraire = event.get("numeraire")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("numeraire is not an address".to_string())
            })?;

            let precompile_checks: Vec<bool> = vec![asset, numeraire].into_iter().map(|addr| is_precompile_address(addr.into())).collect();
            if precompile_checks.contains(&true) {
                continue;
            }

            let hook = event.get("poolOrHook")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("poolOrHook is not an address".to_string())
            })?;

            let asset_metadata = ctx.calls_for_address(asset)
                .filter(|call| call.function_name == "once")
                .next()
                .ok_or_else(|| {
                    let available_calls: Vec<_> = ctx.calls_for_address(asset)
                        .map(|c| format!("{}:{}", c.source_name, c.function_name))
                        .collect();
                    TransformationError::MissingData(format!(
                        "No 'once' call found for asset {} at block {} tx {}. Available calls: {:?}",
                        Address::from(asset), event.block_number, B256::from(event.transaction_hash), available_calls
                    ))
                })?;

            let asset_metadata = {
                let call = asset_metadata;
                let field_err = |field: &str, expected: &str| {
                    TransformationError::TypeConversion(format!(
                        "asset {} field '{}': expected {} but got {:?} at block {} tx {}",
                        Address::from(asset), field, expected, call.result.get(field),
                        event.block_number, B256::from(event.transaction_hash)
                    ))
                };
                let missing_err = |field: &str| {
                    TransformationError::MissingData(format!(
                        "asset {} missing field '{}' at block {} tx {}. Available fields: {:?}",
                        Address::from(asset), field, event.block_number, B256::from(event.transaction_hash),
                        call.result.keys().collect::<Vec<_>>()
                    ))
                };

                AssetTokenMetadata {
                    name: call.result.get("name")
                        .ok_or_else(|| missing_err("name"))?
                        .as_string()
                        .ok_or_else(|| field_err("name", "string"))?
                        .to_string(),
                    symbol: call.result.get("symbol")
                        .ok_or_else(|| missing_err("symbol"))?
                        .as_string()
                        .ok_or_else(|| field_err("symbol", "string"))?
                        .to_string(),
                    decimals: 18,
                    token_uri: call.result.get("tokenURI")
                        .ok_or_else(|| missing_err("tokenURI"))?
                        .as_string()
                        .ok_or_else(|| field_err("tokenURI", "string"))?
                        .to_string(),
                    total_supply: call.result.get("getAssetData.totalSupply")
                        .ok_or_else(|| missing_err("getAssetData.totalSupply"))?
                        .as_uint256()
                        .ok_or_else(|| field_err("getAssetData.totalSupply", "uint256"))?,
                    governance: call.result.get("getAssetData.governance")
                        .ok_or_else(|| missing_err("getAssetData.governance"))?
                        .as_address()
                        .ok_or_else(|| field_err("getAssetData.governance", "address"))?
                        .into(),
                    integrator: call.result.get("getAssetData.integrator")
                        .ok_or_else(|| missing_err("getAssetData.integrator"))?
                        .as_address()
                        .ok_or_else(|| field_err("getAssetData.integrator", "address"))?
                        .into(),
                    initializer: call.result.get("getAssetData.poolInitializer")
                        .ok_or_else(|| missing_err("getAssetData.poolInitializer"))?
                        .as_address()
                        .ok_or_else(|| field_err("getAssetData.poolInitializer", "address"))?
                        .into(),
                    migrator: call.result.get("getAssetData.liquidityMigrator")
                        .ok_or_else(|| missing_err("getAssetData.liquidityMigrator"))?
                        .as_address()
                        .ok_or_else(|| field_err("getAssetData.liquidityMigrator", "address"))?
                        .into(),
                    migration_pool: {
                        let val = call.result.get("getAssetData.migrationPool")
                            .ok_or_else(|| missing_err("getAssetData.migrationPool"))?;
                        if let Some(addr) = val.as_address() {
                            PoolAddressOrPoolId::Address(addr.into())
                        } else {
                            PoolAddressOrPoolId::PoolId(
                                val.as_bytes32()
                                    .ok_or_else(|| field_err("getAssetData.migrationPool", "address or bytes32"))?
                            )
                        }
                    }
                }
            };

            let numeraire_metadata = if Address::from(numeraire).is_zero() {
                // Native ETH represented as zero address
                TokenMetadata {
                    name: "Native Ether".to_string(),
                    symbol: "ETH".to_string(),
                    decimals: 18,
                }
            } else {
                let call = ctx.calls_for_address(numeraire)
                    .filter(|call| call.function_name == "once")
                    .next()
                    .ok_or_else(|| {
                        let available_calls: Vec<_> = ctx.calls_for_address(numeraire)
                            .map(|c| format!("{}:{}", c.source_name, c.function_name))
                            .collect();
                        TransformationError::MissingData(format!(
                            "No 'once' call found for numeraire {} at block {} tx {}. Available calls: {:?}",
                            Address::from(numeraire), event.block_number, B256::from(event.transaction_hash), available_calls
                        ))
                    })?;

                let field_err = |field: &str, expected: &str| {
                    TransformationError::TypeConversion(format!(
                        "numeraire {} field '{}': expected {} but got {:?} at block {} tx {}",
                        Address::from(numeraire), field, expected, call.result.get(field),
                        event.block_number, B256::from(event.transaction_hash)
                    ))
                };
                let missing_err = |field: &str| {
                    TransformationError::MissingData(format!(
                        "numeraire {} missing field '{}' at block {} tx {}. Available fields: {:?}",
                        Address::from(numeraire), field, event.block_number, B256::from(event.transaction_hash),
                        call.result.keys().collect::<Vec<_>>()
                    ))
                };

                TokenMetadata {
                    name: call.result.get("name")
                        .ok_or_else(|| missing_err("name"))?
                        .as_string()
                        .ok_or_else(|| field_err("name", "string"))?
                        .to_string(),
                    symbol: call.result.get("symbol")
                        .ok_or_else(|| missing_err("symbol"))?
                        .as_string()
                        .ok_or_else(|| field_err("symbol", "string"))?
                        .to_string(),
                    decimals: call.result.get("decimals")
                        .ok_or_else(|| missing_err("decimals"))?
                        .as_u8()
                        .ok_or_else(|| field_err("decimals", "u8"))?,
                }
            };

            let hook_call = ctx.calls_for_address(hook)
                .filter(|call| call.function_name == "once")
                .next()
                .ok_or_else(|| {
                    let available_calls: Vec<_> = ctx.calls_for_address(hook)
                        .map(|c| format!("{}:{}", c.source_name, c.function_name))
                        .collect();
                    TransformationError::MissingData(format!(
                        "No 'once' call found for hook {} at block {} tx {}. Available calls: {:?}",
                        Address::from(hook), event.block_number, B256::from(event.transaction_hash), available_calls
                    ))
                })?;

            let pool_key = {
                let call = hook_call;
                let field_err = |field: &str, expected: &str| {
                    TransformationError::TypeConversion(format!(
                        "hook {} field '{}': expected {} but got {:?} at block {} tx {}",
                        Address::from(hook), field, expected, call.result.get(field),
                        event.block_number, B256::from(event.transaction_hash)
                    ))
                };
                let missing_err = |field: &str| {
                    TransformationError::MissingData(format!(
                        "hook {} missing field '{}' at block {} tx {}. Available fields: {:?}",
                        Address::from(hook), field, event.block_number, B256::from(event.transaction_hash),
                        call.result.keys().collect::<Vec<_>>()
                    ))
                };

                PoolKey {
                    currency0: call.result.get("poolKey.currency0")
                        .ok_or_else(|| missing_err("poolKey.currency0"))?
                        .as_address()
                        .ok_or_else(|| field_err("poolKey.currency0", "address"))?
                        .into(),
                    currency1: call.result.get("poolKey.currency1")
                        .ok_or_else(|| missing_err("poolKey.currency1"))?
                        .as_address()
                        .ok_or_else(|| field_err("poolKey.currency1", "address"))?
                        .into(),
                    fee: call.result.get("poolKey.fee")
                        .ok_or_else(|| missing_err("poolKey.fee"))?
                        .as_u32()
                        .ok_or_else(|| field_err("poolKey.fee", "u32"))?,
                    tick_spacing: call.result.get("poolKey.tickSpacing")
                        .ok_or_else(|| missing_err("poolKey.tickSpacing"))?
                        .as_i32()
                        .ok_or_else(|| field_err("poolKey.tickSpacing", "i32"))?,
                    hooks: call.result.get("poolKey.hooks")
                        .ok_or_else(|| missing_err("poolKey.hooks"))?
                        .as_address()
                        .ok_or_else(|| field_err("poolKey.hooks", "address"))?
                        .into(),
                }
            };

            let pool_id = pool_key.pool_id();
            
            let pool_config = {
                let call = ctx.calls_for_address(hook)
                    .filter(|call| call.function_name == "once")
                    .next()
                    .ok_or_else(|| {
                        TransformationError::MissingData(format!(
                            "No 'once' call found for hook {} (pool_config) at block {} tx {}",
                            Address::from(hook), event.block_number, B256::from(event.transaction_hash)
                        ))
                    })?;

                let field_err = |field: &str, expected: &str| {
                    TransformationError::TypeConversion(format!(
                        "hook {} (pool_config) field '{}': expected {} but got {:?} at block {} tx {}",
                        Address::from(hook), field, expected, call.result.get(field),
                        event.block_number, B256::from(event.transaction_hash)
                    ))
                };
                let missing_err = |field: &str| {
                    TransformationError::MissingData(format!(
                        "hook {} (pool_config) missing field '{}' at block {} tx {}. Available fields: {:?}",
                        Address::from(hook), field, event.block_number, B256::from(event.transaction_hash),
                        call.result.keys().collect::<Vec<_>>()
                    ))
                };

                // Helper to parse u64 from either native, i64, or string encoding
                let parse_u64 = |field: &str| -> Result<u64, TransformationError> {
                    let val = call.result.get(field).ok_or_else(|| missing_err(field))?;
                    val.as_u64()
                        .or_else(|| val.as_i64().and_then(|v| u64::try_from(v).ok()))
                        .or_else(|| val.as_string().and_then(|s| s.parse().ok()))
                        .ok_or_else(|| field_err(field, "u64 or numeric string"))
                };

                // Helper to parse i32 from either native, u32, or string encoding
                let parse_i32 = |field: &str| -> Result<i32, TransformationError> {
                    let val = call.result.get(field).ok_or_else(|| missing_err(field))?;
                    val.as_i32()
                        .or_else(|| val.as_u32().and_then(|v| i32::try_from(v).ok()))
                        .or_else(|| val.as_string().and_then(|s| s.parse().ok()))
                        .ok_or_else(|| field_err(field, "i32 or numeric string"))
                };

                // Helper to parse u32 from either native, i32, or string encoding
                let parse_u32 = |field: &str| -> Result<u32, TransformationError> {
                    let val = call.result.get(field).ok_or_else(|| missing_err(field))?;
                    val.as_u32()
                        .or_else(|| val.as_i32().and_then(|v| u32::try_from(v).ok()))
                        .or_else(|| val.as_string().and_then(|s| s.parse().ok()))
                        .ok_or_else(|| field_err(field, "u32 or numeric string"))
                };

                V4PoolConfig {
                    num_tokens_to_sell: call.result.get("numTokensToSell")
                        .ok_or_else(|| missing_err("numTokensToSell"))?
                        .as_uint256()
                        .ok_or_else(|| field_err("numTokensToSell", "uint256"))?,
                    min_proceeds: call.result.get("minimumProceeds")
                        .ok_or_else(|| missing_err("minimumProceeds"))?
                        .as_uint256()
                        .ok_or_else(|| field_err("minimumProceeds", "uint256"))?,
                    max_proceeds: call.result.get("maximumProceeds")
                        .ok_or_else(|| missing_err("maximumProceeds"))?
                        .as_uint256()
                        .ok_or_else(|| field_err("maximumProceeds", "uint256"))?,
                    starting_time: parse_u64("startingTime")?,
                    ending_time: parse_u64("endingTime")?,
                    starting_tick: parse_i32("startingTick")?,
                    ending_tick: parse_i32("endingTick")?,
                    epoch_length: call.result.get("epochLength")
                        .ok_or_else(|| missing_err("epochLength"))?
                        .as_uint256()
                        .ok_or_else(|| field_err("epochLength", "uint256"))?,
                    gamma: parse_u32("gamma")?,
                    is_token_0: call.result.get("isToken0")
                        .ok_or_else(|| missing_err("isToken0"))?
                        .as_bool()
                        .ok_or_else(|| field_err("isToken0", "bool"))?,
                    num_pd_slugs: call.result.get("numPDSlugs")
                        .ok_or_else(|| missing_err("numPDSlugs"))?
                        .as_uint256()
                        .ok_or_else(|| field_err("numPDSlugs", "uint256"))?,
                }
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
                None,
                pool_key,
                pool_config.starting_time,
                pool_config.ending_time,
                ctx
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
            "Create(address,address,address)"
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