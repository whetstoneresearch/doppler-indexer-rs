use async_trait::async_trait;

use alloy_primitives::{Address, B256, U256};

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::TransformationContext;
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::token::{PoolAddressOrPoolId, insert_token};
use crate::transformations::util::db::v4_pool_configs::insert_pool_config;
use crate::transformations::util::db::pool::insert_pool;

use crate::types::uniswap::v4::{PoolKey, V4PoolConfig};

struct TokenMetadata {
    name: String,
    symbol: String,
    decimals: u8,
}

struct AssetTokenMetadata {
    name: String,
    symbol: String,
    decimals: u8,
    token_uri: String,
    total_supply: U256,
    governance: Address,
    integrator: Address,
    initializer: Address,
    migrator: Address,
    migration_pool: PoolAddressOrPoolId,
}

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
        ctx: &TransformationContext<'_>,
    ) -> Result<Vec<DbOperation>, TransformationError>{
        let mut ops = Vec::new();

        for event in ctx.events_of_type("UniswapV4Initializer", "Create") {
            let asset = event.get("asset")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("asset is not an address".to_string())
            })?;

            let numeraire = event.get("numeraire")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("numeraire is not an address".to_string())
            })?;

            let hook = event.get("poolOrHook")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("poolOrHook is not an address".to_string())
            })?;

            let asset_metadata = ctx.calls_for_address(asset)
                .filter(|call| call.function_name == "once")
                .map(|call| AssetTokenMetadata {
                    name: call.result.get("name").unwrap().as_string().unwrap().to_string(),
                    symbol: call.result.get("symbol").unwrap().as_string().unwrap().to_string(),
                    decimals: call.result.get("decimals").unwrap().as_u8().unwrap(),
                    token_uri: call.result.get("tokenURI").unwrap().as_string().unwrap().to_string(),
                    total_supply: call.result.get("getAssetData.totalSupply").unwrap().as_uint256().unwrap(),
                    governance: call.result.get("getAssetData.governance").unwrap().as_address().unwrap().into(),
                    integrator: call.result.get("getAssetData.integrator").unwrap().as_address().unwrap().into(),
                    initializer: call.result.get("getAssetData.poolInitializer").unwrap().as_address().unwrap().into(),
                    migrator: call.result.get("getAssetData.liquidityMigrator").unwrap().as_address().unwrap().into(),
                    migration_pool: {
                        let val = call.result.get("getAssetData.migrationPool").unwrap();
                        if let Some(addr) = val.as_address() {
                            PoolAddressOrPoolId::Address(addr.into())
                        } else {
                            PoolAddressOrPoolId::PoolId(val.as_bytes32().unwrap())
                        }
                    }
                })
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

            let numeraire_metadata = ctx.calls_for_address(numeraire)
                .filter(|call| call.function_name == "once")
                .map(|call| TokenMetadata {
                    name: call.result.get("name").unwrap().as_string().unwrap().to_string(),
                    symbol: call.result.get("symbol").unwrap().as_string().unwrap().to_string(),
                    decimals: call.result.get("decimals").unwrap().as_u8().unwrap(),
                })
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

            let pool_key = ctx.calls_for_address(hook)
                .filter(|call| call.function_name == "once")
                .map(|call| PoolKey {
                    currency0: call.result.get("poolKey.currency0").unwrap().as_address().unwrap().into(),
                    currency1: call.result.get("poolKey.currency1").unwrap().as_address().unwrap().into(),
                    fee: call.result.get("poolKey.fee").unwrap().as_u32().unwrap(),
                    tick_spacing: call.result.get("poolKey.tickSpacing").unwrap().as_i32().unwrap(),
                    hooks: call.result.get("poolKey.hooks").unwrap().as_address().unwrap().into(),
                })
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

            let pool_id = pool_key.pool_id();
            
            let pool_config = ctx.calls_for_address(hook)
                .filter(|call| call.function_name == "once")
                .map(|call| V4PoolConfig {
                    num_tokens_to_sell: call.result.get("numTokensToSell").unwrap().as_uint256().unwrap(),
                    min_proceeds: call.result.get("minProceeds").unwrap().as_uint256().unwrap(),
                    max_proceeds: call.result.get("maxProceeds").unwrap().as_uint256().unwrap(),
                    starting_time: call.result.get("startingTime").unwrap().as_u64().unwrap(),
                    ending_time: call.result.get("endingTime").unwrap().as_u64().unwrap(),
                    starting_tick: call.result.get("startingTick").unwrap().as_i32().unwrap(),
                    ending_tick: call.result.get("endingTick").unwrap().as_i32().unwrap(),
                    epoch_length: call.result.get("epochLength").unwrap().as_uint256().unwrap(),
                    gamma: call.result.get("gamma").unwrap().as_u32().unwrap(),
                    is_token_0: call.result.get("isToken0").unwrap().as_bool().unwrap(),
                    num_pd_slugs: call.result.get("numPdSlugs").unwrap().as_uint256().unwrap()
                }).next()
                .ok_or_else(|| {
                    TransformationError::MissingData(format!(
                        "No 'once' call found for hook {} (pool_config) at block {} tx {}",
                        Address::from(hook), event.block_number, B256::from(event.transaction_hash)
                    ))
                })?;

            ops.push(insert_token(
                event.block_number,
                event.block_timestamp,
                &event.transaction_hash,
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
                &asset_metadata.governance,
                ctx
            ));

            ops.push(insert_token(
                event.block_number,
                event.block_timestamp,
                &event.transaction_hash,
                &numeraire,
                None,
                &numeraire_metadata.name,
                &numeraire_metadata.symbol,
                numeraire_metadata.decimals,
                None,
                None,
                true,
                false,
                false,
                None,
                &asset_metadata.governance,
                ctx
            ));    

            ops.push(insert_pool_config(
                event.block_number,
                event.block_timestamp,
                pool_id.into(),
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
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(V4CreateHandler);
}