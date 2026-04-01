use async_trait::async_trait;

use alloy_primitives::U256;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::dhook_pool_configs::{
    insert_dhook_pool_config, DhookPoolConfigData,
};
use crate::transformations::util::db::pool::{
    insert_pool, BeneficiariesData, Beneficiary, PoolData,
};
use crate::transformations::util::db::token::{insert_token, TokenData};
use crate::transformations::util::metadata::get_metadata;
use crate::transformations::util::migration::resolve_migration_type;
use crate::types::decoded::DecodedValue;
use crate::types::uniswap::v4::{PoolAddressOrPoolId, PoolKey};

pub struct DopplerHookCreateHandler;

#[async_trait]
impl TransformationHandler for DopplerHookCreateHandler {
    fn name(&self) -> &'static str {
        "DopplerHookCreateHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/tokens.sql",
            "migrations/tables/pools.sql",
            "migrations/tables/dhook_pool_configs.sql",
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

        for event in ctx.events_of_type("DopplerHookInitializer", "Create") {
            let asset = event.extract_address("asset")?;
            let numeraire = event.extract_address("numeraire")?;

            let (asset_metadata, numeraire_metadata) =
                get_metadata(&asset, &numeraire, event, ctx)?;

            let get_state_call = ctx
                .calls_of_type("DopplerHookInitializer", "getState")
                .find(|call| call.trigger_log_index.unwrap() == event.log_index)
                .ok_or_else(|| {
                    TransformationError::MissingData(format!(
                        "No getState call at block {} tx index {}",
                        event.block_number, event.log_index
                    ))
                })?;

            // Extract dhook-specific fields from getState
            let gs_numeraire = get_state_call.extract_address("numeraire")?;
            let total_tokens_on_bonding_curve =
                get_state_call.extract_uint256("totalTokensOnBondingCurve")?;
            let doppler_hook = get_state_call.extract_address("dopplerHook")?;
            let status = get_state_call.extract_u8("status")?;
            let far_tick = get_state_call.extract_i32_flexible("farTick")?;

            // Extract poolKey (needed for pool_id and insert_pool)
            let pool_key = PoolKey {
                currency0: get_state_call.extract_address("poolKey.currency0")?.into(),
                currency1: get_state_call.extract_address("poolKey.currency1")?.into(),
                fee: get_state_call.extract_u32("poolKey.fee")?,
                tick_spacing: get_state_call.extract_i32_flexible("poolKey.tickSpacing")?,
                hooks: get_state_call.extract_address("poolKey.hooks")?.into(),
            };

            let hook: [u8; 20] = pool_key.hooks.into();
            let pool_id = pool_key.pool_id();
            let is_token_0 = asset < numeraire;

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

            ops.push(insert_dhook_pool_config(
                &DhookPoolConfigData {
                    pool_id: pool_id.into(),
                    hook_address: hook,
                    numeraire: gs_numeraire,
                    total_tokens_on_bonding_curve,
                    doppler_hook,
                    status,
                    far_tick,
                    is_token_0,
                },
                ctx,
            ));

            let beneficiaries: Option<BeneficiariesData> = ctx
                .calls_of_type("DopplerHookInitializer", "getBeneficiaries")
                .find(|call| call.trigger_log_index.unwrap() == event.log_index)
                .and_then(|call| call.result.get("getBeneficiaries"))
                .map(|val| match val {
                    DecodedValue::Array(elements) => elements
                        .iter()
                        .filter_map(|elem| {
                            if let DecodedValue::UnnamedTuple(fields) = elem {
                                let address = fields.first()?.as_address()?;
                                let shares = fields.get(1)?.as_u64()?;
                                Some(Beneficiary::new(address, shares))
                            } else {
                                None
                            }
                        })
                        .collect(),
                    _ => Vec::new(),
                });

            let migration_type = resolve_migration_type(ctx, asset_metadata.migrator.into());

            let pool_type = if ctx.match_contract_address(hook, &["RehypeHook"]).is_some() {
                "rehype"
            } else {
                "dhook"
            };

            ops.push(insert_pool(
                &PoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    address: PoolAddressOrPoolId::PoolId(pool_id.into()),
                    base_token: asset,
                    quote_token: numeraire,
                    is_token_0,
                    pool_type: pool_type.to_string(),
                    integrator: asset_metadata.integrator.into(),
                    initializer: asset_metadata.initializer.into(),
                    fee: pool_key.fee,
                    min_threshold: U256::ZERO,
                    max_threshold: U256::ZERO,
                    migrator: asset_metadata.migrator.into(),
                    migrated_at: None,
                    migration_pool: asset_metadata.migration_pool,
                    migration_type: migration_type.to_string(),
                    lock_duration: None,
                    beneficiaries,
                    pool_key: Some(pool_key),
                    starting_time: 0,
                    ending_time: 0,
                },
                ctx,
            ));
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("DopplerHookCreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for DopplerHookCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "DopplerHookInitializer",
            "Create(address,address,address)",
        )]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![
            ("DERC20".to_string(), "once".to_string()),
            ("Numeraires".to_string(), "once".to_string()),
            ("DopplerHookInitializer".to_string(), "getState".to_string()),
            (
                "DopplerHookInitializer".to_string(),
                "getBeneficiaries".to_string(),
            ),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(DopplerHookCreateHandler);
}
