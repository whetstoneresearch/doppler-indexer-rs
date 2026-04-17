use std::sync::OnceLock;

use alloy_primitives::U256;
use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{
    dep, EventHandler, EventTrigger, HandlerDependencySpec, TransformationHandler,
};
use crate::transformations::util::db::pool::{insert_pool, PoolData};
use crate::transformations::util::pool_metadata::VersionedSource;
use crate::types::uniswap::v4::{PoolAddressOrPoolId, PoolKey};

use super::create::{ZORA_CREATE_HANDLER_NAME, ZORA_CREATE_HANDLER_VERSION};

const CREATOR_COIN_SOURCE: &str = "ZoraCreatorCoinV4";
const ZERO_ADDRESS: [u8; 20] = [0u8; 20];

pub const ZORA_MIGRATE_HANDLER_NAME: &str = "ZoraMigrateHandler";
pub const ZORA_MIGRATE_HANDLER_VERSION: u32 = 1;
pub const ZORA_MIGRATE_HANDLER_SCOPE: VersionedSource =
    VersionedSource::new(ZORA_MIGRATE_HANDLER_NAME, ZORA_MIGRATE_HANDLER_VERSION);

pub struct ZoraMigrateHandler {
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for ZoraMigrateHandler {
    fn name(&self) -> &'static str {
        ZORA_MIGRATE_HANDLER_NAME
    }

    fn version(&self) -> u32 {
        ZORA_MIGRATE_HANDLER_VERSION
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/pools.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pools"]
    }

    fn requires_sequential(&self) -> bool {
        false
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type(CREATOR_COIN_SOURCE, "LiquidityMigrated") {
            let from_pool_key_hash = event.extract_bytes32("fromPoolKeyHash")?;
            let to_pool_key_hash = event.extract_bytes32("toPoolKeyHash")?;

            let to_pool_key = PoolKey {
                currency0: event.extract_address("toPoolKey.currency0")?.into(),
                currency1: event.extract_address("toPoolKey.currency1")?.into(),
                fee: event.extract_u32("toPoolKey.fee")?,
                tick_spacing: event.extract_i32_flexible("toPoolKey.tickSpacing")?,
                hooks: event.extract_address("toPoolKey.hooks")?.into(),
            };

            let client = self
                .db_pool
                .get()
                .expect("db_pool must be set before handle()")
                .get()
                .await?;

            let rows = client
                .query(
                    "SELECT base_token, quote_token, is_token_0, fee, integrator, initializer \
                     FROM pools \
                     WHERE chain_id = $1 AND address = $2 AND source = $3 AND source_version = $4 \
                     LIMIT 1",
                    &[
                        &(ctx.chain_id as i64),
                        &from_pool_key_hash.to_vec(),
                        &ZORA_CREATE_HANDLER_NAME,
                        &(ZORA_CREATE_HANDLER_VERSION as i32),
                    ],
                )
                .await?;

            let Some(row) = rows.first() else {
                tracing::warn!(
                    from_pool = hex::encode(from_pool_key_hash),
                    to_pool = hex::encode(to_pool_key_hash),
                    block = event.block_number,
                    "no ZoraCreateHandler pool found for LiquidityMigrated; skipping"
                );
                continue;
            };

            let base_token_bytes: Vec<u8> = row.get("base_token");
            let quote_token_bytes: Vec<u8> = row.get("quote_token");
            let integrator_bytes: Vec<u8> = row.get("integrator");
            let initializer_bytes: Vec<u8> = row.get("initializer");

            if base_token_bytes.len() != 20
                || quote_token_bytes.len() != 20
                || integrator_bytes.len() != 20
                || initializer_bytes.len() != 20
            {
                tracing::warn!(
                    from_pool = hex::encode(from_pool_key_hash),
                    block = event.block_number,
                    "malformed address bytes in original pool row; skipping"
                );
                continue;
            }

            let mut base_token = [0u8; 20];
            let mut quote_token = [0u8; 20];
            let mut integrator = [0u8; 20];
            let mut initializer = [0u8; 20];
            base_token.copy_from_slice(&base_token_bytes);
            quote_token.copy_from_slice(&quote_token_bytes);
            integrator.copy_from_slice(&integrator_bytes);
            initializer.copy_from_slice(&initializer_bytes);

            let is_token_0: bool = row.get("is_token_0");
            let fee: i32 = row.get("fee");

            // Insert the migrated pool. inject_source_version adds
            // source="ZoraMigrateHandler", source_version=1 automatically.
            ops.push(insert_pool(
                &PoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    address: PoolAddressOrPoolId::PoolId(to_pool_key_hash),
                    base_token,
                    quote_token,
                    is_token_0,
                    pool_type: "zora_creator_coin".to_string(),
                    integrator,
                    initializer,
                    fee: fee as u32,
                    min_threshold: U256::ZERO,
                    max_threshold: U256::ZERO,
                    migrator: ZERO_ADDRESS,
                    migrated_at: None,
                    migration_pool: PoolAddressOrPoolId::Address(ZERO_ADDRESS),
                    migration_type: "unknown".to_string(),
                    lock_duration: None,
                    beneficiaries: None,
                    pool_key: Some(to_pool_key),
                    starting_time: 0,
                    ending_time: 0,
                },
                ctx,
            ));

            // Mark the original pool as migrated. RawSql bypasses inject_source_version
            // so the WHERE clause targets the ZoraCreateHandler-owned row directly.
            ops.push(DbOperation::RawSql {
                query: "UPDATE pools \
                    SET migration_pool = $1, migrated_at = to_timestamp($2), migration_type = $3 \
                    WHERE chain_id = $4 AND address = $5 AND source = $6 AND source_version = $7"
                    .to_string(),
                params: vec![
                    DbValue::Bytes32(to_pool_key_hash),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::VarChar("zora".to_string()),
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Bytes(from_pool_key_hash.to_vec()),
                    DbValue::VarChar(ZORA_CREATE_HANDLER_NAME.to_string()),
                    DbValue::Int32(ZORA_CREATE_HANDLER_VERSION as i32),
                ],
                snapshot: None,
            });
        }

        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        tracing::info!("ZoraMigrateHandler initialized");
        Ok(())
    }
}

impl EventHandler for ZoraMigrateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            CREATOR_COIN_SOURCE,
            "LiquidityMigrated((address,address,uint24,int24,address),bytes32,(address,address,uint24,int24,address),bytes32)",
        )]
    }

    fn contiguous_handler_dependency_specs(&self) -> Vec<HandlerDependencySpec> {
        vec![dep("ZoraCreateHandler")]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(ZoraMigrateHandler {
        db_pool: OnceLock::new(),
    });
}
