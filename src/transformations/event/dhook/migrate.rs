//! Dhook migration pool create handler.
//!
//! Inserts a `pools` row for each dhook/rehype pool that graduates to a
//! standard UniswapV4 pool via `DopplerHookMigrator.Migrate`. The new row
//! carries `migrated_from` pointing to the original dhook pool's bytes32
//! address, enabling `MigrationPoolSwapMetricsHandler` to discover the pool.

use std::sync::OnceLock;

use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::dhook::create::{
    DOPPLER_HOOK_CREATE_HANDLER_NAME, DOPPLER_HOOK_CREATE_HANDLER_VERSION,
};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{
    dep, EventHandler, EventTrigger, HandlerDependencySpec, TransformationHandler,
};
use crate::transformations::util::db::pool::{insert_migration_pool, MigrationPoolData};
use crate::transformations::util::pool_metadata::VersionedSource;
use crate::types::uniswap::v4::PoolKey;

const SOURCE: &str = "DopplerHookMigrator";
pub const DHOOK_MIGRATION_POOL_CREATE_HANDLER_NAME: &str = "DhookMigrationPoolCreateHandler";
pub const DHOOK_MIGRATION_POOL_CREATE_HANDLER_VERSION: u32 = 1;
pub const DHOOK_MIGRATION_POOL_CREATE_HANDLER_SCOPE: VersionedSource = VersionedSource::new(
    DHOOK_MIGRATION_POOL_CREATE_HANDLER_NAME,
    DHOOK_MIGRATION_POOL_CREATE_HANDLER_VERSION,
);

pub struct DhookMigrationPoolCreateHandler {
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for DhookMigrationPoolCreateHandler {
    fn name(&self) -> &'static str {
        DHOOK_MIGRATION_POOL_CREATE_HANDLER_NAME
    }

    fn version(&self) -> u32 {
        DHOOK_MIGRATION_POOL_CREATE_HANDLER_VERSION
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

        for event in ctx.events_of_type(SOURCE, "Migrate") {
            let asset = event.extract_address("asset")?;

            let pool_key = PoolKey {
                currency0: event.extract_address("poolKey.currency0")?.into(),
                currency1: event.extract_address("poolKey.currency1")?.into(),
                fee: event.extract_u32("poolKey.fee")?,
                tick_spacing: event.extract_i32_flexible("poolKey.tickSpacing")?,
                hooks: event.extract_address("poolKey.hooks")?.into(),
            };
            let pool_id = pool_key.pool_id();

            let client = self
                .db_pool
                .get()
                .expect("db_pool must be set before handle()")
                .get()
                .await?;

            let rows = client
                .query(
                    "SELECT p.address, p.base_token, p.quote_token, p.is_token_0, p.fee, \
                            p.integrator, p.initializer, p.type \
                     FROM pools p \
                     WHERE p.chain_id = $1 \
                       AND p.base_token = $2 \
                       AND p.source = $3 \
                       AND p.source_version = $4 \
                     LIMIT 1",
                    &[
                        &(ctx.chain_id as i64),
                        &asset.to_vec(),
                        &DOPPLER_HOOK_CREATE_HANDLER_NAME,
                        &(DOPPLER_HOOK_CREATE_HANDLER_VERSION as i32),
                    ],
                )
                .await?;

            let Some(row) = rows.first() else {
                tracing::warn!(
                    asset = hex::encode(asset),
                    block = event.block_number,
                    "no dhook pool found for DopplerHookMigrator.Migrate asset; skipping"
                );
                continue;
            };

            let base_token_bytes: Vec<u8> = row.get("base_token");
            let quote_token_bytes: Vec<u8> = row.get("quote_token");
            let integrator_bytes: Vec<u8> = row.get("integrator");
            let initializer_bytes: Vec<u8> = row.get("initializer");
            let original_address: Vec<u8> = row.get("address");
            let original_type: String = row.get("type");

            if base_token_bytes.len() != 20
                || quote_token_bytes.len() != 20
                || integrator_bytes.len() != 20
                || initializer_bytes.len() != 20
            {
                tracing::warn!(
                    asset = hex::encode(asset),
                    block = event.block_number,
                    "malformed address bytes in dhook pool row; skipping"
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

            let (pool_type, migration_type) = if original_type == "rehype" {
                ("migration_rehype", "rehype")
            } else {
                ("migration_dhook", "dhook")
            };

            ops.push(insert_migration_pool(
                &MigrationPoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    pool_id: &pool_id.0,
                    base_token,
                    quote_token,
                    is_token_0,
                    integrator,
                    initializer,
                    fee: fee as u32,
                    migrated_from: &original_address,
                    pool_type,
                    migration_type,
                },
                ctx,
            ));
        }

        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        tracing::info!("DhookMigrationPoolCreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for DhookMigrationPoolCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            SOURCE,
            "Migrate(address,(address,address,uint24,int24,address))",
        )]
    }

    fn contiguous_handler_dependency_specs(&self) -> Vec<HandlerDependencySpec> {
        vec![dep("DopplerHookCreateHandler").except([130, 57073])]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(DhookMigrationPoolCreateHandler {
        db_pool: OnceLock::new(),
    });
}
