//! V2 migration pool create handler.
//!
//! Inserts a `pools` row for each Doppler pool that graduates to a UniswapV2
//! pair via `Airlock.Migrate`. The new row carries `migrated_from` pointing to
//! the original Doppler pool's address, enabling `V2MigrationSwapMetricsHandler`
//! to discover the pair address set at initialization.

use std::sync::OnceLock;

use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{
    dep, EventHandler, EventTrigger, HandlerDependencySpec, TransformationHandler,
};
use crate::transformations::util::db::pool::{insert_v2_migration_pool, MigrationV2PoolData};
use crate::transformations::util::pool_metadata::VersionedSource;

const SOURCE: &str = "Airlock";
pub const V2_MIGRATION_POOL_CREATE_HANDLER_NAME: &str = "V2MigrationPoolCreateHandler";
pub const V2_MIGRATION_POOL_CREATE_HANDLER_VERSION: u32 = 1;
pub const V2_MIGRATION_POOL_CREATE_HANDLER_SCOPE: VersionedSource = VersionedSource::new(
    V2_MIGRATION_POOL_CREATE_HANDLER_NAME,
    V2_MIGRATION_POOL_CREATE_HANDLER_VERSION,
);

pub struct V2MigrationPoolCreateHandler {
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for V2MigrationPoolCreateHandler {
    fn name(&self) -> &'static str {
        V2_MIGRATION_POOL_CREATE_HANDLER_NAME
    }

    fn version(&self) -> u32 {
        V2_MIGRATION_POOL_CREATE_HANDLER_VERSION
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
            let pair_address = event.extract_address("pool")?;

            // V4/dhook migrations have pool == address(0); skip them.
            if pair_address == [0u8; 20] {
                continue;
            }

            let client = self
                .db_pool
                .get()
                .expect("db_pool must be set before handle()")
                .get()
                .await?;

            let rows = client
                .query(
                    "SELECT p.address, p.base_token, p.quote_token, p.is_token_0, p.fee, \
                            p.integrator, p.initializer \
                     FROM pools p \
                     WHERE p.chain_id = $1 \
                       AND p.migration_pool = $2 \
                       AND p.migration_type = 'v2' \
                     LIMIT 1",
                    &[&(ctx.chain_id as i64), &pair_address.to_vec()],
                )
                .await?;

            let Some(row) = rows.first() else {
                // Not a V2 migration (could be V3/V4 with same Airlock.Migrate event).
                continue;
            };

            let base_token_bytes: Vec<u8> = row.get("base_token");
            let quote_token_bytes: Vec<u8> = row.get("quote_token");
            let integrator_bytes: Vec<u8> = row.get("integrator");
            let initializer_bytes: Vec<u8> = row.get("initializer");
            let original_address: Vec<u8> = row.get("address");

            if base_token_bytes.len() != 20
                || quote_token_bytes.len() != 20
                || integrator_bytes.len() != 20
                || initializer_bytes.len() != 20
            {
                tracing::warn!(
                    pair = hex::encode(pair_address),
                    block = event.block_number,
                    "malformed address bytes in original V2 pool row; skipping"
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

            ops.push(insert_v2_migration_pool(
                &MigrationV2PoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    pair_address: &pair_address,
                    base_token,
                    quote_token,
                    is_token_0,
                    integrator,
                    initializer,
                    fee: fee as u32,
                    migrated_from: &original_address,
                },
                ctx,
            ));
        }

        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        tracing::info!("V2MigrationPoolCreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for V2MigrationPoolCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            SOURCE,
            "Migrate(address,address)",
        )]
    }

    fn contiguous_handler_dependency_specs(&self) -> Vec<HandlerDependencySpec> {
        vec![
            dep("V4CreateHandler"),
            dep("V4MulticurveCreateHandler").only([8453, 84532]),
            dep("V4ScheduledMulticurveCreateHandler").except([130, 57073]),
            dep("V4DecayMulticurveCreateHandler").only([8453, 11155111, 84532]),
            dep("DopplerHookCreateHandler").except([130, 57073]),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(V2MigrationPoolCreateHandler {
        db_pool: OnceLock::new(),
    });
}
