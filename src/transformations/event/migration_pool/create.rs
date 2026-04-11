//! Migration pool create handler.
//!
//! Inserts a `pools` row for each Doppler V4 pool that graduates to a standard
//! UniswapV4 pool via `UniswapV4Migrator.Migrate`. The new row carries
//! `migrated_from` pointing to the original Doppler pool's bytes32 address,
//! which is how `MigrationPoolSwapMetricsHandler` discovers the migration pool
//! ID set at startup.
//!
//! ## Dependencies
//!
//! Depends on every Doppler pool create handler that can populate
//! `pools.migration_pool`, so the original pool row exists in the DB before
//! this handler queries for it during catchup.

use std::sync::OnceLock;

use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::decay_multicurve::create::V4_DECAY_MULTICURVE_CREATE_HANDLER_SCOPE;
use crate::transformations::event::dhook::create::DOPPLER_HOOK_CREATE_HANDLER_SCOPE;
use crate::transformations::event::multicurve::create::V4_MULTICURVE_CREATE_HANDLER_SCOPE;
use crate::transformations::event::scheduled_multicurve::create::V4_SCHEDULED_MULTICURVE_CREATE_HANDLER_SCOPE;
use crate::transformations::event::v4::create::V4_CREATE_HANDLER_SCOPE;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::db::pool::{insert_migration_pool, MigrationPoolData};
use crate::transformations::util::pool_metadata::VersionedSource;

const MIGRATOR_SOURCE: &str = "UniswapV4Migrator";
pub const MIGRATION_POOL_CREATE_HANDLER_NAME: &str = "MigrationPoolCreateHandler";
pub const MIGRATION_POOL_CREATE_HANDLER_VERSION: u32 = 1;
pub const MIGRATION_POOL_CREATE_HANDLER_SCOPE: VersionedSource = VersionedSource::new(
    MIGRATION_POOL_CREATE_HANDLER_NAME,
    MIGRATION_POOL_CREATE_HANDLER_VERSION,
);

const ORIGINAL_POOL_SCOPES: &[VersionedSource] = &[
    V4_CREATE_HANDLER_SCOPE,
    V4_MULTICURVE_CREATE_HANDLER_SCOPE,
    V4_SCHEDULED_MULTICURVE_CREATE_HANDLER_SCOPE,
    V4_DECAY_MULTICURVE_CREATE_HANDLER_SCOPE,
    DOPPLER_HOOK_CREATE_HANDLER_SCOPE,
];

pub struct MigrationPoolCreateHandler {
    db_pool: OnceLock<Pool>,
}

#[async_trait]
impl TransformationHandler for MigrationPoolCreateHandler {
    fn name(&self) -> &'static str {
        MIGRATION_POOL_CREATE_HANDLER_NAME
    }

    fn version(&self) -> u32 {
        MIGRATION_POOL_CREATE_HANDLER_VERSION
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
        let (original_pool_sources, original_pool_versions) = original_pool_scope_params();

        for event in ctx.events_of_type(MIGRATOR_SOURCE, "Migrate") {
            let pool_id = event.extract_bytes32("poolId")?;

            let client = self
                .db_pool
                .get()
                .expect("db_pool must be set before handle()")
                .get()
                .await?;

            let rows = client
                .query(
                    "WITH original_pool_scope AS ( \
                        SELECT * FROM unnest($3::text[], $4::int4[]) AS scope(source, source_version) \
                     ) \
                     SELECT p.address, p.base_token, p.quote_token, p.is_token_0, p.fee, \
                            p.integrator, p.initializer \
                     FROM pools p \
                     JOIN original_pool_scope ops \
                       ON p.source = ops.source AND p.source_version = ops.source_version \
                     WHERE p.chain_id = $1 AND p.migration_pool = $2 \
                     LIMIT 1",
                    &[
                        &(ctx.chain_id as i64),
                        &pool_id.to_vec(),
                        &original_pool_sources,
                        &original_pool_versions,
                    ],
                )
                .await?;

            let Some(row) = rows.first() else {
                tracing::warn!(
                    pool_id = hex::encode(pool_id),
                    block = event.block_number,
                    "no original pool found for migration pool; skipping"
                );
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
                    pool_id = hex::encode(pool_id),
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

            ops.push(insert_migration_pool(
                &MigrationPoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    pool_id: &pool_id,
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
        tracing::info!("MigrationPoolCreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for MigrationPoolCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            MIGRATOR_SOURCE,
            "Migrate(bytes32,uint160,int24,int24,uint256,uint256,uint256)",
        )]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec![
            "V4CreateHandler",
            "V4MulticurveCreateHandler",
            "V4ScheduledMulticurveCreateHandler",
            "V4DecayMulticurveCreateHandler",
            "DopplerHookCreateHandler",
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(MigrationPoolCreateHandler {
        db_pool: OnceLock::new(),
    });
}

fn original_pool_scope_params() -> (Vec<String>, Vec<i32>) {
    (
        ORIGINAL_POOL_SCOPES
            .iter()
            .map(|scope| scope.source.to_string())
            .collect(),
        ORIGINAL_POOL_SCOPES
            .iter()
            .map(|scope| scope.version as i32)
            .collect(),
    )
}
