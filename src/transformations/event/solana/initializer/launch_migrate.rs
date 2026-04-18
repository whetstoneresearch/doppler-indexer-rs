use async_trait::async_trait;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct LaunchMigrateHandler;

#[async_trait]
impl TransformationHandler for LaunchMigrateHandler {
    fn name(&self) -> &'static str {
        "LaunchMigrateHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Solana
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pools.sql",
            "migrations/tables/pools_solana_nullable.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec![]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerInitializer", "LaunchMigrated") {
            let launch = event.extract_pubkey("launch")?;

            // Target the pools row created by LaunchCreateHandler.
            // RawSql is used to explicitly scope the update to the originating handler's source.
            ops.push(DbOperation::RawSql {
                query: "UPDATE pools \
                    SET migrated_at = to_timestamp($1) \
                    WHERE chain_id = $2 AND address = $3 AND source = $4 AND source_version = $5"
                    .to_string(),
                params: vec![
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Pubkey(launch),
                    DbValue::Text("LaunchCreateHandler".to_string()),
                    DbValue::Int32(1),
                ],
                snapshot: None,
            });
        }

        Ok(ops)
    }
}

impl EventHandler for LaunchMigrateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new("DopplerInitializer", "LaunchMigrated")]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["LaunchCreateHandler"]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(LaunchMigrateHandler);
}
