use async_trait::async_trait;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct CpmmMigratorHandler;

#[async_trait]
impl TransformationHandler for CpmmMigratorHandler {
    fn name(&self) -> &'static str {
        "CpmmMigratorHandler"
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
            "migrations/tables/sol_cpmm_migrator.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["sol_cpmm_migrator"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerCpmmMigrator", "LaunchRegistered") {
            let launch = event.extract_pubkey("launch")?;
            let state = event.extract_pubkey("state")?;
            let cpmm_config = event.extract_pubkey("cpmm_config")?;
            let min_raise_quote = event.extract_u64("min_raise_quote")?;

            ops.push(DbOperation::Upsert {
                table: "sol_cpmm_migrator".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "launch".into(),
                    "state".into(),
                    "cpmm_config".into(),
                    "min_raise_quote".into(),
                    "pool".into(),
                    "quote_for_liquidity".into(),
                    "base_for_liquidity".into(),
                    "migrated".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(launch),
                    DbValue::Pubkey(state),
                    DbValue::Pubkey(cpmm_config),
                    DbValue::Uint64(min_raise_quote),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Bool(false),
                ],
                conflict_columns: vec!["chain_id".into(), "launch".into()],
                update_columns: vec![],
                update_condition: None,
            });
        }

        for event in ctx.events_of_type("DopplerCpmmMigrator", "LaunchMigrated") {
            let launch = event.extract_pubkey("launch")?;
            let pool = event.extract_pubkey("pool")?;
            let quote_for_liquidity = event.extract_u64("quote_for_liquidity")?;
            let base_for_liquidity = event.extract_u64("base_for_liquidity")?;

            ops.push(DbOperation::Upsert {
                table: "sol_cpmm_migrator".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "launch".into(),
                    "state".into(),
                    "cpmm_config".into(),
                    "min_raise_quote".into(),
                    "pool".into(),
                    "quote_for_liquidity".into(),
                    "base_for_liquidity".into(),
                    "migrated".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(launch),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Pubkey(pool),
                    DbValue::Uint64(quote_for_liquidity),
                    DbValue::Uint64(base_for_liquidity),
                    DbValue::Bool(true),
                ],
                conflict_columns: vec!["chain_id".into(), "launch".into()],
                update_columns: vec![
                    "pool".into(),
                    "quote_for_liquidity".into(),
                    "base_for_liquidity".into(),
                    "migrated".into(),
                ],
                update_condition: None,
            });

            // Update the originating pools row with the final CPMM pool address.
            // RawSql targets the LaunchCreateHandler-owned row explicitly.
            ops.push(DbOperation::RawSql {
                query: "UPDATE pools \
                    SET migration_pool = $1, migrated_at = to_timestamp($2) \
                    WHERE chain_id = $3 AND address = $4 AND source = $5 AND source_version = $6"
                    .to_string(),
                params: vec![
                    DbValue::Pubkey(pool),
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

impl EventHandler for CpmmMigratorHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new("DopplerCpmmMigrator", "LaunchRegistered"),
            EventTrigger::new("DopplerCpmmMigrator", "LaunchMigrated"),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(CpmmMigratorHandler);
}
