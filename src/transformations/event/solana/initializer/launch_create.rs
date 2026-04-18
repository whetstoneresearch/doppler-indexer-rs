use async_trait::async_trait;
use serde_json::json;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct LaunchCreateHandler;

#[async_trait]
impl TransformationHandler for LaunchCreateHandler {
    fn name(&self) -> &'static str {
        "LaunchCreateHandler"
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
        vec!["pools"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerInitializer", "LaunchInitialized") {
            let launch = event.extract_pubkey("launch")?;
            let namespace = event.extract_pubkey("namespace")?;
            let launch_id = event.extract_bytes32("launch_id")?;
            let base_mint = event.extract_pubkey("base_mint")?;
            let quote_mint = event.extract_pubkey("quote_mint")?;
            let base_total_supply = event.extract_u64("base_total_supply")?;
            let curve_kind = event.extract_u8("curve_kind")?;
            let migrator_program = event.extract_pubkey("migrator_program")?;
            let sentinel_program = event.extract_pubkey("sentinel_program")?;

            let pool_key = json!({
                "namespace": bs58::encode(&namespace).into_string(),
                "launch_id": hex::encode(&launch_id),
                "base_total_supply": base_total_supply,
                "curve_kind": curve_kind,
                "sentinel_program": bs58::encode(&sentinel_program).into_string(),
            });

            ops.push(DbOperation::Upsert {
                table: "pools".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "address".into(),
                    "base_token".into(),
                    "quote_token".into(),
                    "is_token_0".into(),
                    "type".into(),
                    "integrator".into(),
                    "initializer".into(),
                    "fee".into(),
                    "min_threshold".into(),
                    "max_threshold".into(),
                    "migrator".into(),
                    "migrated_at".into(),
                    "migration_pool".into(),
                    "migrated_from".into(),
                    "migration_type".into(),
                    "lock_duration".into(),
                    "beneficiaries".into(),
                    "pool_key".into(),
                    "starting_time".into(),
                    "ending_time".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(launch),
                    DbValue::Pubkey(base_mint),
                    DbValue::Pubkey(quote_mint),
                    DbValue::Bool(true),
                    DbValue::VarChar(format!("curve_{}", curve_kind)),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Pubkey(migrator_program),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::JsonB(pool_key),
                    DbValue::Null,
                    DbValue::Null,
                ],
                conflict_columns: vec!["chain_id".into(), "address".into()],
                update_columns: vec![],
                update_condition: None,
            });
        }

        Ok(ops)
    }
}

impl EventHandler for LaunchCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new("DopplerInitializer", "LaunchInitialized")]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(LaunchCreateHandler);
}
