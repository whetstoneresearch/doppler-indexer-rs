use async_trait::async_trait;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct PredictionEntryHandler;

#[async_trait]
impl TransformationHandler for PredictionEntryHandler {
    fn name(&self) -> &'static str {
        "PredictionEntryHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Solana
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/sol_prediction_entries.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["sol_prediction_entries"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerPredictionMigrator", "EntryRegistered") {
            let market = event.extract_pubkey("market")?;
            let oracle = event.extract_pubkey("oracle")?;
            let entry_id = event.extract_bytes32("entry_id")?;
            let base_mint = event.extract_pubkey("base_mint")?;

            ops.push(DbOperation::Upsert {
                table: "sol_prediction_entries".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "market".into(),
                    "oracle".into(),
                    "entry_id".into(),
                    "base_mint".into(),
                    "contribution".into(),
                    "is_winner".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(market),
                    DbValue::Pubkey(oracle),
                    DbValue::Bytes32(entry_id),
                    DbValue::Pubkey(base_mint),
                    DbValue::Null,
                    DbValue::Null,
                ],
                conflict_columns: vec!["chain_id".into(), "market".into(), "entry_id".into()],
                update_columns: vec![],
                update_condition: None,
            });
        }

        for event in ctx.events_of_type("DopplerPredictionMigrator", "EntryMigrated") {
            let market = event.extract_pubkey("market")?;
            let oracle = event.extract_pubkey("oracle")?;
            let entry_id = event.extract_bytes32("entry_id")?;
            let base_mint = event.extract_pubkey("base_mint")?;
            let contribution = event.extract_u64("contribution")?;
            let is_winner = event.extract_bool("is_winner")?;

            ops.push(DbOperation::Upsert {
                table: "sol_prediction_entries".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "market".into(),
                    "oracle".into(),
                    "entry_id".into(),
                    "base_mint".into(),
                    "contribution".into(),
                    "is_winner".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(market),
                    DbValue::Pubkey(oracle),
                    DbValue::Bytes32(entry_id),
                    DbValue::Pubkey(base_mint),
                    DbValue::Uint64(contribution),
                    DbValue::Bool(is_winner),
                ],
                conflict_columns: vec!["chain_id".into(), "market".into(), "entry_id".into()],
                update_columns: vec!["contribution".into(), "is_winner".into()],
                update_condition: None,
            });
        }

        Ok(ops)
    }
}

impl EventHandler for PredictionEntryHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new("DopplerPredictionMigrator", "EntryRegistered"),
            EventTrigger::new("DopplerPredictionMigrator", "EntryMigrated"),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(PredictionEntryHandler);
}
