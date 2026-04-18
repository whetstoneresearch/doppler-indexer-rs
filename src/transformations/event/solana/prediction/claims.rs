use async_trait::async_trait;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct PredictionClaimHandler;

#[async_trait]
impl TransformationHandler for PredictionClaimHandler {
    fn name(&self) -> &'static str {
        "PredictionClaimHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Solana
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/sol_prediction_claims.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["sol_prediction_claims"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerPredictionMigrator", "RewardsClaimed") {
            let market = event.extract_pubkey("market")?;
            let claimer = event.extract_pubkey("claimer")?;
            let burned_amount = event.extract_u64("burned_amount")?;
            let reward_amount = event.extract_u64("reward_amount")?;
            let total_burned = event.extract_u64("total_burned")?;
            let tx_sig = event
                .transaction_id
                .as_solana()
                .expect("Solana event has Solana tx id");
            let log_position = event.position.packed_ordinal_i64();

            ops.push(DbOperation::Upsert {
                table: "sol_prediction_claims".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "tx_id".into(),
                    "log_position".into(),
                    "market".into(),
                    "claimer".into(),
                    "burned_amount".into(),
                    "reward_amount".into(),
                    "total_burned".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Bytes(tx_sig.to_vec()),
                    DbValue::Int64(log_position),
                    DbValue::Pubkey(market),
                    DbValue::Pubkey(claimer),
                    DbValue::Uint64(burned_amount),
                    DbValue::Uint64(reward_amount),
                    DbValue::Uint64(total_burned),
                ],
                conflict_columns: vec![
                    "chain_id".into(),
                    "tx_id".into(),
                    "log_position".into(),
                ],
                update_columns: vec![],
                update_condition: None,
            });
        }

        Ok(ops)
    }
}

impl EventHandler for PredictionClaimHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new("DopplerPredictionMigrator", "RewardsClaimed")]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(PredictionClaimHandler);
}
