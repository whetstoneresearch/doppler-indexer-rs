use async_trait::async_trait;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct PredictionMarketHandler;

#[async_trait]
impl TransformationHandler for PredictionMarketHandler {
    fn name(&self) -> &'static str {
        "PredictionMarketHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Solana
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/sol_prediction_markets.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["sol_prediction_markets"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerPredictionMigrator", "MarketCreated") {
            let market = event.extract_pubkey("market")?;
            let oracle = event.extract_pubkey("oracle")?;
            let quote_mint = event.extract_pubkey("quote_mint")?;

            ops.push(DbOperation::Upsert {
                table: "sol_prediction_markets".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "resolved_at".into(),
                    "market".into(),
                    "oracle".into(),
                    "quote_mint".into(),
                    "winner_mint".into(),
                    "claimable_supply".into(),
                    "total_pot".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Null,
                    DbValue::Pubkey(market),
                    DbValue::Pubkey(oracle),
                    DbValue::Pubkey(quote_mint),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                ],
                conflict_columns: vec!["chain_id".into(), "market".into()],
                update_columns: vec![],
                update_condition: None,
            });
        }

        for event in ctx.events_of_type("DopplerPredictionMigrator", "MarketResolved") {
            let market = event.extract_pubkey("market")?;
            let oracle = event.extract_pubkey("oracle")?;
            let winner_mint = event.extract_pubkey("winner_mint")?;
            let claimable_supply = event.extract_u64("claimable_supply")?;
            let total_pot = event.extract_u64("total_pot")?;

            ops.push(DbOperation::Upsert {
                table: "sol_prediction_markets".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "resolved_at".into(),
                    "market".into(),
                    "oracle".into(),
                    "quote_mint".into(),
                    "winner_mint".into(),
                    "claimable_supply".into(),
                    "total_pot".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(market),
                    DbValue::Pubkey(oracle),
                    DbValue::Null, // quote_mint unknown at resolve; preserved from create on conflict
                    DbValue::Pubkey(winner_mint),
                    DbValue::Uint64(claimable_supply),
                    DbValue::Uint64(total_pot),
                ],
                conflict_columns: vec!["chain_id".into(), "market".into()],
                update_columns: vec![
                    "winner_mint".into(),
                    "claimable_supply".into(),
                    "total_pot".into(),
                    "resolved_at".into(),
                ],
                update_condition: None,
            });
        }

        Ok(ops)
    }
}

impl EventHandler for PredictionMarketHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new("DopplerPredictionMigrator", "MarketCreated"),
            EventTrigger::new("DopplerPredictionMigrator", "MarketResolved"),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(PredictionMarketHandler);
}
