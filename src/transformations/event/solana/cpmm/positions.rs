use async_trait::async_trait;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct CpmmPositionHandler;

#[async_trait]
impl TransformationHandler for CpmmPositionHandler {
    fn name(&self) -> &'static str {
        "CpmmPositionHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Solana
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/sol_cpmm_positions.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["sol_cpmm_positions"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerCPMM", "PositionCreated") {
            let pool = event.extract_pubkey("pool")?;
            let owner = event.extract_pubkey("owner")?;
            let position = event.extract_pubkey("position")?;
            let position_id = event.extract_u64("position_id")?;

            ops.push(DbOperation::Upsert {
                table: "sol_cpmm_positions".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "closed_at".into(),
                    "position".into(),
                    "pool".into(),
                    "owner".into(),
                    "position_id".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Null,
                    DbValue::Pubkey(position),
                    DbValue::Pubkey(pool),
                    DbValue::Pubkey(owner),
                    DbValue::Int64(position_id as i64),
                ],
                conflict_columns: vec!["chain_id".into(), "position".into()],
                update_columns: vec![],
                update_condition: None,
            });
        }

        for event in ctx.events_of_type("DopplerCPMM", "PositionClosed") {
            let pool = event.extract_pubkey("pool")?;
            let owner = event.extract_pubkey("owner")?;
            let position = event.extract_pubkey("position")?;

            ops.push(DbOperation::Upsert {
                table: "sol_cpmm_positions".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "block_height".into(),
                    "created_at".into(),
                    "closed_at".into(),
                    "position".into(),
                    "pool".into(),
                    "owner".into(),
                    "position_id".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(position),
                    DbValue::Pubkey(pool),
                    DbValue::Pubkey(owner),
                    DbValue::Int64(0i64), // unknown at close time; preserved from create on conflict
                ],
                conflict_columns: vec!["chain_id".into(), "position".into()],
                update_columns: vec!["closed_at".into()],
                update_condition: None,
            });
        }

        Ok(ops)
    }
}

impl EventHandler for CpmmPositionHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new("DopplerCPMM", "PositionCreated"),
            EventTrigger::new("DopplerCPMM", "PositionClosed"),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(CpmmPositionHandler);
}
