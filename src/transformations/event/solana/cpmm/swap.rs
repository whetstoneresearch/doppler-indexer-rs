use async_trait::async_trait;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct CpmmSwapHandler;

#[async_trait]
impl TransformationHandler for CpmmSwapHandler {
    fn name(&self) -> &'static str {
        "CpmmSwapHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Solana
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/swaps.sql",
            "migrations/tables/swaps_solana_nullable.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["swaps"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DopplerCPMM", "Swap") {
            let pool = event.extract_pubkey("pool")?;
            let direction = event.extract_u8("direction")?;
            let amount_in = event.extract_u64("amount_in")?;
            let amount_out = event.extract_u64("amount_out")?;
            let fee_total = event.extract_u64("fee_total")?;
            let fee_dist = event.extract_u64("fee_dist")?;
            let tx_sig = event
                .transaction_id
                .as_solana()
                .expect("Solana event has Solana tx id");
            let log_position = event.position.packed_ordinal_i64();

            ops.push(DbOperation::Upsert {
                table: "swaps".to_string(),
                columns: vec![
                    "chain_id".into(),
                    "tx_id".into(),
                    "block_height".into(),
                    "timestamp".into(),
                    "pool".into(),
                    "asset".into(),
                    "amountin".into(),
                    "amountout".into(),
                    "is_buy".into(),
                    "current_tick".into(),
                    "graduation_tick".into(),
                    "fee0_delta".into(),
                    "fee1_delta".into(),
                    "tokens_sold_delta".into(),
                    "graduation_balance_delta".into(),
                    "max_threshold_delta".into(),
                    "market_cap_usd_delta".into(),
                    "liquidity_usd_delta".into(),
                    "value_usd".into(),
                    "log_position".into(),
                ],
                values: vec![
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Bytes(tx_sig.to_vec()),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Pubkey(pool),
                    DbValue::Null,
                    DbValue::Uint64(amount_in),
                    DbValue::Uint64(amount_out),
                    DbValue::Bool(direction == 0),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Uint64(fee_total),
                    DbValue::Uint64(fee_dist),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Int64(log_position),
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

impl EventHandler for CpmmSwapHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new("DopplerCPMM", "Swap")]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(CpmmSwapHandler);
}
