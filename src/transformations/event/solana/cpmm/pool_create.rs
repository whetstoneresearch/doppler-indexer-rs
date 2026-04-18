use async_trait::async_trait;
use serde_json::json;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct CpmmPoolCreateHandler;

#[async_trait]
impl TransformationHandler for CpmmPoolCreateHandler {
    fn name(&self) -> &'static str {
        "CpmmPoolCreateHandler"
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

        for event in ctx.events_of_type("DopplerCPMM", "PoolInitialized") {
            let pool = event.extract_pubkey("pool")?;
            let token0_mint = event.extract_pubkey("token0_mint")?;
            let token1_mint = event.extract_pubkey("token1_mint")?;
            let vault0 = event.extract_pubkey("vault0")?;
            let vault1 = event.extract_pubkey("vault1")?;

            let pool_key = json!({
                "vault0": bs58::encode(&vault0).into_string(),
                "vault1": bs58::encode(&vault1).into_string(),
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
                    DbValue::Pubkey(pool),
                    DbValue::Pubkey(token0_mint),
                    DbValue::Pubkey(token1_mint),
                    DbValue::Bool(true),
                    DbValue::VarChar("cpmm".into()),
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
                    DbValue::Null,
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

impl EventHandler for CpmmPoolCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new("DopplerCPMM", "PoolInitialized")]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(CpmmPoolCreateHandler);
}
