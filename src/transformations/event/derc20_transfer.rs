use async_trait::async_trait;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::TransformationContext;
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

use crate::transformations::util::db::users::upsert_user;
use crate::transformations::util::db::transfers::insert_transfer;

pub struct DERC20TransferHandler;

#[async_trait]
impl TransformationHandler for DERC20TransferHandler {
    fn name(&self) -> &'static str {
        "DERC20TransferHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/transfers.sql",
            "migrations/tables/users.sql",
        ]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError>{
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DERC20", "Transfer") {
            let from_address = event.get("from")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("from is not an address".to_string())
            })?;

            let to_address = event.get("to")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("to is not an address".to_string())
            })?;
            
            let value = event.get("value")?.as_uint256().ok_or_else(|| {
                TransformationError::TypeConversion("value is not a uint256".to_string())
            })?;

            ops.push(upsert_user(&from_address, &event.block_timestamp, &ctx));
            ops.push(upsert_user(&to_address, &event.block_timestamp, &ctx));
            ops.push(insert_transfer(event.block_number, event.block_timestamp, &event.contract_address, &from_address, &to_address, &value, ctx))
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("DERC20TransferHandler initialized");
        Ok(())
    }
}

impl EventHandler for DERC20TransferHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "DERC20",
            "Transfer(address,address,uint256)"
        )]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(DERC20TransferHandler);
}