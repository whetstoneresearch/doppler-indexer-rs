use async_trait::async_trait;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::db::transfers::insert_transfer;
use crate::transformations::util::db::users::upsert_user;
use crate::transformations::util::sanitize::is_precompile_address;

const CREATOR_COIN_SOURCE: &str = "ZoraCreatorCoinV4";
const CONTENT_COIN_SOURCE: &str = "ZoraCoinV4";

pub struct ZoraTransferHandler;

#[async_trait]
impl TransformationHandler for ZoraTransferHandler {
    fn name(&self) -> &'static str {
        "ZoraTransferHandler"
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

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["transfers"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for source in [CREATOR_COIN_SOURCE, CONTENT_COIN_SOURCE] {
            for event in ctx.events_of_type(source, "CoinTransfer") {
                let from_address = event.extract_address("sender")?;
                if is_precompile_address(from_address.into()) {
                    continue;
                }

                let to_address = event.extract_address("recipient")?;
                let value = event.extract_uint256("amount")?;

                ops.push(upsert_user(&from_address, &event.block_timestamp, ctx));
                ops.push(upsert_user(&to_address, &event.block_timestamp, ctx));
                ops.push(insert_transfer(
                    event.block_number,
                    event.log_index,
                    event.block_timestamp,
                    &event.contract_address,
                    &from_address,
                    &to_address,
                    &value,
                    ctx,
                ));
            }
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("ZoraTransferHandler initialized");
        Ok(())
    }
}

impl EventHandler for ZoraTransferHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new(
                CREATOR_COIN_SOURCE,
                "CoinTransfer(address,address,uint256,uint256,uint256)",
            ),
            EventTrigger::new(
                CONTENT_COIN_SOURCE,
                "CoinTransfer(address,address,uint256,uint256,uint256)",
            ),
        ]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(ZoraTransferHandler);
}
