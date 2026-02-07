//! Uniswap V3 event handlers.
//!
//! Handlers for V3 pool creation and other V3 events.

use async_trait::async_trait;

use crate::db::{DbOperation, DbPool, DbValue};
use crate::transformations::context::TransformationContext;
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

/// Handler for V3 Create events from UniswapV3Initializer.
///
/// Tracks pool creation events with:
/// - poolOrHook: The created pool address
/// - asset: The asset token address
/// - numeraire: The numeraire (quote) token address
pub struct V3CreateHandler;

#[async_trait]
impl TransformationHandler for V3CreateHandler {
    fn name(&self) -> &'static str {
        "V3CreateHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/pools.sql"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext<'_>,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        // Process all Create events for UniswapV3Initializer
        for event in ctx.events_of_type("UniswapV3Initializer", "Create") {
            // Extract decoded parameters from:
            // Create(address indexed poolOrHook, address indexed asset, address indexed numeraire)

            let pool_address = event.get("poolOrHook")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("poolOrHook is not address".to_string())
            })?;

            let asset = event.get("asset")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("asset is not address".to_string())
            })?;

            let numeraire = event.get("numeraire")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("numeraire is not address".to_string())
            })?;

            // Build upsert operation
            ops.push(DbOperation::Upsert {
                table: "pools".to_string(),
                columns: vec![
                    "pool_address".to_string(),
                    "asset".to_string(),
                    "numeraire".to_string(),
                    "block_number".to_string(),
                    "log_index".to_string(),
                    "tx_hash".to_string(),
                    "timestamp".to_string(),
                    "chain_id".to_string(),
                ],
                values: vec![
                    DbValue::Address(pool_address),
                    DbValue::Address(asset),
                    DbValue::Address(numeraire),
                    DbValue::Int64(event.block_number as i64),
                    DbValue::Int32(event.log_index as i32),
                    DbValue::Bytes32(event.transaction_hash),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Int64(ctx.chain_id as i64),
                ],
                conflict_columns: vec!["pool_address".to_string(), "chain_id".to_string()],
                update_columns: vec![
                    "asset".to_string(),
                    "numeraire".to_string(),
                    "block_number".to_string(),
                    "log_index".to_string(),
                    "tx_hash".to_string(),
                    "timestamp".to_string(),
                ],
            });

            tracing::debug!(
                "Processing V3 Create: pool=0x{} asset=0x{} numeraire=0x{} block={}",
                hex::encode(pool_address),
                hex::encode(asset),
                hex::encode(numeraire),
                event.block_number
            );
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("V3CreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for V3CreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "UniswapV3Initializer",
            "Create(address,address,address)",
        )]
    }
}

/// Register all V3 handlers with the registry.
pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(V3CreateHandler);
}
