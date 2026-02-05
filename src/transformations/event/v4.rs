//! Uniswap V4 event handlers.
//!
//! Example handlers for V4 pool events like Swap, Initialize, etc.

use async_trait::async_trait;

use crate::db::{DbOperation, DbPool, DbValue};
use crate::transformations::context::TransformationContext;
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};

/// Handler for V4 Swap events.
///
/// This is an example handler that demonstrates how to:
/// - Extract decoded event parameters
/// - Access chain and block context
/// - Build database operations
pub struct V4SwapHandler;

#[async_trait]
impl TransformationHandler for V4SwapHandler {
    fn name(&self) -> &'static str {
        "V4SwapHandler"
    }

    async fn handle(
        &self,
        ctx: &TransformationContext<'_>,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        // Process all Swap events for UniswapV4PoolManager
        for event in ctx.events_of_type("UniswapV4PoolManager", "Swap") {
            // Extract decoded parameters
            // The parameter names match the event signature:
            // Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1,
            //      uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)

            let pool_id = event.get("id")?.as_bytes32().ok_or_else(|| {
                TransformationError::TypeConversion("id is not bytes32".to_string())
            })?;

            let sender = event.get("sender")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("sender is not address".to_string())
            })?;

            let amount0 = event
                .get("amount0")?
                .to_numeric_string()
                .ok_or_else(|| TransformationError::TypeConversion("amount0 not numeric".to_string()))?;

            let amount1 = event
                .get("amount1")?
                .to_numeric_string()
                .ok_or_else(|| TransformationError::TypeConversion("amount1 not numeric".to_string()))?;

            let sqrt_price_x96 = event
                .get("sqrtPriceX96")?
                .to_numeric_string()
                .ok_or_else(|| {
                    TransformationError::TypeConversion("sqrtPriceX96 not numeric".to_string())
                })?;

            let liquidity = event
                .get("liquidity")?
                .to_numeric_string()
                .ok_or_else(|| TransformationError::TypeConversion("liquidity not numeric".to_string()))?;

            let tick = event.get("tick")?.as_i32().ok_or_else(|| {
                TransformationError::TypeConversion("tick is not i32".to_string())
            })?;

            let fee = event.get("fee")?.as_u64().ok_or_else(|| {
                TransformationError::TypeConversion("fee is not u64".to_string())
            })?;

            // Build upsert operation
            ops.push(DbOperation::Upsert {
                table: "v4_swaps".to_string(),
                columns: vec![
                    "pool_id".to_string(),
                    "block_number".to_string(),
                    "log_index".to_string(),
                    "tx_hash".to_string(),
                    "sender".to_string(),
                    "amount0".to_string(),
                    "amount1".to_string(),
                    "sqrt_price_x96".to_string(),
                    "liquidity".to_string(),
                    "tick".to_string(),
                    "fee".to_string(),
                    "timestamp".to_string(),
                    "chain_id".to_string(),
                ],
                values: vec![
                    DbValue::Bytes32(pool_id),
                    DbValue::Uint64(event.block_number),
                    DbValue::Uint64(event.log_index as u64),
                    DbValue::Bytes32(event.transaction_hash),
                    DbValue::Address(sender),
                    DbValue::Numeric(amount0),
                    DbValue::Numeric(amount1),
                    DbValue::Numeric(sqrt_price_x96),
                    DbValue::Numeric(liquidity),
                    DbValue::Int32(tick),
                    DbValue::Uint64(fee),
                    DbValue::Timestamp(event.block_timestamp as i64),
                    DbValue::Uint64(ctx.chain_id),
                ],
                conflict_columns: vec![
                    "pool_id".to_string(),
                    "block_number".to_string(),
                    "log_index".to_string(),
                ],
                update_columns: vec![
                    "amount0".to_string(),
                    "amount1".to_string(),
                    "sqrt_price_x96".to_string(),
                    "liquidity".to_string(),
                    "tick".to_string(),
                    "fee".to_string(),
                ],
            });
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        // Could create indexes or perform other setup here
        Ok(())
    }
}

impl EventHandler for V4SwapHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "UniswapV4PoolManager",
            "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)",
        )]
    }
}

/// Register all V4 handlers with the registry.
pub fn register_handlers(registry: &mut TransformationRegistry) {
    // Uncomment to enable the V4SwapHandler:
    // registry.register_event_handler(V4SwapHandler);

    // Add more V4 handlers here:
    // registry.register_event_handler(V4InitializeHandler);
    // registry.register_event_handler(V4ModifyLiquidityHandler);

    let _ = registry; // Suppress unused warning when handlers are commented out
}
