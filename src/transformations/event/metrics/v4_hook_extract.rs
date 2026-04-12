//! Shared extraction functions for V4 hook pool metrics handlers.
//!
//! All V4 hook pool types (multicurve, decay_multicurve, scheduled_multicurve, dhook)
//! share the same Swap event format and need getSlot0 call results for sqrtPriceX96/tick.
//! ModifyLiquidity comes in two formats: tuple (multicurve, dhook) and flat (decay, scheduled).

use std::collections::HashMap;

use alloy_primitives::U256;

use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::types::uniswap::v4::PoolKey;

use super::swap_data::{LiquidityInput, SwapInput};

/// Extract swap inputs from V4 hook Swap events, matching each with its getSlot0 call result.
///
/// V4 hook Swap events contain amount0/amount1 but not sqrtPriceX96/tick. The getSlot0
/// on_event call (configured in JSON) provides these values. Each call result is matched
/// to its triggering Swap event via `trigger_log_index`.
pub fn extract_v4_hook_swaps(
    ctx: &TransformationContext,
    event_source: &str,
    call_source: &str,
) -> Result<Vec<SwapInput>, TransformationError> {
    // Build a lookup map once so each swap event can find its matching getSlot0 result
    // in O(1) rather than re-scanning the full call list for every event (O(n²)).
    let slot0_by_event_key: HashMap<(u64, u32), _> = ctx
        .calls_of_type(call_source, "getSlot0")
        .filter_map(|call| {
            call.trigger_log_index()
                .map(|idx| ((call.block_number, idx), call))
        })
        .collect();

    let mut swaps = Vec::new();

    for event in ctx.events_of_type(event_source, "Swap") {
        let pool_id = event.extract_bytes32("poolId")?;

        let Some(slot0) = slot0_by_event_key
            .get(&(event.block_number, event.log_index()))
            .copied()
        else {
            return Err(TransformationError::MissingData(format!(
                "No getSlot0 call for swap at block {} log_index {} (source: {})",
                event.block_number,
                event.log_index(),
                call_source
            )));
        };

        if slot0.is_reverted {
            tracing::warn!(
                "getSlot0 reverted for pool {} at block {} log_index {}, skipping",
                hex::encode(pool_id),
                event.block_number,
                event.log_index()
            );
            continue;
        }

        let sqrt_price_x96 = slot0.extract_uint256("sqrtPriceX96")?;
        let tick = slot0.extract_i32_flexible("tick")?;

        swaps.push(SwapInput {
            pool_id: pool_id.to_vec(),
            transaction_hash: event.evm_tx_hash(),
            block_number: event.block_number,
            block_timestamp: event.block_timestamp,
            log_index: event.log_index(),
            amount0: event.extract_int256("amount0")?,
            amount1: event.extract_int256("amount1")?,
            sqrt_price_x96,
            tick,
            liquidity: U256::ZERO, // not in event; populated by TVL pass later
        });
    }

    Ok(swaps)
}

/// Extract liquidity deltas from tuple-format ModifyLiquidity events (multicurve, dhook).
///
/// Event signature: `ModifyLiquidity((PoolKey) key, (Params) params)`
/// Pool ID is computed from PoolKey fields via keccak256.
pub fn extract_tuple_modify_liquidity(
    ctx: &TransformationContext,
    source: &str,
) -> Result<Vec<LiquidityInput>, TransformationError> {
    let mut deltas = Vec::new();

    for event in ctx.events_of_type(source, "ModifyLiquidity") {
        let pool_key = PoolKey {
            currency0: event.extract_address("key.currency0")?.into(),
            currency1: event.extract_address("key.currency1")?.into(),
            fee: event.extract_u32("key.fee")?,
            tick_spacing: event.extract_i32_flexible("key.tickSpacing")?,
            hooks: event.extract_address("key.hooks")?.into(),
        };
        let pool_id = pool_key.pool_id();

        deltas.push(LiquidityInput {
            pool_id: pool_id.0.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index(),
            tick_lower: event.extract_i32_flexible("params.tickLower")?,
            tick_upper: event.extract_i32_flexible("params.tickUpper")?,
            liquidity_delta: event.extract_int256("params.liquidityDelta")?,
        });
    }

    Ok(deltas)
}

/// Extract liquidity deltas from flat-format ModifyLiquidity events (decay, scheduled).
///
/// Event signature: `ModifyLiquidity(bytes32 indexed id, address indexed sender, int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt)`
/// Pool ID is directly available as the `id` field.
pub fn extract_flat_modify_liquidity(
    ctx: &TransformationContext,
    source: &str,
) -> Result<Vec<LiquidityInput>, TransformationError> {
    let mut deltas = Vec::new();

    for event in ctx.events_of_type(source, "ModifyLiquidity") {
        let pool_id = event.extract_bytes32("id")?;

        deltas.push(LiquidityInput {
            pool_id: pool_id.to_vec(),
            block_number: event.block_number,
            log_index: event.log_index(),
            tick_lower: event.extract_i32_flexible("tickLower")?,
            tick_upper: event.extract_i32_flexible("tickUpper")?,
            liquidity_delta: event.extract_int256("liquidityDelta")?,
        });
    }

    Ok(deltas)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use alloy_primitives::{I256, U256};

    use crate::rpc::UnifiedRpcClient;
    use crate::transformations::context::{
        DecodedCall, DecodedEvent, DecodedValue, TransformationContext,
    };
    use crate::transformations::historical::HistoricalDataReader;
    use crate::types::chain::{ChainAddress, LogPosition, TxId};
    use crate::types::uniswap::v4::PoolKey;

    use super::{
        extract_flat_modify_liquidity, extract_tuple_modify_liquidity, extract_v4_hook_swaps,
    };

    const SOURCE: &str = "UniswapV4MulticurveInitializerHook";

    /// Construct a TransformationContext with the given events and calls.
    fn make_test_ctx(events: Vec<DecodedEvent>, calls: Vec<DecodedCall>) -> TransformationContext {
        let historical = Arc::new(
            HistoricalDataReader::new("test_v4_extract")
                .expect("HistoricalDataReader::new should succeed"),
        );
        let rpc = Arc::new(
            UnifiedRpcClient::from_url("http://localhost:8545")
                .expect("RPC client construction should succeed"),
        );
        TransformationContext::new(
            "test".to_string(),
            8453,
            100,
            200,
            Arc::new(events),
            Arc::new(calls),
            Arc::new(Vec::new()),
            HashMap::new(),
            historical,
            rpc,
            Arc::new(HashMap::new()),
        )
    }

    fn swap_event(log_index: u32, pool_id: [u8; 32], amount0: i128, amount1: i128) -> DecodedEvent {
        swap_event_at_block(100, log_index, pool_id, amount0, amount1)
    }

    fn swap_event_at_block(
        block_number: u64,
        log_index: u32,
        pool_id: [u8; 32],
        amount0: i128,
        amount1: i128,
    ) -> DecodedEvent {
        let mut params = HashMap::new();
        params.insert("poolId".to_string(), DecodedValue::Bytes32(pool_id));
        params.insert("amount0".to_string(), DecodedValue::Int128(amount0));
        params.insert("amount1".to_string(), DecodedValue::Int128(amount1));
        DecodedEvent {
            block_number,
            block_timestamp: 1000,
            transaction_id: TxId::Evm([0u8; 32]),
            position: LogPosition::Evm { log_index },
            contract_address: ChainAddress::Evm([0u8; 20]),
            source_name: SOURCE.to_string(),
            event_name: "Swap".to_string(),
            event_signature: "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)".to_string(),
            params,
        }
    }

    fn slot0_call(
        trigger_log_index: u32,
        sqrt_price_x96: U256,
        tick: i32,
        is_reverted: bool,
    ) -> DecodedCall {
        slot0_call_at_block(100, trigger_log_index, sqrt_price_x96, tick, is_reverted)
    }

    fn slot0_call_at_block(
        block_number: u64,
        trigger_log_index: u32,
        sqrt_price_x96: U256,
        tick: i32,
        is_reverted: bool,
    ) -> DecodedCall {
        let mut result = HashMap::new();
        result.insert(
            "sqrtPriceX96".to_string(),
            DecodedValue::Uint256(sqrt_price_x96),
        );
        result.insert("tick".to_string(), DecodedValue::Int32(tick));
        DecodedCall {
            block_number,
            block_timestamp: 1000,
            contract_address: ChainAddress::Evm([0u8; 20]),
            source_name: SOURCE.to_string(),
            function_name: "getSlot0".to_string(),
            trigger_position: Some(LogPosition::Evm { log_index: trigger_log_index }),
            result,
            is_reverted,
            revert_reason: None,
        }
    }

    fn q96() -> U256 {
        U256::from(1u64) << 96
    }

    // --- extract_v4_hook_swaps ---

    #[test]
    fn test_swap_missing_slot0_returns_err() {
        let pool_id = [1u8; 32];
        let ctx = make_test_ctx(vec![swap_event(0, pool_id, 1000, -1000)], vec![]);
        let result = extract_v4_hook_swaps(&ctx, SOURCE, SOURCE);
        assert!(result.is_err(), "missing getSlot0 should return Err");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("getSlot0"),
            "error should mention getSlot0: {err}"
        );
    }

    #[test]
    fn test_swap_reverted_slot0_is_skipped() {
        let pool_id = [1u8; 32];
        let ctx = make_test_ctx(
            vec![swap_event(0, pool_id, 1000, -1000)],
            vec![slot0_call(0, q96(), 0, true)],
        );
        let result = extract_v4_hook_swaps(&ctx, SOURCE, SOURCE).unwrap();
        assert!(
            result.is_empty(),
            "reverted slot0 should produce no SwapInput"
        );
    }

    #[test]
    fn test_swap_success() {
        let pool_id = [1u8; 32];
        let sqrt_price = q96();
        let ctx = make_test_ctx(
            vec![swap_event(0, pool_id, 500, -500)],
            vec![slot0_call(0, sqrt_price, 42, false)],
        );
        let result = extract_v4_hook_swaps(&ctx, SOURCE, SOURCE).unwrap();
        assert_eq!(result.len(), 1);
        let swap = &result[0];
        assert_eq!(swap.pool_id, pool_id.to_vec());
        assert_eq!(swap.block_number, 100);
        assert_eq!(swap.log_index, 0);
        assert_eq!(swap.amount0, I256::try_from(500i64).unwrap());
        assert_eq!(swap.amount1, I256::try_from(-500i64).unwrap());
        assert_eq!(swap.sqrt_price_x96, sqrt_price);
        assert_eq!(swap.tick, 42);
        assert_eq!(swap.liquidity, U256::ZERO);
    }

    #[test]
    fn test_swap_multiple_events_matched_by_log_index() {
        let pool_a = [1u8; 32];
        let pool_b = [2u8; 32];
        let ctx = make_test_ctx(
            vec![
                swap_event(0, pool_a, 100, -100),
                swap_event(1, pool_b, 200, -200),
            ],
            vec![
                slot0_call(0, q96(), 10, false),
                slot0_call(1, q96(), 20, false),
            ],
        );
        let result = extract_v4_hook_swaps(&ctx, SOURCE, SOURCE).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].pool_id, pool_a.to_vec());
        assert_eq!(result[0].tick, 10);
        assert_eq!(result[1].pool_id, pool_b.to_vec());
        assert_eq!(result[1].tick, 20);
    }

    #[test]
    fn test_swap_duplicate_log_index_across_blocks_matches_on_block_and_log_index() {
        let pool_a = [1u8; 32];
        let pool_b = [2u8; 32];
        let ctx = make_test_ctx(
            vec![
                swap_event_at_block(100, 0, pool_a, 100, -100),
                swap_event_at_block(101, 0, pool_b, 200, -200),
            ],
            vec![
                slot0_call_at_block(100, 0, q96(), 10, false),
                slot0_call_at_block(101, 0, q96(), 20, false),
            ],
        );

        let result = extract_v4_hook_swaps(&ctx, SOURCE, SOURCE).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].pool_id, pool_a.to_vec());
        assert_eq!(result[0].block_number, 100);
        assert_eq!(result[0].tick, 10);
        assert_eq!(result[1].pool_id, pool_b.to_vec());
        assert_eq!(result[1].block_number, 101);
        assert_eq!(result[1].tick, 20);
    }

    // --- extract_tuple_modify_liquidity ---

    #[test]
    fn test_tuple_modify_liquidity() {
        let currency0 = [0u8; 20];
        let currency1 = [1u8; 20];
        let hooks = [2u8; 20];
        let fee = 3000u32;
        let tick_spacing = 60i32;

        let mut params = HashMap::new();
        params.insert(
            "key.currency0".to_string(),
            DecodedValue::Address(currency0),
        );
        params.insert(
            "key.currency1".to_string(),
            DecodedValue::Address(currency1),
        );
        params.insert("key.fee".to_string(), DecodedValue::Uint32(fee));
        params.insert(
            "key.tickSpacing".to_string(),
            DecodedValue::Int32(tick_spacing),
        );
        params.insert("key.hooks".to_string(), DecodedValue::Address(hooks));
        params.insert("params.tickLower".to_string(), DecodedValue::Int32(-100));
        params.insert("params.tickUpper".to_string(), DecodedValue::Int32(100));
        params.insert(
            "params.liquidityDelta".to_string(),
            DecodedValue::Int256(I256::try_from(500_000i64).unwrap()),
        );

        let event = DecodedEvent {
            block_number: 100,
            block_timestamp: 1000,
            transaction_id: TxId::Evm([0u8; 32]),
            position: LogPosition::Evm { log_index: 5 },
            contract_address: ChainAddress::Evm([0u8; 20]),
            source_name: SOURCE.to_string(),
            event_name: "ModifyLiquidity".to_string(),
            event_signature: "ModifyLiquidity((address,address,uint24,int24,address),(int24,int24,int256,bytes32))".to_string(),
            params,
        };

        let ctx = make_test_ctx(vec![event], vec![]);
        let result = extract_tuple_modify_liquidity(&ctx, SOURCE).unwrap();
        assert_eq!(result.len(), 1);

        let delta = &result[0];
        let expected_pool_id = PoolKey {
            currency0: currency0.into(),
            currency1: currency1.into(),
            fee,
            tick_spacing,
            hooks: hooks.into(),
        }
        .pool_id()
        .0
        .to_vec();
        assert_eq!(delta.pool_id, expected_pool_id);
        assert_eq!(delta.block_number, 100);
        assert_eq!(delta.log_index, 5);
        assert_eq!(delta.tick_lower, -100);
        assert_eq!(delta.tick_upper, 100);
        assert_eq!(delta.liquidity_delta, I256::try_from(500_000i64).unwrap());
    }

    // --- extract_flat_modify_liquidity ---

    #[test]
    fn test_flat_modify_liquidity() {
        let pool_id = [3u8; 32];

        let mut params = HashMap::new();
        params.insert("id".to_string(), DecodedValue::Bytes32(pool_id));
        params.insert("tickLower".to_string(), DecodedValue::Int32(-200));
        params.insert("tickUpper".to_string(), DecodedValue::Int32(200));
        params.insert(
            "liquidityDelta".to_string(),
            DecodedValue::Int256(I256::try_from(-250_000i64).unwrap()),
        );

        let event = DecodedEvent {
            block_number: 101,
            block_timestamp: 1010,
            transaction_id: TxId::Evm([0u8; 32]),
            position: LogPosition::Evm { log_index: 7 },
            contract_address: ChainAddress::Evm([0u8; 20]),
            source_name: "DecayMulticurveHook".to_string(),
            event_name: "ModifyLiquidity".to_string(),
            event_signature: "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)"
                .to_string(),
            params,
        };

        let ctx = make_test_ctx(vec![event], vec![]);
        let result = extract_flat_modify_liquidity(&ctx, "DecayMulticurveHook").unwrap();
        assert_eq!(result.len(), 1);

        let delta = &result[0];
        assert_eq!(delta.pool_id, pool_id.to_vec());
        assert_eq!(delta.block_number, 101);
        assert_eq!(delta.log_index, 7);
        assert_eq!(delta.tick_lower, -200);
        assert_eq!(delta.tick_upper, 200);
        assert_eq!(delta.liquidity_delta, I256::try_from(-250_000i64).unwrap());
    }
}
