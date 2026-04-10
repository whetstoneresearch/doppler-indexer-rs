//! Range completion and regular-call processing for live mode.

use tokio::sync::mpsc::Sender;

use crate::decoding::eth_calls::{
    build_result_map, decode_value, CallDecodeConfig, EthCallDecodingError,
};
use crate::live::{LiveDecodedCall, LiveStorage, TransformRetryRequest};
use crate::transformations::{
    DecodedCall as TransformDecodedCall, DecodedCallsMessage, RangeCompleteKind,
    RangeCompleteMessage,
};
use crate::types::chain::ChainAddress;

/// Handle a live-mode `EthCallsReady` message for regular calls: decode results,
/// persist to bincode, and optionally forward to the transformation engine.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_regular_calls_live(
    live_storage: &LiveStorage,
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    function_name: &str,
    results: &[crate::decoding::EthCallResult],
    config: &CallDecodeConfig,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    retry_transform_after_decode: bool,
) -> Result<(), EthCallDecodingError> {
    let mut decoded_calls: Vec<LiveDecodedCall> = Vec::with_capacity(results.len());
    let mut transform_calls: Vec<TransformDecodedCall> = Vec::with_capacity(results.len());

    for result in results {
        match decode_value(&result.value, &config.output_type) {
            Ok(decoded) => {
                transform_calls.push(TransformDecodedCall {
                    block_number: result.block_number,
                    block_timestamp: result.block_timestamp,
                    contract_address: ChainAddress::Evm(result.contract_address),
                    source_name: contract_name.to_string(),
                    function_name: function_name.to_string(),
                    trigger_position: None,
                    result: build_result_map(&decoded, &config.output_type, function_name),
                    is_reverted: false,
                    revert_reason: None,
                });

                decoded_calls.push(LiveDecodedCall {
                    block_number: result.block_number,
                    block_timestamp: result.block_timestamp,
                    contract_address: result.contract_address,
                    decoded_value: decoded.clone(),
                });
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to decode eth_call {}/{} at block {}: address={}, raw_bytes=0x{}, error={}",
                    contract_name, function_name, result.block_number,
                    alloy::primitives::Address::from(result.contract_address),
                    hex::encode(&result.value),
                    e
                );
            }
        }
    }

    if !decoded_calls.is_empty() {
        if let Err(e) = live_storage.write_decoded_calls(
            range_start,
            contract_name,
            function_name,
            &decoded_calls,
        ) {
            tracing::warn!(
                "Failed to write decoded eth_calls for {}/{} at block {}: {}",
                contract_name,
                function_name,
                range_start,
                e
            );
        }
    }

    // Send to transformation engine
    if !retry_transform_after_decode {
        if let Some(tx) = transform_tx {
            if !transform_calls.is_empty() {
                let msg = DecodedCallsMessage {
                    range_start,
                    range_end,
                    source_name: contract_name.to_string(),
                    function_name: function_name.to_string(),
                    calls: transform_calls,
                };
                let _ = tx.send(msg).await;
            }
        }
    }

    Ok(())
}

/// Handle an `EthCallsBlockComplete` message: update block status, send range-complete
/// signal, and optionally queue a deferred transform retry.
pub(super) async fn handle_block_complete(
    live_storage: &LiveStorage,
    range_start: u64,
    range_end: u64,
    retry_transform_after_decode: bool,
    complete_tx: Option<&Sender<RangeCompleteMessage>>,
    transform_retry_tx: Option<&Sender<TransformRetryRequest>>,
) {
    if range_end.saturating_sub(range_start) == 1 {
        match live_storage.update_status_atomic(range_start, |status| {
            status.eth_calls_decoded = true;
        }) {
            Ok(()) => {
                tracing::debug!("Block {} eth_calls decoded", range_start);
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to update block status after eth_call block completion: {}",
                    e
                );
            }
        }
    } else {
        tracing::debug!(
            "Eth_call range {}-{} decoded",
            range_start,
            range_end.saturating_sub(1)
        );
    }

    if !retry_transform_after_decode {
        if let Some(tx) = complete_tx {
            let msg = RangeCompleteMessage {
                range_start,
                range_end,
                kind: RangeCompleteKind::EthCalls,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!("Failed to send eth_call range complete: {}", e);
            }
        }
    }

    if retry_transform_after_decode {
        if let Some(tx) = transform_retry_tx {
            if let Err(e) = tx
                .send(TransformRetryRequest {
                    block_number: range_start,
                    missing_handlers: None,
                })
                .await
            {
                tracing::warn!(
                    "Failed to queue deferred transform retry for block {}: {}",
                    range_start,
                    e
                );
            }
        } else {
            tracing::warn!(
                "Block {} requested deferred transform retry but no retry channel is configured",
                range_start
            );
        }
    }
}
