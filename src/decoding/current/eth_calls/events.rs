//! Event-triggered call message handling for live mode.

use tokio::sync::mpsc::Sender;

use crate::decoding::eth_calls::EthCallDecodingError;
use crate::decoding::eth_calls::{build_result_map, decode_value, EventCallDecodeConfig};
use crate::live::{LiveDecodedEventCall, LiveStorage};
use crate::transformations::{DecodedCall as TransformDecodedCall, DecodedCallsMessage};

/// Handle a live-mode `EventCallsReady` message: decode results, persist to bincode,
/// and optionally forward to the transformation engine.
pub(super) async fn handle_event_calls_live(
    live_storage: &LiveStorage,
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    function_name: &str,
    results: &[crate::decoding::EventCallResult],
    config: &EventCallDecodeConfig,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    retry_transform_after_decode: bool,
) -> Result<(), EthCallDecodingError> {
    let mut decoded_event_calls: Vec<LiveDecodedEventCall> = Vec::with_capacity(results.len());
    let mut transform_calls: Vec<TransformDecodedCall> = Vec::with_capacity(results.len());

    for result in results {
        match decode_value(&result.value, &config.output_type) {
            Ok(decoded) => {
                transform_calls.push(TransformDecodedCall {
                    block_number: result.block_number,
                    block_timestamp: result.block_timestamp,
                    contract_address: result.target_address,
                    source_name: contract_name.to_string(),
                    function_name: function_name.to_string(),
                    trigger_log_index: Some(result.log_index),
                    result: build_result_map(&decoded, &config.output_type, function_name),
                });

                decoded_event_calls.push(LiveDecodedEventCall {
                    block_number: result.block_number,
                    block_timestamp: result.block_timestamp,
                    log_index: result.log_index,
                    target_address: result.target_address,
                    decoded_value: decoded.clone(),
                });
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to decode event_call {}/{} at block {}: address={}, log_index={}, raw_bytes=0x{}, error={}",
                    contract_name, function_name, result.block_number,
                    alloy::primitives::Address::from(result.target_address),
                    result.log_index,
                    hex::encode(&result.value),
                    e
                );
            }
        }
    }

    if !decoded_event_calls.is_empty() {
        if let Err(e) = live_storage.write_decoded_event_calls(
            range_start,
            contract_name,
            function_name,
            &decoded_event_calls,
        ) {
            tracing::warn!(
                "Failed to write decoded event_calls for {}/{} at block {}: {}",
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
