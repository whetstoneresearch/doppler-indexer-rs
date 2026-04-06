//! Factory / once-call message handling for live mode.

use std::collections::HashSet;
use std::path::Path;

use tokio::sync::mpsc::Sender;

use crate::decoding::eth_calls::column_index::{
    load_or_build_decoded_column_index, read_once_calls_from_parquet,
    read_raw_parquet_function_names,
};
use crate::decoding::eth_calls::{
    build_result_map, decode_value, process_once_calls, CallDecodeConfig, EthCallDecodingError,
};
use crate::live::{LiveDecodedOnceCall, LiveStorage};
use crate::transformations::{DecodedCall as TransformDecodedCall, DecodedCallsMessage};
use crate::types::decoded::DecodedValue;

/// Handle a live-mode `OnceCallsReady` message: decode results, persist to bincode,
/// and optionally forward to the transformation engine.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_once_calls_live(
    live_storage: &LiveStorage,
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    results: &[crate::decoding::OnceCallResult],
    configs: &[&CallDecodeConfig],
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    retry_transform_after_decode: bool,
    chain_name: &str,
) -> Result<(), EthCallDecodingError> {
    let decode_start = std::time::Instant::now();
    let mut decoded_once_calls: Vec<LiveDecodedOnceCall> = Vec::with_capacity(results.len());
    // Group transform calls by function name
    let mut transform_calls_by_fn: std::collections::HashMap<String, Vec<TransformDecodedCall>> =
        std::collections::HashMap::new();
    let mut decode_successes = 0u64;
    let mut decode_failures = 0u64;

    for result in results {
        let mut decoded_values: Vec<(String, DecodedValue)> = Vec::new();

        for config in configs {
            if let Some(raw_value) = result.results.get(&config.function_name) {
                match decode_value(raw_value, &config.output_type) {
                    Ok(decoded) => {
                        decode_successes += 1;
                        let transform_call = TransformDecodedCall {
                            block_number: result.block_number,
                            block_timestamp: result.block_timestamp,
                            contract_address: result.contract_address,
                            source_name: contract_name.to_string(),
                            function_name: config.function_name.clone(),
                            trigger_log_index: None,
                            result: build_result_map(
                                &decoded,
                                &config.output_type,
                                &config.function_name,
                            ),
                            is_reverted: false,
                            revert_reason: None,
                        };
                        transform_calls_by_fn
                            .entry(config.function_name.clone())
                            .or_default()
                            .push(transform_call);

                        decoded_values.push((config.function_name.clone(), decoded.clone()));
                    }
                    Err(e) => {
                        decode_failures += 1;
                        tracing::warn!(
                            "Failed to decode once_call {}/{} at block {}: address={}, raw_bytes=0x{}, error={}",
                            contract_name, config.function_name, result.block_number,
                            alloy::primitives::Address::from(result.contract_address),
                            hex::encode(raw_value),
                            e
                        );
                    }
                }
            }
        }

        if !decoded_values.is_empty() {
            decoded_once_calls.push(LiveDecodedOnceCall {
                block_number: result.block_number,
                block_timestamp: result.block_timestamp,
                contract_address: result.contract_address,
                decoded_values,
            });
        }
    }

    // Record eth_call decode metrics for live mode
    crate::metrics::decoding::record_eth_call_decode_metrics(
        chain_name,
        "live",
        decode_successes,
        decode_failures,
        decode_start.elapsed(),
    );

    if !decoded_once_calls.is_empty() {
        if let Err(e) =
            live_storage.write_decoded_once_calls(range_start, contract_name, &decoded_once_calls)
        {
            tracing::warn!(
                "Failed to write decoded once_calls for {} at block {}: {}",
                contract_name,
                range_start,
                e
            );
        }
    }

    // Send to transformation engine (one message per function)
    if !retry_transform_after_decode {
        for (function_name, calls) in transform_calls_by_fn {
            if let Some(tx) = transform_tx {
                if calls.is_empty() {
                    continue;
                }
                let msg = DecodedCallsMessage {
                    range_start,
                    range_end,
                    source_name: contract_name.to_string(),
                    function_name,
                    calls,
                };
                let _ = tx.send(msg).await;
            }
        }
    }

    Ok(())
}

/// Handle an `OnceFileBackfilled` message: decode missing columns from a backfilled once-call file.
pub(super) async fn handle_once_file_backfilled(
    raw_calls_dir: &Path,
    output_base: &Path,
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    once_configs: &[CallDecodeConfig],
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    chain_name: &str,
) -> Result<(), EthCallDecodingError> {
    let configs: Vec<&CallDecodeConfig> = once_configs
        .iter()
        .filter(|c| c.contract_name == contract_name)
        .collect();

    if configs.is_empty() {
        return Ok(());
    }

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let raw_path = raw_calls_dir
        .join(contract_name)
        .join("once")
        .join(&file_name);

    if !raw_path.exists() {
        return Ok(());
    }

    // Read raw columns from parquet
    let raw_cols: HashSet<String> = read_raw_parquet_function_names(&raw_path);

    // Read decoded columns from index or parquet
    let decoded_dir = output_base.join(contract_name).join("once");
    let raw_once_dir = raw_calls_dir.join(contract_name).join("once");
    let decoded_index = load_or_build_decoded_column_index(&decoded_dir, &raw_once_dir);
    let decoded_cols: HashSet<String> = decoded_index
        .get(&file_name)
        .map(|cols| cols.iter().cloned().collect())
        .unwrap_or_default();

    // Find configs that need decoding
    let missing_configs: Vec<CallDecodeConfig> = configs
        .iter()
        .filter(|c| raw_cols.contains(&c.function_name) && !decoded_cols.contains(&c.function_name))
        .cloned()
        .cloned()
        .collect();

    if !missing_configs.is_empty() {
        tracing::info!(
            "OnceFileBackfilled: decoding {} new columns for {}/{}",
            missing_configs.len(),
            contract_name,
            file_name
        );

        // Read raw parquet and decode the missing columns
        let configs_ref: Vec<&CallDecodeConfig> = missing_configs.iter().collect();
        let results = read_once_calls_from_parquet(&raw_path, &configs_ref)?;
        process_once_calls(
            &results,
            range_start,
            range_end,
            contract_name,
            &configs_ref,
            output_base,
            transform_tx,
            false,
            chain_name,
            "historical",
        )
        .await?;
    }

    Ok(())
}
