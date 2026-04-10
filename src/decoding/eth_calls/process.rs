//! Process functions: process_regular_calls, process_once_calls, process_event_calls.

use std::collections::HashMap;
use std::path::Path;

use tokio::sync::mpsc::Sender;

use super::column_index::{read_decoded_column_index, write_decoded_column_index};
use super::decode::decode_value;
use super::parquet_io::{
    merge_decoded_once_calls, write_decoded_calls_to_parquet, write_decoded_event_calls_to_parquet,
    write_decoded_once_calls_to_parquet,
};
use super::transform::{
    build_result_map_for_merge, convert_event_call_to_transform_call, convert_to_transform_call,
};
use super::types::{
    CallDecodeConfig, DecodedCallRecord, DecodedEventCallRecord, DecodedOnceRecord,
    EthCallDecodingError, EventCallDecodeConfig, OnceCallsResult,
};
use crate::decoding::types::{EthCallResult, EventCallResult, OnceCallResult};
use crate::transformations::{DecodedCall as TransformDecodedCall, DecodedCallsMessage};

pub async fn process_regular_calls(
    results: &[EthCallResult],
    range_start: u64,
    range_end: u64,
    config: &CallDecodeConfig,
    output_base: &Path,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
) -> Result<(), EthCallDecodingError> {
    // Skip if entire range is before contract's start_block
    if let Some(sb) = config.start_block {
        if range_end <= sb {
            return Ok(());
        }
    }

    let mut decoded_records = Vec::new();
    let mut decode_failures = 0u64;
    let mut non_empty_count = 0u64;

    for result in results {
        // Skip if block is before contract's start_block
        if let Some(sb) = config.start_block {
            if result.block_number < sb {
                continue;
            }
        }
        if result.value.is_empty() {
            continue;
        }
        non_empty_count += 1;

        match decode_value(&result.value, &config.output_type) {
            Ok(decoded) => {
                decoded_records.push(DecodedCallRecord {
                    block_number: result.block_number,
                    block_timestamp: result.block_timestamp,
                    contract_address: result.contract_address,
                    decoded_value: decoded,
                });
            }
            Err(e) => {
                decode_failures += 1;
                tracing::debug!(
                    "Failed to decode {}.{} at block {}: {}",
                    config.contract_name,
                    config.function_name,
                    result.block_number,
                    e
                );
            }
        }
    }

    if decode_failures > 0 && decode_failures == non_empty_count {
        tracing::warn!(
            "All {} decode attempts failed for {}.{} (output_type: {:?}) in blocks {}-{} — check output_type in config",
            decode_failures,
            config.contract_name,
            config.function_name,
            config.output_type,
            range_start,
            range_end - 1
        );
    } else if decode_failures > 0 {
        tracing::warn!(
            "{}/{} decode failures for {}.{} in blocks {}-{}",
            decode_failures,
            non_empty_count,
            config.contract_name,
            config.function_name,
            range_start,
            range_end - 1
        );
    }

    // Send to transformation channel FIRST (before parquet I/O)
    // to unblock the transformation engine while parquet writes happen.
    if let Some(tx) = transform_tx {
        let transform_calls: Vec<TransformDecodedCall> = decoded_records
            .iter()
            .map(|r| {
                convert_to_transform_call(
                    r,
                    &config.contract_name,
                    &config.function_name,
                    &config.output_type,
                )
            })
            .collect();

        if !transform_calls.is_empty() {
            let msg = DecodedCallsMessage {
                range_start,
                range_end,
                source_name: config.contract_name.clone(),
                function_name: config.function_name.clone(),
                calls: transform_calls,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!(
                    "Failed to send decoded calls to transformation channel: {}",
                    e
                );
            }
        }
    }

    // Write decoded data
    let output_dir = output_base
        .join(&config.contract_name)
        .join(&config.function_name);
    std::fs::create_dir_all(&output_dir)?;

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let output_path = output_dir.join(&file_name);

    write_decoded_calls_to_parquet(&decoded_records, &config.output_type, &output_path)?;

    if decoded_records.is_empty() {
        tracing::debug!(
            "Wrote 0 decoded {}.{} results to {}",
            config.contract_name,
            config.function_name,
            output_path.display()
        );
    } else {
        tracing::info!(
            "Wrote {} decoded {}.{} results to {}",
            decoded_records.len(),
            config.contract_name,
            config.function_name,
            output_path.display()
        );
    }

    Ok(())
}

/// Process "once" call results.
/// Returns `OnceCallsResult` with column index info for batch updating (avoids race conditions).
/// When `return_index_info` is true, skips writing the column index (caller will batch update).
#[allow(clippy::too_many_arguments)]
pub async fn process_once_calls(
    results: &[OnceCallResult],
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    configs: &[&CallDecodeConfig],
    output_base: &Path,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    return_index_info: bool,
    #[allow(unused_variables)] force_overwrite: bool,
) -> Result<Option<OnceCallsResult>, EthCallDecodingError> {
    // Get start_block from first config (all configs for a contract share the same start_block)
    let start_block = configs.first().and_then(|c| c.start_block);

    // Skip if entire range is before contract's start_block
    if let Some(sb) = start_block {
        if range_end <= sb {
            return Ok(None);
        }
    }

    let mut decoded_records = Vec::new();
    // Track decode failures per function: fn_name -> (successes, failures)
    let mut decode_stats: HashMap<String, (u64, u64)> = HashMap::new();

    for result in results {
        // Skip if block is before contract's start_block
        if let Some(sb) = start_block {
            if result.block_number < sb {
                continue;
            }
        }
        let mut decoded_values = HashMap::new();

        for config in configs {
            if let Some(raw_value) = result.results.get(&config.function_name) {
                if !raw_value.is_empty() {
                    let stats = decode_stats
                        .entry(config.function_name.clone())
                        .or_insert((0, 0));
                    match decode_value(raw_value, &config.output_type) {
                        Ok(decoded) => {
                            stats.0 += 1;
                            decoded_values.insert(config.function_name.clone(), decoded);
                        }
                        Err(e) => {
                            stats.1 += 1;
                            tracing::debug!(
                                "Failed to decode {}.{} at block {}: {}",
                                contract_name,
                                config.function_name,
                                result.block_number,
                                e
                            );
                        }
                    }
                }
            }
        }

        decoded_records.push(DecodedOnceRecord {
            block_number: result.block_number,
            block_timestamp: result.block_timestamp,
            contract_address: result.contract_address,
            decoded_values,
        });
    }

    // Log warnings for functions with decode failures
    for (fn_name, (successes, failures)) in &decode_stats {
        if *failures > 0 {
            let config = configs.iter().find(|c| &c.function_name == fn_name);
            let output_type_str = config
                .map(|c| format!("{:?}", c.output_type))
                .unwrap_or_default();
            if *successes == 0 {
                tracing::warn!(
                    "All {} decode attempts failed for {}/{} (output_type: {}) in blocks {}-{} — check output_type in config",
                    failures,
                    contract_name,
                    fn_name,
                    output_type_str,
                    range_start,
                    range_end - 1
                );
            } else {
                tracing::warn!(
                    "{}/{} decode failures for {}/{} in blocks {}-{}",
                    failures,
                    successes + failures,
                    contract_name,
                    fn_name,
                    range_start,
                    range_end - 1
                );
            }
        }
    }

    // Send to transformation channel FIRST (before parquet I/O)
    // to unblock the transformation engine while parquet writes happen.
    if let Some(tx) = transform_tx {
        // For "once" calls, we consolidate ALL functions per address into a single DecodedCall
        // with function_name = "once" and ALL results merged. This matches:
        // 1. How handlers specify call_dependencies (e.g., ("DERC20", "once"))
        // 2. How catchup reads from parquet (one row per address with all columns as result fields)
        // 3. How handlers filter (call.function_name == "once") and access (call.result.get("name"))
        let transform_calls: Vec<TransformDecodedCall> = decoded_records
            .iter()
            .map(|record| {
                // Merge all function results for this address into one result map
                let mut merged_result = HashMap::new();
                for config in configs {
                    if let Some(decoded_value) = record.decoded_values.get(&config.function_name) {
                        let partial_result = build_result_map_for_merge(
                            decoded_value,
                            &config.output_type,
                            &config.function_name,
                        );
                        merged_result.extend(partial_result);
                    }
                }
                TransformDecodedCall {
                    block_number: record.block_number,
                    block_timestamp: record.block_timestamp,
                    contract_address: record.contract_address,
                    source_name: contract_name.to_string(),
                    function_name: "once".to_string(),
                    trigger_log_index: None,
                    result: merged_result,
                    is_reverted: false,
                    revert_reason: None,
                }
            })
            .filter(|call| !call.result.is_empty())
            .collect();

        if !transform_calls.is_empty() {
            tracing::info!(
                "Sending DecodedCallsMessage: source_name={}, function_name=once, range=({}, {}), {} calls",
                contract_name, range_start, range_end, transform_calls.len()
            );
            let msg = DecodedCallsMessage {
                range_start,
                range_end,
                source_name: contract_name.to_string(),
                function_name: "once".to_string(),
                calls: transform_calls,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!(
                    "Failed to send decoded calls to transformation channel: {}",
                    e
                );
            }
        } else {
            tracing::debug!(
                "No transform_calls to send for {}/once range ({}, {})",
                contract_name,
                range_start,
                range_end
            );
        }
    }

    // Write decoded data
    let output_dir = output_base.join(contract_name).join("once");
    std::fs::create_dir_all(&output_dir)?;

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let output_path = output_dir.join(&file_name);

    // Check if we need to merge with existing decoded data
    if output_path.exists() && !force_overwrite {
        tracing::info!(
            "Merging new decoded columns into existing file {}",
            output_path.display()
        );
        merge_decoded_once_calls(&output_path, &decoded_records, configs)?;
    } else {
        write_decoded_once_calls_to_parquet(&decoded_records, configs, &output_path)?;
    }

    // Derive column names from configs instead of re-reading the file we just wrote
    let actual_cols: Vec<String> = configs.iter().map(|c| c.function_name.clone()).collect();

    if decoded_records.is_empty() {
        tracing::debug!(
            "Wrote 0 decoded {}/once results to {} ({} columns: {:?})",
            contract_name,
            output_path.display(),
            actual_cols.len(),
            actual_cols
        );
    } else {
        tracing::info!(
            "Wrote {} decoded {}/once results to {} ({} columns: {:?})",
            decoded_records.len(),
            contract_name,
            output_path.display(),
            actual_cols.len(),
            actual_cols
        );
    }

    // If not returning index info, update column index directly (live mode)
    if !return_index_info {
        let mut index = read_decoded_column_index(&output_dir);
        index.insert(file_name.clone(), actual_cols.clone());
        write_decoded_column_index(&output_dir, &index)?;
    }

    // Return index info for batch updating (catchup mode)
    if return_index_info {
        Ok(Some(OnceCallsResult {
            contract_name: contract_name.to_string(),
            file_name,
            columns: actual_cols,
        }))
    } else {
        Ok(None)
    }
}

/// Process event-triggered call results
pub async fn process_event_calls(
    results: &[EventCallResult],
    range_start: u64,
    range_end: u64,
    config: &EventCallDecodeConfig,
    output_base: &Path,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
) -> Result<(), EthCallDecodingError> {
    // Skip if entire range is before contract's start_block
    if let Some(sb) = config.start_block {
        if range_end <= sb {
            return Ok(());
        }
    }

    let mut decoded_records = Vec::new();
    let mut decode_failures = 0u64;
    let mut non_empty_count = 0u64;

    let mut reverted_calls: Vec<TransformDecodedCall> = Vec::new();

    for result in results {
        // Skip if block is before contract's start_block
        if let Some(sb) = config.start_block {
            if result.block_number < sb {
                continue;
            }
        }

        // Handle reverted results: skip decode, create transform call with empty result
        if result.is_reverted {
            reverted_calls.push(TransformDecodedCall {
                block_number: result.block_number,
                block_timestamp: result.block_timestamp,
                contract_address: result.target_address,
                source_name: config.contract_name.clone(),
                function_name: config.function_name.clone(),
                trigger_log_index: Some(result.log_index),
                result: HashMap::new(),
                is_reverted: true,
                revert_reason: result.revert_reason.clone(),
            });
            continue;
        }

        if result.value.is_empty() {
            continue;
        }
        non_empty_count += 1;

        match decode_value(&result.value, &config.output_type) {
            Ok(decoded) => {
                decoded_records.push(DecodedEventCallRecord {
                    block_number: result.block_number,
                    block_timestamp: result.block_timestamp,
                    log_index: result.log_index,
                    target_address: result.target_address,
                    decoded_value: decoded,
                });
            }
            Err(e) => {
                decode_failures += 1;
                tracing::debug!(
                    "Failed to decode {}.{} at block {}: {}",
                    config.contract_name,
                    config.function_name,
                    result.block_number,
                    e
                );
            }
        }
    }

    if decode_failures > 0 && decode_failures == non_empty_count {
        tracing::warn!(
            "All {} decode attempts failed for {}.{} (output_type: {:?}) in blocks {}-{} — check output_type in config",
            decode_failures,
            config.contract_name,
            config.function_name,
            config.output_type,
            range_start,
            range_end - 1
        );
    } else if decode_failures > 0 {
        tracing::warn!(
            "{}/{} decode failures for {}.{} in blocks {}-{}",
            decode_failures,
            non_empty_count,
            config.contract_name,
            config.function_name,
            range_start,
            range_end - 1
        );
    }

    // Send to transformation channel FIRST (before parquet I/O)
    // to unblock the transformation engine while parquet writes happen.
    if let Some(tx) = transform_tx {
        let mut transform_calls: Vec<TransformDecodedCall> = decoded_records
            .iter()
            .map(|r| {
                convert_event_call_to_transform_call(
                    r,
                    &config.contract_name,
                    &config.function_name,
                    &config.output_type,
                )
            })
            .collect();

        // Include reverted calls so the engine can unblock pending events
        transform_calls.extend(reverted_calls);

        if !transform_calls.is_empty() {
            let msg = DecodedCallsMessage {
                range_start,
                range_end,
                source_name: config.contract_name.clone(),
                function_name: config.function_name.clone(),
                calls: transform_calls,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!(
                    "Failed to send decoded event calls to transformation channel: {}",
                    e
                );
            }
        }
    }

    // Write decoded data to on_events/ subdirectory
    let output_dir = output_base
        .join(&config.contract_name)
        .join(&config.function_name)
        .join("on_events");
    std::fs::create_dir_all(&output_dir)?;

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let output_path = output_dir.join(&file_name);

    // Always write file (even if empty) for catchup idempotency
    write_decoded_event_calls_to_parquet(&decoded_records, &config.output_type, &output_path)?;

    if decoded_records.is_empty() {
        tracing::debug!(
            "Wrote 0 decoded {}.{} event call results to {}",
            config.contract_name,
            config.function_name,
            output_path.display()
        );
    } else {
        tracing::info!(
            "Wrote {} decoded {}.{} event call results to {}",
            decoded_records.len(),
            config.contract_name,
            config.function_name,
            output_path.display()
        );
    }

    Ok(())
}
