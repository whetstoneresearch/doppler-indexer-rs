//! Catchup phase for eth_call decoding - processes existing raw eth_call parquet files.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::decoding::eth_calls::{
    CallDecodeConfig, EthCallDecodingError, EventCallDecodeConfig, process_event_calls,
    process_once_calls, process_regular_calls,
};
use crate::decoding::types::{EthCallResult, EventCallResult, OnceCallResult};
use crate::transformations::DecodedCallsMessage;
use crate::types::config::raw_data::RawDataCollectionConfig;

/// Work item for concurrent catchup processing
pub(crate) enum CatchupWorkItem {
    Regular {
        file_path: std::path::PathBuf,
        range_start: u64,
        range_end: u64,
        config: CallDecodeConfig,
    },
    Once {
        file_path: std::path::PathBuf,
        range_start: u64,
        range_end: u64,
        contract_name: String,
        configs: Vec<CallDecodeConfig>,
    },
    EventTriggered {
        file_path: std::path::PathBuf,
        range_start: u64,
        range_end: u64,
        config: EventCallDecodeConfig,
    },
}

/// Result of processing once calls, used to batch update column indexes after all tasks complete.
/// This avoids race conditions when multiple concurrent tasks try to read/modify/write the same index.
pub(crate) struct OnceCallsResult {
    pub contract_name: String,
    pub file_name: String,
    pub columns: Vec<String>,
}

/// Catchup phase: decode existing raw eth_call files
pub async fn catchup_decode_eth_calls(
    raw_calls_dir: &Path,
    output_base: &Path,
    regular_configs: &[CallDecodeConfig],
    once_configs: &[CallDecodeConfig],
    event_configs: &[EventCallDecodeConfig],
    raw_data_config: &RawDataCollectionConfig,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
) -> Result<(), EthCallDecodingError> {
    let concurrency = raw_data_config.decoding_concurrency.unwrap_or(4);

    // Scan existing decoded files
    let existing_decoded = scan_existing_decoded_files(output_base);

    // Build set of unique contract names with once configs
    let once_contract_names: HashSet<String> = once_configs
        .iter()
        .map(|c| c.contract_name.clone())
        .collect();

    // Pre-load column indexes for all once directories
    let mut raw_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
    let mut decoded_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

    for contract_name in &once_contract_names {
        let raw_once_dir = raw_calls_dir.join(contract_name).join("once");
        let raw_index = if raw_once_dir.exists() {
            let mut idx = read_raw_column_index(&raw_once_dir);
            // If index is empty, try to build from parquet schemas
            if idx.is_empty() {
                if let Ok(entries) = std::fs::read_dir(&raw_once_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
                            let cols: Vec<String> =
                                read_raw_parquet_function_names(&path).into_iter().collect();
                            if !cols.is_empty() {
                                idx.insert(file_name, cols);
                            }
                        }
                    }
                }
            }
            idx
        } else {
            HashMap::new()
        };
        raw_column_indexes.insert(contract_name.clone(), raw_index);

        let decoded_once_dir = output_base.join(contract_name).join("once");
        let decoded_index =
            load_or_build_decoded_column_index(&decoded_once_dir, &raw_once_dir);
        decoded_column_indexes.insert(contract_name.clone(), decoded_index);
    }

    // Collect all work items
    let mut work_items = Vec::new();

    // Scan raw call directories
    if let Ok(contract_entries) = std::fs::read_dir(raw_calls_dir) {
        for contract_entry in contract_entries.flatten() {
            let contract_path = contract_entry.path();
            if !contract_path.is_dir() {
                continue;
            }
            let contract_name = match contract_entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => continue,
            };

            if let Ok(function_entries) = std::fs::read_dir(&contract_path) {
                for function_entry in function_entries.flatten() {
                    let function_path = function_entry.path();
                    if !function_path.is_dir() {
                        continue;
                    }
                    let function_name = match function_entry.file_name().to_str() {
                        Some(name) => name.to_string(),
                        None => continue,
                    };

                    let is_once = function_name == "once";

                    if is_once {
                        let configs: Vec<CallDecodeConfig> = once_configs
                            .iter()
                            .filter(|c| c.contract_name == contract_name)
                            .cloned()
                            .collect();

                        if configs.is_empty() {
                            continue;
                        }

                        // Get pre-loaded column indexes
                        let raw_index = raw_column_indexes.get(&contract_name);
                        let decoded_index = decoded_column_indexes.get(&contract_name);

                        // Collect once call files
                        if let Ok(file_entries) = std::fs::read_dir(&function_path) {
                            for file_entry in file_entries.flatten() {
                                let file_path = file_entry.path();
                                if !file_path
                                    .extension()
                                    .map(|e| e == "parquet")
                                    .unwrap_or(false)
                                {
                                    continue;
                                }

                                let file_name =
                                    file_path.file_name().and_then(|s| s.to_str()).unwrap_or("");

                                // Parse range from filename first (needed for start_block check)
                                let range_str = file_name.strip_suffix(".parquet").unwrap_or("");
                                let parts: Vec<&str> = range_str.split('-').collect();
                                if parts.len() != 2 {
                                    continue;
                                }
                                let range_start: u64 = match parts[0].parse() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };
                                let range_end: u64 = match parts[1].parse::<u64>() {
                                    Ok(v) => v + 1,
                                    Err(_) => continue,
                                };

                                // Get raw columns from index (source of truth for what's been collected)
                                let raw_cols: HashSet<String> = raw_index
                                    .and_then(|idx| idx.get(file_name))
                                    .map(|cols| cols.iter().cloned().collect())
                                    .unwrap_or_else(|| read_raw_parquet_function_names(&file_path));

                                // Get decoded columns for this file
                                let decoded_cols: HashSet<String> = decoded_index
                                    .and_then(|idx| idx.get(file_name))
                                    .map(|cols| cols.iter().cloned().collect())
                                    .unwrap_or_default();

                                // Find functions that exist in raw but need decoding
                                let missing_configs: Vec<CallDecodeConfig> = configs
                                    .iter()
                                    .filter(|c| {
                                        // Skip configs that can't have data in this range
                                        if let Some(start) = c.start_block {
                                            if start >= range_end {
                                                return false;
                                            }
                                        }
                                        raw_cols.contains(&c.function_name)
                                            && !decoded_cols.contains(&c.function_name)
                                    })
                                    .cloned()
                                    .collect();

                                if missing_configs.is_empty() {
                                    continue; // All columns already decoded
                                }

                                tracing::debug!(
                                    "File {}/{} missing decoded columns: {:?}",
                                    contract_name,
                                    file_name,
                                    missing_configs
                                        .iter()
                                        .map(|c| &c.function_name)
                                        .collect::<Vec<_>>()
                                );

                                work_items.push(CatchupWorkItem::Once {
                                    file_path,
                                    range_start,
                                    range_end,
                                    contract_name: contract_name.clone(),
                                    configs: missing_configs,
                                });
                            }
                        }
                    } else {
                        let config = regular_configs.iter().find(|c| {
                            c.contract_name == contract_name && c.function_name == function_name
                        });

                        if config.is_none() {
                            continue;
                        }
                        let config = config.unwrap().clone();

                        // Collect regular call files
                        if let Ok(file_entries) = std::fs::read_dir(&function_path) {
                            for file_entry in file_entries.flatten() {
                                let file_path = file_entry.path();
                                if !file_path
                                    .extension()
                                    .map(|e| e == "parquet")
                                    .unwrap_or(false)
                                {
                                    continue;
                                }

                                let file_name =
                                    file_path.file_name().and_then(|s| s.to_str()).unwrap_or("");

                                // Check if already decoded
                                let rel_path =
                                    format!("{}/{}/{}", contract_name, function_name, file_name);
                                if existing_decoded.contains(&rel_path) {
                                    continue;
                                }

                                // Parse range from filename
                                let range_str = file_name.strip_suffix(".parquet").unwrap_or("");
                                let parts: Vec<&str> = range_str.split('-').collect();
                                if parts.len() != 2 {
                                    continue;
                                }
                                let range_start: u64 = match parts[0].parse() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };
                                let range_end: u64 = match parts[1].parse::<u64>() {
                                    Ok(v) => v + 1,
                                    Err(_) => continue,
                                };

                                // Skip if config's start_block is beyond this range
                                if let Some(start) = config.start_block {
                                    if start >= range_end {
                                        continue;
                                    }
                                }

                                work_items.push(CatchupWorkItem::Regular {
                                    file_path,
                                    range_start,
                                    range_end,
                                    config: config.clone(),
                                });
                            }
                        }

                        // Check for on_events/ subdirectory within this function directory
                        let on_events_path = function_path.join("on_events");
                        if on_events_path.exists() && on_events_path.is_dir() {
                            // Find event config for this contract/function
                            let event_config = event_configs.iter().find(|c| {
                                c.contract_name == contract_name && c.function_name == function_name
                            });

                            if let Some(event_config) = event_config {
                                if let Ok(file_entries) = std::fs::read_dir(&on_events_path) {
                                    for file_entry in file_entries.flatten() {
                                        let file_path = file_entry.path();
                                        if !file_path
                                            .extension()
                                            .map(|e| e == "parquet")
                                            .unwrap_or(false)
                                        {
                                            continue;
                                        }

                                        let file_name =
                                            file_path.file_name().and_then(|s| s.to_str()).unwrap_or("");

                                        // Check if already decoded
                                        let rel_path =
                                            format!("{}/{}/on_events/{}", contract_name, function_name, file_name);
                                        if existing_decoded.contains(&rel_path) {
                                            continue;
                                        }

                                        // Parse range from filename
                                        let range_str = file_name.strip_suffix(".parquet").unwrap_or("");
                                        let parts: Vec<&str> = range_str.split('-').collect();
                                        if parts.len() != 2 {
                                            continue;
                                        }
                                        let range_start: u64 = match parts[0].parse() {
                                            Ok(v) => v,
                                            Err(_) => continue,
                                        };
                                        let range_end: u64 = match parts[1].parse::<u64>() {
                                            Ok(v) => v + 1,
                                            Err(_) => continue,
                                        };

                                        // Skip if config's start_block is beyond this range
                                        if let Some(start) = event_config.start_block {
                                            if start >= range_end {
                                                continue;
                                            }
                                        }

                                        work_items.push(CatchupWorkItem::EventTriggered {
                                            file_path,
                                            range_start,
                                            range_end,
                                            config: event_config.clone(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if work_items.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Eth_call decoding catchup: processing {} files with concurrency {}",
        work_items.len(),
        concurrency
    );

    // Process work items concurrently
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let output_base = Arc::new(output_base.to_path_buf());
    let transform_tx = transform_tx.cloned();
    let mut join_set = JoinSet::new();

    for item in work_items {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let output_base = output_base.clone();
        let transform_tx = transform_tx.clone();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task completes

            match item {
                CatchupWorkItem::Regular {
                    file_path,
                    range_start,
                    range_end,
                    config,
                } => {
                    let results = read_regular_calls_from_parquet(&file_path)?;
                    process_regular_calls(
                        &results,
                        range_start,
                        range_end,
                        &config,
                        &output_base,
                        transform_tx.as_ref(),
                    )
                    .await?;
                    // Regular calls don't need index updates
                    Ok(None)
                }
                CatchupWorkItem::Once {
                    file_path,
                    range_start,
                    range_end,
                    contract_name,
                    configs,
                } => {
                    let config_refs: Vec<&CallDecodeConfig> = configs.iter().collect();
                    let results = read_once_calls_from_parquet(&file_path, &config_refs)?;
                    // Return index info for batch update (avoids race condition)
                    process_once_calls(
                        &results,
                        range_start,
                        range_end,
                        &contract_name,
                        &config_refs,
                        &output_base,
                        transform_tx.as_ref(),
                        true, // return_index_info: batch update after all tasks complete
                    )
                    .await
                }
                CatchupWorkItem::EventTriggered {
                    file_path,
                    range_start,
                    range_end,
                    config,
                } => {
                    let results = read_event_calls_from_parquet(&file_path)?;
                    process_event_calls(
                        &results,
                        range_start,
                        range_end,
                        &config,
                        &output_base,
                        transform_tx.as_ref(),
                    )
                    .await?;
                    // Event calls don't need index updates
                    Ok(None)
                }
            }
        });
    }

    // Collect results and column index updates
    // Using a HashMap to group updates by contract_name for batch updating
    let mut index_updates: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(Some(once_result))) => {
                // Collect index updates for batch processing
                index_updates
                    .entry(once_result.contract_name)
                    .or_default()
                    .push((once_result.file_name, once_result.columns));
            }
            Ok(Ok(None)) => {} // Regular call, no index update needed
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(EthCallDecodingError::JoinError(e.to_string())),
        }
    }

    // Batch update all column indexes (no race condition - sequential after all tasks complete)
    for (contract_name, updates) in index_updates {
        let output_dir = output_base.join(&contract_name).join("once");
        let mut index = read_decoded_column_index(&output_dir);
        for (file_name, columns) in updates {
            index.insert(file_name, columns);
        }
        write_decoded_column_index(&output_dir, &index)?;
        tracing::info!(
            "Updated decoded column index for {}: {} files tracked",
            contract_name,
            index.len()
        );
    }

    Ok(())
}

/// Scan existing decoded files
fn scan_existing_decoded_files(output_base: &Path) -> HashSet<String> {
    let mut files = HashSet::new();

    fn scan_recursive(dir: &Path, base: &Path, files: &mut HashSet<String>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan_recursive(&path, base, files);
                } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    if let Ok(rel) = path.strip_prefix(base) {
                        files.insert(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    scan_recursive(output_base, output_base, &mut files);
    files
}

/// Read regular call results from parquet
pub fn read_regular_calls_from_parquet(path: &Path) -> Result<Vec<EthCallResult>, EthCallDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let address_idx = schema.index_of("address").ok();
        let value_idx = schema.index_of("value").ok();

        for row in 0..batch.num_rows() {
            let block_number = block_number_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let block_timestamp = block_timestamp_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let contract_address = address_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 20 {
                                let mut arr = [0u8; 20];
                                arr.copy_from_slice(bytes);
                                Some(arr)
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let value = value_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|a| a.value(row).to_vec())
                })
                .unwrap_or_default();

            results.push(EthCallResult {
                block_number,
                block_timestamp,
                contract_address,
                value,
            });
        }
    }

    Ok(results)
}

/// Read event-triggered call results from parquet
pub fn read_event_calls_from_parquet(path: &Path) -> Result<Vec<EventCallResult>, EthCallDecodingError> {
    use arrow::array::UInt32Array;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let log_index_idx = schema.index_of("log_index").ok();
        let address_idx = schema.index_of("target_address").ok();
        let value_idx = schema.index_of("value").ok();

        for row in 0..batch.num_rows() {
            let block_number = block_number_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let block_timestamp = block_timestamp_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let log_index = log_index_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let target_address = address_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 20 {
                                let mut arr = [0u8; 20];
                                arr.copy_from_slice(bytes);
                                Some(arr)
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let value = value_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|a| a.value(row).to_vec())
                })
                .unwrap_or_default();

            results.push(EventCallResult {
                block_number,
                block_timestamp,
                log_index,
                target_address,
                value,
            });
        }
    }

    Ok(results)
}

/// Read "once" call results from parquet
pub fn read_once_calls_from_parquet(
    path: &Path,
    configs: &[&CallDecodeConfig],
) -> Result<Vec<OnceCallResult>, EthCallDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let address_idx = schema.index_of("address")
            .or_else(|_| schema.index_of("address"))
            .ok();

        // Find function result columns
        let function_indices: Vec<(String, Option<usize>)> = configs
            .iter()
            .map(|c| {
                let col_name = format!("{}_result", c.function_name);
                (c.function_name.clone(), schema.index_of(&col_name).ok())
            })
            .collect();

        for row in 0..batch.num_rows() {
            let block_number = block_number_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let block_timestamp = block_timestamp_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let contract_address = address_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 20 {
                                let mut arr = [0u8; 20];
                                arr.copy_from_slice(bytes);
                                Some(arr)
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let mut function_results = HashMap::new();
            for (function_name, idx_opt) in &function_indices {
                if let Some(idx) = idx_opt {
                    if let Some(arr) = batch.column(*idx).as_any().downcast_ref::<BinaryArray>() {
                        if !arr.is_null(row) {
                            function_results
                                .insert(function_name.clone(), arr.value(row).to_vec());
                        }
                    }
                }
            }

            results.push(OnceCallResult {
                block_number,
                block_timestamp,
                contract_address,
                results: function_results,
            });
        }
    }

    Ok(results)
}

// ============================================================================
// Column Index Functions for Decoded Data
// ============================================================================

/// Read the column index sidecar file from a decoded `once/` directory.
/// Returns a map of filename -> list of decoded function names.
pub fn read_decoded_column_index(decoded_once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = decoded_once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> =
                serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read decoded column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No decoded column index at {} ({}), starting fresh",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Write the column index sidecar file to a decoded `once/` directory.
pub fn write_decoded_column_index(
    decoded_once_dir: &Path,
    index: &HashMap<String, Vec<String>>,
) -> Result<(), EthCallDecodingError> {
    let index_path = decoded_once_dir.join("column_index.json");
    tracing::debug!(
        "Writing decoded column index to {}: {} files tracked",
        index_path.display(),
        index.len()
    );
    let content = serde_json::to_string_pretty(index).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("JSON serialize error: {}", e),
        )
    })?;
    std::fs::write(&index_path, content)?;
    Ok(())
}

/// Find decoded function names that have null values fillable from raw data.
///
/// For each column in the decoded parquet that has null values, checks whether
/// the corresponding raw parquet has non-null `{fn_name}_result` values at
/// those positions. Returns the set of function names that can be filled.
fn find_columns_with_fillable_nulls(decoded_path: &Path, raw_path: &Path) -> HashSet<String> {
    let mut fillable = HashSet::new();

    // Read decoded parquet
    let decoded_file = match File::open(decoded_path) {
        Ok(f) => f,
        Err(_) => return fillable,
    };
    let decoded_reader = match ParquetRecordBatchReaderBuilder::try_new(decoded_file) {
        Ok(b) => match b.build() {
            Ok(r) => r,
            Err(_) => return fillable,
        },
        Err(_) => return fillable,
    };
    let decoded_batches: Vec<RecordBatch> = decoded_reader.filter_map(|r| r.ok()).collect();
    if decoded_batches.is_empty() {
        return fillable;
    }
    let decoded_batch = &decoded_batches[0];
    let decoded_schema = decoded_batch.schema();

    let skip_columns: HashSet<&str> = ["block_number", "block_timestamp", "address"]
        .into_iter()
        .collect();

    // Find columns with nulls, grouped by base function name
    let mut fn_null_positions: HashMap<String, Vec<usize>> = HashMap::new();
    for (col_idx, field) in decoded_schema.fields().iter().enumerate() {
        let name = field.name().as_str();
        if skip_columns.contains(name) {
            continue;
        }
        let col = decoded_batch.column(col_idx);
        if col.null_count() == 0 {
            continue;
        }
        let fn_name = name.split('.').next().unwrap_or(name).to_string();
        // Collect null row positions (only if not already tracked for this fn)
        let positions = fn_null_positions.entry(fn_name).or_default();
        for row in 0..col.len() {
            if col.is_null(row) && !positions.contains(&row) {
                positions.push(row);
            }
        }
    }

    if fn_null_positions.is_empty() {
        return fillable;
    }

    // Read raw parquet
    let raw_file = match File::open(raw_path) {
        Ok(f) => f,
        Err(_) => return fillable,
    };
    let raw_reader = match ParquetRecordBatchReaderBuilder::try_new(raw_file) {
        Ok(b) => match b.build() {
            Ok(r) => r,
            Err(_) => return fillable,
        },
        Err(_) => return fillable,
    };
    let raw_batches: Vec<RecordBatch> = raw_reader.filter_map(|r| r.ok()).collect();
    if raw_batches.is_empty() {
        return fillable;
    }
    let raw_batch = &raw_batches[0];
    let raw_schema = raw_batch.schema();

    // Build address -> row index lookup from decoded parquet
    let decoded_addr_idx = match decoded_schema.index_of("address") {
        Ok(idx) => idx,
        Err(_) => return fillable,
    };
    let decoded_addr_arr = match decoded_batch
        .column(decoded_addr_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
    {
        Some(a) => a,
        None => return fillable,
    };

    // Build address -> row index lookup from raw parquet
    let raw_addr_idx = match raw_schema.index_of("address") {
        Ok(idx) => idx,
        Err(_) => return fillable,
    };
    let raw_addr_arr = match raw_batch
        .column(raw_addr_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
    {
        Some(a) => a,
        None => return fillable,
    };
    let mut raw_addr_to_row: HashMap<Vec<u8>, usize> = HashMap::new();
    for i in 0..raw_addr_arr.len() {
        raw_addr_to_row.insert(raw_addr_arr.value(i).to_vec(), i);
    }

    // For each function with nulls, check if raw has non-null values
    for (fn_name, null_positions) in &fn_null_positions {
        let result_col_name = format!("{}_result", fn_name);
        let raw_col_idx = match raw_schema.index_of(&result_col_name) {
            Ok(idx) => idx,
            Err(_) => continue,
        };
        let raw_col = raw_batch.column(raw_col_idx);

        // Short-circuit: check if any null position in decoded has a non-null in raw
        for &row in null_positions {
            let addr = decoded_addr_arr.value(row).to_vec();
            if let Some(&raw_row) = raw_addr_to_row.get(&addr) {
                if !raw_col.is_null(raw_row) {
                    fillable.insert(fn_name.clone());
                    break;
                }
            }
        }
    }

    fillable
}

/// Load or build the column index for a decoded once/ directory.
/// If the index file exists, load it. Otherwise, scan all parquet files to build it.
/// When rebuilding from parquet, checks for columns with fillable nulls and excludes them
/// so that catchup will re-decode those columns from raw data.
pub fn load_or_build_decoded_column_index(
    decoded_once_dir: &Path,
    raw_once_dir: &Path,
) -> HashMap<String, Vec<String>> {
    let index_path = decoded_once_dir.join("column_index.json");

    // Try to load existing index
    if index_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&index_path) {
            if let Ok(index) = serde_json::from_str::<HashMap<String, Vec<String>>>(&content) {
                tracing::debug!(
                    "Loaded decoded column index from {}: {} files tracked",
                    index_path.display(),
                    index.len()
                );
                return index;
            }
        }
    }

    // Index doesn't exist or couldn't be loaded - build from parquet files
    if !decoded_once_dir.exists() {
        return HashMap::new();
    }

    let mut index = HashMap::new();
    let parquet_files: Vec<_> = match std::fs::read_dir(decoded_once_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .collect(),
        Err(_) => return HashMap::new(),
    };

    if parquet_files.is_empty() {
        return HashMap::new();
    }

    tracing::info!(
        "Building decoded column index for {} from {} parquet files",
        decoded_once_dir.display(),
        parquet_files.len()
    );

    for entry in parquet_files {
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let cols: Vec<String> = read_decoded_parquet_function_names(&path)
            .into_iter()
            .collect();
        if !cols.is_empty() {
            // Check if any columns have nulls fillable from raw data
            let raw_path = raw_once_dir.join(&file_name);
            if raw_path.exists() {
                let fillable = find_columns_with_fillable_nulls(&path, &raw_path);
                if !fillable.is_empty() {
                    tracing::info!(
                        "Excluded {} columns with fillable nulls from index for {}: {:?}",
                        fillable.len(),
                        file_name,
                        fillable
                    );
                    let filtered: Vec<String> = cols
                        .into_iter()
                        .filter(|c| !fillable.contains(c))
                        .collect();
                    if !filtered.is_empty() {
                        index.insert(file_name, filtered);
                    }
                    continue;
                }
            }
            index.insert(file_name, cols);
        }
    }

    // Write the newly built index
    if !index.is_empty() {
        if let Err(e) = write_decoded_column_index(decoded_once_dir, &index) {
            tracing::warn!(
                "Failed to write decoded column index to {}: {}",
                decoded_once_dir.display(),
                e
            );
        } else {
            tracing::info!(
                "Built and saved decoded column index for {}: {} files tracked",
                decoded_once_dir.display(),
                index.len()
            );
        }
    }

    index
}

/// Read function names from a decoded parquet file's schema.
/// Decoded columns are named like:
/// - `name` (simple type) -> returns `name`
/// - `getAssetData.numeraire` (tuple field) -> returns `getAssetData`
/// Excludes standard columns like block_number, block_timestamp, address.
pub fn read_decoded_parquet_function_names(path: &Path) -> HashSet<String> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    // Standard columns to skip
    let skip_columns: HashSet<&str> =
        ["block_number", "block_timestamp", "address", "address"]
            .into_iter()
            .collect();

    for field in schema.columns() {
        let name = field.name();

        // Skip standard columns
        if skip_columns.contains(name) {
            continue;
        }

        // Extract base function name (part before '.' for tuple fields)
        let fn_name = name.split('.').next().unwrap_or(name);
        fn_names.insert(fn_name.to_string());
    }

    fn_names
}

/// Read the raw column index from a raw `once/` directory.
/// Returns a map of filename -> list of function names whose `{name}_result` columns exist.
pub fn read_raw_column_index(raw_once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = raw_once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> =
                serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read raw column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No raw column index at {} ({}), will scan parquet schemas",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Read function names from a raw parquet file's schema.
/// Raw columns are named like `{function_name}_result` -> returns `function_name`.
pub fn read_raw_parquet_function_names(path: &Path) -> HashSet<String> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    for field in schema.columns() {
        let name = field.name();
        // Extract function name from column name (e.g., "getAssetData_result" -> "getAssetData")
        if let Some(fn_name) = name.strip_suffix("_result") {
            fn_names.insert(fn_name.to_string());
        }
    }

    fn_names
}
