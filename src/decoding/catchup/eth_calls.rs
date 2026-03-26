//! Catchup phase for eth_call decoding - processes existing raw eth_call parquet files.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::decoding::eth_calls::column_index::{
    load_or_build_decoded_column_index, read_decoded_column_index, read_once_calls_from_parquet,
    read_raw_column_index, read_raw_parquet_function_names, write_decoded_column_index,
};
use crate::decoding::eth_calls::{
    process_event_calls, process_once_calls, process_regular_calls, CallDecodeConfig,
    EthCallDecodingError, EventCallDecodeConfig,
};
use crate::storage::decoded_index::scan_existing_decoded_files;
use crate::storage::parquet_readers::{
    read_event_calls_from_parquet, read_regular_calls_from_parquet,
};
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
        let decoded_index = load_or_build_decoded_column_index(&decoded_once_dir, &raw_once_dir);
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
                        // Collect regular call files (if a regular config exists)
                        let regular_config = regular_configs.iter().find(|c| {
                            c.contract_name == contract_name && c.function_name == function_name
                        });

                        if let Some(config) = regular_config {
                            let config = config.clone();
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

                                    let file_name = file_path
                                        .file_name()
                                        .and_then(|s| s.to_str())
                                        .unwrap_or("");

                                    // Check if already decoded
                                    let rel_path = format!(
                                        "{}/{}/{}",
                                        contract_name, function_name, file_name
                                    );
                                    if existing_decoded.contains(&rel_path) {
                                        continue;
                                    }

                                    // Parse range from filename
                                    let range_str =
                                        file_name.strip_suffix(".parquet").unwrap_or("");
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
                        }

                        // Check for on_events/ subdirectory independently of regular config
                        let on_events_path = function_path.join("on_events");
                        if on_events_path.exists() && on_events_path.is_dir() {
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

                                        let file_name = file_path
                                            .file_name()
                                            .and_then(|s| s.to_str())
                                            .unwrap_or("");

                                        // Check if already decoded
                                        let rel_path = format!(
                                            "{}/{}/on_events/{}",
                                            contract_name, function_name, file_name
                                        );
                                        if existing_decoded.contains(&rel_path) {
                                            continue;
                                        }

                                        // Parse range from filename
                                        let range_str =
                                            file_name.strip_suffix(".parquet").unwrap_or("");
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
        tracing::info!("Eth_call decoding catchup: all files already decoded");
        return Ok(());
    }

    let regular_count = work_items
        .iter()
        .filter(|w| matches!(w, CatchupWorkItem::Regular { .. }))
        .count();
    let once_count = work_items
        .iter()
        .filter(|w| matches!(w, CatchupWorkItem::Once { .. }))
        .count();
    let event_count = work_items
        .iter()
        .filter(|w| matches!(w, CatchupWorkItem::EventTriggered { .. }))
        .count();

    tracing::info!(
        "Eth_call decoding catchup: processing {} files ({} regular, {} once, {} event) with concurrency {}",
        work_items.len(),
        regular_count,
        once_count,
        event_count,
        concurrency
    );

    // Process work items concurrently
    let start_time = Instant::now();
    let total_files = work_items.len();
    let completed = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let output_base = Arc::new(output_base.to_path_buf());
    let transform_tx = transform_tx.cloned();
    let mut join_set = JoinSet::new();

    for item in work_items {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let output_base = output_base.clone();
        let transform_tx = transform_tx.clone();
        let completed = completed.clone();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task completes

            let result = match item {
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
            };

            if result.is_ok() {
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 50 == 0 || done == total_files {
                    tracing::info!(
                        "Eth_call decoding catchup: decoded {}/{} files",
                        done,
                        total_files
                    );
                }
            }

            result
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

    tracing::info!(
        "Eth_call decoding catchup complete: decoded {} files in {:.1}s",
        total_files,
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}
