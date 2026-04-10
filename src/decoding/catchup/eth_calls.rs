//! Catchup phase for eth_call decoding - processes existing raw eth_call parquet files.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::decoding::eth_calls::column_index::{
    load_or_build_decoded_column_index, read_decoded_column_index, read_once_calls_from_parquet,
    read_raw_column_index, read_raw_parquet_function_names, rebuild_decoded_column_index,
    write_decoded_column_index,
};
use crate::decoding::eth_calls::{
    process_event_calls, process_once_calls, process_regular_calls, CallDecodeConfig,
    EthCallDecodingError, EventCallDecodeConfig,
};
use crate::raw_data::historical::eth_calls::rebuild_once_column_index;
use crate::storage::decoded_index::scan_existing_decoded_files;
use crate::storage::parquet_readers::{
    read_event_calls_from_parquet, read_regular_calls_from_parquet,
};
use crate::transformations::DecodedCallsMessage;
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::shared::repair::RepairScope;

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

type EventRowKeyCounts = HashMap<(u64, u32), usize>;

fn build_event_row_key_counts<I>(rows: I) -> EventRowKeyCounts
where
    I: IntoIterator<Item = (u64, u32)>,
{
    let mut counts = HashMap::new();
    for key in rows {
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

fn read_decoded_event_row_key_counts(
    path: &Path,
) -> Result<EventRowKeyCounts, EthCallDecodingError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut counts = HashMap::new();

    for batch in reader {
        let batch = batch?;
        let block_numbers = batch
            .column_by_name("block_number")
            .ok_or_else(|| {
                EthCallDecodingError::Decode(
                    "decoded event parquet missing block_number".to_string(),
                )
            })?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                EthCallDecodingError::Decode(
                    "decoded event parquet block_number is not UInt64".to_string(),
                )
            })?;
        let log_indices = batch
            .column_by_name("log_index")
            .ok_or_else(|| {
                EthCallDecodingError::Decode("decoded event parquet missing log_index".to_string())
            })?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                EthCallDecodingError::Decode(
                    "decoded event parquet log_index is not UInt32".to_string(),
                )
            })?;

        for row in 0..batch.num_rows() {
            *counts
                .entry((block_numbers.value(row), log_indices.value(row)))
                .or_insert(0) += 1;
        }
    }

    Ok(counts)
}

fn repair_scope_matches_range(
    repair_scope: Option<&RepairScope>,
    range_start: u64,
    range_end: u64,
) -> bool {
    match repair_scope {
        Some(scope) => scope.matches_range(range_start, range_end),
        None => true,
    }
}

fn repair_scope_matches_source_function(
    repair_scope: Option<&RepairScope>,
    contract_name: &str,
    function_name: &str,
) -> bool {
    match repair_scope {
        Some(scope) => scope.matches_source_function(contract_name, function_name),
        None => true,
    }
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
    chain_name: &str,
    repair: bool,
    repair_scope: Option<RepairScope>,
) -> Result<(), EthCallDecodingError> {
    let concurrency = raw_data_config.decoding_concurrency.unwrap_or(8);
    let repair_scope = repair.then_some(repair_scope.as_ref()).flatten();

    if repair {
        tracing::info!(
            "Eth_call decoding catchup repair mode enabled: rebuilding once column indexes from parquet schemas"
        );
    }

    // Scan existing decoded files
    let existing_decoded = scan_existing_decoded_files(output_base);

    // Build set of unique contract names with once configs
    let once_contract_names: HashSet<String> = once_configs
        .iter()
        .filter(|config| {
            repair_scope_matches_source_function(
                repair_scope,
                &config.contract_name,
                &config.function_name,
            )
        })
        .map(|c| c.contract_name.clone())
        .collect();

    // Pre-load column indexes for all once directories
    let mut raw_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
    let mut decoded_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

    for contract_name in &once_contract_names {
        let raw_once_dir = raw_calls_dir.join(contract_name).join("once");
        let raw_index = if raw_once_dir.exists() {
            if repair {
                rebuild_once_column_index(&raw_once_dir)
            } else {
                let mut idx = read_raw_column_index(&raw_once_dir);
                // If index is empty, try to build from parquet schemas
                if idx.is_empty() {
                    if let Ok(entries) = std::fs::read_dir(&raw_once_dir) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                                let file_name =
                                    path.file_name().unwrap().to_string_lossy().to_string();
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
            }
        } else {
            HashMap::new()
        };
        raw_column_indexes.insert(contract_name.clone(), raw_index);

        let decoded_once_dir = output_base.join(contract_name).join("once");
        let decoded_index = if repair {
            rebuild_decoded_column_index(&decoded_once_dir, &raw_once_dir)
        } else {
            load_or_build_decoded_column_index(&decoded_once_dir, &raw_once_dir)
        };
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
                            .filter(|c| {
                                repair_scope_matches_source_function(
                                    repair_scope,
                                    &c.contract_name,
                                    &c.function_name,
                                )
                            })
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

                                if !repair_scope_matches_range(repair_scope, range_start, range_end)
                                {
                                    continue;
                                }

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

                                let missing_decoded_cols: HashSet<String> =
                                    raw_cols.difference(&decoded_cols).cloned().collect();
                                let extra_decoded_cols: HashSet<String> =
                                    decoded_cols.difference(&raw_cols).cloned().collect();

                                if repair
                                    && (!missing_decoded_cols.is_empty()
                                        || !extra_decoded_cols.is_empty())
                                {
                                    tracing::warn!(
                                        "Repair detected once schema mismatch for {}/once/{}: raw_only={:?}, decoded_only={:?}",
                                        contract_name,
                                        file_name,
                                        missing_decoded_cols,
                                        extra_decoded_cols
                                    );
                                }

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
                                        missing_decoded_cols.contains(&c.function_name)
                                    })
                                    .cloned()
                                    .collect();

                                if missing_configs.is_empty() {
                                    // All columns decoded. Check if raw has more
                                    // rows (new factory addresses added after
                                    // initial decode).
                                    let decoded_path = output_base
                                        .join(&contract_name)
                                        .join("once")
                                        .join(file_name);
                                    let needs_redecode = {
                                        use parquet::file::reader::FileReader;
                                        let raw_rows = std::fs::File::open(&file_path)
                                            .ok()
                                            .and_then(|f| {
                                                parquet::file::reader::SerializedFileReader::new(f)
                                                    .ok()
                                            })
                                            .map(|r| {
                                                (0..r.metadata().num_row_groups())
                                                    .map(|i| {
                                                        r.metadata().row_group(i).num_rows() as u64
                                                    })
                                                    .sum::<u64>()
                                            })
                                            .unwrap_or(0);
                                        let dec_rows = std::fs::File::open(&decoded_path)
                                            .ok()
                                            .and_then(|f| {
                                                parquet::file::reader::SerializedFileReader::new(f)
                                                    .ok()
                                            })
                                            .map(|r| {
                                                (0..r.metadata().num_row_groups())
                                                    .map(|i| {
                                                        r.metadata().row_group(i).num_rows() as u64
                                                    })
                                                    .sum::<u64>()
                                            })
                                            .unwrap_or(0);
                                        raw_rows > dec_rows
                                    };
                                    if !needs_redecode {
                                        continue;
                                    }
                                    tracing::info!(
                                        "Once file {}/once/{}: raw has more rows than decoded, re-decoding",
                                        contract_name, file_name,
                                    );
                                    let all_configs: Vec<CallDecodeConfig> = configs
                                        .iter()
                                        .filter(|c| {
                                            if let Some(start) = c.start_block {
                                                if start >= range_end {
                                                    return false;
                                                }
                                            }
                                            raw_cols.contains(&c.function_name)
                                        })
                                        .cloned()
                                        .collect();
                                    if all_configs.is_empty() {
                                        continue;
                                    }
                                    work_items.push(CatchupWorkItem::Once {
                                        file_path,
                                        range_start,
                                        range_end,
                                        contract_name: contract_name.to_string(),
                                        configs: all_configs,
                                    });
                                    continue;
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
                            c.contract_name == contract_name
                                && c.function_name == function_name
                                && repair_scope_matches_source_function(
                                    repair_scope,
                                    &c.contract_name,
                                    &c.function_name,
                                )
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

                                    if !repair_scope_matches_range(
                                        repair_scope,
                                        range_start,
                                        range_end,
                                    ) {
                                        continue;
                                    }

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
                                c.contract_name == contract_name
                                    && c.function_name == function_name
                                    && repair_scope_matches_source_function(
                                        repair_scope,
                                        &c.contract_name,
                                        &c.function_name,
                                    )
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

                                        // In repair mode, force re-decode of event-triggered
                                        // files to refresh stale or partially written on_events.
                                        let rel_path = format!(
                                            "{}/{}/on_events/{}",
                                            contract_name, function_name, file_name
                                        );
                                        if !repair && existing_decoded.contains(&rel_path) {
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

                                        if !repair_scope_matches_range(
                                            repair_scope,
                                            range_start,
                                            range_end,
                                        ) {
                                            continue;
                                        }

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

    let chain_name_arc = Arc::new(chain_name.to_string());

    for item in work_items {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let output_base = output_base.clone();
        let transform_tx = transform_tx.clone();
        let completed = completed.clone();
        let chain_name = chain_name_arc.clone();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task completes

            let result = match item {
                CatchupWorkItem::Regular {
                    file_path,
                    range_start,
                    range_end,
                    config,
                } => {
                    let results = tokio::task::spawn_blocking(move || {
                        read_regular_calls_from_parquet(&file_path)
                    })
                    .await
                    .expect("parquet read task panicked")?;
                    process_regular_calls(
                        &results,
                        range_start,
                        range_end,
                        &config,
                        &output_base,
                        transform_tx.as_ref(),
                        &chain_name,
                        "catchup",
                    )
                    .await?;
                    Ok(None)
                }
                CatchupWorkItem::Once {
                    file_path,
                    range_start,
                    range_end,
                    contract_name,
                    configs,
                } => {
                    let configs_for_read = configs.clone();
                    let results = tokio::task::spawn_blocking(move || {
                        let config_refs: Vec<&CallDecodeConfig> = configs_for_read.iter().collect();
                        read_once_calls_from_parquet(&file_path, &config_refs)
                    })
                    .await
                    .expect("parquet read task panicked")?;
                    let config_refs: Vec<&CallDecodeConfig> = configs.iter().collect();
                    process_once_calls(
                        &results,
                        range_start,
                        range_end,
                        &contract_name,
                        &config_refs,
                        &output_base,
                        transform_tx.as_ref(),
                        true, // return_index_info: batch update after all tasks complete
                        true, // force_overwrite: catchup always overwrites to handle row additions
                        &chain_name,
                        "catchup",
                    )
                    .await
                }
                CatchupWorkItem::EventTriggered {
                    file_path,
                    range_start,
                    range_end,
                    config,
                } => {
                    let file_name = file_path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    let results = tokio::task::spawn_blocking(move || {
                        read_event_calls_from_parquet(&file_path)
                    })
                    .await
                    .expect("parquet read task panicked")?;
                    let decoded_path = output_base
                        .join(&config.contract_name)
                        .join(&config.function_name)
                        .join("on_events")
                        .join(&file_name);
                    if repair && decoded_path.exists() {
                        let raw_success_keys = build_event_row_key_counts(
                            results
                                .iter()
                                .filter(|r| !r.is_reverted)
                                .map(|r| (r.block_number, r.log_index)),
                        );
                        let decoded_keys = tokio::task::spawn_blocking(move || {
                            read_decoded_event_row_key_counts(&decoded_path)
                        })
                        .await
                        .expect("decoded parquet read task panicked")?;
                        if raw_success_keys == decoded_keys {
                            return Ok(None);
                        }
                    }
                    process_event_calls(
                        &results,
                        range_start,
                        range_end,
                        &config,
                        &output_base,
                        transform_tx.as_ref(),
                        &chain_name,
                        "catchup",
                    )
                    .await?;
                    Ok(None)
                }
            };

            if result.is_ok() {
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done.is_multiple_of(50) || done == total_files {
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

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use arrow::array::{StringArray, UInt32Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::TempDir;

    use super::catchup_decode_eth_calls;
    use crate::decoding::eth_calls::process_event_calls;
    use crate::decoding::types::EventCallResult;
    use crate::raw_data::historical::eth_calls::event_triggers::write_event_call_results_to_parquet;
    use crate::raw_data::historical::eth_calls::types::EventCallResult as RawEventCallResult;
    use crate::types::config::eth_call::EvmType;
    use crate::types::config::raw_data::{FieldsConfig, RawDataCollectionConfig};

    fn raw_config() -> RawDataCollectionConfig {
        RawDataCollectionConfig {
            parquet_block_range: None,
            rpc_batch_size: None,
            fields: FieldsConfig {
                block_fields: None,
                receipt_fields: None,
                log_fields: None,
            },
            contract_logs_only: None,
            channel_capacity: None,
            factory_channel_capacity: None,
            block_receipt_concurrency: None,
            decoding_concurrency: Some(1),
            factory_concurrency: None,
            event_call_concurrency: None,
            live_mode: None,
            reorg_depth: None,
            compaction_interval_secs: None,
            transform_retry_grace_period_secs: None,
        }
    }

    fn uint256_bytes(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[24..].copy_from_slice(&value.to_be_bytes());
        bytes
    }

    fn first_log_index(path: &Path) -> u32 {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();
            if batch.num_rows() == 0 {
                continue;
            }
            let indices = batch
                .column_by_name("log_index")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            return indices.value(0);
        }

        panic!("no rows found in {}", path.display());
    }

    fn first_utf8_value(path: &Path, column: &str) -> String {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();
            if batch.num_rows() == 0 {
                continue;
            }
            let values = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            return values.value(0).to_string();
        }

        panic!("no rows found in {}", path.display());
    }

    #[tokio::test]
    async fn repair_redecodes_event_files_even_when_decoded_exists() {
        let tmp = TempDir::new().unwrap();
        let raw_dir = tmp.path().join("raw");
        let decoded_dir = tmp.path().join("decoded");

        let contract_name = "TestContract";
        let function_name = "getState";
        let range_start = 100u64;
        let range_end = 200u64;

        let raw_subdir = raw_dir
            .join(contract_name)
            .join(function_name)
            .join("on_events");
        std::fs::create_dir_all(&raw_subdir).unwrap();
        let raw_path = raw_subdir.join("100-199.parquet");

        let fresh_raw = vec![RawEventCallResult {
            block_number: 150,
            block_timestamp: 1,
            log_index: 42,
            target_address: [0x11; 20],
            value_bytes: uint256_bytes(1),
            param_values: vec![],
            is_reverted: false,
            revert_reason: None,
        }];
        write_event_call_results_to_parquet(&fresh_raw, &raw_path, 0).unwrap();

        let config = crate::decoding::eth_calls::EventCallDecodeConfig {
            contract_name: contract_name.to_string(),
            function_name: function_name.to_string(),
            output_type: EvmType::Uint256,
            start_block: None,
        };

        let stale_decoded = vec![EventCallResult {
            block_number: 150,
            block_timestamp: 1,
            log_index: 7,
            target_address: [0x11; 20],
            value: uint256_bytes(2),
            is_reverted: false,
            revert_reason: None,
        }];
        process_event_calls(
            &stale_decoded,
            range_start,
            range_end,
            &config,
            &decoded_dir,
            None,
            "test",
            "catchup",
        )
        .await
        .unwrap();

        let decoded_path = decoded_dir
            .join(contract_name)
            .join(function_name)
            .join("on_events")
            .join("100-199.parquet");
        assert_eq!(first_log_index(&decoded_path), 7);

        catchup_decode_eth_calls(
            &raw_dir,
            &decoded_dir,
            &[],
            &[],
            std::slice::from_ref(&config),
            &raw_config(),
            None,
            "test",
            false,
            None,
        )
        .await
        .unwrap();
        assert_eq!(first_log_index(&decoded_path), 7);

        catchup_decode_eth_calls(
            &raw_dir,
            &decoded_dir,
            &[],
            &[],
            std::slice::from_ref(&config),
            &raw_config(),
            None,
            "test",
            true,
            None,
        )
        .await
        .unwrap();
        assert_eq!(first_log_index(&decoded_path), 42);
    }

    #[tokio::test]
    async fn repair_keeps_event_file_when_trigger_keys_already_match() {
        let tmp = TempDir::new().unwrap();
        let raw_dir = tmp.path().join("raw");
        let decoded_dir = tmp.path().join("decoded");

        let contract_name = "TestContract";
        let function_name = "getState";
        let range_start = 100u64;
        let range_end = 200u64;

        let raw_subdir = raw_dir
            .join(contract_name)
            .join(function_name)
            .join("on_events");
        std::fs::create_dir_all(&raw_subdir).unwrap();
        let raw_path = raw_subdir.join("100-199.parquet");

        let raw = vec![RawEventCallResult {
            block_number: 150,
            block_timestamp: 1,
            log_index: 42,
            target_address: [0x11; 20],
            value_bytes: uint256_bytes(1),
            param_values: vec![],
            is_reverted: false,
            revert_reason: None,
        }];
        write_event_call_results_to_parquet(&raw, &raw_path, 0).unwrap();

        let config = crate::decoding::eth_calls::EventCallDecodeConfig {
            contract_name: contract_name.to_string(),
            function_name: function_name.to_string(),
            output_type: EvmType::Uint256,
            start_block: None,
        };

        let stale_but_key_aligned = vec![EventCallResult {
            block_number: 150,
            block_timestamp: 1,
            log_index: 42,
            target_address: [0x11; 20],
            value: uint256_bytes(2),
            is_reverted: false,
            revert_reason: None,
        }];
        process_event_calls(
            &stale_but_key_aligned,
            range_start,
            range_end,
            &config,
            &decoded_dir,
            None,
            "test",
            "catchup",
        )
        .await
        .unwrap();

        let decoded_path = decoded_dir
            .join(contract_name)
            .join(function_name)
            .join("on_events")
            .join("100-199.parquet");
        assert_eq!(first_log_index(&decoded_path), 42);
        assert_eq!(first_utf8_value(&decoded_path, "decoded_value"), "2");

        catchup_decode_eth_calls(
            &raw_dir,
            &decoded_dir,
            &[],
            &[],
            std::slice::from_ref(&config),
            &raw_config(),
            None,
            "test",
            true,
            None,
        )
        .await
        .unwrap();

        assert_eq!(first_log_index(&decoded_path), 42);
        assert_eq!(first_utf8_value(&decoded_path, "decoded_value"), "2");
    }
}
