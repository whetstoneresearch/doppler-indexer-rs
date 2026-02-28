//! Catchup phase for log decoding - processes existing raw log parquet files.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::decoding::logs::{
    process_logs, EventMatcher, LogDecodingError,
};
use crate::raw_data::historical::factories::RecollectRequest;
use crate::raw_data::historical::receipts::LogData;
use crate::transformations::DecodedEventsMessage;
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

/// Catchup phase: decode existing raw log files
pub async fn catchup_decode_logs(
    raw_logs_dir: &Path,
    output_base: &Path,
    _range_size: u64,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    transform_tx: Option<&Sender<DecodedEventsMessage>>,
    recollect_tx: Option<&Sender<RecollectRequest>>,
) -> Result<(), LogDecodingError> {
    let concurrency = raw_data_config.decoding_concurrency.unwrap_or(4);

    // Scan existing decoded files
    let existing_decoded = Arc::new(scan_existing_decoded_files(output_base));

    // Scan raw log files
    let entries = std::fs::read_dir(raw_logs_dir)?;
    let mut raw_files: Vec<(u64, u64, PathBuf)> = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

        // Parse range from filename: logs_0-999.parquet
        if !file_name.starts_with("logs_") || !file_name.ends_with(".parquet") {
            continue;
        }

        let range_str = file_name
            .strip_prefix("logs_")
            .and_then(|s| s.strip_suffix(".parquet"))
            .unwrap_or("");

        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            continue;
        }

        if let (Ok(start), Ok(end)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
            raw_files.push((start, end + 1, path));
        }
    }

    raw_files.sort_by_key(|(start, _, _)| *start);

    tracing::info!(
        "Log decoding catchup: found {} raw log files to check",
        raw_files.len()
    );

    // Load factory addresses from factory parquet files for catchup
    let factory_addresses = Arc::new(load_factory_addresses_for_catchup(chain)?);

    let total_factory_addrs: usize = factory_addresses
        .values()
        .flat_map(|m| m.values())
        .map(|s| s.len())
        .sum();
    tracing::info!(
        "Log decoding catchup: loaded {} factory addresses across {} ranges",
        total_factory_addrs,
        factory_addresses.len()
    );

    // Filter files that need processing and compute per-range missing matchers
    let files_to_process: Vec<(u64, u64, PathBuf, Vec<EventMatcher>, HashMap<String, Vec<EventMatcher>>)> = raw_files
        .into_iter()
        .filter_map(|(range_start, range_end, path)| {
            let (missing_regular, missing_factory) = get_missing_matchers(
                range_start,
                range_end,
                regular_matchers,
                factory_matchers,
                &existing_decoded,
            );
            if missing_regular.is_empty() && missing_factory.is_empty() {
                tracing::debug!(
                    "Log decoding catchup: skipping range {}-{}, all events already decoded",
                    range_start,
                    range_end - 1
                );
                None
            } else {
                tracing::debug!(
                    "Log decoding catchup: range {}-{} has {} regular and {} factory matchers to decode",
                    range_start,
                    range_end - 1,
                    missing_regular.len(),
                    missing_factory.len()
                );
                Some((range_start, range_end, path, missing_regular, missing_factory))
            }
        })
        .collect();

    if files_to_process.is_empty() {
        tracing::info!("Log decoding catchup complete for chain {}", chain.name);
        return Ok(());
    }

    tracing::info!(
        "Log decoding catchup: processing {} files with concurrency {}",
        files_to_process.len(),
        concurrency
    );

    let output_base = Arc::new(output_base.to_path_buf());
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let transform_tx = transform_tx.cloned();

    let mut join_set = JoinSet::new();

    let recollect_tx = recollect_tx.cloned();

    for (range_start, range_end, file_path, missing_regular, missing_factory) in files_to_process {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let factory_addresses = factory_addresses.clone();
        let output_base = output_base.clone();
        let transform_tx = transform_tx.clone();
        let recollect_tx = recollect_tx.clone();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task completes

            tracing::debug!(
                "Log decoding catchup: processing range {}-{}",
                range_start,
                range_end - 1
            );

            // Read raw logs from parquet
            let file_path_for_read = file_path.clone();
            let logs = match read_logs_from_parquet(&file_path_for_read) {
                Ok(logs) => logs,
                Err(e) => {
                    tracing::warn!(
                        "Corrupted log file {}: {} - deleting and requesting recollection for range {}-{}",
                        file_path.display(),
                        e,
                        range_start,
                        range_end - 1
                    );

                    // Delete the corrupted file
                    if let Err(del_err) = std::fs::remove_file(&file_path) {
                        tracing::error!(
                            "Failed to delete corrupted log file {}: {}",
                            file_path.display(),
                            del_err
                        );
                    } else {
                        tracing::info!("Deleted corrupted log file: {}", file_path.display());
                    }

                    // Send recollect request
                    if let Some(tx) = recollect_tx {
                        let request = RecollectRequest {
                            range_start,
                            range_end,
                            file_path: file_path.clone(),
                        };
                        if let Err(send_err) = tx.send(request).await {
                            tracing::error!(
                                "Failed to send recollect request for range {}-{}: {}",
                                range_start,
                                range_end - 1,
                                send_err
                            );
                        }
                    }

                    return Ok(());
                }
            };

            // Get factory addresses for this range
            let factory_addrs = factory_addresses
                .get(&range_start)
                .cloned()
                .unwrap_or_default();

            process_logs(
                &logs,
                range_start,
                range_end,
                &missing_regular,
                &missing_factory,
                &factory_addrs,
                &output_base,
                transform_tx.as_ref(),
                None, // catchup already handles progress recording correctly
            )
            .await
        });
    }

    // Collect results and propagate errors
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(LogDecodingError::JoinError(e.to_string())),
        }
    }

    Ok(())
}

/// Get matchers whose decoded output files don't exist yet for a given range.
/// Returns only the matchers that still need decoding.
fn get_missing_matchers(
    range_start: u64,
    range_end: u64,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    existing: &HashSet<String>,
) -> (Vec<EventMatcher>, HashMap<String, Vec<EventMatcher>>) {
    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

    let missing_regular: Vec<EventMatcher> = regular_matchers
        .iter()
        .filter(|matcher| {
            // Skip matchers that can't have events in this range
            if let Some(start) = matcher.start_block {
                if start >= range_end {
                    return false;
                }
            }
            // Check if output file is missing
            let rel_path = format!("{}/{}/{}", matcher.name, matcher.event_name, file_name);
            !existing.contains(&rel_path)
        })
        .cloned()
        .collect();

    let missing_factory: HashMap<String, Vec<EventMatcher>> = factory_matchers
        .iter()
        .filter_map(|(collection, matchers)| {
            let missing: Vec<EventMatcher> = matchers
                .iter()
                .filter(|matcher| {
                    // Skip matchers that can't have events in this range
                    if let Some(start) = matcher.start_block {
                        if start >= range_end {
                            return false;
                        }
                    }
                    // Check if output file is missing
                    let rel_path =
                        format!("{}/{}/{}", matcher.name, matcher.event_name, file_name);
                    !existing.contains(&rel_path)
                })
                .cloned()
                .collect();
            if missing.is_empty() {
                None
            } else {
                Some((collection.clone(), missing))
            }
        })
        .collect();

    (missing_regular, missing_factory)
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

/// Load factory addresses from parquet files for catchup
fn load_factory_addresses_for_catchup(
    chain: &ChainConfig,
) -> Result<HashMap<u64, HashMap<String, HashSet<[u8; 20]>>>, LogDecodingError> {
    let mut result: HashMap<u64, HashMap<String, HashSet<[u8; 20]>>> = HashMap::new();

    // Look for factory parquet files
    let factories_dir = PathBuf::from(format!("data/derived/{}/factories", chain.name));
    if !factories_dir.exists() {
        return Ok(result);
    }

    // Scan factory directories
    if let Ok(entries) = std::fs::read_dir(&factories_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let collection_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();

            // Read parquet files in this collection
            if let Ok(files) = std::fs::read_dir(&path) {
                for file_entry in files.flatten() {
                    let file_path = file_entry.path();
                    if !file_path
                        .extension()
                        .map(|e| e == "parquet")
                        .unwrap_or(false)
                    {
                        continue;
                    }

                    // Parse range from filename
                    let file_name = file_path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("");
                    let range_str = file_name.strip_suffix(".parquet").unwrap_or("");
                    let parts: Vec<&str> = range_str.split('-').collect();
                    if parts.len() != 2 {
                        continue;
                    }

                    let range_start: u64 = match parts[0].parse() {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    // Read addresses from parquet
                    if let Ok(addresses) = read_factory_addresses_from_parquet(&file_path) {
                        result
                            .entry(range_start)
                            .or_default()
                            .entry(collection_name.clone())
                            .or_default()
                            .extend(addresses);
                    }
                }
            }
        }
    }

    Ok(result)
}

/// Read factory addresses from a factory parquet file
fn read_factory_addresses_from_parquet(path: &Path) -> Result<HashSet<[u8; 20]>, LogDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut addresses = HashSet::new();

    for batch_result in reader {
        let batch = batch_result?;

        // Look for "factory_address" column (the address of factory-created contracts)
        if let Some(col_idx) = batch.schema().index_of("factory_address").ok() {
            let col = batch.column(col_idx);
            if let Some(addr_array) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                for i in 0..addr_array.len() {
                    if !addr_array.is_null(i) {
                        let bytes = addr_array.value(i);
                        if bytes.len() == 20 {
                            let mut addr = [0u8; 20];
                            addr.copy_from_slice(bytes);
                            addresses.insert(addr);
                        }
                    }
                }
            }
        }
    }

    Ok(addresses)
}

/// Read logs from a raw parquet file
pub fn read_logs_from_parquet(path: &Path) -> Result<Vec<LogData>, LogDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut logs = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let tx_hash_idx = schema.index_of("transaction_hash").ok();
        let log_index_idx = schema.index_of("log_index").ok();
        let address_idx = schema.index_of("address").ok();
        let topics_idx = schema.index_of("topics").ok();
        let data_idx = schema.index_of("data").ok();

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

            let transaction_hash = tx_hash_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 32 {
                                let mut arr = [0u8; 32];
                                arr.copy_from_slice(bytes);
                                Some(alloy::primitives::B256::from(arr))
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let log_index = log_index_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let address = address_idx
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

            let topics = topics_idx
                .and_then(|i| {
                    use arrow::array::ListArray;
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .and_then(|list| {
                            let values = list.value(row);
                            values
                                .as_any()
                                .downcast_ref::<FixedSizeBinaryArray>()
                                .map(|topics_arr| {
                                    (0..topics_arr.len())
                                        .filter_map(|j| {
                                            let bytes = topics_arr.value(j);
                                            if bytes.len() == 32 {
                                                let mut arr = [0u8; 32];
                                                arr.copy_from_slice(bytes);
                                                Some(arr)
                                            } else {
                                                None
                                            }
                                        })
                                        .collect::<Vec<[u8; 32]>>()
                                })
                        })
                })
                .unwrap_or_default();

            let data = data_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|a| a.value(row).to_vec())
                })
                .unwrap_or_default();

            logs.push(LogData {
                block_number,
                block_timestamp,
                transaction_hash,
                log_index,
                address,
                topics,
                data,
            });
        }
    }

    Ok(logs)
}
