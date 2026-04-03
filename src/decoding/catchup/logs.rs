//! Catchup phase for log decoding - processes existing raw log parquet files.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::decoding::logs::{process_logs, EventMatcher, LogDecodingError};
use crate::decoding::types::{FileProcessingEntry, LogDecoderOutputs, LogMatcherConfig};
use crate::raw_data::historical::factories::RecollectRequest;
use crate::storage::contract_index::{
    build_expected_factory_contracts, get_missing_contracts, range_key, read_contract_index,
    ContractIndex, ExpectedContracts,
};
use crate::storage::decoded_index::scan_existing_decoded_files;
use crate::storage::factory_data::load_factory_addresses_by_range;
use crate::storage::parquet_readers::read_raw_logs_from_parquet;
use crate::transformations::DecodedEventsMessage;
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

/// Catchup phase: decode existing raw log files
pub async fn catchup_decode_logs(
    raw_logs_dir: &Path,
    output_base: &Path,
    matchers: &LogMatcherConfig<'_>,
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    transform_tx: Option<&Sender<DecodedEventsMessage>>,
    recollect_tx: Option<&Sender<RecollectRequest>>,
) -> Result<(), LogDecodingError> {
    let regular_matchers = matchers.regular_matchers;
    let factory_matchers = matchers.factory_matchers;
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
    let factory_addresses = Arc::new(load_factory_addresses_by_range(&chain.name)?);

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

    // Build expected contracts per collection for contract index checks
    let expected_by_collection = build_expected_factory_contracts(&chain.contracts);

    // Pre-load contract indexes for each factory collection
    let factory_collection_names: HashSet<String> =
        factory_matchers.keys().cloned().collect();
    let mut contract_indexes: HashMap<String, ContractIndex> = HashMap::new();
    for collection in &factory_collection_names {
        // Each factory collection's decoded logs use the same output_base/{collection}/{event}/ layout
        // We load one index per (collection, event) pair
        for matchers in factory_matchers.get(collection).iter().flat_map(|v| v.iter()) {
            let dir = output_base.join(collection).join(&matchers.event_name);
            let idx = {
                let dir_clone = dir.clone();
                tokio::task::spawn_blocking(move || read_contract_index(&dir_clone))
                    .await
                    .unwrap_or_default()
            };
            let key = format!("{}/{}", collection, matchers.event_name);
            contract_indexes.insert(key, idx);
        }
    }

    // Filter files that need processing and compute per-range missing matchers
    let files_to_process: Vec<FileProcessingEntry> = raw_files
        .into_iter()
        .filter_map(|(range_start, range_end, path)| {
            let (missing_regular, missing_factory) = get_missing_matchers(
                range_start,
                range_end,
                regular_matchers,
                factory_matchers,
                &existing_decoded,
                output_base,
                &expected_by_collection,
                &contract_indexes,
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
    let expected_by_collection = Arc::new(expected_by_collection);

    let mut join_set = JoinSet::new();

    let recollect_tx = recollect_tx.cloned();

    for (range_start, range_end, file_path, missing_regular, missing_factory) in files_to_process {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let factory_addresses = factory_addresses.clone();
        let output_base = output_base.clone();
        let transform_tx = transform_tx.clone();
        let recollect_tx = recollect_tx.clone();
        let expected_by_collection = expected_by_collection.clone();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task completes

            tracing::debug!(
                "Log decoding catchup: processing range {}-{}",
                range_start,
                range_end - 1
            );

            // Read raw logs from parquet
            let file_path_for_read = file_path.clone();
            let logs = match read_raw_logs_from_parquet(&file_path_for_read) {
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
                            _file_path: file_path.clone(),
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

            let matchers = LogMatcherConfig {
                regular_matchers: &missing_regular,
                factory_matchers: &missing_factory,
            };
            let outputs = LogDecoderOutputs {
                transform_tx: transform_tx.as_ref(),
                complete_tx: None, // catchup already handles progress recording correctly
            };
            process_logs(
                &logs,
                range_start,
                range_end,
                &matchers,
                &factory_addrs,
                &output_base,
                &outputs,
                Some(&expected_by_collection),
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
///
/// For factory matchers, even when the parquet file exists, the contract index
/// is checked for missing contracts/addresses. If the index shows gaps, the
/// matcher is included so the range gets reprocessed.
fn get_missing_matchers(
    range_start: u64,
    range_end: u64,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    existing: &HashSet<String>,
    _output_base: &Path,
    expected_by_collection: &HashMap<String, ExpectedContracts>,
    contract_indexes: &HashMap<String, ContractIndex>,
) -> (Vec<EventMatcher>, HashMap<String, Vec<EventMatcher>>) {
    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let rk = range_key(range_start, range_end - 1);

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
                    let rel_path = format!("{}/{}/{}", matcher.name, matcher.event_name, file_name);
                    if !existing.contains(&rel_path) {
                        return true; // File missing — needs processing
                    }

                    // File exists — check contract index for missing contracts
                    if let Some(expected) = expected_by_collection.get(collection) {
                        let index_key = format!("{}/{}", collection, matcher.event_name);
                        let index = contract_indexes
                            .get(&index_key)
                            .cloned()
                            .unwrap_or_default();
                        let missing = get_missing_contracts(&index, &rk, expected);
                        if !missing.is_empty() {
                            tracing::debug!(
                                "Range {} file exists for {}/{} but contract index has {} missing contracts",
                                rk,
                                collection,
                                matcher.event_name,
                                missing.len()
                            );
                            return true; // Index shows gaps — reprocess
                        }
                    }

                    false // File exists and index is complete
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
