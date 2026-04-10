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
    build_expected_factory_contracts_for_range, get_missing_contracts, range_key,
    read_contract_index, update_contract_index, write_contract_index, ContractIndex,
};
use crate::storage::decoded_index::scan_existing_decoded_files;
use crate::storage::factory_data::load_factory_addresses_by_range;
use crate::storage::parquet_readers::read_raw_logs_from_parquet;
use crate::transformations::DecodedEventsMessage;
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::Contracts;
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
    let concurrency = raw_data_config.decoding_concurrency.unwrap_or(8);

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

    // Pre-load contract indexes for each factory collection
    let factory_collection_names: HashSet<String> = factory_matchers.keys().cloned().collect();
    let mut contract_indexes: HashMap<String, ContractIndex> = HashMap::new();
    for collection in &factory_collection_names {
        // Each factory collection's decoded logs use the same output_base/{collection}/{event}/ layout
        // We load one index per (collection, event) pair
        for matchers in factory_matchers
            .get(collection)
            .iter()
            .flat_map(|v| v.iter())
        {
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
                &chain.contracts,
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

    // Build (range_start, range_end) -> missing_factory lookup before spawning
    // (files_to_process is consumed by the spawn loop below)
    let factory_by_range: HashMap<(u64, u64), HashMap<String, Vec<EventMatcher>>> =
        files_to_process
            .iter()
            .map(|(rs, re, _, _, mf)| ((*rs, *re), mf.clone()))
            .collect();

    // Pre-load contract indexes for all (collection, event) pairs that will be written.
    // Re-use the already-loaded contract_indexes to avoid duplicate disk reads.
    let mut live_indexes: HashMap<(String, String), ContractIndex> = HashMap::new();
    for (_, _, _, _, missing_factory) in &files_to_process {
        for (collection, matchers) in missing_factory {
            for matcher in matchers {
                let key = (collection.clone(), matcher.event_name.clone());
                live_indexes.entry(key).or_insert_with(|| {
                    let index_key = format!("{}/{}", collection, matcher.event_name);
                    contract_indexes
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_default()
                });
            }
        }
    }

    let output_base = Arc::new(output_base.to_path_buf());
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let transform_tx = transform_tx.cloned();

    let mut join_set: JoinSet<Result<Option<(u64, u64)>, LogDecodingError>> = JoinSet::new();

    let recollect_tx = recollect_tx.cloned();

    let chain_name_arc = Arc::new(chain.name.clone());

    for (range_start, range_end, file_path, missing_regular, missing_factory) in files_to_process {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let factory_addresses = factory_addresses.clone();
        let output_base = output_base.clone();
        let transform_tx = transform_tx.clone();
        let recollect_tx = recollect_tx.clone();
        let chain_name = chain_name_arc.clone();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task completes

            tracing::debug!(
                "Log decoding catchup: processing range {}-{}",
                range_start,
                range_end - 1
            );

            // Read raw logs from parquet — use spawn_blocking to avoid starving
            // the tokio runtime with synchronous file I/O + decompression
            let file_path_for_read = file_path.clone();
            let logs = match tokio::task::spawn_blocking(move || {
                read_raw_logs_from_parquet(&file_path_for_read)
            })
            .await
            .expect("parquet read task panicked")
            {
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

                    return Ok(None);
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
                None, // contract index writes are done serially after drain (Fix 3)
                &chain_name,
                "catchup",
            )
            .await
            .map(|_| Some((range_start, range_end)))
        });
    }

    // Drain JoinSet with deferred error — collect processed ranges for serial index write
    let mut processed_ranges: Vec<(u64, u64)> = Vec::new();
    let mut first_error: Option<LogDecodingError> = None;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(Some((start, end)))) => {
                processed_ranges.push((start, end));
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => {
                first_error.get_or_insert(e);
            }
            Err(e) => {
                first_error.get_or_insert(LogDecodingError::JoinError(e.to_string()));
            }
        }
    }

    // Serial contract index update — no concurrent R-M-W races (Fix 3)
    for (start, end) in &processed_ranges {
        let rk = range_key(*start, end - 1);
        if let Some(missing_factory) = factory_by_range.get(&(*start, *end)) {
            for (collection, matchers) in missing_factory {
                for matcher in matchers {
                    let key = (collection.clone(), matcher.event_name.clone());
                    if let Some(index) = live_indexes.get_mut(&key) {
                        let expected_for_range =
                            build_expected_factory_contracts_for_range(&chain.contracts, *end);
                        if let Some(expected) = expected_for_range.get(collection) {
                            update_contract_index(index, &rk, expected);
                        }
                    }
                }
            }
        }
    }

    // Write each index once — no per-range repeated R-M-W
    for ((collection, event_name), index) in &live_indexes {
        let dir = output_base.join(collection).join(event_name);
        if let Err(e) = write_contract_index(&dir, index) {
            first_error.get_or_insert(LogDecodingError::Io(e));
        }
    }

    if let Some(e) = first_error {
        return Err(e);
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
    contracts: &Contracts,
    contract_indexes: &HashMap<String, ContractIndex>,
) -> (Vec<EventMatcher>, HashMap<String, Vec<EventMatcher>>) {
    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let rk = range_key(range_start, range_end - 1);
    let expected_for_range = build_expected_factory_contracts_for_range(contracts, range_end);

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
                    if let Some(expected) = expected_for_range.get(collection) {
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
