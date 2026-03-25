//! Factory address loading helpers and log range management.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::primitives::Address;

use super::types::{
    EthCallCollectionError, EventCallKey, EventTriggeredCallConfig, OnceCallConfig,
};
use crate::raw_data::historical::receipts::LogData;
use crate::storage::parquet::factories::{
    read_factory_addresses_from_parquet, read_factory_addresses_with_blocks,
};
use crate::storage::paths::{parse_range_from_filename, raw_logs_dir};
use crate::storage::{S3Manifest, StorageManager};

/// Load historical factory addresses from parquet files for event trigger catchup.
///
/// This ensures factory addresses are known before processing historical event triggers.
/// Uses the shared `scan_factory_files` scanner then reads addresses from each file.
pub(crate) async fn load_historical_factory_addresses(
    chain_name: &str,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> HashMap<String, HashSet<Address>> {
    let mut result: HashMap<String, HashSet<Address>> = HashMap::new();

    // Get collection names that need factory addresses (is_factory = true)
    let factory_collections: HashSet<String> = event_call_configs
        .values()
        .flatten()
        .filter(|c| c.is_factory)
        .map(|c| c.contract_name.clone())
        .collect();

    if factory_collections.is_empty() {
        return result;
    }

    let (files_to_process, data_loader) = crate::storage::factory_loader::scan_factory_files(
        chain_name,
        Some(&factory_collections),
        s3_manifest,
        storage_manager,
    )
    .await;

    for (collection_name, file_paths) in files_to_process {
        for file_path in file_paths {
            match data_loader.ensure_local(&file_path).await {
                Ok(true) => {
                    if let Ok(addresses) = read_factory_addresses_from_parquet(&file_path) {
                        result
                            .entry(collection_name.clone())
                            .or_default()
                            .extend(addresses.into_iter().map(Address::from));
                    }
                }
                Ok(false) => {
                    tracing::debug!(
                        "Factory file {} not available locally or in S3",
                        file_path.display()
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to ensure factory file {} is local: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }
    }

    if !result.is_empty() {
        tracing::info!(
            "Loaded historical factory addresses for {} collections",
            result.len()
        );
        for (name, addrs) in &result {
            tracing::debug!("  {} - {} addresses", name, addrs.len());
        }
    }

    result
}

/// Load factory addresses with block numbers and timestamps for once-call catchup.
///
/// Uses the shared `scan_factory_files` scanner then reads addresses with block info.
pub(crate) async fn load_factory_addresses_for_once_catchup(
    chain_name: &str,
    factory_once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> HashMap<String, Vec<(Address, u64, u64)>> {
    let mut result: HashMap<String, Vec<(Address, u64, u64)>> = HashMap::new();

    let collections_needed: HashSet<String> = factory_once_configs.keys().cloned().collect();
    if collections_needed.is_empty() {
        return result;
    }

    let (files_to_process, data_loader) = crate::storage::factory_loader::scan_factory_files(
        chain_name,
        Some(&collections_needed),
        s3_manifest,
        storage_manager,
    )
    .await;

    for (collection_name, file_paths) in files_to_process {
        for file_path in file_paths {
            match data_loader.ensure_local(&file_path).await {
                Ok(true) => {
                    if let Ok(addresses) = read_factory_addresses_with_blocks(&file_path) {
                        result
                            .entry(collection_name.clone())
                            .or_default()
                            .extend(addresses);
                    }
                }
                Ok(false) => {
                    tracing::debug!(
                        "Factory file {} not available locally or in S3",
                        file_path.display()
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to ensure factory file {} is local: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }
    }

    if !result.is_empty() {
        tracing::info!(
            "Loaded factory addresses for once-call catchup: {} collections",
            result.len()
        );
        for (name, addrs) in &result {
            tracing::info!("  {} - {} addresses", name, addrs.len());
        }
    }

    result
}

/// Existing log range info for catchup
#[derive(Debug, Clone)]
pub(crate) struct ExistingLogRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) file_path: PathBuf,
}

/// Get existing log file ranges for catchup.
/// Also includes ranges from S3 manifest that aren't present locally.
pub(crate) fn get_existing_log_ranges(
    chain_name: &str,
    s3_manifest: Option<&S3Manifest>,
) -> Vec<ExistingLogRange> {
    let logs_dir = raw_logs_dir(chain_name);
    let mut ranges = Vec::new();
    let mut local_ranges: HashSet<(u64, u64)> = HashSet::new();

    let entries = match std::fs::read_dir(&logs_dir) {
        Ok(entries) => entries,
        Err(_) => {
            // No local directory - just use S3 ranges if available
            if let Some(manifest) = s3_manifest {
                for &(start, end) in &manifest.raw_logs {
                    let file_name = format!("logs_{}-{}.parquet", start, end);
                    let file_path = logs_dir.join(&file_name);
                    ranges.push(ExistingLogRange {
                        start,
                        end: end + 1, // Convert inclusive end to exclusive
                        file_path,
                    });
                }
                ranges.sort_by_key(|r| r.start);
            }
            return ranges;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.extension().map(|e| e == "parquet").unwrap_or(false) {
            continue;
        }

        let Some((start, end_inclusive)) = parse_range_from_filename(&path) else {
            continue;
        };

        let end = end_inclusive + 1; // Convert inclusive end to exclusive
        local_ranges.insert((start, end));
        ranges.push(ExistingLogRange {
            start,
            end,
            file_path: path,
        });
    }

    // Add S3-only ranges that aren't present locally
    if let Some(manifest) = s3_manifest {
        for &(start, end) in &manifest.raw_logs {
            let exclusive_end = end + 1; // S3 manifest uses inclusive end
            if !local_ranges.contains(&(start, exclusive_end)) {
                let file_name = format!("logs_{}-{}.parquet", start, end);
                let file_path = logs_dir.join(&file_name);
                ranges.push(ExistingLogRange {
                    start,
                    end: exclusive_end,
                    file_path,
                });
            }
        }
    }

    ranges.sort_by_key(|r| r.start);
    ranges
}

/// Read logs from a parquet file for catchup.
///
/// Delegates to the shared reader in `storage::parquet::logs`.
pub(crate) fn read_logs_from_parquet(
    file_path: &Path,
) -> Result<Vec<LogData>, EthCallCollectionError> {
    Ok(crate::storage::parquet::logs::read_logs_from_parquet(
        file_path,
    )?)
}

/// Check if on_events output already exists for a range (locally or in S3)
pub(crate) fn event_output_exists(
    output_dir: &Path,
    contract_name: &str,
    function_name: &str,
    range_start: u64,
    range_end: u64,
    s3_manifest: Option<&S3Manifest>,
) -> bool {
    // Check local first
    let sub_dir = output_dir
        .join(contract_name)
        .join(function_name)
        .join("on_events");
    if sub_dir.exists() {
        // Check for any file that overlaps with this range
        // Note: range_end is EXCLUSIVE (e.g., 200 means up to block 199)
        // Output files are named {min_block}-{max_block} where both are INCLUSIVE
        if let Ok(entries) = std::fs::read_dir(&sub_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    continue;
                }

                if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                    let parts: Vec<&str> = file_stem.split('-').collect();
                    if parts.len() == 2 {
                        if let (Ok(file_start), Ok(file_end_inclusive)) =
                            (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                        {
                            // Convert to exclusive end for consistent comparison
                            // Ranges overlap if: start1 < end2 AND start2 < end1 (using exclusive ends)
                            let file_end_exclusive = file_end_inclusive + 1;
                            if range_start < file_end_exclusive && file_start < range_end {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }

    // Check S3 manifest
    // The S3 manifest uses key format "contract/function/on_events" for event-triggered calls
    // range_end is exclusive but S3 manifest stores inclusive end
    if let Some(manifest) = s3_manifest {
        let func_key = format!("{}/on_events", function_name);
        if manifest.has_raw_eth_calls_granular(contract_name, &func_key, range_start, range_end - 1)
        {
            return true;
        }
    }

    false
}
