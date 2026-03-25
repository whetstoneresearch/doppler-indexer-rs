//! Factory address loading helpers and log range management.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::primitives::Address;
use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, UInt32Array, UInt64Array};

use super::types::{
    EthCallCollectionError, EventCallKey, EventTriggeredCallConfig, OnceCallConfig,
};
use crate::raw_data::historical::receipts::LogData;
use crate::storage::paths::{
    factories_dir as factories_dir_path, parse_range_from_filename, raw_logs_dir,
};
use crate::storage::{DataLoader, S3Manifest, StorageManager};

/// Load historical factory addresses from parquet files for event trigger catchup
/// This ensures factory addresses are known before processing historical event triggers
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

    let factories_dir = factories_dir_path(chain_name);
    let data_loader = DataLoader::new(
        storage_manager.map(|sm| sm.clone()),
        chain_name,
        PathBuf::from("data"),
    );

    // Collect files to process: local files + S3 manifest ranges
    let mut files_to_process: HashMap<String, Vec<PathBuf>> = HashMap::new();

    // First, scan local factory directories
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

            // Only load if this collection is needed for event triggers
            if !factory_collections.contains(&collection_name) {
                continue;
            }

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
                    files_to_process
                        .entry(collection_name.clone())
                        .or_default()
                        .push(file_path);
                }
            }
        }
    }

    // Add S3 manifest factory ranges that aren't already local
    if let Some(manifest) = s3_manifest {
        for collection_name in &factory_collections {
            if let Some(ranges) = manifest.factories.get(collection_name) {
                let collection_dir = factories_dir.join(collection_name);
                for &(start, end) in ranges {
                    let file_name = format!("{}-{}.parquet", start, end);
                    let file_path = collection_dir.join(&file_name);

                    // Only add if not already in the list
                    let already_present = files_to_process
                        .get(collection_name)
                        .map(|files| files.iter().any(|f| f == &file_path))
                        .unwrap_or(false);

                    if !already_present {
                        files_to_process
                            .entry(collection_name.clone())
                            .or_default()
                            .push(file_path);
                    }
                }
            }
        }
    }

    // Process all files, downloading from S3 if needed
    for (collection_name, file_paths) in files_to_process {
        for file_path in file_paths {
            // Ensure file is local (download from S3 if needed)
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
/// Returns a map of collection_name -> Vec<(address, block_number, timestamp)>
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

    let factories_dir = factories_dir_path(chain_name);
    let data_loader = DataLoader::new(
        storage_manager.map(|sm| sm.clone()),
        chain_name,
        PathBuf::from("data"),
    );

    // Collect files to process: local files + S3 manifest ranges
    let mut files_to_process: HashMap<String, Vec<PathBuf>> = HashMap::new();

    // First, scan local factory directories
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

            if !collections_needed.contains(&collection_name) {
                continue;
            }

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
                    files_to_process
                        .entry(collection_name.clone())
                        .or_default()
                        .push(file_path);
                }
            }
        }
    }

    // Add S3 manifest factory ranges that aren't already local
    if let Some(manifest) = s3_manifest {
        for collection_name in &collections_needed {
            if let Some(ranges) = manifest.factories.get(collection_name) {
                let collection_dir = factories_dir.join(collection_name);
                for &(start, end) in ranges {
                    let file_name = format!("{}-{}.parquet", start, end);
                    let file_path = collection_dir.join(&file_name);

                    // Only add if not already in the list
                    let already_present = files_to_process
                        .get(collection_name)
                        .map(|files| files.iter().any(|f| f == &file_path))
                        .unwrap_or(false);

                    if !already_present {
                        files_to_process
                            .entry(collection_name.clone())
                            .or_default()
                            .push(file_path);
                    }
                }
            }
        }
    }

    // Process all files, downloading from S3 if needed
    for (collection_name, file_paths) in files_to_process {
        for file_path in file_paths {
            // Ensure file is local (download from S3 if needed)
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

/// Read factory addresses with block number and timestamp from a parquet file
pub(crate) fn read_factory_addresses_with_blocks(
    path: &Path,
) -> Result<Vec<(Address, u64, u64)>, std::io::Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(builder) => match builder.build() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "Failed to build parquet reader for {}: {}",
                    path.display(),
                    e
                );
                return Ok(Vec::new());
            }
        },
        Err(e) => {
            tracing::warn!(
                "Failed to create parquet reader for {}: {}",
                path.display(),
                e
            );
            return Ok(Vec::new());
        }
    };

    let mut addresses = Vec::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                continue;
            }
        };

        let addr_col_idx = batch.schema().index_of("factory_address").ok();
        let block_col_idx = batch.schema().index_of("block_number").ok();
        let ts_col_idx = batch.schema().index_of("block_timestamp").ok();

        if addr_col_idx.is_none() || block_col_idx.is_none() {
            continue;
        }

        let addr_col = batch.column(addr_col_idx.unwrap());
        let block_col = batch.column(block_col_idx.unwrap());
        let ts_col = ts_col_idx.map(|i| batch.column(i));

        let addr_array = match addr_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(a) => a,
            None => continue,
        };
        let block_array = match block_col.as_any().downcast_ref::<UInt64Array>() {
            Some(a) => a,
            None => continue,
        };
        let ts_array = ts_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        for i in 0..addr_array.len() {
            if addr_array.is_null(i) || block_array.is_null(i) {
                continue;
            }
            let bytes = addr_array.value(i);
            if bytes.len() != 20 {
                continue;
            }
            let mut addr_bytes = [0u8; 20];
            addr_bytes.copy_from_slice(bytes);
            let block_num = block_array.value(i);
            let timestamp = ts_array.map(|a| a.value(i)).unwrap_or(0);
            addresses.push((Address::from(addr_bytes), block_num, timestamp));
        }
    }

    Ok(addresses)
}

/// Read factory addresses from a parquet file
pub(crate) fn read_factory_addresses_from_parquet(
    path: &Path,
) -> Result<HashSet<[u8; 20]>, std::io::Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(builder) => match builder.build() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "Failed to build parquet reader for {}: {}",
                    path.display(),
                    e
                );
                return Ok(HashSet::new());
            }
        },
        Err(e) => {
            tracing::warn!(
                "Failed to create parquet reader for {}: {}",
                path.display(),
                e
            );
            return Ok(HashSet::new());
        }
    };

    let mut addresses = HashSet::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                continue;
            }
        };

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

/// Read logs from a parquet file for catchup
pub(crate) fn read_logs_from_parquet(
    file_path: &Path,
) -> Result<Vec<LogData>, EthCallCollectionError> {
    use arrow::array::{Array, ListArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .build()
        .map_err(|e| EthCallCollectionError::Parquet(e))?;

    let mut logs = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(EthCallCollectionError::Arrow)?;

        let block_numbers = batch
            .column_by_name("block_number")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let block_timestamps = batch
            .column_by_name("block_timestamp")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let tx_hashes = batch
            .column_by_name("transaction_hash")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        let log_indices = batch
            .column_by_name("log_index")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>());

        let addresses = batch
            .column_by_name("address")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        let topics_col = batch.column_by_name("topics");

        let data_col = batch
            .column_by_name("data")
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());

        let (
            Some(block_numbers),
            Some(block_timestamps),
            Some(tx_hashes),
            Some(log_indices),
            Some(addresses),
            Some(data_col),
        ) = (
            block_numbers,
            block_timestamps,
            tx_hashes,
            log_indices,
            addresses,
            data_col,
        )
        else {
            tracing::warn!(
                "Missing required columns in log parquet file: {:?}",
                file_path
            );
            continue;
        };

        for i in 0..batch.num_rows() {
            let block_number = block_numbers.value(i);
            let block_timestamp = block_timestamps.value(i);

            let mut tx_hash_bytes = [0u8; 32];
            tx_hash_bytes.copy_from_slice(tx_hashes.value(i));
            let transaction_hash = alloy::primitives::B256::from(tx_hash_bytes);

            let log_index = log_indices.value(i);

            let mut address = [0u8; 20];
            address.copy_from_slice(addresses.value(i));

            // Extract topics from the list column
            let topics: Vec<[u8; 32]> = if let Some(col) = topics_col {
                if let Some(list_array) = col.as_any().downcast_ref::<ListArray>() {
                    let values = list_array.value(i);
                    if let Some(fixed_array) =
                        values.as_any().downcast_ref::<FixedSizeBinaryArray>()
                    {
                        (0..fixed_array.len())
                            .map(|j| {
                                let mut topic = [0u8; 32];
                                topic.copy_from_slice(fixed_array.value(j));
                                topic
                            })
                            .collect()
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            let data = data_col.value(i).to_vec();

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
