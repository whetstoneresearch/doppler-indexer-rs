//! Composable factory-address loading functions built on the shared parquet readers.
//!
//! Provides three high-level loaders that replace duplicated factory-loading logic
//! previously scattered across `decoding/catchup/logs.rs`,
//! `raw_data/historical/eth_calls/factory.rs`, and `decoding/current/logs.rs`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::primitives::Address;

use crate::storage::parquet_readers::{
    read_factory_addresses_from_parquet, read_factory_addresses_with_blocks, ParquetReadError,
};
use crate::storage::paths::factories_dir as factories_dir_path;
use crate::storage::{DataLoader, S3Manifest, StorageManager};

/// Factory addresses grouped by range_start and collection name.
pub type FactoryAddressesByRange = HashMap<u64, HashMap<String, HashSet<[u8; 20]>>>;

/// Scan a factories directory and return files grouped by collection name.
///
/// Returns `HashMap<collection_name, Vec<PathBuf>>` where each collection is a
/// subdirectory under the factories dir containing `.parquet` files.
pub fn list_factory_parquet_files(factories_dir: &Path) -> HashMap<String, Vec<PathBuf>> {
    let mut result: HashMap<String, Vec<PathBuf>> = HashMap::new();

    let entries = match std::fs::read_dir(factories_dir) {
        Ok(e) => e,
        Err(_) => return result,
    };

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

        if let Ok(files) = std::fs::read_dir(&path) {
            for file_entry in files.flatten() {
                let file_path = file_entry.path();
                if file_path
                    .extension()
                    .map(|e| e == "parquet")
                    .unwrap_or(false)
                {
                    result
                        .entry(collection_name.clone())
                        .or_default()
                        .push(file_path);
                }
            }
        }
    }

    result
}

/// Augment a set of files-to-process with S3 manifest factory ranges that aren't already local.
///
/// This is a shared helper for the async factory loading functions.
fn augment_with_s3_ranges(
    files_to_process: &mut HashMap<String, Vec<PathBuf>>,
    factories_dir: &Path,
    collections_needed: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
) {
    let Some(manifest) = s3_manifest else {
        return;
    };

    for collection_name in collections_needed {
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

/// Load factory addresses as flat sets by collection. Async, S3-aware.
///
/// Replaces `raw_data/historical/eth_calls/factory.rs::load_historical_factory_addresses`.
/// Returns `HashMap<collection_name, HashSet<Address>>`.
pub async fn load_factory_addresses_by_collection(
    chain_name: &str,
    filter_collections: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> HashMap<String, HashSet<Address>> {
    let mut result: HashMap<String, HashSet<Address>> = HashMap::new();

    if filter_collections.is_empty() {
        return result;
    }

    let factories_dir = factories_dir_path(chain_name);
    let data_loader = DataLoader::new(storage_manager.cloned(), chain_name, PathBuf::from("data"));

    // Collect files to process: local files filtered by needed collections
    let all_local = list_factory_parquet_files(&factories_dir);
    let mut files_to_process: HashMap<String, Vec<PathBuf>> = all_local
        .into_iter()
        .filter(|(name, _)| filter_collections.contains(name))
        .collect();

    // Add S3 manifest ranges that aren't already local
    augment_with_s3_ranges(
        &mut files_to_process,
        &factories_dir,
        filter_collections,
        s3_manifest,
    );

    // Process all files, downloading from S3 if needed
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

/// Load factory addresses with block metadata. Async, S3-aware.
///
/// Replaces `raw_data/historical/eth_calls/factory.rs::load_factory_addresses_for_once_catchup`.
/// Returns `HashMap<collection_name, Vec<(Address, block_number, timestamp)>>`.
pub async fn load_factory_addresses_with_metadata(
    chain_name: &str,
    filter_collections: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> HashMap<String, Vec<(Address, u64, u64)>> {
    let mut result: HashMap<String, Vec<(Address, u64, u64)>> = HashMap::new();

    if filter_collections.is_empty() {
        return result;
    }

    let factories_dir = factories_dir_path(chain_name);
    let data_loader = DataLoader::new(storage_manager.cloned(), chain_name, PathBuf::from("data"));

    // Collect files to process: local files filtered by needed collections
    let all_local = list_factory_parquet_files(&factories_dir);
    let mut files_to_process: HashMap<String, Vec<PathBuf>> = all_local
        .into_iter()
        .filter(|(name, _)| filter_collections.contains(name))
        .collect();

    // Add S3 manifest ranges that aren't already local
    augment_with_s3_ranges(
        &mut files_to_process,
        &factories_dir,
        filter_collections,
        s3_manifest,
    );

    // Process all files, downloading from S3 if needed
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

/// Load factory addresses grouped by range_start and collection. Sync, local-only.
///
/// Replaces `decoding/catchup/logs.rs::load_factory_addresses_for_catchup`.
/// Returns `HashMap<range_start, HashMap<collection_name, HashSet<[u8; 20]>>>`.
pub fn load_factory_addresses_by_range(
    chain_name: &str,
) -> Result<FactoryAddressesByRange, ParquetReadError> {
    let mut result: FactoryAddressesByRange = HashMap::new();

    let factories_dir = factories_dir_path(chain_name);
    if !factories_dir.exists() {
        return Ok(result);
    }

    let all_files = list_factory_parquet_files(&factories_dir);

    for (collection_name, file_paths) in all_files {
        for file_path in file_paths {
            // Parse range from filename
            let file_name = file_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
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

    Ok(result)
}
