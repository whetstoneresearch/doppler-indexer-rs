//! Shared factory directory scanning for loading factory addresses.
//!
//! Several subsystems need to enumerate factory parquet files across local storage
//! and (optionally) S3. This module extracts the duplicated scanning logic into a
//! single `scan_factory_files` function that the callers compose with their own
//! per-file reader.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::paths::factories_dir;
use crate::storage::{DataLoader, S3Manifest, StorageManager};

/// Scan factory directories (local + optional S3) and return parquet file paths
/// grouped by collection name.
///
/// - `chain_name`: used to build the local `data/{chain}/historical/factories` path.
/// - `collections_filter`: when `Some`, only collections in the set are returned.
///   When `None`, all collections found on disk are returned.
/// - `s3_manifest`: when `Some`, factory ranges from the manifest that are not
///   already present locally are added to the result.
/// - `storage_manager`: passed through to `DataLoader` for potential S3 downloads.
///
/// After calling this function, each file path may or may not exist locally.
/// Callers should use `DataLoader::ensure_local()` before reading.
pub async fn scan_factory_files(
    chain_name: &str,
    collections_filter: Option<&HashSet<String>>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> (HashMap<String, Vec<PathBuf>>, DataLoader) {
    let fdir = factories_dir(chain_name);
    let data_loader = DataLoader::new(storage_manager.cloned(), chain_name, PathBuf::from("data"));

    let mut files: HashMap<String, Vec<PathBuf>> = HashMap::new();

    // 1. Scan local factory directories
    if let Ok(entries) = std::fs::read_dir(&fdir) {
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

            // Apply filter if provided
            if let Some(filter) = collections_filter {
                if !filter.contains(&collection_name) {
                    continue;
                }
            }

            if let Ok(dir_entries) = std::fs::read_dir(&path) {
                for file_entry in dir_entries.flatten() {
                    let file_path = file_entry.path();
                    if file_path
                        .extension()
                        .map(|e| e == "parquet")
                        .unwrap_or(false)
                    {
                        files
                            .entry(collection_name.clone())
                            .or_default()
                            .push(file_path);
                    }
                }
            }
        }
    }

    // 2. Add S3 manifest factory ranges not already present locally
    if let Some(manifest) = s3_manifest {
        let collections_to_check: Vec<&String> = match collections_filter {
            Some(filter) => filter.iter().collect(),
            None => manifest.factories.keys().collect(),
        };

        for collection_name in collections_to_check {
            if let Some(ranges) = manifest.factories.get(collection_name) {
                let collection_dir = fdir.join(collection_name);
                for &(start, end) in ranges {
                    let file_name = format!("{}-{}.parquet", start, end);
                    let file_path = collection_dir.join(&file_name);

                    let already_present = files
                        .get(collection_name)
                        .map(|paths| paths.iter().any(|f| f == &file_path))
                        .unwrap_or(false);

                    if !already_present {
                        files
                            .entry(collection_name.clone())
                            .or_default()
                            .push(file_path);
                    }
                }
            }
        }
    }

    (files, data_loader)
}
