//! Factory address loading helpers and log range management.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::types::EthCallCollectionError;
use crate::storage::contract_index::{
    get_missing_contracts, range_key, read_contract_index, ExpectedContracts,
};
use crate::storage::paths::{parse_range_from_filename, raw_logs_dir};
use crate::storage::S3Manifest;

// Re-export from shared modules for callers that used these from this module.
pub(crate) use crate::storage::parquet_readers::read_event_trigger_log_batches_from_parquet;
pub(crate) use crate::storage::parquet_readers::read_raw_logs_from_parquet as read_logs_from_parquet;

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

/// Check if on_events output already exists for a range (locally or in S3)
///
/// When `expected_contracts` is provided, also checks the contract index for
/// completeness. Returns `false` if the file exists but the contract index
/// shows missing contracts (forcing re-processing).
pub(crate) fn event_output_exists(
    output_dir: &Path,
    contract_name: &str,
    function_name: &str,
    range_start: u64,
    range_end: u64,
    s3_manifest: Option<&S3Manifest>,
    expected_contracts: Option<&ExpectedContracts>,
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
                                // File exists, but check contract index if expected contracts provided
                                if let Some(expected) = expected_contracts {
                                    let index = read_contract_index(&sub_dir);
                                    let rk = range_key(file_start, file_end_inclusive);
                                    let missing = get_missing_contracts(&index, &rk, expected);
                                    if !missing.is_empty() {
                                        tracing::info!(
                                            "on_events {}.{} range {}-{}: file exists but contract index has missing contracts: {:?}",
                                            contract_name,
                                            function_name,
                                            file_start,
                                            file_end_inclusive,
                                            missing.keys().collect::<Vec<_>>()
                                        );
                                        return false;
                                    }
                                }
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
            if let Some(expected) = expected_contracts {
                // Only check local sidecar if it actually exists.
                // In S3-backed deployments with local files pruned, sub_dir won't exist;
                // treat the manifest hit as complete in that case.
                let index_path = sub_dir.join("contract_index.json");
                if index_path.exists() {
                    let index = read_contract_index(&sub_dir);
                    let rk = range_key(range_start, range_end - 1);
                    if !get_missing_contracts(&index, &rk, expected).is_empty() {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    false
}

/// Async wrapper for read_logs_from_parquet
#[allow(dead_code)]
pub(crate) async fn read_logs_from_parquet_async(
    file_path: PathBuf,
) -> Result<
    Vec<crate::raw_data::historical::receipts::LogData>,
    crate::storage::parquet_readers::ParquetReadError,
> {
    tokio::task::spawn_blocking(move || read_logs_from_parquet(&file_path))
        .await
        .map_err(|e| {
            crate::storage::parquet_readers::ParquetReadError::Io(std::io::Error::other(
                e.to_string(),
            ))
        })?
}

/// Async wrapper for read_event_trigger_log_batches_from_parquet
pub(crate) async fn read_event_trigger_log_batches_from_parquet_async(
    file_path: PathBuf,
) -> Result<Vec<RecordBatch>, crate::storage::parquet_readers::ParquetReadError> {
    tokio::task::spawn_blocking(move || read_event_trigger_log_batches_from_parquet(&file_path))
        .await
        .map_err(|e| {
            crate::storage::parquet_readers::ParquetReadError::Io(std::io::Error::other(
                e.to_string(),
            ))
        })?
}

/// Async wrapper for get_existing_log_ranges
pub(crate) async fn get_existing_log_ranges_async(
    chain_name: String,
    s3_manifest: Option<S3Manifest>,
) -> Vec<ExistingLogRange> {
    tokio::task::spawn_blocking(move || get_existing_log_ranges(&chain_name, s3_manifest.as_ref()))
        .await
        .unwrap_or_default()
}

/// Async wrapper for event_output_exists
pub(crate) async fn event_output_exists_async(
    output_dir: PathBuf,
    contract_name: String,
    function_name: String,
    range_start: u64,
    range_end: u64,
    s3_manifest: Option<Arc<S3Manifest>>,
    expected_contracts: Option<ExpectedContracts>,
) -> Result<bool, EthCallCollectionError> {
    tokio::task::spawn_blocking(move || {
        event_output_exists(
            &output_dir,
            &contract_name,
            &function_name,
            range_start,
            range_end,
            s3_manifest.as_deref(),
            expected_contracts.as_ref(),
        )
    })
    .await
    .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::TempDir;

    use crate::storage::contract_index::{range_key, write_contract_index, ExpectedContracts};
    use crate::storage::S3Manifest;

    #[test]
    fn test_event_output_exists_s3_no_local_sidecar() {
        let tmp = TempDir::new().unwrap();
        // Do NOT create any subdirectories — no on_events/ subdir.

        let mut manifest = S3Manifest::new();
        manifest.add_raw_eth_calls_granular("test_contract", "test_func/on_events", 0, 999);

        let mut expected: ExpectedContracts = HashMap::new();
        expected.insert("ContractX".to_string(), vec!["0xaaa".to_string()]);

        let result = event_output_exists(
            tmp.path(),
            "test_contract",
            "test_func",
            0,
            1000,
            Some(&manifest),
            Some(&expected),
        );

        assert!(
            result,
            "S3 manifest hit should be trusted when no local sidecar exists"
        );
    }

    #[test]
    fn test_event_output_exists_local_sidecar_present_missing_contracts() {
        let tmp = TempDir::new().unwrap();

        let on_events_dir = tmp
            .path()
            .join("test_contract")
            .join("test_func")
            .join("on_events");
        fs::create_dir_all(&on_events_dir).unwrap();

        // Write a contract_index.json with only ContractX (missing ContractY).
        let mut index = HashMap::new();
        let mut range_map = HashMap::new();
        range_map.insert("ContractX".to_string(), vec!["0xaaa".to_string()]);
        index.insert(range_key(0, 999), range_map);
        write_contract_index(&on_events_dir, &index).unwrap();

        // Create a dummy parquet file so the S3 path is what we're testing.
        fs::write(on_events_dir.join("0-999.parquet"), b"dummy").unwrap();

        let mut manifest = S3Manifest::new();
        manifest.add_raw_eth_calls_granular("test_contract", "test_func/on_events", 0, 999);

        let mut expected: ExpectedContracts = HashMap::new();
        expected.insert("ContractX".to_string(), vec!["0xaaa".to_string()]);
        expected.insert("ContractY".to_string(), vec!["0xbbb".to_string()]);

        let result = event_output_exists(
            tmp.path(),
            "test_contract",
            "test_func",
            0,
            1000,
            Some(&manifest),
            Some(&expected),
        );

        assert!(
            !result,
            "Should return false when local sidecar shows missing contracts"
        );
    }
}
