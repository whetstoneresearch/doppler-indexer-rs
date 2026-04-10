//! Shared helper functions for eth_call collection modules.
//!
//! These helpers extract common patterns duplicated across `once_calls.rs`,
//! `regular_calls.rs`, and `event_triggers.rs`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use alloy::primitives::Address;

use super::parquet_io::{
    read_once_column_index_async, read_once_parquet_state_async, write_once_column_index_async,
    write_once_results_to_parquet_async, OnceParquetState,
};
use super::types::{EthCallCollectionError, OnceCallConfig};
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::storage::contract_index::{
    get_missing_contracts, range_key, read_contract_index, ExpectedContracts,
};

/// Extract function names from a slice of `OnceCallConfig`.
///
/// Replaces the recurring pattern:
/// ```ignore
/// call_configs.iter().map(|c| c.function_name.clone()).collect()
/// ```
pub(crate) fn extract_fn_names(call_configs: &[OnceCallConfig]) -> Vec<String> {
    call_configs
        .iter()
        .map(|c| c.function_name.clone())
        .collect()
}

/// Parse the bare function name from a Solidity function signature.
///
/// Given `"balanceOf(address)"`, returns `"balanceOf"`.
/// Given `"totalSupply"`, returns `"totalSupply"`.
///
/// Replaces the recurring pattern:
/// ```ignore
/// call.function.split('(').next().unwrap_or(&call.function).to_string()
/// ```
pub(crate) fn parse_function_name(function_signature: &str) -> String {
    function_signature
        .split('(')
        .next()
        .unwrap_or(function_signature)
        .to_string()
}

/// Result of checking the parquet state for a regular (non-factory) once-call file.
///
/// When `check_regular_once_file_state` returns `None`, the caller should skip
/// this contract (all columns present and no nulls to patch).
pub(crate) struct OnceFileState {
    /// Function names that are missing from the existing parquet file.
    pub missing_fn_names: Vec<String>,
    /// Whether an existing file was found.
    pub has_existing_file: bool,
    /// Per-column null entries: fn_name -> set of addresses with null values.
    pub null_entries: HashMap<String, HashSet<[u8; 20]>>,
    /// Pre-read parquet state (batches for merge). Only `Some` when `has_existing_file` is true.
    pub parquet_state: Option<OnceParquetState>,
}

/// Check the parquet file state for a regular (non-factory) once-call file.
///
/// Reads the column index and parquet state in a single pass, determines which
/// columns are missing and which have null entries that need patching.
///
/// Returns `None` when the file exists and is already complete (skip this contract).
/// Returns `Some(OnceFileState)` when work needs to be done.
///
/// This encapsulates the pattern found in `process_once_calls_regular` and
/// `process_once_calls_multicall`.
pub(crate) async fn check_regular_once_file_state(
    rel_path: &str,
    file_name: &str,
    sub_dir: &Path,
    output_path: &Path,
    all_fn_names: &[String],
    existing_files: &HashSet<String>,
    contract_name: &str,
    range_start: u64,
    range_end: u64,
) -> Result<Option<OnceFileState>, EthCallCollectionError> {
    if existing_files.contains(rel_path) {
        let index = read_once_column_index_async(sub_dir.to_path_buf()).await;
        let mut state = read_once_parquet_state_async(output_path.to_path_buf()).await?;
        let existing_cols: HashSet<String> = if let Some(cols) = index.get(file_name) {
            cols.iter().cloned().collect()
        } else {
            tracing::debug!(
                "File {} exists but not in index, using schema from parquet state",
                output_path.display()
            );
            state.column_names.clone()
        };
        let missing: Vec<String> = all_fn_names
            .iter()
            .filter(|f| !existing_cols.contains(*f))
            .cloned()
            .collect();
        // Filter null entries to only existing (non-missing) columns
        let null_entries: HashMap<String, HashSet<[u8; 20]>> = state
            .null_entries
            .drain()
            .filter(|(k, _)| existing_cols.contains(k) && !missing.contains(k))
            .collect();
        if missing.is_empty() && null_entries.is_empty() {
            tracing::debug!(
                "Skipping once eth_calls for {} blocks {}-{} (all columns present, no nulls)",
                contract_name,
                range_start,
                range_end - 1
            );
            return Ok(None);
        }
        if !missing.is_empty() {
            tracing::info!(
                "Found {} missing once columns for {} blocks {}-{}: {:?}",
                missing.len(),
                contract_name,
                range_start,
                range_end - 1,
                missing
            );
        }
        if !null_entries.is_empty() {
            let null_count: usize = null_entries.values().map(|s| s.len()).sum();
            tracing::info!(
                "Found {} null entries across {} columns for {} blocks {}-{}, will re-fetch",
                null_count,
                null_entries.len(),
                contract_name,
                range_start,
                range_end - 1,
            );
        }
        Ok(Some(OnceFileState {
            missing_fn_names: missing,
            has_existing_file: true,
            null_entries,
            parquet_state: Some(state),
        }))
    } else {
        Ok(Some(OnceFileState {
            missing_fn_names: all_fn_names.to_vec(),
            has_existing_file: false,
            null_entries: HashMap::new(),
            parquet_state: None,
        }))
    }
}

/// Result of checking the parquet state for a factory once-call file.
pub(crate) struct FactoryOnceFileState {
    /// Function names that are missing from the existing parquet file.
    pub missing_fn_names: Vec<String>,
    /// Whether an existing file was found.
    pub has_existing_file: bool,
    /// Per-column null entries: fn_name -> set of addresses with null values.
    pub null_entries: HashMap<String, HashSet<[u8; 20]>>,
    /// Pre-read parquet state (batches for merge). Only `Some` when `has_existing_file` is true.
    pub parquet_state: Option<OnceParquetState>,
}

/// Check the parquet file state for a factory once-call file.
///
/// Similar to `check_regular_once_file_state` but uses a pre-loaded column
/// index map, checks contract-index completeness, and verifies row count
/// against factory address count.
///
/// Returns `None` when the file exists and is already complete (skip this collection).
/// Returns `Some(FactoryOnceFileState)` when work needs to be done.
///
/// This encapsulates the pattern found in `process_factory_once_calls` and
/// `process_factory_once_calls_multicall`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn check_factory_once_file_state(
    rel_path: &str,
    file_name: &str,
    sub_dir: &Path,
    output_path: &Path,
    all_fn_names: &[String],
    existing_files: &HashSet<String>,
    column_indexes: &HashMap<String, HashMap<String, Vec<String>>>,
    collection_name: &str,
    range: &super::types::BlockRange,
    factory_data: &FactoryAddressData,
    expected_contracts: Option<&HashMap<String, ExpectedContracts>>,
    repair: bool,
) -> Result<Option<FactoryOnceFileState>, EthCallCollectionError> {
    if existing_files.contains(rel_path) {
        let index = column_indexes.get(collection_name);
        let indexed_cols: HashSet<String> = index
            .and_then(|idx| idx.get(file_name))
            .map(|cols| cols.iter().cloned().collect())
            .unwrap_or_default();

        // Single read: get schema, null entries, row count, and batches
        let mut state = read_once_parquet_state_async(output_path.to_path_buf()).await?;
        let parquet_cols = &state.column_names;

        // Missing: in config but not in the parquet file
        let missing: Vec<String> = all_fn_names
            .iter()
            .filter(|f| !parquet_cols.contains(*f))
            .cloned()
            .collect();

        // Only try to fill null results if:
        // 1) there is no column index file (indexed_cols is empty)
        // 2) the block range file is not in the column index
        // 3) the column with nulls is not in the column index for that file
        // If a column IS in the index, nulls are considered permanent.
        let null_entries: HashMap<String, HashSet<[u8; 20]>> = {
            let has_unindexed = all_fn_names
                .iter()
                .any(|f| parquet_cols.contains(f) && !indexed_cols.contains(f));
            if !has_unindexed {
                HashMap::new()
            } else {
                state
                    .null_entries
                    .drain()
                    .filter(|(k, _)| !indexed_cols.contains(k))
                    .collect()
            }
        };

        if missing.is_empty() && null_entries.is_empty() {
            // Column check passes. Also verify contract-index completeness.
            let index_complete = match expected_contracts.and_then(|m| m.get(collection_name)) {
                None => true,
                Some(expected) => {
                    let ci = read_contract_index(sub_dir);
                    let rk = range_key(range.start, range.end - 1);
                    get_missing_contracts(&ci, &rk, expected).is_empty()
                }
            };
            let factory_addr_count = unique_factory_address_count(factory_data, collection_name);
            let rows_complete = factory_addr_count == 0 || state.row_count >= factory_addr_count;

            if index_complete && rows_complete {
                return Ok(None);
            }

            if !rows_complete {
                tracing::info!(
                    "Factory once {} blocks {}-{}: file has {} rows but factory has {} unique addresses, re-checking",
                    collection_name, range.start, range.end - 1,
                    state.row_count, factory_addr_count,
                );
            } else if !repair {
                return Ok(None);
            }
            if !index_complete {
                tracing::info!(
                    "Factory once {} blocks {}-{}: columns complete but contract index incomplete, re-checking",
                    collection_name,
                    range.start,
                    range.end - 1
                );
            }
        }
        if !missing.is_empty() {
            tracing::info!(
                "Found {} missing factory once columns for {} blocks {}-{}: {:?}",
                missing.len(),
                collection_name,
                range.start,
                range.end - 1,
                missing
            );
        }
        if !null_entries.is_empty() {
            let null_count: usize = null_entries.values().map(|s| s.len()).sum();
            tracing::info!(
                "Found {} null entries across {} unindexed factory columns for {} blocks {}-{}, will re-fetch",
                null_count,
                null_entries.len(),
                collection_name,
                range.start,
                range.end - 1,
            );
        }
        Ok(Some(FactoryOnceFileState {
            missing_fn_names: missing,
            has_existing_file: true,
            null_entries,
            parquet_state: Some(state),
        }))
    } else {
        Ok(Some(FactoryOnceFileState {
            missing_fn_names: all_fn_names.to_vec(),
            has_existing_file: false,
            null_entries: HashMap::new(),
            parquet_state: None,
        }))
    }
}

/// Read-update-write the once column index in a single operation.
///
/// Replaces the recurring three-line pattern:
/// ```ignore
/// let mut index = read_once_column_index_async(sub_dir.clone()).await;
/// index.insert(file_name.clone(), cols.clone());
/// write_once_column_index_async(sub_dir.to_path_buf(), index).await?;
/// ```
pub(crate) async fn update_column_index(
    sub_dir: &Path,
    file_name: &str,
    cols: &[String],
) -> Result<(), EthCallCollectionError> {
    let mut index = read_once_column_index_async(sub_dir.to_path_buf()).await;
    index.insert(file_name.to_string(), cols.to_vec());
    write_once_column_index_async(sub_dir.to_path_buf(), index).await
}

/// Create an empty once-call parquet file with the given schema and update the column index.
///
/// Replaces the recurring pattern:
/// ```ignore
/// tokio::fs::create_dir_all(&sub_dir).await?;
/// write_once_results_to_parquet_async(vec![], output_path.clone(), all_fn_names.clone()).await?;
/// update_column_index(&sub_dir, &file_name, &all_fn_names).await?;
/// ```
pub(crate) async fn write_empty_once_file(
    sub_dir: &Path,
    output_path: &Path,
    file_name: &str,
    all_fn_names: &[String],
) -> Result<(), EthCallCollectionError> {
    tokio::fs::create_dir_all(sub_dir).await?;
    write_once_results_to_parquet_async(vec![], output_path.to_path_buf(), all_fn_names.to_vec())
        .await?;
    update_column_index(sub_dir, file_name, all_fn_names).await
}

/// Collect factory addresses grouped by collection name.
///
/// Replaces the recurring pattern:
/// ```ignore
/// let mut addresses_by_collection: HashMap<String, HashSet<Address>> = HashMap::new();
/// for addrs in factory_data.addresses_by_block.values() {
///     for (_, addr, collection_name) in addrs {
///         addresses_by_collection
///             .entry(collection_name.clone())
///             .or_default()
///             .insert(*addr);
///     }
/// }
/// ```
pub(crate) fn collect_factory_addresses_by_collection(
    factory_data: &FactoryAddressData,
) -> HashMap<String, HashSet<Address>> {
    let mut result: HashMap<String, HashSet<Address>> = HashMap::new();
    for addrs in factory_data.addresses_by_block.values() {
        for (_, addr, collection_name) in addrs {
            result
                .entry(collection_name.clone())
                .or_default()
                .insert(*addr);
        }
    }
    result
}

/// Count unique factory addresses for a given collection.
///
/// Used by factory once-call file state checks to verify row completeness.
pub(crate) fn unique_factory_address_count(
    factory_data: &FactoryAddressData,
    collection_name: &str,
) -> u64 {
    factory_data
        .addresses_by_block
        .values()
        .flat_map(|addrs| addrs.iter())
        .filter(|(_, _, coll)| coll == collection_name)
        .map(|(_, addr, _)| addr.0 .0)
        .collect::<HashSet<[u8; 20]>>()
        .len() as u64
}
