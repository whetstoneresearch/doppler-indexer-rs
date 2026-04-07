//! Shared column index management for decoded and raw eth_call parquet files.
//!
//! These utilities are used by both catchup and current/live decoding phases.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;

use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

use super::{CallDecodeConfig, EthCallDecodingError};
use crate::decoding::types::OnceCallResult;

/// Read "once" call results from parquet
pub fn read_once_calls_from_parquet(
    path: &Path,
    configs: &[&CallDecodeConfig],
) -> Result<Vec<OnceCallResult>, EthCallDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let address_idx = schema.index_of("contract_address").ok();

        // Find function result columns
        let function_indices: Vec<(String, Option<usize>)> = configs
            .iter()
            .map(|c| {
                let col_name = format!("{}_result", c.function_name);
                (c.function_name.clone(), schema.index_of(&col_name).ok())
            })
            .collect();

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

            let contract_address = address_idx
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

            let mut function_results = HashMap::new();
            for (function_name, idx_opt) in &function_indices {
                if let Some(idx) = idx_opt {
                    if let Some(arr) = batch.column(*idx).as_any().downcast_ref::<BinaryArray>() {
                        if !arr.is_null(row) {
                            function_results.insert(function_name.clone(), arr.value(row).to_vec());
                        }
                    }
                }
            }

            results.push(OnceCallResult {
                block_number,
                block_timestamp,
                contract_address,
                results: function_results,
            });
        }
    }

    Ok(results)
}

// ============================================================================
// Column Index Functions for Decoded Data
// ============================================================================

/// Read the column index sidecar file from a decoded `once/` directory.
/// Returns a map of filename -> list of decoded function names.
pub fn read_decoded_column_index(decoded_once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = decoded_once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> =
                serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read decoded column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No decoded column index at {} ({}), starting fresh",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Write the column index sidecar file to a decoded `once/` directory.
pub fn write_decoded_column_index(
    decoded_once_dir: &Path,
    index: &HashMap<String, Vec<String>>,
) -> Result<(), EthCallDecodingError> {
    let index_path = decoded_once_dir.join("column_index.json");
    tracing::debug!(
        "Writing decoded column index to {}: {} files tracked",
        index_path.display(),
        index.len()
    );
    let content = serde_json::to_string_pretty(index)
        .map_err(|e| std::io::Error::other(format!("JSON serialize error: {}", e)))?;
    std::fs::write(&index_path, content)?;
    Ok(())
}

/// Find decoded function names that have null values fillable from raw data.
///
/// For each column in the decoded parquet that has null values, checks whether
/// the corresponding raw parquet has non-null `{fn_name}_result` values at
/// those positions. Returns the set of function names that can be filled.
fn find_columns_with_fillable_nulls(decoded_path: &Path, raw_path: &Path) -> HashSet<String> {
    let mut fillable = HashSet::new();

    // Read decoded parquet
    let decoded_file = match File::open(decoded_path) {
        Ok(f) => f,
        Err(_) => return fillable,
    };
    let decoded_reader = match ParquetRecordBatchReaderBuilder::try_new(decoded_file) {
        Ok(b) => match b.build() {
            Ok(r) => r,
            Err(_) => return fillable,
        },
        Err(_) => return fillable,
    };
    let decoded_batches: Vec<RecordBatch> = decoded_reader.filter_map(|r| r.ok()).collect();
    if decoded_batches.is_empty() {
        return fillable;
    }
    let decoded_batch = &decoded_batches[0];
    let decoded_schema = decoded_batch.schema();

    let skip_columns: HashSet<&str> = ["block_number", "block_timestamp", "contract_address"]
        .into_iter()
        .collect();

    // Find columns with nulls, grouped by base function name
    let mut fn_null_positions: HashMap<String, Vec<usize>> = HashMap::new();
    for (col_idx, field) in decoded_schema.fields().iter().enumerate() {
        let name = field.name().as_str();
        if skip_columns.contains(name) {
            continue;
        }
        let col = decoded_batch.column(col_idx);
        if col.null_count() == 0 {
            continue;
        }
        let fn_name = name.split('.').next().unwrap_or(name).to_string();
        // Collect null row positions (only if not already tracked for this fn)
        let positions = fn_null_positions.entry(fn_name).or_default();
        for row in 0..col.len() {
            if col.is_null(row) && !positions.contains(&row) {
                positions.push(row);
            }
        }
    }

    if fn_null_positions.is_empty() {
        return fillable;
    }

    // Read raw parquet
    let raw_file = match File::open(raw_path) {
        Ok(f) => f,
        Err(_) => return fillable,
    };
    let raw_reader = match ParquetRecordBatchReaderBuilder::try_new(raw_file) {
        Ok(b) => match b.build() {
            Ok(r) => r,
            Err(_) => return fillable,
        },
        Err(_) => return fillable,
    };
    let raw_batches: Vec<RecordBatch> = raw_reader.filter_map(|r| r.ok()).collect();
    if raw_batches.is_empty() {
        return fillable;
    }
    let raw_batch = &raw_batches[0];
    let raw_schema = raw_batch.schema();

    // Build address -> row index lookup from decoded parquet
    let decoded_addr_idx = match decoded_schema.index_of("contract_address") {
        Ok(idx) => idx,
        Err(_) => return fillable,
    };
    let decoded_addr_arr = match decoded_batch
        .column(decoded_addr_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
    {
        Some(a) => a,
        None => return fillable,
    };

    // Build address -> row index lookup from raw parquet
    let raw_addr_idx = match raw_schema.index_of("contract_address") {
        Ok(idx) => idx,
        Err(_) => return fillable,
    };
    let raw_addr_arr = match raw_batch
        .column(raw_addr_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
    {
        Some(a) => a,
        None => return fillable,
    };
    let mut raw_addr_to_row: HashMap<Vec<u8>, usize> = HashMap::new();
    for i in 0..raw_addr_arr.len() {
        raw_addr_to_row.insert(raw_addr_arr.value(i).to_vec(), i);
    }

    // For each function with nulls, check if raw has non-null values
    for (fn_name, null_positions) in &fn_null_positions {
        let result_col_name = format!("{}_result", fn_name);
        let raw_col_idx = match raw_schema.index_of(&result_col_name) {
            Ok(idx) => idx,
            Err(_) => continue,
        };
        let raw_col = raw_batch.column(raw_col_idx);

        // Short-circuit: check if any null position in decoded has a non-null in raw
        for &row in null_positions {
            let addr = decoded_addr_arr.value(row).to_vec();
            if let Some(&raw_row) = raw_addr_to_row.get(&addr) {
                if !raw_col.is_null(raw_row) {
                    fillable.insert(fn_name.clone());
                    break;
                }
            }
        }
    }

    fillable
}

/// Load or build the column index for a decoded once/ directory.
/// If the index file exists, load it. Otherwise, scan all parquet files to build it.
/// When rebuilding from parquet, checks for columns with fillable nulls and excludes them
/// so that catchup will re-decode those columns from raw data.
pub fn load_or_build_decoded_column_index(
    decoded_once_dir: &Path,
    raw_once_dir: &Path,
) -> HashMap<String, Vec<String>> {
    let index_path = decoded_once_dir.join("column_index.json");

    // Try to load existing index
    if index_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&index_path) {
            if let Ok(index) = serde_json::from_str::<HashMap<String, Vec<String>>>(&content) {
                tracing::debug!(
                    "Loaded decoded column index from {}: {} files tracked",
                    index_path.display(),
                    index.len()
                );
                return index;
            }
        }
    }

    // Index doesn't exist or couldn't be loaded - build from parquet files
    if !decoded_once_dir.exists() {
        return HashMap::new();
    }

    let mut index = HashMap::new();
    let parquet_files: Vec<_> = match std::fs::read_dir(decoded_once_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .collect(),
        Err(_) => return HashMap::new(),
    };

    if parquet_files.is_empty() {
        return HashMap::new();
    }

    tracing::info!(
        "Building decoded column index for {} from {} parquet files",
        decoded_once_dir.display(),
        parquet_files.len()
    );

    for entry in parquet_files {
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let cols: Vec<String> = read_decoded_parquet_function_names(&path)
            .into_iter()
            .collect();
        if !cols.is_empty() {
            // Check if any columns have nulls fillable from raw data
            let raw_path = raw_once_dir.join(&file_name);
            if raw_path.exists() {
                let fillable = find_columns_with_fillable_nulls(&path, &raw_path);
                if !fillable.is_empty() {
                    tracing::info!(
                        "Excluded {} columns with fillable nulls from index for {}: {:?}",
                        fillable.len(),
                        file_name,
                        fillable
                    );
                    let filtered: Vec<String> =
                        cols.into_iter().filter(|c| !fillable.contains(c)).collect();
                    if !filtered.is_empty() {
                        index.insert(file_name, filtered);
                    }
                    continue;
                }
            }
            index.insert(file_name, cols);
        }
    }

    // Write the newly built index
    if !index.is_empty() {
        if let Err(e) = write_decoded_column_index(decoded_once_dir, &index) {
            tracing::warn!(
                "Failed to write decoded column index to {}: {}",
                decoded_once_dir.display(),
                e
            );
        } else {
            tracing::info!(
                "Built and saved decoded column index for {}: {} files tracked",
                decoded_once_dir.display(),
                index.len()
            );
        }
    }

    index
}

/// Read function names from a decoded parquet file's schema.
/// Decoded columns are named like:
/// - `name` (simple type) -> returns `name`
/// - `getAssetData.numeraire` (tuple field) -> returns `getAssetData`
///
/// Excludes standard columns like block_number, block_timestamp, contract_address.
pub fn read_decoded_parquet_function_names(path: &Path) -> HashSet<String> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    // Standard columns to skip
    let skip_columns: HashSet<&str> = ["block_number", "block_timestamp", "contract_address"]
        .into_iter()
        .collect();

    for field in schema.columns() {
        let name = field.name();

        // Skip standard columns
        if skip_columns.contains(name) {
            continue;
        }

        // Extract base function name (part before '.' for tuple fields)
        let fn_name = name.split('.').next().unwrap_or(name);
        fn_names.insert(fn_name.to_string());
    }

    fn_names
}

/// Read the raw column index from a raw `once/` directory.
/// Returns a map of filename -> list of function names whose `{name}_result` columns exist.
pub fn read_raw_column_index(raw_once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = raw_once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> =
                serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read raw column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No raw column index at {} ({}), will scan parquet schemas",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Read function names from a raw parquet file's schema.
/// Raw columns are named like `{function_name}_result` -> returns `function_name`.
pub fn read_raw_parquet_function_names(path: &Path) -> HashSet<String> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    for field in schema.columns() {
        let name = field.name();
        // Extract function name from column name (e.g., "getAssetData_result" -> "getAssetData")
        if let Some(fn_name) = name.strip_suffix("_result") {
            fn_names.insert(fn_name.to_string());
        }
    }

    fn_names
}
