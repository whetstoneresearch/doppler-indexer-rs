//! Parquet I/O, column indexes, and merge logic for eth_call collection.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::types::{CallResult, EthCallCollectionError, OnceCallResult};
use crate::storage::paths::scan_nested_parquet_files_2;

pub(crate) fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    scan_nested_parquet_files_2(dir)
}

pub(crate) async fn scan_existing_parquet_files_async(dir: PathBuf) -> HashSet<String> {
    tokio::task::spawn_blocking(move || scan_existing_parquet_files(&dir))
        .await
        .unwrap_or_default()
}

pub(crate) fn build_schema(num_params: usize) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("contract_address", DataType::FixedSizeBinary(20), false),
        Field::new("value", DataType::Binary, false),
    ];

    for i in 0..num_params {
        fields.push(Field::new(format!("param_{}", i), DataType::Binary, true));
    }

    Arc::new(Schema::new(fields))
}

pub(crate) fn write_results_to_parquet(
    results: &[CallResult],
    output_path: &Path,
    num_params: usize,
) -> Result<(), EthCallCollectionError> {
    let schema = build_schema(num_params);
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    if results.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            results.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    let arr: BinaryArray = results
        .iter()
        .map(|r| Some(r.value_bytes.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    for i in 0..num_params {
        let arr: BinaryArray = results
            .iter()
            .map(|r| r.param_values.get(i).map(|v| v.as_slice()))
            .collect();
        arrays.push(Arc::new(arr));
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

pub(crate) fn build_once_schema(function_names: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("contract_address", DataType::FixedSizeBinary(20), false),
    ];

    for fn_name in function_names {
        fields.push(Field::new(
            format!("{}_result", fn_name),
            DataType::Binary,
            true,
        ));
    }

    Arc::new(Schema::new(fields))
}

/// Read the column index sidecar file from a `once/` directory.
/// Returns a map of filename -> list of function names whose `{name}_result` columns are present.
pub(crate) fn read_once_column_index(once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> =
                serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No column index at {} ({}), starting fresh",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Write the column index sidecar file to a `once/` directory.
pub(crate) fn write_once_column_index(
    once_dir: &Path,
    index: &HashMap<String, Vec<String>>,
) -> Result<(), EthCallCollectionError> {
    let index_path = once_dir.join("column_index.json");
    tracing::debug!(
        "Writing column index to {}: {} files tracked",
        index_path.display(),
        index.len()
    );
    let content = serde_json::to_string_pretty(index)
        .map_err(|e| std::io::Error::other(format!("JSON serialize error: {}", e)))?;
    std::fs::write(&index_path, content)?;
    Ok(())
}

/// Load or build the column index for a once/ directory.
/// If the index file exists, load it. Otherwise, scan all parquet files to build it.
pub(crate) fn load_or_build_once_column_index(once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = once_dir.join("column_index.json");

    // Try to load existing index
    if index_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&index_path) {
            if let Ok(index) = serde_json::from_str::<HashMap<String, Vec<String>>>(&content) {
                tracing::debug!(
                    "Loaded column index from {}: {} files tracked",
                    index_path.display(),
                    index.len()
                );
                return index;
            }
        }
    }

    // Index doesn't exist or couldn't be loaded - build from parquet files
    if !once_dir.exists() {
        return HashMap::new();
    }

    let mut index = HashMap::new();
    let parquet_files: Vec<_> = match std::fs::read_dir(once_dir) {
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
        "Building column index for {} from {} parquet files",
        once_dir.display(),
        parquet_files.len()
    );

    for entry in parquet_files {
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let cols: Vec<String> = read_parquet_column_names(&path).into_iter().collect();
        if !cols.is_empty() {
            index.insert(file_name, cols);
        }
    }

    // Write the newly built index
    if !index.is_empty() {
        if let Err(e) = write_once_column_index(once_dir, &index) {
            tracing::warn!(
                "Failed to write column index to {}: {}",
                once_dir.display(),
                e
            );
        } else {
            tracing::info!(
                "Built and saved column index for {}: {} files tracked",
                once_dir.display(),
                index.len()
            );
        }
    }

    index
}

/// All state extracted from a single read of a once-call parquet file.
///
/// Eliminates redundant file opens by reading once and extracting all
/// validation info (schema, null positions, row count) plus the full
/// record batches in a single pass.
pub(crate) struct OnceParquetState {
    /// Function names extracted from schema (stripped `_result` suffix).
    pub column_names: HashSet<String>,
    /// Total row count across all batches.
    pub row_count: u64,
    /// Per-column null entries: fn_name -> set of addresses with null values.
    pub null_entries: HashMap<String, HashSet<[u8; 20]>>,
    /// The full record batches (use `.take()` when ownership is needed for merge).
    pub batches: Option<Vec<RecordBatch>>,
}

/// Read a once-call parquet file once, extracting all state needed for
/// validation and processing in a single pass.
pub(crate) fn read_once_parquet_state(
    path: &Path,
) -> Result<OnceParquetState, EthCallCollectionError> {
    let batches = read_existing_once_parquet(path)?;

    // Extract column names from schema
    let column_names = if let Some(first) = batches.first() {
        extract_column_names_from_schema(&first.schema())
    } else {
        HashSet::new()
    };

    // Compute row count
    let row_count = batches.iter().map(|b| b.num_rows() as u64).sum();

    // Extract null entries from batches
    let null_entries = extract_null_entries_from_batches(&batches);

    Ok(OnceParquetState {
        column_names,
        row_count,
        null_entries,
        batches: Some(batches),
    })
}

pub(crate) async fn read_once_parquet_state_async(
    path: PathBuf,
) -> Result<OnceParquetState, EthCallCollectionError> {
    tokio::task::spawn_blocking(move || read_once_parquet_state(&path))
        .await
        .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))?
}

/// Extract function names from a schema by stripping the `_result` suffix.
fn extract_column_names_from_schema(schema: &Schema) -> HashSet<String> {
    schema
        .fields()
        .iter()
        .filter_map(|f| f.name().strip_suffix("_result").map(|s| s.to_string()))
        .collect()
}

/// Extract per-address null entries from already-read batches.
fn extract_null_entries_from_batches(
    batches: &[RecordBatch],
) -> HashMap<String, HashSet<[u8; 20]>> {
    if batches.is_empty() {
        return HashMap::new();
    }
    let schema = batches[0].schema();
    let result_fn_names: Vec<String> = schema
        .fields()
        .iter()
        .filter_map(|f| f.name().strip_suffix("_result").map(|s| s.to_string()))
        .collect();
    if result_fn_names.is_empty() {
        return HashMap::new();
    }

    let mut null_entries: HashMap<String, HashSet<[u8; 20]>> = HashMap::new();
    for batch in batches {
        let address_col = match batch.column_by_name("contract_address") {
            Some(col) => col,
            None => continue,
        };
        let address_arr = match address_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(arr) => arr,
            None => continue,
        };
        for fn_name in &result_fn_names {
            let col_name = format!("{}_result", fn_name);
            let col = match batch.column_by_name(&col_name) {
                Some(c) => c,
                None => continue,
            };
            if col.null_count() == 0 {
                continue;
            }
            for i in 0..batch.num_rows() {
                if col.is_null(i) {
                    let addr_bytes: [u8; 20] = match address_arr.value(i).try_into() {
                        Ok(b) => b,
                        Err(_) => continue,
                    };
                    null_entries
                        .entry(fn_name.clone())
                        .or_default()
                        .insert(addr_bytes);
                }
            }
        }
    }
    null_entries
}

/// Read column names from a parquet file's schema (for fallback when index is missing).
/// Returns function names by stripping the `_result` suffix from column names.
pub(crate) fn read_parquet_column_names(path: &Path) -> HashSet<String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

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

/// Find per-address null entries in `_result` columns.
/// Returns a map of function_name -> set of addresses that have null values for that function.
/// Used to identify which specific (address, column) pairs need re-fetching.
#[allow(dead_code)]
pub(crate) fn find_null_entries(path: &Path) -> HashMap<String, HashSet<[u8; 20]>> {
    let batches = match read_existing_once_parquet(path) {
        Ok(b) => b,
        Err(_) => return HashMap::new(),
    };
    if batches.is_empty() {
        return HashMap::new();
    }
    let schema = batches[0].schema();
    let result_fn_names: Vec<String> = schema
        .fields()
        .iter()
        .filter_map(|f| f.name().strip_suffix("_result").map(|s| s.to_string()))
        .collect();
    if result_fn_names.is_empty() {
        return HashMap::new();
    }

    let mut null_entries: HashMap<String, HashSet<[u8; 20]>> = HashMap::new();
    for batch in &batches {
        let address_col = match batch.column_by_name("contract_address") {
            Some(col) => col,
            None => continue,
        };
        let address_arr = match address_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(arr) => arr,
            None => continue,
        };
        for fn_name in &result_fn_names {
            let col_name = format!("{}_result", fn_name);
            let col = match batch.column_by_name(&col_name) {
                Some(c) => c,
                None => continue,
            };
            // Skip column entirely if no nulls (O(1) check)
            if col.null_count() == 0 {
                continue;
            }
            for i in 0..batch.num_rows() {
                if col.is_null(i) {
                    let addr_bytes: [u8; 20] = match address_arr.value(i).try_into() {
                        Ok(b) => b,
                        Err(_) => continue,
                    };
                    null_entries
                        .entry(fn_name.clone())
                        .or_default()
                        .insert(addr_bytes);
                }
            }
        }
    }
    null_entries
}

/// Read an existing once-call parquet file and return all record batches.
pub(crate) fn read_existing_once_parquet(
    path: &Path,
) -> Result<Vec<RecordBatch>, EthCallCollectionError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    tracing::debug!("Reading existing once parquet from {}", path.display());
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let num_cols = batches.first().map(|b| b.num_columns()).unwrap_or(0);
    tracing::debug!(
        "Read {} batches ({} rows, {} columns) from {}",
        batches.len(),
        total_rows,
        num_cols,
        path.display()
    );
    Ok(batches)
}

/// Extract addresses from existing once-call parquet file.
/// Returns a map of address bytes -> (block_number, timestamp).
pub(crate) fn extract_addresses_from_once_parquet(
    batches: &[RecordBatch],
) -> HashMap<[u8; 20], (u64, u64)> {
    let mut addresses = HashMap::new();

    for batch in batches {
        let address_col = match batch.column_by_name("contract_address") {
            Some(col) => col,
            None => continue,
        };
        let address_arr = match address_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(arr) => arr,
            None => continue,
        };

        let block_col = batch.column_by_name("block_number");
        let timestamp_col = batch.column_by_name("block_timestamp");

        let block_arr = block_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let timestamp_arr = timestamp_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        for i in 0..batch.num_rows() {
            let addr_bytes: [u8; 20] = match address_arr.value(i).try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };

            let block_num = block_arr.map(|a| a.value(i)).unwrap_or(0);
            let timestamp = timestamp_arr.map(|a| a.value(i)).unwrap_or(0);

            addresses
                .entry(addr_bytes)
                .or_insert((block_num, timestamp));
        }
    }

    addresses
}

/// Extract existing non-null results for specific function columns from parquet.
/// Returns a map of address bytes -> function_name -> result bytes.
/// Only includes addresses that have non-null, non-empty data for at least one of the requested columns.
pub(crate) fn extract_existing_results_from_parquet(
    batches: &[RecordBatch],
    fn_names: &[String],
) -> HashMap<[u8; 20], HashMap<String, Vec<u8>>> {
    let mut results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = HashMap::new();

    for batch in batches {
        let address_col = match batch.column_by_name("contract_address") {
            Some(col) => col,
            None => continue,
        };
        let address_arr = match address_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(arr) => arr,
            None => continue,
        };

        // Get result columns for each function
        let result_columns: Vec<(&String, Option<&BinaryArray>)> = fn_names
            .iter()
            .map(|fn_name| {
                let col_name = format!("{}_result", fn_name);
                let col = batch
                    .column_by_name(&col_name)
                    .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());
                (fn_name, col)
            })
            .collect();

        for i in 0..batch.num_rows() {
            let addr_bytes: [u8; 20] = match address_arr.value(i).try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };

            for (fn_name, maybe_col) in &result_columns {
                if let Some(col) = maybe_col {
                    if !col.is_null(i) {
                        let value = col.value(i);
                        if !value.is_empty() {
                            results
                                .entry(addr_bytes)
                                .or_default()
                                .insert((*fn_name).clone(), value.to_vec());
                        }
                    }
                }
            }
        }
    }

    results
}

/// Merge new function result columns into existing once-call record batches.
/// Matches rows by the `address` column (FixedSizeBinary(20)).
/// Returns a single merged RecordBatch.
pub(crate) fn merge_once_columns(
    existing_batches: &[RecordBatch],
    new_results: &HashMap<[u8; 20], HashMap<String, Vec<u8>>>,
    new_fn_names: &[String],
) -> Result<RecordBatch, EthCallCollectionError> {
    tracing::debug!(
        "Merging {} new columns into existing data ({} addresses in new results)",
        new_fn_names.len(),
        new_results.len()
    );

    // Concatenate existing batches into one
    let existing = arrow::compute::concat_batches(&existing_batches[0].schema(), existing_batches)?;

    let num_rows = existing.num_rows();
    let address_col = existing
        .column_by_name("contract_address")
        .expect("existing once parquet must have contract_address column");
    let address_arr = address_col
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("contract_address column must be FixedSizeBinary(20)");

    tracing::debug!(
        "Existing data has {} rows, {} columns",
        num_rows,
        existing.num_columns()
    );

    // Build new columns by matching on address
    let mut new_columns: Vec<ArrayRef> = Vec::new();
    for fn_name in new_fn_names {
        let mut matched_count = 0;
        let arr: BinaryArray = (0..num_rows)
            .map(|i| {
                let addr_bytes: [u8; 20] = address_arr.value(i).try_into().unwrap();
                let result = new_results
                    .get(&addr_bytes)
                    .and_then(|m| m.get(fn_name))
                    .filter(|v| !v.is_empty())
                    .map(|v| v.as_slice());
                if result.is_some() {
                    matched_count += 1;
                }
                result
            })
            .collect();
        tracing::debug!(
            "Column {}_result: matched {}/{} addresses",
            fn_name,
            matched_count,
            num_rows
        );
        new_columns.push(Arc::new(arr));
    }

    // Build merged schema with cell-level merge for existing columns
    let existing_schema = existing.schema();

    // Map new fn_name -> index in new_columns
    let new_col_map: HashMap<String, usize> = new_fn_names
        .iter()
        .enumerate()
        .map(|(i, fn_name)| (format!("{}_result", fn_name), i))
        .collect();

    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut merged_new_indices: HashSet<usize> = HashSet::new();

    // Copy existing columns, doing cell-level merge where new data is available
    for (i, field) in existing_schema.fields().iter().enumerate() {
        if let Some(&new_idx) = new_col_map.get(field.name()) {
            // Column exists in both old and new: cell-level merge
            // Keep existing non-null values, fill nulls with new data
            let existing_col = existing.column(i);
            let new_col = &new_columns[new_idx];
            let existing_binary = existing_col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("existing result column must be Binary");
            let new_binary = new_col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("new result column must be Binary");
            let merged: BinaryArray = (0..existing_binary.len())
                .map(|row| {
                    if existing_binary.is_valid(row) {
                        Some(existing_binary.value(row))
                    } else if new_binary.is_valid(row) {
                        Some(new_binary.value(row))
                    } else {
                        None
                    }
                })
                .collect();
            tracing::debug!("Cell-level merged column: {}", field.name());
            fields.push(field.as_ref().clone());
            columns.push(Arc::new(merged));
            merged_new_indices.insert(new_idx);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(existing.column(i).clone());
        }
    }

    // Add truly new columns (not present in existing schema)
    for (i, fn_name) in new_fn_names.iter().enumerate() {
        if merged_new_indices.contains(&i) {
            continue;
        }
        fields.push(Field::new(
            format!("{}_result", fn_name),
            DataType::Binary,
            true,
        ));
        columns.push(new_columns[i].clone());
    }

    let merged_schema = Arc::new(Schema::new(fields));
    let merged = RecordBatch::try_new(merged_schema.clone(), columns)?;
    tracing::debug!(
        "Merged result: {} rows, {} columns (schema: {:?})",
        merged.num_rows(),
        merged.num_columns(),
        merged_schema
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );
    Ok(merged)
}

pub(crate) fn write_once_results_to_parquet(
    results: &[OnceCallResult],
    output_path: &Path,
    function_names: &[String],
) -> Result<(), EthCallCollectionError> {
    let schema = build_once_schema(function_names);
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    if results.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            results.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    for fn_name in function_names {
        let arr: BinaryArray = results
            .iter()
            .map(|r| {
                r.function_results
                    .get(fn_name)
                    .filter(|v| !v.is_empty())
                    .map(|v| v.as_slice())
            })
            .collect();
        arrays.push(Arc::new(arr));
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync helper: write a RecordBatch directly to a parquet file
// ---------------------------------------------------------------------------

pub(crate) fn write_record_batch_to_parquet(
    batch: &RecordBatch,
    output_path: &Path,
) -> Result<(), EthCallCollectionError> {
    crate::storage::atomic_write_parquet_fast(batch, output_path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Async wrappers: offload blocking file I/O to spawn_blocking
// ---------------------------------------------------------------------------

pub(crate) async fn write_results_to_parquet_async(
    results: Vec<CallResult>,
    output_path: PathBuf,
    num_params: usize,
) -> Result<(), EthCallCollectionError> {
    tokio::task::spawn_blocking(move || {
        write_results_to_parquet(&results, &output_path, num_params)
    })
    .await
    .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))?
}

pub(crate) async fn write_once_results_to_parquet_async(
    results: Vec<OnceCallResult>,
    output_path: PathBuf,
    function_names: Vec<String>,
) -> Result<(), EthCallCollectionError> {
    tokio::task::spawn_blocking(move || {
        write_once_results_to_parquet(&results, &output_path, &function_names)
    })
    .await
    .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))?
}

pub(crate) async fn write_record_batch_to_parquet_async(
    batch: RecordBatch,
    output_path: PathBuf,
) -> Result<(), EthCallCollectionError> {
    tokio::task::spawn_blocking(move || write_record_batch_to_parquet(&batch, &output_path))
        .await
        .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))?
}

#[allow(dead_code)]
pub(crate) async fn read_parquet_column_names_async(path: PathBuf) -> HashSet<String> {
    tokio::task::spawn_blocking(move || read_parquet_column_names(&path))
        .await
        .unwrap_or_default()
}

#[allow(dead_code)]
pub(crate) async fn read_parquet_row_count_async(path: PathBuf) -> u64 {
    tokio::task::spawn_blocking(move || {
        use parquet::file::reader::FileReader;
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return 0,
        };
        parquet::file::reader::SerializedFileReader::new(file)
            .map(|r| {
                let meta = r.metadata();
                (0..meta.num_row_groups())
                    .map(|i| meta.row_group(i).num_rows() as u64)
                    .sum()
            })
            .unwrap_or(0)
    })
    .await
    .unwrap_or(0)
}

#[allow(dead_code)]
pub(crate) async fn read_existing_once_parquet_async(
    path: PathBuf,
) -> Result<Vec<RecordBatch>, EthCallCollectionError> {
    tokio::task::spawn_blocking(move || read_existing_once_parquet(&path))
        .await
        .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))?
}

pub(crate) async fn load_or_build_once_column_index_async(
    once_dir: PathBuf,
) -> HashMap<String, Vec<String>> {
    tokio::task::spawn_blocking(move || load_or_build_once_column_index(&once_dir))
        .await
        .unwrap_or_default()
}

pub(crate) async fn read_once_column_index_async(
    once_dir: PathBuf,
) -> HashMap<String, Vec<String>> {
    tokio::task::spawn_blocking(move || read_once_column_index(&once_dir))
        .await
        .unwrap_or_default()
}

pub(crate) async fn write_once_column_index_async(
    once_dir: PathBuf,
    index: HashMap<String, Vec<String>>,
) -> Result<(), EthCallCollectionError> {
    tokio::task::spawn_blocking(move || write_once_column_index(&once_dir, &index))
        .await
        .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))?
}

#[allow(dead_code)]
pub(crate) async fn find_null_entries_async(path: PathBuf) -> HashMap<String, HashSet<[u8; 20]>> {
    tokio::task::spawn_blocking(move || find_null_entries(&path))
        .await
        .unwrap_or_default()
}
