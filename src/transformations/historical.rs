//! Historical data reader for querying decoded parquet files.
//!
//! Provides access to previously decoded events and eth_calls from parquet files.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int16Array, Int32Array, Int64Array,
    Int8Array, ListArray, StringArray, StructArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::context::{
    DecodedCall, DecodedEvent, DecodedValue, HistoricalCallQuery, HistoricalEventQuery,
};
use super::error::TransformationError;
use crate::storage::paths::{decoded_base_dir, scan_parquet_ranges};

/// Index mapping (source_name, event_or_function_name) to available parquet file ranges.
type FileIndex = HashMap<(String, String), Vec<(u64, u64, PathBuf)>>;

/// Reader for historical decoded data stored in parquet files.
pub struct HistoricalDataReader {
    chain_name: String,
    /// Base path for decoded data
    decoded_base: PathBuf,
    /// Cache of available file ranges per (source, event/function)
    file_index: RwLock<FileIndex>,
}

impl HistoricalDataReader {
    /// Create a new historical data reader for a chain.
    pub fn new(chain_name: &str) -> Result<Self, TransformationError> {
        let decoded_base = decoded_base_dir(chain_name);

        let reader = Self {
            chain_name: chain_name.to_string(),
            decoded_base,
            file_index: RwLock::new(HashMap::new()),
        };

        // Build initial index if directory exists
        if reader.decoded_base.exists() {
            reader.rebuild_index()?;
        }

        Ok(reader)
    }

    /// Rebuild the file index by scanning the decoded directories.
    pub fn rebuild_index(&self) -> Result<(), TransformationError> {
        let mut index = self.file_index.write().unwrap();
        index.clear();

        // Index log files: data/{chain}/historical/decoded/logs/{source}/{event}/
        let logs_dir = self.decoded_base.join("logs");
        if logs_dir.exists() {
            self.index_directory(&logs_dir, &mut index)?;
        }

        // Index eth_call files: data/{chain}/historical/decoded/eth_calls/{source}/{function}/
        let calls_dir = self.decoded_base.join("eth_calls");
        if calls_dir.exists() {
            self.index_directory(&calls_dir, &mut index)?;
        }

        tracing::debug!(
            "Built historical data index for {} with {} entries",
            self.chain_name,
            index.len()
        );

        Ok(())
    }

    /// Index a directory structure.
    fn index_directory(
        &self,
        base: &Path,
        index: &mut FileIndex,
    ) -> Result<(), TransformationError> {
        // Iterate over source directories
        for source_entry in std::fs::read_dir(base)? {
            let source_entry = source_entry?;
            if !source_entry.file_type()?.is_dir() {
                continue;
            }
            let source_name = source_entry.file_name().to_string_lossy().to_string();

            // Iterate over event/function directories
            for name_entry in std::fs::read_dir(source_entry.path())? {
                let name_entry = name_entry?;
                if !name_entry.file_type()?.is_dir() {
                    continue;
                }
                let name = name_entry.file_name().to_string_lossy().to_string();

                // Find parquet files and extract ranges (already sorted by start)
                let files = scan_parquet_ranges(&name_entry.path())?;

                let key = (source_name.clone(), name);
                index.insert(key, files);
            }
        }

        Ok(())
    }

    /// Query historical events from parquet files.
    /// All matching files are read concurrently using `spawn_blocking`.
    #[allow(dead_code)]
    pub async fn query_events(
        &self,
        query: HistoricalEventQuery,
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        // Determine which files to read
        let files = {
            let index = self.file_index.read().unwrap();
            self.find_files_for_range(
                &index,
                query.source.as_deref(),
                query.event_name.as_deref(),
                query.from_block,
                query.to_block,
            )
        };

        let mut read_tasks = tokio::task::JoinSet::new();
        for (source, event_name, file_path, _range_start, _range_end) in files {
            let query = query.clone();
            read_tasks.spawn_blocking(move || {
                read_events_from_parquet(&file_path, &query, &source, &event_name)
            });
        }

        let mut results = Vec::new();
        while let Some(result) = read_tasks.join_next().await {
            let events = result.map_err(|e| {
                TransformationError::IoError(std::io::Error::other(e.to_string()))
            })??;
            results.extend(events);
        }

        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Query historical call results from parquet files.
    /// All matching files are read concurrently using `spawn_blocking`.
    #[allow(dead_code)]
    pub async fn query_calls(
        &self,
        query: HistoricalCallQuery,
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let files = {
            let index = self.file_index.read().unwrap();
            self.find_files_for_range(
                &index,
                query.source.as_deref(),
                query.function_name.as_deref(),
                query.from_block,
                query.to_block,
            )
        };

        let mut read_tasks = tokio::task::JoinSet::new();
        for (source, function_name, file_path, _range_start, _range_end) in files {
            let query = query.clone();
            read_tasks.spawn_blocking(move || {
                read_calls_from_parquet(&file_path, &query, &source, &function_name)
            });
        }

        let mut results = Vec::new();
        while let Some(result) = read_tasks.join_next().await {
            let calls = result.map_err(|e| {
                TransformationError::IoError(std::io::Error::other(e.to_string()))
            })??;
            results.extend(calls);
        }

        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Read all events from a specific parquet file (no filtering).
    /// Used by the transformation engine for catchup.
    pub fn read_events_from_file(
        &self,
        file_path: &Path,
        source_name: &str,
        event_name: &str,
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let file = std::fs::File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut events = Vec::new();

        for batch_result in reader {
            let batch = batch_result?;
            let batch_events = batch_to_events(batch, source_name, event_name)?;
            events.extend(batch_events);
        }

        Ok(events)
    }

    /// Read all calls from a specific parquet file (no filtering).
    /// Used by the transformation engine for call handler catchup.
    pub fn read_calls_from_file(
        &self,
        file_path: &Path,
        source_name: &str,
        function_name: &str,
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let file = std::fs::File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut calls = Vec::new();

        for batch_result in reader {
            let batch = batch_result?;
            let batch_calls = batch_to_calls(batch, source_name, function_name)?;
            calls.extend(batch_calls);
        }

        Ok(calls)
    }

    /// Find files that overlap with the requested block range.
    #[allow(dead_code)]
    fn find_files_for_range(
        &self,
        index: &FileIndex,
        source_filter: Option<&str>,
        name_filter: Option<&str>,
        from_block: u64,
        to_block: u64,
    ) -> Vec<(String, String, PathBuf, u64, u64)> {
        let mut files = Vec::new();

        for ((source, name), file_list) in index.iter() {
            // Apply filters
            if let Some(sf) = source_filter {
                if source != sf {
                    continue;
                }
            }
            if let Some(nf) = name_filter {
                if name != nf {
                    continue;
                }
            }

            // Find files that overlap with the requested range
            for (range_start, range_end, path) in file_list {
                // Filenames store inclusive end blocks, while queries are half-open.
                if *range_end >= from_block && *range_start < to_block {
                    files.push((
                        source.clone(),
                        name.clone(),
                        path.clone(),
                        *range_start,
                        *range_end,
                    ));
                }
            }
        }

        // Sort by range start
        files.sort_by_key(|(_, _, _, start, _)| *start);
        files
    }
}

/// Read events from a parquet file with filtering.
#[allow(dead_code)]
fn read_events_from_parquet(
    file_path: &Path,
    query: &HistoricalEventQuery,
    source: &str,
    event_name: &str,
) -> Result<Vec<DecodedEvent>, TransformationError> {
    let file = std::fs::File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut events = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let batch_events = batch_to_events(batch, source, event_name)?;

        for event in batch_events {
            // Apply filters
            let block_match =
                event.block_number >= query.from_block && event.block_number < query.to_block;
            let address_match = query
                .contract_address
                .map(|addr| event.contract_address == addr)
                .unwrap_or(true);

            if block_match && address_match {
                events.push(event);
            }
        }
    }

    Ok(events)
}

/// Read calls from a parquet file with filtering.
#[allow(dead_code)]
fn read_calls_from_parquet(
    file_path: &Path,
    query: &HistoricalCallQuery,
    source: &str,
    function_name: &str,
) -> Result<Vec<DecodedCall>, TransformationError> {
    let file = std::fs::File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut calls = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let batch_calls = batch_to_calls(batch, source, function_name)?;

        for call in batch_calls {
            let block_match =
                call.block_number >= query.from_block && call.block_number < query.to_block;
            let address_match = query
                .contract_address
                .map(|addr| call.contract_address == addr)
                .unwrap_or(true);

            if block_match && address_match {
                calls.push(call);
            }
        }
    }

    Ok(calls)
}

/// Convert a RecordBatch to DecodedEvents.
fn batch_to_events(
    batch: RecordBatch,
    source_name: &str,
    event_name: &str,
) -> Result<Vec<DecodedEvent>, TransformationError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Extract standard columns
    let block_numbers = get_u64_column(&batch, "block_number")?;
    let block_timestamps = get_u64_column(&batch, "block_timestamp")?;
    let tx_hashes = get_bytes32_column(&batch, "transaction_hash")?;
    let log_indices = get_u32_column(&batch, "log_index")?;
    let addresses = get_address_column(&batch, "contract_address")?;

    // Get parameter columns (everything except standard columns)
    let standard_cols = [
        "block_number",
        "block_timestamp",
        "transaction_hash",
        "log_index",
        "contract_address",
    ];
    let schema = batch.schema();
    let param_columns: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !standard_cols.contains(&f.name().as_str()))
        .collect();

    let mut events = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut params = HashMap::new();

        for (col_idx, field) in &param_columns {
            if let Some(value) = extract_value_from_batch(&batch, *col_idx, row)? {
                params.insert(field.name().clone(), value);
            }
        }

        events.push(DecodedEvent {
            block_number: block_numbers[row],
            block_timestamp: block_timestamps[row],
            transaction_hash: tx_hashes[row],
            log_index: log_indices[row],
            contract_address: addresses[row],
            source_name: source_name.to_string(),
            event_name: event_name.to_string(),
            event_signature: String::new(), // Not stored in parquet
            params,
        });
    }

    Ok(events)
}

/// Convert a RecordBatch to DecodedCalls.
fn batch_to_calls(
    batch: RecordBatch,
    source_name: &str,
    function_name: &str,
) -> Result<Vec<DecodedCall>, TransformationError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Extract standard columns
    let block_numbers = get_u64_column(&batch, "block_number")?;
    let block_timestamps = get_u64_column(&batch, "block_timestamp")?;
    let addresses = get_address_column(&batch, "contract_address")?;

    // Extract log_index if present (event-triggered calls have this column)
    let log_indices = batch
        .column_by_name("log_index")
        .map(|_| get_u32_column(&batch, "log_index"))
        .transpose()?;

    // Get result columns (exclude standard + log_index)
    let standard_cols = ["block_number", "block_timestamp", "contract_address", "log_index"];
    let schema = batch.schema();
    let result_columns: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !standard_cols.contains(&f.name().as_str()))
        .collect();

    let mut calls = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut result = HashMap::new();

        for (col_idx, field) in &result_columns {
            if let Some(value) = extract_value_from_batch(&batch, *col_idx, row)? {
                // Map generic "decoded_value" column to the function name for handler access
                let key = if field.name() == "decoded_value" {
                    function_name.to_string()
                } else {
                    field.name().clone()
                };
                result.insert(key, value);
            }
        }

        calls.push(DecodedCall {
            block_number: block_numbers[row],
            block_timestamp: block_timestamps[row],
            contract_address: addresses[row],
            source_name: source_name.to_string(),
            function_name: function_name.to_string(),
            trigger_log_index: log_indices.as_ref().map(|indices| indices[row]),
            result,
            is_reverted: false,
            revert_reason: None,
        });
    }

    Ok(calls)
}

// Helper functions to extract typed columns

fn get_u64_column(batch: &RecordBatch, name: &str) -> Result<Vec<u64>, TransformationError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TransformationError::MissingColumn(name.to_string()))?;
    let arr = col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| TransformationError::TypeConversion(format!("{} is not UInt64", name)))?;
    Ok(arr.values().to_vec())
}

fn get_u32_column(batch: &RecordBatch, name: &str) -> Result<Vec<u32>, TransformationError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TransformationError::MissingColumn(name.to_string()))?;
    let arr = col
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| TransformationError::TypeConversion(format!("{} is not UInt32", name)))?;
    Ok(arr.values().to_vec())
}

fn get_bytes32_column(
    batch: &RecordBatch,
    name: &str,
) -> Result<Vec<[u8; 32]>, TransformationError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TransformationError::MissingColumn(name.to_string()))?;
    let arr = col
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            TransformationError::TypeConversion(format!("{} is not FixedSizeBinary", name))
        })?;

    let mut result = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        let bytes: [u8; 32] = arr.value(i).try_into().map_err(|_| {
            TransformationError::TypeConversion(format!("{} value is not 32 bytes", name))
        })?;
        result.push(bytes);
    }
    Ok(result)
}

fn get_address_column(
    batch: &RecordBatch,
    name: &str,
) -> Result<Vec<[u8; 20]>, TransformationError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TransformationError::MissingColumn(name.to_string()))?;
    let arr = col
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            TransformationError::TypeConversion(format!("{} is not FixedSizeBinary", name))
        })?;

    let mut result = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        let bytes: [u8; 20] = arr.value(i).try_into().map_err(|_| {
            TransformationError::TypeConversion(format!("{} value is not 20 bytes", name))
        })?;
        result.push(bytes);
    }
    Ok(result)
}

/// Extract a value from a batch at a specific row.
fn extract_value_from_batch(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
) -> Result<Option<DecodedValue>, TransformationError> {
    let col = batch.column(col_idx);

    if col.is_null(row) {
        return Ok(None);
    }

    // Try different array types
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(DecodedValue::Uint64(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(DecodedValue::Int64(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(DecodedValue::Uint32(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(DecodedValue::Int32(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt16Array>() {
        return Ok(Some(DecodedValue::Uint32(arr.value(row) as u32)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
        return Ok(Some(DecodedValue::Int32(arr.value(row) as i32)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
        return Ok(Some(DecodedValue::Int8(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt8Array>() {
        return Ok(Some(DecodedValue::Uint8(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(DecodedValue::Bool(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(DecodedValue::String(arr.value(row).to_string())));
    }
    if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(DecodedValue::Bytes(arr.value(row).to_vec())));
    }
    if let Some(arr) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        let bytes = arr.value(row);
        return match bytes.len() {
            20 => {
                let addr: [u8; 20] = bytes.try_into().unwrap();
                Ok(Some(DecodedValue::Address(addr)))
            }
            32 => {
                let hash: [u8; 32] = bytes.try_into().unwrap();
                Ok(Some(DecodedValue::Bytes32(hash)))
            }
            _ => Ok(Some(DecodedValue::Bytes(bytes.to_vec()))),
        };
    }

    // List(Struct(...)) → Array of NamedTuple/UnnamedTuple
    if let Some(list_arr) = col.as_any().downcast_ref::<ListArray>() {
        let offsets = list_arr.offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let values = list_arr.values();

        if let Some(struct_arr) = values.as_any().downcast_ref::<StructArray>() {
            let mut elements = Vec::with_capacity(end - start);
            for i in start..end {
                elements.push(extract_struct_value(struct_arr, i)?);
            }
            return Ok(Some(DecodedValue::Array(elements)));
        }
    }

    // Standalone Struct → NamedTuple/UnnamedTuple
    if let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() {
        return Ok(Some(extract_struct_value(struct_arr, row)?));
    }

    // Unknown type - log warning so we don't silently drop data
    let schema = batch.schema();
    let field_name = schema.field(col_idx).name();
    let data_type = col.data_type();
    tracing::warn!(
        field = %field_name,
        data_type = ?data_type,
        "Unhandled Arrow data type in extract_value_from_batch - value will be dropped"
    );
    Ok(None)
}

/// Extract a single row from a StructArray into a DecodedValue.
/// Fields with numeric names ("0", "1", ...) produce UnnamedTuple, otherwise NamedTuple.
fn extract_struct_value(
    struct_arr: &StructArray,
    row: usize,
) -> Result<DecodedValue, TransformationError> {
    let fields = struct_arr.fields();
    let is_unnamed = fields.iter().all(|f| f.name().parse::<usize>().is_ok());

    let mut values = Vec::with_capacity(fields.len());
    for (col_idx, field) in fields.iter().enumerate() {
        let col = struct_arr.column(col_idx);
        let val = extract_value_from_array(col.as_ref(), row)?;
        values.push((field.name().clone(), val));
    }

    if is_unnamed {
        Ok(DecodedValue::UnnamedTuple(
            values.into_iter().map(|(_, v)| v).collect(),
        ))
    } else {
        Ok(DecodedValue::NamedTuple(values))
    }
}

/// Extract a value from an Arrow array at the given row index.
fn extract_value_from_array(
    col: &dyn Array,
    row: usize,
) -> Result<DecodedValue, TransformationError> {
    if col.is_null(row) {
        return Ok(DecodedValue::String("null".to_string()));
    }

    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(DecodedValue::Uint64(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(DecodedValue::Int64(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Ok(DecodedValue::Uint32(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(DecodedValue::Int32(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt16Array>() {
        return Ok(DecodedValue::Uint32(arr.value(row) as u32));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
        return Ok(DecodedValue::Int32(arr.value(row) as i32));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
        return Ok(DecodedValue::Int8(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt8Array>() {
        return Ok(DecodedValue::Uint8(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return Ok(DecodedValue::Bool(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(DecodedValue::String(arr.value(row).to_string()));
    }
    if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
        return Ok(DecodedValue::Bytes(arr.value(row).to_vec()));
    }
    if let Some(arr) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        let bytes = arr.value(row);
        return match bytes.len() {
            20 => {
                let addr: [u8; 20] = bytes.try_into().unwrap();
                Ok(DecodedValue::Address(addr))
            }
            32 => {
                let hash: [u8; 32] = bytes.try_into().unwrap();
                Ok(DecodedValue::Bytes32(hash))
            }
            _ => Ok(DecodedValue::Bytes(bytes.to_vec())),
        };
    }
    if let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() {
        return extract_struct_value(struct_arr, row);
    }
    if let Some(list_arr) = col.as_any().downcast_ref::<ListArray>() {
        let offsets = list_arr.offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let values = list_arr.values();
        let mut elements = Vec::with_capacity(end - start);
        for i in start..end {
            elements.push(extract_value_from_array(values.as_ref(), i)?);
        }
        return Ok(DecodedValue::Array(elements));
    }

    tracing::warn!(
        data_type = ?col.data_type(),
        "Unhandled Arrow data type in struct field extraction"
    );
    Ok(DecodedValue::String("unsupported".to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, FixedSizeBinaryArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{batch_to_events, HistoricalDataReader};
    use crate::types::decoded::DecodedValue;

    #[test]
    fn find_files_for_range_includes_inclusive_range_end() {
        let reader = HistoricalDataReader::new("missing-chain").unwrap();
        let mut index = HashMap::new();
        index.insert(
            ("v3".to_string(), "Swap".to_string()),
            vec![(0, 9999, "decoded_0-9999.parquet".into())],
        );

        let files = reader.find_files_for_range(&index, Some("v3"), Some("Swap"), 9999, 10000);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].3, 0);
        assert_eq!(files[0].4, 9999);
    }

    #[test]
    fn batch_to_events_excludes_contract_address_from_params() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("transaction_hash", DataType::FixedSizeBinary(32), false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("contract_address", DataType::FixedSizeBinary(20), false),
            Field::new("amount0", DataType::UInt64, true),
        ]));

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![7u64])),
            Arc::new(UInt64Array::from(vec![99u64])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(std::iter::once(&[0x11u8; 32][..])).unwrap(),
            ),
            Arc::new(UInt32Array::from(vec![3u32])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(std::iter::once(&[0x22u8; 20][..])).unwrap(),
            ),
            Arc::new(UInt64Array::from(vec![42u64])),
        ];

        let batch = RecordBatch::try_new(schema, arrays).unwrap();

        let events = batch_to_events(batch, "v3", "Swap").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].contract_address, [0x22u8; 20]);
        assert_eq!(events[0].params.len(), 1);
        assert!(matches!(
            events[0].params.get("amount0"),
            Some(DecodedValue::Uint64(42))
        ));
        assert!(!events[0].params.contains_key("contract_address"));
    }
}
