//! Historical data reader for querying decoded parquet files.
//!
//! Provides access to previously decoded events and eth_calls from parquet files.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int32Array, Int64Array, Int8Array,
    StringArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::context::{DecodedCall, DecodedEvent, DecodedValue, HistoricalCallQuery, HistoricalEventQuery};
use super::error::TransformationError;

/// Reader for historical decoded data stored in parquet files.
pub struct HistoricalDataReader {
    chain_name: String,
    /// Base path for decoded data
    decoded_base: PathBuf,
    /// Cache of available file ranges per (source, event/function)
    /// Key: (source_name, event_or_function_name)
    /// Value: Vec<(range_start, range_end, file_path)>
    file_index: RwLock<HashMap<(String, String), Vec<(u64, u64, PathBuf)>>>,
}

impl HistoricalDataReader {
    /// Create a new historical data reader for a chain.
    pub fn new(chain_name: &str) -> Result<Self, TransformationError> {
        let decoded_base = PathBuf::from(format!("data/derived/{}/decoded", chain_name));

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

        // Index log files: data/derived/{chain}/decoded/logs/{source}/{event}/
        let logs_dir = self.decoded_base.join("logs");
        if logs_dir.exists() {
            self.index_directory(&logs_dir, &mut index)?;
        }

        // Index eth_call files: data/derived/{chain}/decoded/eth_calls/{source}/{function}/
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
        index: &mut HashMap<(String, String), Vec<(u64, u64, PathBuf)>>,
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

                // Find parquet files and extract ranges
                let mut files: Vec<(u64, u64, PathBuf)> = Vec::new();

                for file_entry in std::fs::read_dir(name_entry.path())? {
                    let file_entry = file_entry?;
                    let path = file_entry.path();

                    if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        // Parse range from filename like "decoded_0-9999.parquet"
                        if let Some(range) = parse_range_from_filename(&path) {
                            files.push((range.0, range.1, path));
                        }
                    }
                }

                // Sort by range start
                files.sort_by_key(|(start, _, _)| *start);

                let key = (source_name.clone(), name);
                index.insert(key, files);
            }
        }

        Ok(())
    }

    /// Query historical events from parquet files.
    pub async fn query_events(
        &self,
        query: HistoricalEventQuery,
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let mut results = Vec::new();

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

        for (source, event_name, file_path, _range_start, _range_end) in files {
            let events = tokio::task::spawn_blocking({
                let file_path = file_path.clone();
                let query = query.clone();
                let source = source.clone();
                let event_name = event_name.clone();
                move || read_events_from_parquet(&file_path, &query, &source, &event_name)
            })
            .await
            .map_err(|e| TransformationError::IoError(std::io::Error::other(e.to_string())))??;

            results.extend(events);

            if let Some(limit) = query.limit {
                if results.len() >= limit {
                    results.truncate(limit);
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Query historical call results from parquet files.
    pub async fn query_calls(
        &self,
        query: HistoricalCallQuery,
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let mut results = Vec::new();

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

        for (source, function_name, file_path, _range_start, _range_end) in files {
            let calls = tokio::task::spawn_blocking({
                let file_path = file_path.clone();
                let query = query.clone();
                let source = source.clone();
                let function_name = function_name.clone();
                move || read_calls_from_parquet(&file_path, &query, &source, &function_name)
            })
            .await
            .map_err(|e| TransformationError::IoError(std::io::Error::other(e.to_string())))??;

            results.extend(calls);

            if let Some(limit) = query.limit {
                if results.len() >= limit {
                    results.truncate(limit);
                    break;
                }
            }
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
    fn find_files_for_range(
        &self,
        index: &HashMap<(String, String), Vec<(u64, u64, PathBuf)>>,
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
                // Check for overlap: range intersects if range_end > from_block AND range_start < to_block
                if *range_end > from_block && *range_start < to_block {
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

/// Parse range from filename like "decoded_0-9999.parquet".
fn parse_range_from_filename(path: &Path) -> Option<(u64, u64)> {
    let filename = path.file_stem()?.to_str()?;

    // Try different filename patterns
    // Pattern 1: "decoded_0-9999"
    // Pattern 2: "0-9999"
    let parts: Vec<&str> = filename
        .trim_start_matches("decoded_")
        .split('-')
        .collect();

    if parts.len() == 2 {
        let start = parts[0].parse().ok()?;
        let end = parts[1].parse().ok()?;
        return Some((start, end));
    }

    None
}

/// Read events from a parquet file with filtering.
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
    let standard_cols = ["block_number", "block_timestamp", "transaction_hash", "log_index", "contract_address"];
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

    // Get result columns
    let standard_cols = ["block_number", "block_timestamp", "contract_address"];
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
                result.insert(field.name().clone(), value);
            }
        }

        calls.push(DecodedCall {
            block_number: block_numbers[row],
            block_timestamp: block_timestamps[row],
            contract_address: addresses[row],
            source_name: source_name.to_string(),
            function_name: function_name.to_string(),
            trigger_log_index: None,
            result,
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

fn get_bytes32_column(batch: &RecordBatch, name: &str) -> Result<Vec<[u8; 32]>, TransformationError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TransformationError::MissingColumn(name.to_string()))?;
    let arr = col
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TransformationError::TypeConversion(format!("{} is not FixedSizeBinary", name)))?;

    let mut result = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        let bytes: [u8; 32] = arr.value(i).try_into().map_err(|_| {
            TransformationError::TypeConversion(format!("{} value is not 32 bytes", name))
        })?;
        result.push(bytes);
    }
    Ok(result)
}

fn get_address_column(batch: &RecordBatch, name: &str) -> Result<Vec<[u8; 20]>, TransformationError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TransformationError::MissingColumn(name.to_string()))?;
    let arr = col
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TransformationError::TypeConversion(format!("{} is not FixedSizeBinary", name)))?;

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
    if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
        return Ok(Some(DecodedValue::Int8(arr.value(row))));
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

    // Unknown type, skip
    Ok(None)
}
