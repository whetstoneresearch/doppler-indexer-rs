use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, ListBuilder, FixedSizeBinaryBuilder, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;

use crate::raw_data::historical::receipts::LogData;
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::{LogField, RawDataCollectionConfig};

#[derive(Debug, Error)]
pub enum LogCollectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

#[derive(Debug, Clone)]
struct BlockRange {
    start: u64,
    end: u64,
}

impl BlockRange {
    fn file_name(&self) -> String {
        format!("logs_{}-{}.parquet", self.start, self.end - 1)
    }
}

#[derive(Debug)]
struct FullLogRecord {
    block_number: u64,
    block_timestamp: u64,
    transaction_hash: [u8; 32],
    log_index: u32,
    address: [u8; 20],
    topics: Vec<[u8; 32]>,
    data: Vec<u8>,
}

#[derive(Debug)]
struct MinimalLogRecord {
    block_number: u64,
    block_timestamp: u64,
    transaction_hash: [u8; 32],
    log_index: u32,
    address: [u8; 20],
    topics: Vec<[u8; 32]>,
    data: Vec<u8>,
}

pub async fn collect_logs(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    mut log_rx: Receiver<Vec<LogData>>,
) -> Result<(), LogCollectionError> {
    let output_dir = PathBuf::from(format!("data/raw/{}/logs", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let log_fields = &raw_data_config.fields.log_fields;
    let schema = build_log_schema(log_fields);

    // Track which ranges we've already written
    let existing_files = scan_existing_parquet_files(&output_dir);

    // Accumulate logs by range
    let mut range_data: HashMap<u64, Vec<LogData>> = HashMap::new();
    // Track which blocks we've seen in each range
    let mut range_blocks: HashMap<u64, HashSet<u64>> = HashMap::new();

    tracing::info!("Starting log collection for chain {}", chain.name);

    while let Some(logs) = log_rx.recv().await {
        for log in logs {
            let range_start = (log.block_number / range_size) * range_size;

            range_blocks
                .entry(range_start)
                .or_default()
                .insert(log.block_number);

            range_data.entry(range_start).or_default().push(log);
        }

        // Check for complete ranges and write them
        let complete_ranges: Vec<u64> = range_blocks
            .iter()
            .filter(|(range_start, blocks)| {
                let expected: HashSet<u64> =
                    (**range_start..**range_start + range_size).collect();
                expected.is_subset(blocks)
            })
            .map(|(range_start, _)| *range_start)
            .collect();

        for range_start in complete_ranges {
            let range = BlockRange {
                start: range_start,
                end: range_start + range_size,
            };

            // Skip if already exists
            if existing_files.contains(&range.file_name()) {
                tracing::info!(
                    "Skipping logs for blocks {}-{} (already exists)",
                    range.start,
                    range.end - 1
                );
                range_data.remove(&range_start);
                range_blocks.remove(&range_start);
                continue;
            }

            if let Some(logs) = range_data.remove(&range_start) {
                range_blocks.remove(&range_start);
                process_range(&range, logs, log_fields, &schema, &output_dir)?;
            }
        }
    }

    // Process any remaining partial ranges
    for (range_start, logs) in range_data {
        if logs.is_empty() {
            continue;
        }

        let max_block = logs.iter().map(|l| l.block_number).max().unwrap_or(range_start);
        let range = BlockRange {
            start: range_start,
            end: max_block + 1,
        };

        if existing_files.contains(&range.file_name()) {
            tracing::info!(
                "Skipping logs for blocks {}-{} (already exists)",
                range.start,
                range.end - 1
            );
            continue;
        }

        process_range(&range, logs, log_fields, &schema, &output_dir)?;
    }

    tracing::info!("Log collection complete for chain {}", chain.name);
    Ok(())
}

fn process_range(
    range: &BlockRange,
    logs: Vec<LogData>,
    log_fields: &Option<Vec<LogField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
) -> Result<(), LogCollectionError> {
    tracing::info!(
        "Writing {} logs for blocks {}-{}",
        logs.len(),
        range.start,
        range.end - 1
    );

    match log_fields {
        Some(fields) => {
            let records: Vec<MinimalLogRecord> = logs
                .into_iter()
                .map(|l| MinimalLogRecord {
                    block_number: l.block_number,
                    block_timestamp: l.block_timestamp,
                    transaction_hash: l.transaction_hash.0,
                    log_index: l.log_index,
                    address: l.address,
                    topics: l.topics,
                    data: l.data,
                })
                .collect();
            write_minimal_logs_to_parquet(&records, schema, fields, &output_dir.join(range.file_name()))?;
        }
        None => {
            let records: Vec<FullLogRecord> = logs
                .into_iter()
                .map(|l| FullLogRecord {
                    block_number: l.block_number,
                    block_timestamp: l.block_timestamp,
                    transaction_hash: l.transaction_hash.0,
                    log_index: l.log_index,
                    address: l.address,
                    topics: l.topics,
                    data: l.data,
                })
                .collect();
            write_full_logs_to_parquet(&records, schema, &output_dir.join(range.file_name()))?;
        }
    }

    tracing::info!(
        "Wrote logs to {}",
        output_dir.join(range.file_name()).display()
    );

    Ok(())
}

fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("logs_") && name.ends_with(".parquet") {
                    files.insert(name.to_string());
                }
            }
        }
    }
    files
}

fn build_log_schema(fields: &Option<Vec<LogField>>) -> Arc<Schema> {
    match fields {
        Some(log_fields) => {
            let mut arrow_fields = Vec::new();

            for field in log_fields {
                match field {
                    LogField::BlockNumber => {
                        arrow_fields.push(Field::new("block_number", DataType::UInt64, false));
                    }
                    LogField::BlockTimestamp => {
                        arrow_fields.push(Field::new("block_timestamp", DataType::UInt64, false));
                    }
                    LogField::TransactionHash => {
                        arrow_fields.push(Field::new(
                            "transaction_hash",
                            DataType::FixedSizeBinary(32),
                            false,
                        ));
                    }
                    LogField::LogIndex => {
                        arrow_fields.push(Field::new("log_index", DataType::UInt32, false));
                    }
                    LogField::Address => {
                        arrow_fields.push(Field::new(
                            "address",
                            DataType::FixedSizeBinary(20),
                            false,
                        ));
                    }
                    LogField::Topics => {
                        arrow_fields.push(Field::new(
                            "topics",
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::FixedSizeBinary(32),
                                false,
                            ))),
                            false,
                        ));
                    }
                    LogField::Data => {
                        arrow_fields.push(Field::new("data", DataType::Binary, false));
                    }
                }
            }

            Arc::new(Schema::new(arrow_fields))
        }
        None => Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("transaction_hash", DataType::FixedSizeBinary(32), false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("address", DataType::FixedSizeBinary(20), false),
            Field::new(
                "topics",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeBinary(32),
                    false,
                ))),
                false,
            ),
            Field::new("data", DataType::Binary, false),
        ])),
    }
}

fn write_minimal_logs_to_parquet(
    records: &[MinimalLogRecord],
    schema: &Arc<Schema>,
    fields: &[LogField],
    output_path: &Path,
) -> Result<(), LogCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for field in fields {
        match field {
            LogField::BlockNumber => {
                let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
                arrays.push(Arc::new(arr));
            }
            LogField::BlockTimestamp => {
                let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
                arrays.push(Arc::new(arr));
            }
            LogField::TransactionHash => {
                let arr = FixedSizeBinaryArray::try_from_iter(
                    records.iter().map(|r| r.transaction_hash.as_slice()),
                )?;
                arrays.push(Arc::new(arr));
            }
            LogField::LogIndex => {
                let arr: UInt32Array = records.iter().map(|r| Some(r.log_index)).collect();
                arrays.push(Arc::new(arr));
            }
            LogField::Address => {
                let arr = FixedSizeBinaryArray::try_from_iter(
                    records.iter().map(|r| r.address.as_slice()),
                )?;
                arrays.push(Arc::new(arr));
            }
            LogField::Topics => {
                let arr = build_topics_array(records)?;
                arrays.push(Arc::new(arr));
            }
            LogField::Data => {
                let arr: BinaryArray = records
                    .iter()
                    .map(|r| Some(r.data.as_slice()))
                    .collect();
                arrays.push(Arc::new(arr));
            }
        }
    }

    write_parquet(arrays, schema, output_path)
}

fn write_full_logs_to_parquet(
    records: &[FullLogRecord],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), LogCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.transaction_hash.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    let arr: UInt32Array = records.iter().map(|r| Some(r.log_index)).collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.address.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    // Build topics list array for full records
    let mut list_builder = ListBuilder::new(FixedSizeBinaryBuilder::new(32));
    for record in records {
        for topic in &record.topics {
            list_builder.values().append_value(topic.as_slice())?;
        }
        list_builder.append(true);
    }
    arrays.push(Arc::new(list_builder.finish()));

    let arr: BinaryArray = records
        .iter()
        .map(|r| Some(r.data.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    write_parquet(arrays, schema, output_path)
}

fn build_topics_array(records: &[MinimalLogRecord]) -> Result<arrow::array::ListArray, arrow::error::ArrowError> {
    let mut list_builder = ListBuilder::new(FixedSizeBinaryBuilder::new(32));
    for record in records {
        for topic in &record.topics {
            list_builder.values().append_value(topic.as_slice())?;
        }
        list_builder.append(true);
    }
    Ok(list_builder.finish())
}

fn write_parquet(
    arrays: Vec<ArrayRef>,
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), LogCollectionError> {
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
