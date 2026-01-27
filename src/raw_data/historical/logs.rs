use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListBuilder, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;

use crate::raw_data::historical::factories::FactoryAddressData;
use crate::raw_data::historical::receipts::{LogData, LogMessage};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::raw_data::{LogField, RawDataCollectionConfig};

#[derive(Debug, Error)]
pub enum LogCollectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Task join error: {0}")]
    JoinError(String),
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
    mut log_rx: Receiver<LogMessage>,
    mut factory_rx: Option<Receiver<FactoryAddressData>>,
) -> Result<(), LogCollectionError> {
    let output_dir = PathBuf::from(format!("data/raw/{}/logs", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let log_fields = &raw_data_config.fields.log_fields;
    let schema = build_log_schema(log_fields);
    let contract_logs_only = raw_data_config.contract_logs_only.unwrap_or(false);

    let configured_addresses: HashSet<[u8; 20]> = if contract_logs_only {
        build_configured_addresses(&chain.contracts)
    } else {
        HashSet::new()
    };

    let existing_files = scan_existing_parquet_files(&output_dir);

    let mut range_data: HashMap<u64, Vec<LogData>> = HashMap::new();
    let mut range_factory_addresses: HashMap<u64, HashSet<[u8; 20]>> = HashMap::new();

    // Ranges that have received RangeComplete but are waiting for factory data
    let mut pending_ranges: HashMap<u64, u64> = HashMap::new(); // range_start -> range_end
    // Ranges that have received factory data
    let mut factory_ready: HashSet<u64> = HashSet::new();

    let has_factory_rx = factory_rx.is_some();
    let needs_factory_wait = has_factory_rx && contract_logs_only;

    tracing::info!(
        "Starting log collection for chain {} (contract_logs_only: {}, waiting for factories: {})",
        chain.name,
        contract_logs_only,
        needs_factory_wait
    );

    loop {
        tokio::select! {
            log_result = log_rx.recv() => {
                match log_result {
                    Some(message) => {
                        match message {
                            LogMessage::Logs(logs) => {
                                for log in logs {
                                    let range_start = (log.block_number / range_size) * range_size;
                                    range_data.entry(range_start).or_default().push(log);
                                }
                            }
                            LogMessage::RangeComplete { range_start, range_end } => {
                                // Check if we can process this range now
                                let factory_data_ready = !needs_factory_wait || factory_ready.contains(&range_start);

                                if factory_data_ready {
                                    process_completed_range(
                                        range_start,
                                        range_end,
                                        &mut range_data,
                                        &mut range_factory_addresses,
                                        contract_logs_only,
                                        &configured_addresses,
                                        log_fields,
                                        &schema,
                                        &output_dir,
                                        &existing_files,
                                    )
                                    .await?;
                                    factory_ready.remove(&range_start);
                                } else {
                                    // Wait for factory data
                                    pending_ranges.insert(range_start, range_end);
                                }
                            }
                            LogMessage::AllRangesComplete => {
                                // All ranges from receipts are done, but we might still be waiting for factory data
                                if pending_ranges.is_empty() {
                                    break;
                                }
                                // Continue looping to receive remaining factory data
                            }
                        }
                    }
                    None => {
                        // Channel closed unexpectedly
                        tracing::warn!("log_rx channel closed unexpectedly");
                        break;
                    }
                }
            }

            factory_result = async {
                match &mut factory_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match factory_result {
                    Some(factory_data) => {
                        let range_start = factory_data.range_start;

                        let factory_addrs: HashSet<[u8; 20]> = factory_data
                            .addresses_by_block
                            .values()
                            .flatten()
                            .map(|(_, addr, _)| addr.0.0)
                            .collect();

                        tracing::debug!(
                            "Received {} factory addresses for range {}",
                            factory_addrs.len(),
                            range_start
                        );

                        range_factory_addresses
                            .entry(range_start)
                            .or_default()
                            .extend(factory_addrs);

                        // Check if this range was pending
                        if let Some(pending_end) = pending_ranges.remove(&range_start) {
                            process_completed_range(
                                range_start,
                                pending_end,
                                &mut range_data,
                                &mut range_factory_addresses,
                                contract_logs_only,
                                &configured_addresses,
                                log_fields,
                                &schema,
                                &output_dir,
                                &existing_files,
                            )
                            .await?;
                        } else {
                            // Factory data arrived before RangeComplete, mark as ready
                            factory_ready.insert(range_start);
                        }
                    }
                    None => {
                        tracing::debug!("factory_rx channel closed, pending_ranges: {}", pending_ranges.len());
                        factory_rx = None;
                        // If we have no pending ranges and factory channel is closed, we might be done
                        if pending_ranges.is_empty() {
                            tracing::warn!("factory_rx closed with no pending ranges - breaking");
                            break;
                        }
                    }
                }
            }
        }
    }

    tracing::info!("Log collection complete for chain {}", chain.name);
    Ok(())
}

async fn process_completed_range(
    range_start: u64,
    range_end: u64,
    range_data: &mut HashMap<u64, Vec<LogData>>,
    range_factory_addresses: &mut HashMap<u64, HashSet<[u8; 20]>>,
    contract_logs_only: bool,
    configured_addresses: &HashSet<[u8; 20]>,
    log_fields: &Option<Vec<LogField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    existing_files: &HashSet<String>,
) -> Result<(), LogCollectionError> {
    let range = BlockRange {
        start: range_start,
        end: range_end,
    };

    if existing_files.contains(&range.file_name()) {
        tracing::info!(
            "Skipping logs for blocks {}-{} (already exists)",
            range.start,
            range.end - 1
        );
        range_data.remove(&range_start);
        range_factory_addresses.remove(&range_start);
        return Ok(());
    }

    let mut logs = range_data.remove(&range_start).unwrap_or_default();
    let factory_addrs = range_factory_addresses.remove(&range_start).unwrap_or_default();

    if contract_logs_only {
        let before_count = logs.len();
        logs = logs
            .into_iter()
            .filter(|log| {
                configured_addresses.contains(&log.address)
                    || factory_addrs.contains(&log.address)
            })
            .collect();
        tracing::debug!(
            "Filtered logs from {} to {} for range {}",
            before_count,
            logs.len(),
            range_start
        );
    }

    process_range(&range, logs, log_fields, schema, output_dir).await
}

fn build_configured_addresses(contracts: &Contracts) -> HashSet<[u8; 20]> {
    let mut addresses = HashSet::new();
    for (_, contract) in contracts {
        match &contract.address {
            AddressOrAddresses::Single(addr) => {
                addresses.insert(addr.0.0);
            }
            AddressOrAddresses::Multiple(addrs) => {
                for addr in addrs {
                    addresses.insert(addr.0.0);
                }
            }
        }
    }
    addresses
}

async fn process_range(
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
            let schema_clone = schema.clone();
            let fields_vec = fields.to_vec();
            let output_path = output_dir.join(range.file_name());
            tokio::task::spawn_blocking(move || {
                write_minimal_logs_to_parquet(&records, &schema_clone, &fields_vec, &output_path)
            })
            .await
            .map_err(|e| LogCollectionError::JoinError(e.to_string()))??;
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
            let schema_clone = schema.clone();
            let output_path = output_dir.join(range.file_name());
            tokio::task::spawn_blocking(move || {
                write_full_logs_to_parquet(&records, &schema_clone, &output_path)
            })
            .await
            .map_err(|e| LogCollectionError::JoinError(e.to_string()))??;
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
