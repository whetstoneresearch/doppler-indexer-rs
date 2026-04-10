use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListBuilder, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::receipts::LogData;
use crate::storage::paths::scan_parquet_filenames;
use crate::storage::{BlockRange, S3Manifest, StorageManager};
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::raw_data::LogField;

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

pub(crate) struct LogsCatchupState {
    pub(crate) output_dir: PathBuf,
    pub(crate) range_size: u64,
    pub(crate) schema: Arc<Schema>,
    pub(crate) configured_addresses: HashSet<[u8; 20]>,
    pub(crate) existing_files: HashSet<String>,
    pub(crate) contract_logs_only: bool,
    pub(crate) needs_factory_wait: bool,
    pub(crate) log_fields: Option<Vec<LogField>>,
    pub(crate) s3_manifest: Option<S3Manifest>,
    pub(crate) storage_manager: Option<Arc<StorageManager>>,
    pub(crate) chain_name: String,
}

#[derive(Debug)]
pub(crate) struct FullLogRecord {
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: u64,
    pub(crate) transaction_hash: [u8; 32],
    pub(crate) log_index: u32,
    pub(crate) address: [u8; 20],
    pub(crate) topics: Vec<[u8; 32]>,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct MinimalLogRecord {
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: u64,
    pub(crate) transaction_hash: [u8; 32],
    pub(crate) log_index: u32,
    pub(crate) address: [u8; 20],
    pub(crate) topics: Vec<[u8; 32]>,
    pub(crate) data: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_completed_range(
    range_start: u64,
    range_end: u64,
    range_data: &mut HashMap<u64, Vec<LogData>>,
    range_factory_addresses: &mut HashMap<u64, HashSet<[u8; 20]>>,
    contract_logs_only: bool,
    configured_addresses: &HashSet<[u8; 20]>,
    log_fields: &Option<Vec<LogField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    _existing_files: &HashSet<String>,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
    chain_name: &str,
) -> Result<(), LogCollectionError> {
    let range = BlockRange {
        start: range_start,
        end: range_end,
    };

    // Check filesystem directly instead of cache to handle files deleted during recollection
    let output_path = output_dir.join(range.file_name("logs"));
    if output_path.exists() {
        tracing::debug!(
            "Skipping logs for blocks {}-{} (already exists)",
            range.start,
            range.end - 1
        );
        range_data.remove(&range_start);
        range_factory_addresses.remove(&range_start);
        return Ok(());
    }

    // Check if range exists in S3 manifest
    if s3_manifest.is_some_and(|m| m.raw_logs.has(range.start, range.end - 1)) {
        tracing::debug!(
            "Skipping logs range {}-{}: exists in S3 manifest",
            range.start,
            range.end - 1
        );
        range_data.remove(&range_start);
        range_factory_addresses.remove(&range_start);
        return Ok(());
    }

    let mut logs = range_data.remove(&range_start).unwrap_or_default();
    let factory_addrs = range_factory_addresses
        .remove(&range_start)
        .unwrap_or_default();

    if contract_logs_only {
        let before_count = logs.len();
        logs.retain(|log| {
            configured_addresses.contains(&log.address) || factory_addrs.contains(&log.address)
        });
        tracing::debug!(
            "Filtered logs from {} to {} for range {}",
            before_count,
            logs.len(),
            range_start
        );
    }

    // Send logs to decoder before processing (decoder will receive them for decoding).
    if let Some(tx) = decoder_tx {
        let _ = tx
            .send(DecoderMessage::LogsReady {
                range_start,
                range_end,
                logs: std::sync::Arc::new(logs.clone()),
                live_mode: false,            // Historical mode: write to parquet
                has_factory_matchers: false, // Factory addresses handled separately in historical mode
            })
            .await;

        process_range(
            &range,
            &logs,
            log_fields,
            schema,
            output_dir,
            storage_manager,
            chain_name,
        )
        .await
    } else {
        process_range(
            &range,
            &logs,
            log_fields,
            schema,
            output_dir,
            storage_manager,
            chain_name,
        )
        .await
    }
}

pub(crate) fn build_configured_addresses(contracts: &Contracts) -> HashSet<[u8; 20]> {
    let mut addresses = HashSet::new();
    for contract in contracts.values() {
        match &contract.address {
            AddressOrAddresses::Single(addr) => {
                addresses.insert(addr.0 .0);
            }
            AddressOrAddresses::Multiple(addrs) => {
                for addr in addrs {
                    addresses.insert(addr.0 .0);
                }
            }
        }
    }
    addresses
}

pub(crate) async fn process_range(
    range: &BlockRange,
    logs: &[LogData],
    log_fields: &Option<Vec<LogField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    storage_manager: Option<&Arc<StorageManager>>,
    chain_name: &str,
) -> Result<(), LogCollectionError> {
    tracing::info!(
        "Writing {} logs for blocks {}-{}",
        logs.len(),
        range.start,
        range.end - 1
    );

    let write_start = Instant::now();
    let log_count = logs.len();
    match log_fields {
        Some(fields) => {
            let mut records: Vec<MinimalLogRecord> = Vec::with_capacity(log_count);
            for l in logs {
                records.push(MinimalLogRecord {
                    block_number: l.block_number,
                    block_timestamp: l.block_timestamp,
                    transaction_hash: l.transaction_hash.0,
                    log_index: l.log_index,
                    address: l.address,
                    topics: l.topics.clone(),
                    data: l.data.clone(),
                });
            }
            let output_path = output_dir.join(range.file_name("logs"));
            write_minimal_logs_to_parquet_async(
                records,
                schema.clone(),
                fields.to_vec(),
                output_path,
            )
            .await?;
        }
        None => {
            let mut records: Vec<FullLogRecord> = Vec::with_capacity(log_count);
            for l in logs {
                records.push(FullLogRecord {
                    block_number: l.block_number,
                    block_timestamp: l.block_timestamp,
                    transaction_hash: l.transaction_hash.0,
                    log_index: l.log_index,
                    address: l.address,
                    topics: l.topics.clone(),
                    data: l.data.clone(),
                });
            }
            let output_path = output_dir.join(range.file_name("logs"));
            write_full_logs_to_parquet_async(records, schema.clone(), output_path).await?;
        }
    }

    let output_path = output_dir.join(range.file_name("logs"));

    crate::metrics::record_parquet_write(
        chain_name,
        "logs",
        write_start.elapsed().as_secs_f64(),
        &output_path,
    );

    tracing::info!("Wrote logs to {}", output_path.display());

    // Upload to S3 if configured
    if let Some(sm) = storage_manager {
        crate::storage::upload_parquet_to_s3(
            sm,
            &output_path,
            chain_name,
            "raw/logs",
            range.start,
            range.end - 1,
        )
        .await
        .map_err(|e| LogCollectionError::Io(std::io::Error::other(e.to_string())))?;
    }

    Ok(())
}

pub(crate) fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    scan_parquet_filenames(dir, "logs_")
}

pub(crate) fn build_log_schema(fields: &Option<Vec<LogField>>) -> Arc<Schema> {
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
                                true,
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
                    true,
                ))),
                false,
            ),
            Field::new("data", DataType::Binary, false),
        ])),
    }
}

pub(crate) fn write_minimal_logs_to_parquet(
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
                let arr = if records.is_empty() {
                    FixedSizeBinaryBuilder::new(32).finish()
                } else {
                    FixedSizeBinaryArray::try_from_iter(
                        records.iter().map(|r| r.transaction_hash.as_slice()),
                    )?
                };
                arrays.push(Arc::new(arr));
            }
            LogField::LogIndex => {
                let arr: UInt32Array = records.iter().map(|r| Some(r.log_index)).collect();
                arrays.push(Arc::new(arr));
            }
            LogField::Address => {
                let arr = if records.is_empty() {
                    FixedSizeBinaryBuilder::new(20).finish()
                } else {
                    FixedSizeBinaryArray::try_from_iter(
                        records.iter().map(|r| r.address.as_slice()),
                    )?
                };
                arrays.push(Arc::new(arr));
            }
            LogField::Topics => {
                let arr = build_topics_array(records)?;
                arrays.push(Arc::new(arr));
            }
            LogField::Data => {
                let arr: BinaryArray = records.iter().map(|r| Some(r.data.as_slice())).collect();
                arrays.push(Arc::new(arr));
            }
        }
    }

    write_parquet(arrays, schema, output_path)
}

pub(crate) fn write_full_logs_to_parquet(
    records: &[FullLogRecord],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), LogCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(32).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.transaction_hash.as_slice()))?
    };
    arrays.push(Arc::new(arr));

    let arr: UInt32Array = records.iter().map(|r| Some(r.log_index)).collect();
    arrays.push(Arc::new(arr));

    let arr = if records.is_empty() {
        FixedSizeBinaryBuilder::new(20).finish()
    } else {
        FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.address.as_slice()))?
    };
    arrays.push(Arc::new(arr));

    let mut list_builder = ListBuilder::new(FixedSizeBinaryBuilder::new(32));
    for record in records {
        for topic in &record.topics {
            list_builder.values().append_value(topic.as_slice())?;
        }
        list_builder.append(true);
    }
    arrays.push(Arc::new(list_builder.finish()));

    let arr: BinaryArray = records.iter().map(|r| Some(r.data.as_slice())).collect();
    arrays.push(Arc::new(arr));

    write_parquet(arrays, schema, output_path)
}

pub(crate) fn build_topics_array(
    records: &[MinimalLogRecord],
) -> Result<arrow::array::ListArray, arrow::error::ArrowError> {
    let mut list_builder = ListBuilder::new(FixedSizeBinaryBuilder::new(32));
    for record in records {
        for topic in &record.topics {
            list_builder.values().append_value(topic.as_slice())?;
        }
        list_builder.append(true);
    }
    Ok(list_builder.finish())
}

pub(crate) fn write_parquet(
    arrays: Vec<ArrayRef>,
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), LogCollectionError> {
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

pub(crate) async fn write_minimal_logs_to_parquet_async(
    records: Vec<MinimalLogRecord>,
    schema: Arc<Schema>,
    fields: Vec<LogField>,
    output_path: PathBuf,
) -> Result<(), LogCollectionError> {
    tokio::task::spawn_blocking(move || {
        write_minimal_logs_to_parquet(&records, &schema, &fields, &output_path)
    })
    .await
    .map_err(|e| LogCollectionError::JoinError(e.to_string()))?
}

pub(crate) async fn write_full_logs_to_parquet_async(
    records: Vec<FullLogRecord>,
    schema: Arc<Schema>,
    output_path: PathBuf,
) -> Result<(), LogCollectionError> {
    tokio::task::spawn_blocking(move || write_full_logs_to_parquet(&records, &schema, &output_path))
        .await
        .map_err(|e| LogCollectionError::JoinError(e.to_string()))?
}
