use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::Address;
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListArray,
    StringArray, StringBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use thiserror::Error;

use crate::raw_data::historical::receipts::LogData;
use crate::storage::paths::{parse_range_from_filename, raw_logs_dir, scan_nested_parquet_files_1};
use crate::storage::{S3Manifest, StorageManager};
use crate::types::config::contract::{
    resolve_factory_config, AddressOrAddresses, Contracts, FactoryCollections,
    FactoryParameterLocation,
};

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum FactoryCollectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Channel send error: {0}")]
    ChannelSend(String),

    #[error("ABI decode error: {0}")]
    AbiDecode(String),

    #[error("Task join error: {0}")]
    JoinError(String),
}

#[derive(Debug, Clone)]
pub struct FactoryAddressData {
    pub range_start: u64,
    pub range_end: u64,
    pub addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
}

/// Message type for factory -> eth_calls communication
/// Supports incremental address forwarding for early RPC fetching
#[derive(Debug, Clone)]
pub enum FactoryMessage {
    /// Incremental batch of factory addresses discovered (sent per rpc_batch_size logs)
    IncrementalAddresses(FactoryAddressData),
    /// A block range is complete - parquet files written
    RangeComplete { range_start: u64, range_end: u64 },
    /// All processing is complete
    AllComplete,
}

/// State for batch-based factory processing within a range
/// Enables early address forwarding before full range is complete
#[derive(Debug)]
#[allow(dead_code)]
struct FactoryBatchState {
    range_start: u64,
    range_end: u64,
    /// Logs received but not yet processed
    logs_buffered: Vec<LogData>,
    /// Addresses already sent to downstream (avoid duplicates)
    addresses_sent: HashSet<[u8; 20]>,
    /// All factory records for parquet write
    all_records: Vec<FactoryRecord>,
    /// All addresses by block for final range complete message
    addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
}

#[derive(Debug)]
pub(crate) struct FactoryRecord {
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: u64,
    pub(crate) factory_address: [u8; 20],
    pub(crate) collection_name: String,
}

pub struct FactoryMatcher {
    pub factory_contract_address: [u8; 20],
    pub event_topic0: [u8; 32],
    pub param_location: FactoryParameterLocation,
    pub data_types: Vec<DynSolType>,
    pub collection_name: String,
    /// Name of the contract in the config that owns this factory matcher.
    pub contract_name: String,
}

/// State computed during factory catchup, passed to current/streaming phase
#[allow(dead_code)]
pub(crate) struct FactoryCatchupState {
    pub(crate) matchers: Arc<Vec<FactoryMatcher>>,
    pub(crate) existing_files: Arc<HashSet<String>>,
    pub(crate) output_dir: Arc<PathBuf>,
    pub(crate) s3_manifest: Option<S3Manifest>,
    pub(crate) storage_manager: Option<Arc<StorageManager>>,
    pub(crate) chain_name: String,
}

pub fn build_factory_matchers(contracts: &Contracts) -> Vec<FactoryMatcher> {
    let mut matchers = Vec::new();

    for (contract_name, contract) in contracts {
        let contract_addresses: Vec<[u8; 20]> = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![addr.0 .0],
            AddressOrAddresses::Multiple(addrs) => addrs.iter().map(|a| a.0 .0).collect(),
        };

        if let Some(factories) = &contract.factories {
            for factory in factories {
                let events = factory.factory_events.clone().into_vec();

                for event in events {
                    let event_topic0 = event.compute_event_signature();
                    let param_location = event.parse_factory_parameter();

                    let data_types = event
                        .data_signature
                        .as_ref()
                        .map(|sig| parse_data_signature(sig))
                        .unwrap_or_default();

                    for addr in &contract_addresses {
                        tracing::debug!(
                            "Created factory matcher for {} -> {} (topic0: 0x{})",
                            contract_name,
                            factory.collection,
                            hex::encode(&event_topic0[..8])
                        );

                        matchers.push(FactoryMatcher {
                            factory_contract_address: *addr,
                            event_topic0,
                            param_location: param_location.clone(),
                            data_types: data_types.clone(),
                            collection_name: factory.collection.clone(),
                            contract_name: contract_name.clone(),
                        });
                    }
                }
            }
        }
    }

    matchers
}

fn parse_data_signature(sig: &str) -> Vec<DynSolType> {
    if sig.is_empty() {
        return Vec::new();
    }

    let mut parts = Vec::new();
    let mut depth = 0;
    let mut current = String::new();

    for ch in sig.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    parts.push(trimmed);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        parts.push(trimmed);
    }

    parts
        .iter()
        .filter_map(|s| s.parse::<DynSolType>().ok())
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_range(
    range_start: u64,
    range_end: u64,
    logs: Vec<LogData>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
    chain_name: &str,
) -> Result<FactoryAddressData, FactoryCollectionError> {
    let mut records: Vec<FactoryRecord> = Vec::new();
    let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> = HashMap::new();

    for log in &logs {
        for matcher in matchers {
            if log.address != matcher.factory_contract_address {
                continue;
            }

            if log.topics.is_empty() || log.topics[0] != matcher.event_topic0 {
                continue;
            }

            let extracted_address = match &matcher.param_location {
                FactoryParameterLocation::Topic(idx) => {
                    if *idx < log.topics.len() {
                        let topic = &log.topics[*idx];
                        let mut addr = [0u8; 20];
                        addr.copy_from_slice(&topic[12..32]);
                        Some(addr)
                    } else {
                        None
                    }
                }
                FactoryParameterLocation::Data(indices) => {
                    match decode_address_from_data(&log.data, &matcher.data_types, indices) {
                        Ok(addr) => addr,
                        Err(e) => {
                            tracing::warn!(
                                block_number = log.block_number,
                                contract = %Address::from(log.address),
                                topic0 = %hex::encode(log.topics.first().map(|t| t.as_slice()).unwrap_or(&[])),
                                collection = %matcher.collection_name,
                                data_len = log.data.len(),
                                "Failed to decode factory event data: {}",
                                e
                            );
                            None
                        }
                    }
                }
            };

            if let Some(addr) = extracted_address {
                records.push(FactoryRecord {
                    block_number: log.block_number,
                    block_timestamp: log.block_timestamp,
                    factory_address: addr,
                    collection_name: matcher.collection_name.clone(),
                });

                addresses_by_block
                    .entry(log.block_number)
                    .or_default()
                    .push((
                        log.block_timestamp,
                        Address::from(addr),
                        matcher.collection_name.clone(),
                    ));
            }
        }
    }

    write_factory_parquet_files(
        range_start,
        range_end,
        records,
        matchers,
        output_dir,
        existing_files,
        s3_manifest,
        storage_manager,
        chain_name,
        false, // don't force rewrite for streaming
    )
    .await?;

    Ok(FactoryAddressData {
        range_start,
        range_end,
        addresses_by_block,
    })
}

/// Async wrapper for `read_log_batches_from_parquet` that runs on the blocking threadpool.
pub(crate) async fn read_log_batches_from_parquet_async(
    file_path: PathBuf,
) -> Result<Vec<RecordBatch>, FactoryCollectionError> {
    tokio::task::spawn_blocking(move || read_log_batches_from_parquet(&file_path))
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))?
}

/// Read log batches from a parquet file with column projection.
/// Only reads block_number, block_timestamp, address, topics, data — skips transaction_hash and log_index.
pub(crate) fn read_log_batches_from_parquet(
    file_path: &Path,
) -> Result<Vec<RecordBatch>, FactoryCollectionError> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();

    let needed = [
        "block_number",
        "block_timestamp",
        "address",
        "topics",
        "data",
    ];
    let root_indices: Vec<usize> = needed
        .iter()
        .filter_map(|name| arrow_schema.index_of(name).ok())
        .collect();

    let mask = ProjectionMask::roots(builder.parquet_schema(), root_indices);
    let reader = builder.with_projection(mask).build()?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }

    Ok(batches)
}

/// Process log record batches using Arrow-native column access.
/// Works directly on Arrow arrays instead of materializing LogData structs.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_range_batches(
    range_start: u64,
    range_end: u64,
    batches: Vec<RecordBatch>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
    chain_name: &str,
) -> Result<FactoryAddressData, FactoryCollectionError> {
    let mut records: Vec<FactoryRecord> = Vec::new();
    let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> = HashMap::new();

    for batch in &batches {
        let block_nums = match batch
            .column_by_name("block_number")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        {
            Some(arr) => arr,
            None => continue,
        };
        let timestamps = match batch
            .column_by_name("block_timestamp")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        {
            Some(arr) => arr,
            None => continue,
        };
        let addrs = match batch
            .column_by_name("address")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        {
            Some(arr) => arr,
            None => continue,
        };

        let topics_list = batch
            .column_by_name("topics")
            .and_then(|c| c.as_any().downcast_ref::<ListArray>());

        // Pre-extract inner topics array and offsets for zero-copy access
        let topics_inner =
            topics_list.and_then(|l| l.values().as_any().downcast_ref::<FixedSizeBinaryArray>());
        let topics_offsets = topics_list.map(|l| l.offsets());

        let data_col = batch
            .column_by_name("data")
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());

        for i in 0..batch.num_rows() {
            let addr_bytes = addrs.value(i);
            if addr_bytes.len() != 20 {
                continue;
            }

            for matcher in matchers {
                if addr_bytes != matcher.factory_contract_address.as_slice() {
                    continue;
                }

                // Check topic0 match using pre-extracted inner array and offsets
                let topic0_match = match (topics_offsets, topics_inner) {
                    (Some(offsets), Some(inner)) => {
                        let start = offsets[i] as usize;
                        let end = offsets[i + 1] as usize;
                        end > start && inner.value(start) == matcher.event_topic0.as_slice()
                    }
                    _ => false,
                };

                if !topic0_match {
                    continue;
                }

                let block_number = block_nums.value(i);
                let block_timestamp = timestamps.value(i);

                let extracted_address = match &matcher.param_location {
                    FactoryParameterLocation::Topic(idx) => match (topics_offsets, topics_inner) {
                        (Some(offsets), Some(inner)) => {
                            let start = offsets[i] as usize;
                            let end = offsets[i + 1] as usize;
                            let num_topics = end - start;
                            if *idx < num_topics {
                                let topic = inner.value(start + *idx);
                                if topic.len() == 32 {
                                    let mut addr = [0u8; 20];
                                    addr.copy_from_slice(&topic[12..32]);
                                    Some(addr)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    },
                    FactoryParameterLocation::Data(indices) => {
                        let data = data_col.map(|arr| arr.value(i)).unwrap_or(&[]);
                        match decode_address_from_data(data, &matcher.data_types, indices) {
                            Ok(addr) => addr,
                            Err(e) => {
                                tracing::warn!(
                                    block_number,
                                    contract = %Address::from_slice(addr_bytes),
                                    collection = %matcher.collection_name,
                                    "Failed to decode factory event data: {}",
                                    e
                                );
                                None
                            }
                        }
                    }
                };

                if let Some(addr) = extracted_address {
                    records.push(FactoryRecord {
                        block_number,
                        block_timestamp,
                        factory_address: addr,
                        collection_name: matcher.collection_name.clone(),
                    });

                    addresses_by_block.entry(block_number).or_default().push((
                        block_timestamp,
                        Address::from(addr),
                        matcher.collection_name.clone(),
                    ));
                }
            }
        }
    }

    write_factory_parquet_files(
        range_start,
        range_end,
        records,
        matchers,
        output_dir,
        existing_files,
        s3_manifest,
        storage_manager,
        chain_name,
        true, // force rewrite during catchup (contract index handles skip)
    )
    .await?;

    Ok(FactoryAddressData {
        range_start,
        range_end,
        addresses_by_block,
    })
}

/// Write factory records to parquet files, grouped by collection.
/// Writes empty parquet files for collections with no matching events.
///
/// When `force_rewrite` is true, existing files are overwritten (used when the
/// contract index shows missing contracts for a range that already has parquet files).
#[allow(clippy::too_many_arguments)]
async fn write_factory_parquet_files(
    range_start: u64,
    range_end: u64,
    records: Vec<FactoryRecord>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
    chain_name: &str,
    force_rewrite: bool,
) -> Result<(), FactoryCollectionError> {
    let mut by_collection: HashMap<String, Vec<FactoryRecord>> = HashMap::new();
    for record in records {
        by_collection
            .entry(record.collection_name.clone())
            .or_default()
            .push(record);
    }

    let collections_with_events: HashSet<String> = by_collection.keys().cloned().collect();

    for (collection_name, collection_records) in by_collection {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let rel_path = format!("{}/{}", collection_name, file_name);

        if !force_rewrite
            && (existing_files.contains(&rel_path)
                || s3_manifest
                    .is_some_and(|m| m.has_factories(&collection_name, range_start, range_end - 1)))
        {
            tracing::debug!(
                "Skipping factory parquet for {} blocks {}-{} (already exists)",
                collection_name,
                range_start,
                range_end - 1
            );
            continue;
        }

        let record_count = collection_records.len();
        let sub_dir = output_dir.join(&collection_name);
        tokio::fs::create_dir_all(&sub_dir).await?;
        let output_path = sub_dir.join(&file_name);
        write_factory_records_to_parquet_async(collection_records, output_path.clone()).await?;

        tracing::info!(
            "Wrote {} factory addresses for {} to {}",
            record_count,
            collection_name,
            output_path.display()
        );

        // Upload to S3 if configured
        if let Some(sm) = storage_manager {
            let data_type = format!("factories/{}", collection_name);
            crate::storage::upload_parquet_to_s3(
                sm,
                &output_path,
                chain_name,
                &data_type,
                range_start,
                range_end - 1,
            )
            .await
            .map_err(|e| FactoryCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }
    }

    for matcher in matchers {
        let collection_name = &matcher.collection_name;
        if !collections_with_events.contains(collection_name) {
            let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
            let rel_path = format!("{}/{}", collection_name, file_name);

            if force_rewrite
                || (!existing_files.contains(&rel_path)
                    && !s3_manifest.is_some_and(|m| {
                        m.has_factories(collection_name, range_start, range_end - 1)
                    }))
            {
                let sub_dir = output_dir.join(collection_name);
                tokio::fs::create_dir_all(&sub_dir).await?;
                let output_path = sub_dir.join(&file_name);
                write_empty_factory_parquet_async(output_path.clone()).await?;

                tracing::debug!(
                    "Wrote empty factory file for {} range {}-{}",
                    collection_name,
                    range_start,
                    range_end - 1
                );

                // Upload to S3 if configured
                if let Some(sm) = storage_manager {
                    let data_type = format!("factories/{}", collection_name);
                    crate::storage::upload_parquet_to_s3(
                        sm,
                        &output_path,
                        chain_name,
                        &data_type,
                        range_start,
                        range_end - 1,
                    )
                    .await
                    .map_err(|e| {
                        FactoryCollectionError::Io(std::io::Error::other(e.to_string()))
                    })?;
                }
            }
        }
    }

    Ok(())
}

fn extract_address_recursive(values: &[DynSolValue], indices: &[usize]) -> Option<[u8; 20]> {
    if indices.is_empty() {
        return None;
    }

    let first_idx = indices[0];
    if first_idx >= values.len() {
        return None;
    }

    let value = &values[first_idx];

    if indices.len() == 1 {
        // Last index - extract address
        if let DynSolValue::Address(addr) = value {
            return Some(addr.0 .0);
        }
        None
    } else {
        // More indices to traverse - must be a tuple
        if let DynSolValue::Tuple(inner_values) = value {
            extract_address_recursive(inner_values, &indices[1..])
        } else {
            None
        }
    }
}

fn decode_address_from_data(
    data: &[u8],
    types: &[DynSolType],
    indices: &[usize],
) -> Result<Option<[u8; 20]>, alloy::dyn_abi::Error> {
    if types.is_empty() || indices.is_empty() {
        return Ok(None);
    }

    let tuple_type = DynSolType::Tuple(types.to_vec());

    match tuple_type.abi_decode_params(data) {
        Ok(DynSolValue::Tuple(values)) => Ok(extract_address_recursive(&values, indices)),
        Ok(_) => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) async fn write_factory_records_to_parquet_async(
    records: Vec<FactoryRecord>,
    output_path: PathBuf,
) -> Result<(), FactoryCollectionError> {
    tokio::task::spawn_blocking(move || write_factory_records_to_parquet(&records, &output_path))
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))?
}

fn write_factory_records_to_parquet(
    records: &[FactoryRecord],
    output_path: &Path,
) -> Result<(), FactoryCollectionError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("factory_address", DataType::FixedSizeBinary(20), false),
        Field::new("collection_name", DataType::Utf8, false),
    ]));

    let block_numbers: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    let timestamps: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    let addresses =
        FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.factory_address.as_slice()))?;

    let mut names_builder = StringBuilder::new();
    for record in records {
        names_builder.append_value(&record.collection_name);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(block_numbers),
        Arc::new(timestamps),
        Arc::new(addresses),
        Arc::new(names_builder.finish()),
    ];

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet(&batch, output_path)?;
    Ok(())
}

pub(crate) fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    scan_nested_parquet_files_1(dir)
}

pub(crate) async fn load_factory_addresses_from_parquet_async(
    dir: PathBuf,
) -> Result<Vec<FactoryAddressData>, FactoryCollectionError> {
    tokio::task::spawn_blocking(move || load_factory_addresses_from_parquet(&dir))
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))?
}

pub(crate) fn load_factory_addresses_from_parquet(
    dir: &Path,
) -> Result<Vec<FactoryAddressData>, FactoryCollectionError> {
    let mut results = Vec::new();

    // Scan nested directories: dir/collection/*.parquet
    let collection_entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(results),
    };

    for collection_entry in collection_entries.flatten() {
        let collection_path = collection_entry.path();
        if !collection_path.is_dir() {
            continue;
        }

        let file_entries = match std::fs::read_dir(&collection_path) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for file_entry in file_entries.flatten() {
            let path = file_entry.path();
            if !path.extension().map(|e| e == "parquet").unwrap_or(false) {
                continue;
            }

            // Filename is "start-end.parquet" (no prefix)
            let Some((start_inclusive, end_inclusive)) = parse_range_from_filename(&path) else {
                continue;
            };
            let range_start: u64 = start_inclusive;
            let range_end: u64 = end_inclusive + 1;

            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!("Failed to open factory parquet {}: {}", path.display(), e);
                    continue;
                }
            };

            let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
                Ok(builder) => match builder.build() {
                    Ok(reader) => reader,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to build parquet reader for {}: {}",
                            path.display(),
                            e
                        );
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to read factory parquet {}: {}", path.display(), e);
                    continue;
                }
            };

            let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> = HashMap::new();

            for batch_result in reader {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                        continue;
                    }
                };

                let block_numbers = batch
                    .column_by_name("block_number")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

                let timestamps = batch
                    .column_by_name("block_timestamp")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

                let addresses = batch
                    .column_by_name("factory_address")
                    .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

                let collection_names = batch
                    .column_by_name("collection_name")
                    .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>());

                if let (Some(blocks), Some(times), Some(addrs), Some(names)) =
                    (block_numbers, timestamps, addresses, collection_names)
                {
                    for i in 0..batch.num_rows() {
                        let block = blocks.value(i);
                        let timestamp = times.value(i);
                        let addr_bytes = addrs.value(i);
                        let collection = names.value(i);

                        if addr_bytes.len() == 20 {
                            let mut addr = [0u8; 20];
                            addr.copy_from_slice(addr_bytes);
                            addresses_by_block.entry(block).or_default().push((
                                timestamp,
                                Address::from(addr),
                                collection.to_string(),
                            ));
                        }
                    }
                }
            }

            if !addresses_by_block.is_empty() {
                results.push(FactoryAddressData {
                    range_start,
                    range_end,
                    addresses_by_block,
                });
            }
        }
    }

    results.sort_by_key(|r| r.range_start);

    Ok(results)
}

pub fn get_factory_call_configs(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> HashMap<String, Vec<crate::types::config::eth_call::EthCallConfig>> {
    let mut configs: HashMap<String, Vec<crate::types::config::eth_call::EthCallConfig>> =
        HashMap::new();

    for contract in contracts.values() {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                let resolved = resolve_factory_config(factory, factory_collections);
                if !resolved.calls.is_empty() {
                    // Use entry().or_default().extend() to merge calls from multiple contracts
                    let entry = configs.entry(resolved.collection_name).or_default();
                    for call in resolved.calls {
                        // Deduplicate by function signature
                        if !entry.iter().any(|c| c.function == call.function) {
                            entry.push(call);
                        }
                    }
                }
            }
        }
    }

    configs
}

#[allow(dead_code)]
pub fn get_factory_collection_names(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> Vec<String> {
    let mut names = Vec::new();

    // Include collection names from contracts
    for contract in contracts.values() {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if !names.contains(&factory.collection) {
                    names.push(factory.collection.clone());
                }
            }
        }
    }

    // Include collection names from factory_collections that aren't already present
    for name in factory_collections.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }

    names
}

pub(crate) async fn write_empty_factory_parquet_async(
    output_path: PathBuf,
) -> Result<(), FactoryCollectionError> {
    tokio::task::spawn_blocking(move || write_empty_factory_parquet(&output_path))
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))?
}

fn write_empty_factory_parquet(output_path: &Path) -> Result<(), FactoryCollectionError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("factory_address", DataType::FixedSizeBinary(20), false),
        Field::new("collection_name", DataType::Utf8, false),
    ]));

    let block_numbers: UInt64Array = std::iter::empty::<Option<u64>>().collect();
    let timestamps: UInt64Array = std::iter::empty::<Option<u64>>().collect();
    // Use builder to create empty FixedSizeBinaryArray (try_from_iter fails with empty iterators)
    let addresses = FixedSizeBinaryBuilder::new(20).finish();
    let names: StringArray = std::iter::empty::<Option<&str>>().collect();

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(block_numbers),
        Arc::new(timestamps),
        Arc::new(addresses),
        Arc::new(names),
    ];

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    crate::storage::atomic_write_parquet(&batch, output_path)?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ExistingLogRange {
    pub start: u64,
    pub end: u64,
    pub file_path: PathBuf,
}

/// Request to recollect a corrupted log file range
#[derive(Debug, Clone)]
pub struct RecollectRequest {
    pub range_start: u64,
    pub range_end: u64,
    pub _file_path: PathBuf,
}

/// Scan existing logs parquet files and return their ranges.
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
