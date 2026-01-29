use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::Address;
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringBuilder, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::raw_data::historical::receipts::{LogData, LogMessage};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{
    AddressOrAddresses, Contracts, FactoryParameterLocation,
};
use crate::types::config::raw_data::RawDataCollectionConfig;

#[derive(Debug, Error)]
pub enum FactoryCollectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Channel send error")]
    ChannelSend,

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

#[derive(Debug)]
struct FactoryRecord {
    block_number: u64,
    block_timestamp: u64,
    factory_address: [u8; 20],
    collection_name: String,
}

struct FactoryMatcher {
    factory_contract_address: [u8; 20],
    event_topic0: [u8; 32],
    param_location: FactoryParameterLocation,
    data_types: Vec<DynSolType>,
    collection_name: String,
}

pub async fn collect_factories(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    mut log_rx: Receiver<LogMessage>,
    logs_factory_tx: Option<Sender<FactoryAddressData>>,
    eth_calls_factory_tx: Option<Sender<FactoryAddressData>>,
) -> Result<(), FactoryCollectionError> {
    let output_dir = PathBuf::from(format!("data/derived/{}/factories", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;

    let existing_factory_data = load_factory_addresses_from_parquet(&output_dir)?;
    if !existing_factory_data.is_empty() {
        tracing::info!(
            "Loaded {} existing factory ranges from parquet for chain {}",
            existing_factory_data.len(),
            chain.name
        );

        for factory_data in existing_factory_data {
            if let Some(ref tx) = logs_factory_tx {
                if tx.send(factory_data.clone()).await.is_err() {
                    return Err(FactoryCollectionError::ChannelSend);
                }
            }

            if let Some(ref tx) = eth_calls_factory_tx {
                let _ = tx.send(factory_data).await;
            }
        }
    }

    let matchers = build_factory_matchers(&chain.contracts);

    if matchers.is_empty() {
        tracing::info!("No factory matchers configured for chain {}, forwarding empty ranges", chain.name);

        while let Some(message) = log_rx.recv().await {
            match message {
                LogMessage::Logs(_) => {}
                LogMessage::RangeComplete {
                    range_start,
                    range_end,
                } => {
                    let empty_data = FactoryAddressData {
                        range_start,
                        range_end,
                        addresses_by_block: HashMap::new(),
                    };

                    if let Some(ref tx) = logs_factory_tx {
                        if tx.send(empty_data.clone()).await.is_err() {
                            return Err(FactoryCollectionError::ChannelSend);
                        }
                    }

                    if let Some(ref tx) = eth_calls_factory_tx {
                        let _ = tx.send(empty_data).await;
                    }
                }
                LogMessage::AllRangesComplete => {
                    break;
                }
            }
        }

        return Ok(());
    }

    let existing_files = scan_existing_parquet_files(&output_dir);

    let mut range_data: HashMap<u64, Vec<LogData>> = HashMap::new();

    tracing::info!(
        "Starting factory collection for chain {} with {} matchers",
        chain.name,
        matchers.len()
    );

    loop {
        let message = match log_rx.recv().await {
            Some(msg) => msg,
            None => break,
        };

        match message {
            LogMessage::Logs(logs) => {
                for log in logs {
                    let range_start = (log.block_number / range_size) * range_size;
                    range_data.entry(range_start).or_default().push(log);
                }
            }
            LogMessage::RangeComplete {
                range_start,
                range_end,
            } => {
                let logs = range_data.remove(&range_start).unwrap_or_default();

                let factory_data = match process_range(
                    range_start,
                    range_end,
                    logs,
                    &matchers,
                    &output_dir,
                    &existing_files,
                )
                .await
                {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::error!("Factory processing failed for range {}-{}: {:?}", range_start, range_end, e);
                        return Err(e);
                    }
                };

                if let Some(ref tx) = logs_factory_tx {
                    if tx.send(factory_data.clone()).await.is_err() {
                        return Err(FactoryCollectionError::ChannelSend);
                    }
                }

                if let Some(ref tx) = eth_calls_factory_tx {
                    let _ = tx.send(factory_data).await;
                }
            }
            LogMessage::AllRangesComplete => {
                break;
            }
        }
    }

    tracing::info!("Factory collection complete for chain {}", chain.name);
    Ok(())
}

fn build_factory_matchers(contracts: &Contracts) -> Vec<FactoryMatcher> {
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
                            factory.collection_name,
                            hex::encode(&event_topic0[..8])
                        );

                        matchers.push(FactoryMatcher {
                            factory_contract_address: *addr,
                            event_topic0,
                            param_location: param_location.clone(),
                            data_types: data_types.clone(),
                            collection_name: factory.collection_name.clone(),
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

    sig.split(',')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            trimmed.parse::<DynSolType>().ok()
        })
        .collect()
}

async fn process_range(
    range_start: u64,
    range_end: u64,
    logs: Vec<LogData>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
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
                    decode_address_from_data(&log.data, &matcher.data_types, indices)
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

        if existing_files.contains(&rel_path) {
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
        std::fs::create_dir_all(&sub_dir)?;
        let output_path = sub_dir.join(&file_name);
        tokio::task::spawn_blocking(move || {
            write_factory_records_to_parquet(&collection_records, &output_path)
        })
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))??;

        tracing::info!(
            "Wrote {} factory addresses for {} to {}",
            record_count,
            collection_name,
            sub_dir.join(&file_name).display()
        );
    }

    for matcher in matchers {
        let collection_name = &matcher.collection_name;
        if !collections_with_events.contains(collection_name) {
            let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
            let rel_path = format!("{}/{}", collection_name, file_name);

            if !existing_files.contains(&rel_path) {
                let sub_dir = output_dir.join(collection_name);
                std::fs::create_dir_all(&sub_dir)?;
                let output_path = sub_dir.join(&file_name);
                tokio::task::spawn_blocking(move || write_empty_factory_parquet(&output_path))
                    .await
                    .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))??;

                tracing::debug!(
                    "Wrote empty factory file for {} range {}-{}",
                    collection_name,
                    range_start,
                    range_end - 1
                );
            }
        }
    }

    Ok(FactoryAddressData {
        range_start,
        range_end,
        addresses_by_block,
    })
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

fn decode_address_from_data(data: &[u8], types: &[DynSolType], indices: &[usize]) -> Option<[u8; 20]> {
    if types.is_empty() || indices.is_empty() {
        return None;
    }

    let tuple_type = DynSolType::Tuple(types.to_vec());

    match tuple_type.abi_decode(data) {
        Ok(DynSolValue::Tuple(values)) => extract_address_recursive(&values, indices),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!("Failed to decode factory event data: {}", e);
            None
        }
    }
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

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();

    // Scan nested directories: dir/collection/*.parquet
    if let Ok(collection_entries) = std::fs::read_dir(dir) {
        for collection_entry in collection_entries.flatten() {
            let collection_path = collection_entry.path();
            if !collection_path.is_dir() {
                continue;
            }
            let collection_name = match collection_entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => continue,
            };

            if let Ok(file_entries) = std::fs::read_dir(&collection_path) {
                for file_entry in file_entries.flatten() {
                    if let Some(name) = file_entry.file_name().to_str() {
                        if name.ends_with(".parquet") {
                            // Store as collection/filename
                            files.insert(format!("{}/{}", collection_name, name));
                        }
                    }
                }
            }
        }
    }
    files
}

fn load_factory_addresses_from_parquet(
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

            // Filename is now just "start-end.parquet"
            let file_name = match path.file_stem().and_then(|s| s.to_str()) {
                Some(name) => name,
                None => continue,
            };

            let range_parts: Vec<&str> = file_name.split('-').collect();
            if range_parts.len() != 2 {
                continue;
            }

            let range_start: u64 = match range_parts[0].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let range_end: u64 = match range_parts[1].parse::<u64>() {
                Ok(v) => v + 1,
                Err(_) => continue,
            };

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

                let collection_names = batch.column_by_name("collection_name").and_then(|c| {
                    c.as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                });

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
) -> HashMap<String, Vec<crate::types::config::eth_call::EthCallConfig>> {
    let mut configs: HashMap<String, Vec<crate::types::config::eth_call::EthCallConfig>> =
        HashMap::new();

    for (_, contract) in contracts {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if !factory.calls.is_empty() {
                    configs.insert(factory.collection_name.clone(), factory.calls.clone());
                }
            }
        }
    }

    configs
}

pub fn get_factory_collection_names(contracts: &Contracts) -> Vec<String> {
    let mut names = Vec::new();

    for (_, contract) in contracts {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if !names.contains(&factory.collection_name) {
                    names.push(factory.collection_name.clone());
                }
            }
        }
    }

    names
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

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
