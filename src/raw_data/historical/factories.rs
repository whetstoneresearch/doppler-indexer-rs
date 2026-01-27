use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::Address;
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, StringBuilder, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::raw_data::historical::receipts::LogData;
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
    mut log_rx: Receiver<Vec<LogData>>,
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
                tx.send(factory_data.clone())
                    .await
                    .map_err(|_| FactoryCollectionError::ChannelSend)?;
            }

            if let Some(ref tx) = eth_calls_factory_tx {
                tx.send(factory_data)
                    .await
                    .map_err(|_| FactoryCollectionError::ChannelSend)?;
            }
        }
    }

    let matchers = build_factory_matchers(&chain.contracts);

    if matchers.is_empty() {
        tracing::info!("No factories configured for chain {}", chain.name);
        let mut range_blocks: HashMap<u64, HashSet<u64>> = HashMap::new();

        while let Some(logs) = log_rx.recv().await {
            for log in logs {
                let range_start = (log.block_number / range_size) * range_size;
                range_blocks
                    .entry(range_start)
                    .or_default()
                    .insert(log.block_number);
            }

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
                range_blocks.remove(&range_start);
                let range_end = range_start + range_size;

                let empty_data = FactoryAddressData {
                    range_start,
                    range_end,
                    addresses_by_block: HashMap::new(),
                };

                if let Some(ref tx) = logs_factory_tx {
                    tx.send(empty_data.clone())
                        .await
                        .map_err(|_| FactoryCollectionError::ChannelSend)?;
                }

                if let Some(ref tx) = eth_calls_factory_tx {
                    tx.send(empty_data)
                        .await
                        .map_err(|_| FactoryCollectionError::ChannelSend)?;
                }
            }
        }

        for (range_start, blocks) in range_blocks {
            if blocks.is_empty() {
                continue;
            }
            let max_block = *blocks.iter().max().unwrap_or(&range_start);
            let range_end = max_block + 1;

            let empty_data = FactoryAddressData {
                range_start,
                range_end,
                addresses_by_block: HashMap::new(),
            };

            if let Some(ref tx) = logs_factory_tx {
                tx.send(empty_data.clone())
                    .await
                    .map_err(|_| FactoryCollectionError::ChannelSend)?;
            }

            if let Some(ref tx) = eth_calls_factory_tx {
                tx.send(empty_data)
                    .await
                    .map_err(|_| FactoryCollectionError::ChannelSend)?;
            }
        }

        return Ok(());
    }

    let existing_files = scan_existing_parquet_files(&output_dir);

    let mut range_data: HashMap<u64, Vec<LogData>> = HashMap::new();
    let mut range_blocks: HashMap<u64, HashSet<u64>> = HashMap::new();

    tracing::info!(
        "Starting factory collection for chain {} with {} matchers",
        chain.name,
        matchers.len()
    );

    while let Some(logs) = log_rx.recv().await {
        for log in logs {
            let range_start = (log.block_number / range_size) * range_size;
            range_blocks
                .entry(range_start)
                .or_default()
                .insert(log.block_number);
            range_data.entry(range_start).or_default().push(log);
        }

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
            let range_end = range_start + range_size;

            if let Some(logs) = range_data.remove(&range_start) {
                range_blocks.remove(&range_start);

                let factory_data = process_range(
                    range_start,
                    range_end,
                    logs,
                    &matchers,
                    &output_dir,
                    &existing_files,
                )
                .await?;

                if let Some(ref tx) = logs_factory_tx {
                    tx.send(factory_data.clone())
                        .await
                        .map_err(|_| FactoryCollectionError::ChannelSend)?;
                }

                if let Some(ref tx) = eth_calls_factory_tx {
                    tx.send(factory_data)
                        .await
                        .map_err(|_| FactoryCollectionError::ChannelSend)?;
                }
            }
        }
    }

    for (range_start, logs) in range_data {
        if logs.is_empty() {
            continue;
        }

        let max_block = logs
            .iter()
            .map(|l| l.block_number)
            .max()
            .unwrap_or(range_start);
        let range_end = max_block + 1;

        let factory_data = process_range(
            range_start,
            range_end,
            logs,
            &matchers,
            &output_dir,
            &existing_files,
        )
        .await?;

        if let Some(ref tx) = logs_factory_tx {
            tx.send(factory_data.clone())
                .await
                .map_err(|_| FactoryCollectionError::ChannelSend)?;
        }

        if let Some(ref tx) = eth_calls_factory_tx {
            tx.send(factory_data)
                .await
                .map_err(|_| FactoryCollectionError::ChannelSend)?;
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
                let event_topic0 = factory.compute_event_signature();
                let param_location = factory.parse_factory_parameter();

                let data_types = parse_data_signature(&factory.factory_events.data_signature);

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
                FactoryParameterLocation::Data(idx) => {
                    decode_address_from_data(&log.data, &matcher.data_types, *idx)
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

    for (collection_name, collection_records) in by_collection {
        let file_name = format!(
            "{}_{}-{}.parquet",
            collection_name,
            range_start,
            range_end - 1
        );

        if existing_files.contains(&file_name) {
            tracing::debug!(
                "Skipping factory parquet for {} blocks {}-{} (already exists)",
                collection_name,
                range_start,
                range_end - 1
            );
            continue;
        }

        let record_count = collection_records.len();
        let output_path = output_dir.join(&file_name);
        tokio::task::spawn_blocking(move || {
            write_factory_records_to_parquet(&collection_records, &output_path)
        })
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))??;

        tracing::info!(
            "Wrote {} factory addresses for {} to {}",
            record_count,
            collection_name,
            output_dir.join(&file_name).display()
        );
    }

    Ok(FactoryAddressData {
        range_start,
        range_end,
        addresses_by_block,
    })
}

fn decode_address_from_data(data: &[u8], types: &[DynSolType], index: usize) -> Option<[u8; 20]> {
    if types.is_empty() || index >= types.len() {
        return None;
    }

    let tuple_type = DynSolType::Tuple(types.to_vec());

    match tuple_type.abi_decode(data) {
        Ok(DynSolValue::Tuple(values)) => {
            if index < values.len() {
                if let DynSolValue::Address(addr) = &values[index] {
                    return Some(addr.0 .0);
                }
            }
            None
        }
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
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".parquet") {
                    files.insert(name.to_string());
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

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(results),
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.extension().map(|e| e == "parquet").unwrap_or(false) {
            continue;
        }

        let file_name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(name) => name,
            None => continue,
        };

        let parts: Vec<&str> = file_name.rsplitn(2, '_').collect();
        if parts.len() != 2 {
            continue;
        }

        let range_part = parts[0];
        let range_parts: Vec<&str> = range_part.split('-').collect();
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
