use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{I256, U256};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Int64Array, Int8Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;

use super::event_parsing::{EventParam, ParsedEvent, TupleFieldInfo};
use super::types::DecoderMessage;
use crate::raw_data::historical::receipts::LogData;
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{
    resolve_factory_config, AddressOrAddresses, Contracts, FactoryCollections,
};
use crate::types::config::raw_data::RawDataCollectionConfig;

#[derive(Debug, Error)]
pub enum LogDecodingError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Event parsing error: {0}")]
    EventParse(#[from] super::event_parsing::EventParseError),

    #[error("Decoding error: {0}")]
    Decode(String),

    #[error("Task join error: {0}")]
    JoinError(String),
}

/// Matcher for a specific event on specific addresses
#[derive(Debug)]
struct EventMatcher {
    /// Contract or collection name (used for output directory)
    name: String,
    /// Event name for output directory (from config name field, or parsed from signature)
    event_name: String,
    /// Parsed event definition
    event: ParsedEvent,
    /// Addresses to match (for regular contracts)
    addresses: HashSet<[u8; 20]>,
    /// If true, this is a factory event (addresses come from factory discovery)
    is_factory: bool,
}

/// Decoded log record ready for writing
/// The decoded_values are flattened to match event.flattened_fields
#[derive(Debug)]
struct DecodedLogRecord {
    block_number: u64,
    block_timestamp: u64,
    transaction_hash: [u8; 32],
    log_index: u32,
    contract_address: [u8; 20],
    /// Decoded parameters flattened to match flattened_fields order
    decoded_values: Vec<DecodedValue>,
}

/// A decoded value with its type information
#[derive(Debug, Clone)]
enum DecodedValue {
    Address([u8; 20]),
    Uint256(U256),
    Int256(I256),
    Uint64(u64),
    Int64(i64),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    /// Named tuple of (field_name, field_value) pairs - for intermediate decoding
    NamedTuple(Vec<(std::string::String, DecodedValue)>),
}

pub async fn decode_logs(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    mut decoder_rx: Receiver<DecoderMessage>,
) -> Result<(), LogDecodingError> {
    let output_base = PathBuf::from(format!("data/derived/{}/decoded/logs", chain.name));
    std::fs::create_dir_all(&output_base)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;

    // Build event matchers from contract configs
    tracing::debug!("Building event matchers for chain {}", chain.name);
    let (regular_matchers, factory_matchers) =
        match build_event_matchers(&chain.contracts, &chain.factory_collections) {
            Ok(matchers) => matchers,
            Err(e) => {
                tracing::error!("Failed to build event matchers for chain {}: {}", chain.name, e);
                return Err(e);
            }
        };

    if regular_matchers.is_empty() && factory_matchers.is_empty() {
        tracing::info!(
            "No events configured for decoding on chain {}",
            chain.name
        );
        // Drain channel and return
        while decoder_rx.recv().await.is_some() {}
        return Ok(());
    }

    tracing::info!(
        "Log decoder starting for chain {} with {} regular matchers and {} factory matchers",
        chain.name,
        regular_matchers.len(),
        factory_matchers.len()
    );

    // Track factory addresses per range
    let mut factory_addresses: HashMap<u64, HashMap<String, HashSet<[u8; 20]>>> = HashMap::new();

    // =========================================================================
    // Catchup phase: Process existing raw log files
    // =========================================================================
    let raw_logs_dir = PathBuf::from(format!("data/raw/{}/logs", chain.name));
    if raw_logs_dir.exists() {
        catchup_decode_logs(
            &raw_logs_dir,
            &output_base,
            range_size,
            &regular_matchers,
            &factory_matchers,
            chain,
        )
        .await?;
    }

    tracing::info!("Log decoding catchup complete for chain {}", chain.name);

    // =========================================================================
    // Live phase: Process new data as it arrives
    // =========================================================================
    loop {
        match decoder_rx.recv().await {
            Some(DecoderMessage::LogsReady {
                range_start,
                range_end,
                logs,
            }) => {
                // Get factory addresses for this range
                let factory_addrs = factory_addresses.remove(&range_start).unwrap_or_default();

                process_logs(
                    &logs,
                    range_start,
                    range_end,
                    &regular_matchers,
                    &factory_matchers,
                    &factory_addrs,
                    &output_base,
                )
                .await?;
            }
            Some(DecoderMessage::FactoryAddresses {
                range_start,
                addresses,
                ..
            }) => {
                // Store factory addresses for when logs arrive
                let addrs_by_collection: HashMap<String, HashSet<[u8; 20]>> = addresses
                    .into_iter()
                    .map(|(name, addrs)| {
                        let set: HashSet<[u8; 20]> = addrs.iter().map(|a| a.0 .0).collect();
                        (name, set)
                    })
                    .collect();
                factory_addresses.insert(range_start, addrs_by_collection);
            }
            Some(DecoderMessage::AllComplete) => {
                break;
            }
            None => {
                break;
            }
            _ => {}
        }
    }

    tracing::info!("Log decoding complete for chain {}", chain.name);
    Ok(())
}

/// Build event matchers from contract configurations
fn build_event_matchers(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> Result<(Vec<EventMatcher>, HashMap<String, Vec<EventMatcher>>), LogDecodingError> {
    let mut regular = Vec::new();
    let mut factory: HashMap<String, Vec<EventMatcher>> = HashMap::new();

    for (contract_name, contract) in contracts {
        let addresses: HashSet<[u8; 20]> = match &contract.address {
            AddressOrAddresses::Single(addr) => [addr.0 .0].into_iter().collect(),
            AddressOrAddresses::Multiple(addrs) => addrs.iter().map(|a| a.0 .0).collect(),
        };

        // Regular contract events
        if let Some(events) = &contract.events {
            for event_config in events {
                let parsed = match ParsedEvent::from_signature(&event_config.signature) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!(
                            "Failed to parse event signature for contract {}: '{}' - {}",
                            contract_name,
                            event_config.signature,
                            e
                        );
                        return Err(e.into());
                    }
                };
                let event_name = event_config
                    .name
                    .clone()
                    .unwrap_or_else(|| parsed.name.clone());
                regular.push(EventMatcher {
                    name: contract_name.clone(),
                    event_name,
                    event: parsed,
                    addresses: addresses.clone(),
                    is_factory: false,
                });
            }
        }

        // Factory-created contract events
        if let Some(factories) = &contract.factories {
            for factory_config in factories {
                let resolved = resolve_factory_config(factory_config, factory_collections);
                if let Some(events) = &resolved.events {
                    let entry = factory.entry(resolved.collection_name.clone()).or_default();
                    for event_config in events {
                        if let Ok(parsed) = ParsedEvent::from_signature(&event_config.signature) {
                            // Deduplicate by topic0
                            if !entry.iter().any(|m| m.event.topic0 == parsed.topic0) {
                                let event_name = event_config
                                    .name
                                    .clone()
                                    .unwrap_or_else(|| parsed.name.clone());
                                entry.push(EventMatcher {
                                    name: resolved.collection_name.clone(),
                                    event_name,
                                    event: parsed,
                                    addresses: HashSet::new(),
                                    is_factory: true,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    Ok((regular, factory))
}

/// Catchup phase: decode existing raw log files
async fn catchup_decode_logs(
    raw_logs_dir: &Path,
    output_base: &Path,
    range_size: u64,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    chain: &ChainConfig,
) -> Result<(), LogDecodingError> {
    // Scan existing decoded files
    let existing_decoded = scan_existing_decoded_files(output_base);

    // Scan raw log files
    let entries = std::fs::read_dir(raw_logs_dir)?;
    let mut raw_files: Vec<(u64, u64, PathBuf)> = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

        // Parse range from filename: logs_0-999.parquet
        if !file_name.starts_with("logs_") || !file_name.ends_with(".parquet") {
            continue;
        }

        let range_str = file_name
            .strip_prefix("logs_")
            .and_then(|s| s.strip_suffix(".parquet"))
            .unwrap_or("");

        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            continue;
        }

        if let (Ok(start), Ok(end)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
            raw_files.push((start, end + 1, path));
        }
    }

    raw_files.sort_by_key(|(start, _, _)| *start);

    tracing::info!(
        "Log decoding catchup: found {} raw log files to check",
        raw_files.len()
    );

    // Load factory addresses from factory parquet files for catchup
    let factory_addresses = load_factory_addresses_for_catchup(chain)?;

    let total_factory_addrs: usize = factory_addresses
        .values()
        .flat_map(|m| m.values())
        .map(|s| s.len())
        .sum();
    tracing::info!(
        "Log decoding catchup: loaded {} factory addresses across {} ranges",
        total_factory_addrs,
        factory_addresses.len()
    );

    for (range_start, range_end, file_path) in raw_files {
        // Check if all events for this range are already decoded
        let all_decoded = check_all_decoded(
            range_start,
            range_end,
            regular_matchers,
            factory_matchers,
            &existing_decoded,
        );

        if all_decoded {
            tracing::debug!(
                "Log decoding catchup: skipping range {}-{}, all events already decoded",
                range_start,
                range_end - 1
            );
            continue;
        }

        tracing::debug!(
            "Log decoding catchup: processing range {}-{}",
            range_start,
            range_end - 1
        );

        // Read raw logs from parquet
        let logs = read_logs_from_parquet(&file_path)?;

        // Get factory addresses for this range
        let factory_addrs = factory_addresses
            .get(&range_start)
            .cloned()
            .unwrap_or_default();

        process_logs(
            &logs,
            range_start,
            range_end,
            regular_matchers,
            factory_matchers,
            &factory_addrs,
            output_base,
        )
        .await?;
    }

    Ok(())
}

/// Check if all event files for a range are already decoded
fn check_all_decoded(
    range_start: u64,
    range_end: u64,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    existing: &HashSet<String>,
) -> bool {
    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

    // Check regular matchers
    for matcher in regular_matchers {
        let rel_path = format!("{}/{}/{}", matcher.name, matcher.event_name, file_name);
        if !existing.contains(&rel_path) {
            return false;
        }
    }

    // Check factory matchers
    for (_, matchers) in factory_matchers {
        for matcher in matchers {
            let rel_path = format!("{}/{}/{}", matcher.name, matcher.event_name, file_name);
            if !existing.contains(&rel_path) {
                return false;
            }
        }
    }

    true
}

/// Scan existing decoded files
fn scan_existing_decoded_files(output_base: &Path) -> HashSet<String> {
    let mut files = HashSet::new();

    fn scan_recursive(dir: &Path, base: &Path, files: &mut HashSet<String>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan_recursive(&path, base, files);
                } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    if let Ok(rel) = path.strip_prefix(base) {
                        files.insert(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    scan_recursive(output_base, output_base, &mut files);
    files
}

/// Load factory addresses from parquet files for catchup
fn load_factory_addresses_for_catchup(
    chain: &ChainConfig,
) -> Result<HashMap<u64, HashMap<String, HashSet<[u8; 20]>>>, LogDecodingError> {
    let mut result: HashMap<u64, HashMap<String, HashSet<[u8; 20]>>> = HashMap::new();

    // Look for factory parquet files
    let factories_dir = PathBuf::from(format!("data/derived/{}/factories", chain.name));
    if !factories_dir.exists() {
        return Ok(result);
    }

    // Scan factory directories
    if let Ok(entries) = std::fs::read_dir(&factories_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let collection_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();

            // Read parquet files in this collection
            if let Ok(files) = std::fs::read_dir(&path) {
                for file_entry in files.flatten() {
                    let file_path = file_entry.path();
                    if !file_path
                        .extension()
                        .map(|e| e == "parquet")
                        .unwrap_or(false)
                    {
                        continue;
                    }

                    // Parse range from filename
                    let file_name = file_path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("");
                    let range_str = file_name.strip_suffix(".parquet").unwrap_or("");
                    let parts: Vec<&str> = range_str.split('-').collect();
                    if parts.len() != 2 {
                        continue;
                    }

                    let range_start: u64 = match parts[0].parse() {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    // Read addresses from parquet
                    if let Ok(addresses) = read_factory_addresses_from_parquet(&file_path) {
                        result
                            .entry(range_start)
                            .or_default()
                            .entry(collection_name.clone())
                            .or_default()
                            .extend(addresses);
                    }
                }
            }
        }
    }

    Ok(result)
}

/// Read factory addresses from a factory parquet file
fn read_factory_addresses_from_parquet(path: &Path) -> Result<HashSet<[u8; 20]>, LogDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut addresses = HashSet::new();

    for batch_result in reader {
        let batch = batch_result?;

        // Look for "factory_address" column (the address of factory-created contracts)
        if let Some(col_idx) = batch.schema().index_of("factory_address").ok() {
            let col = batch.column(col_idx);
            if let Some(addr_array) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                for i in 0..addr_array.len() {
                    if !addr_array.is_null(i) {
                        let bytes = addr_array.value(i);
                        if bytes.len() == 20 {
                            let mut addr = [0u8; 20];
                            addr.copy_from_slice(bytes);
                            addresses.insert(addr);
                        }
                    }
                }
            }
        }
    }

    Ok(addresses)
}

/// Read logs from a raw parquet file
fn read_logs_from_parquet(path: &Path) -> Result<Vec<LogData>, LogDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut logs = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let tx_hash_idx = schema.index_of("transaction_hash").ok();
        let log_index_idx = schema.index_of("log_index").ok();
        let address_idx = schema.index_of("address").ok();
        let topics_idx = schema.index_of("topics").ok();
        let data_idx = schema.index_of("data").ok();

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

            let transaction_hash = tx_hash_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 32 {
                                let mut arr = [0u8; 32];
                                arr.copy_from_slice(bytes);
                                Some(alloy::primitives::B256::from(arr))
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let log_index = log_index_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let address = address_idx
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

            let topics = topics_idx
                .and_then(|i| {
                    use arrow::array::ListArray;
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .and_then(|list| {
                            let values = list.value(row);
                            values
                                .as_any()
                                .downcast_ref::<FixedSizeBinaryArray>()
                                .map(|topics_arr| {
                                    (0..topics_arr.len())
                                        .filter_map(|j| {
                                            let bytes = topics_arr.value(j);
                                            if bytes.len() == 32 {
                                                let mut arr = [0u8; 32];
                                                arr.copy_from_slice(bytes);
                                                Some(arr)
                                            } else {
                                                None
                                            }
                                        })
                                        .collect::<Vec<[u8; 32]>>()
                                })
                        })
                })
                .unwrap_or_default();

            let data = data_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|a| a.value(row).to_vec())
                })
                .unwrap_or_default();

            logs.push(LogData {
                block_number,
                block_timestamp,
                transaction_hash,
                log_index,
                address,
                topics,
                data,
            });
        }
    }

    Ok(logs)
}

/// Process logs and write decoded data
async fn process_logs(
    logs: &[LogData],
    range_start: u64,
    range_end: u64,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    factory_addresses: &HashMap<String, HashSet<[u8; 20]>>,
    output_base: &Path,
) -> Result<(), LogDecodingError> {
    // Group decoded logs by (contract_name, event_name)
    let mut decoded_by_event: HashMap<(String, String), (Vec<DecodedLogRecord>, &ParsedEvent)> =
        HashMap::new();

    for log in logs {
        // Skip if no topic0
        if log.topics.is_empty() {
            continue;
        }
        let topic0 = log.topics[0];

        // Try regular matchers
        for matcher in regular_matchers {
            if !matcher.addresses.contains(&log.address) {
                continue;
            }
            if topic0 != matcher.event.topic0 {
                continue;
            }

            if let Some(decoded) = decode_log(log, &matcher.event)? {
                let key = (matcher.name.clone(), matcher.event_name.clone());
                decoded_by_event
                    .entry(key)
                    .or_insert_with(|| (Vec::new(), &matcher.event))
                    .0
                    .push(decoded);
            }
        }

        // Try factory matchers
        for (collection_name, matchers) in factory_matchers {
            let addrs = match factory_addresses.get(collection_name) {
                Some(addrs) => addrs,
                None => continue,
            };

            if !addrs.contains(&log.address) {
                continue;
            }

            for matcher in matchers {
                if topic0 != matcher.event.topic0 {
                    continue;
                }

                if let Some(decoded) = decode_log(log, &matcher.event)? {
                    let key = (collection_name.clone(), matcher.event_name.clone());
                    decoded_by_event
                        .entry(key)
                        .or_insert_with(|| (Vec::new(), &matcher.event))
                        .0
                        .push(decoded);
                }
            }
        }
    }

    let total_decoded: usize = decoded_by_event.values().map(|(v, _)| v.len()).sum();
    tracing::debug!(
        "Log decoding range {}-{}: {} logs in file, {} decoded events across {} event types",
        range_start,
        range_end - 1,
        logs.len(),
        total_decoded,
        decoded_by_event.len()
    );

    // Write decoded data to parquet files
    for ((contract_name, event_name), (records, parsed_event)) in decoded_by_event {
        let output_dir = output_base.join(&contract_name).join(&event_name);
        std::fs::create_dir_all(&output_dir)?;

        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let output_path = output_dir.join(&file_name);

        write_decoded_logs_to_parquet(&records, parsed_event, &output_path)?;

        tracing::info!(
            "Wrote {} decoded {} events to {}",
            records.len(),
            event_name,
            output_path.display()
        );
    }

    // Write empty files for events with no matches (to mark range as processed)
    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

    for matcher in regular_matchers {
        let output_dir = output_base.join(&matcher.name).join(&matcher.event_name);
        let output_path = output_dir.join(&file_name);

        if !output_path.exists() {
            std::fs::create_dir_all(&output_dir)?;
            write_decoded_logs_to_parquet(&[], &matcher.event, &output_path)?;
        }
    }

    for (_, matchers) in factory_matchers {
        for matcher in matchers {
            let output_dir = output_base.join(&matcher.name).join(&matcher.event_name);
            let output_path = output_dir.join(&file_name);

            if !output_path.exists() {
                std::fs::create_dir_all(&output_dir)?;
                write_decoded_logs_to_parquet(&[], &matcher.event, &output_path)?;
            }
        }
    }

    Ok(())
}

/// Decode a single log
/// Returns decoded values flattened to match event.flattened_fields order
fn decode_log(
    log: &LogData,
    event: &ParsedEvent,
) -> Result<Option<DecodedLogRecord>, LogDecodingError> {
    let indexed_params = event.indexed_params();
    let data_params = event.data_params();

    // Collect decoded values per param (may be nested for tuples)
    let mut param_values: Vec<DecodedValue> = Vec::new();

    // Decode indexed params from topics (topics[1..])
    for (i, param) in indexed_params.iter().enumerate() {
        let topic_idx = i + 1; // topic[0] is event signature
        if topic_idx >= log.topics.len() {
            return Ok(None); // Not enough topics
        }

        let topic = &log.topics[topic_idx];

        // Check if this is an indexed tuple - store as hash
        if param.tuple_fields.is_some() {
            if let Some(TupleFieldInfo::Tuple(_)) = &param.tuple_fields {
                param_values.push(DecodedValue::Bytes32(*topic));
                continue;
            }
        }

        let value = decode_topic(topic, &param.param_type)?;
        param_values.push(value);
    }

    // Decode non-indexed params from data
    if !data_params.is_empty() {
        let data_types: Vec<DynSolType> = data_params.iter().map(|p| p.param_type.clone()).collect();

        let tuple_type = DynSolType::Tuple(data_types);

        match tuple_type.abi_decode(&log.data) {
            Ok(DynSolValue::Tuple(values)) => {
                for (value, param) in values.iter().zip(data_params.iter()) {
                    let decoded = convert_dyn_sol_value_with_tuple_info(value, &param.tuple_fields)?;
                    param_values.push(decoded);
                }
            }
            Ok(_) => return Ok(None),
            Err(e) => {
                tracing::debug!("Failed to decode log data: {}", e);
                return Ok(None);
            }
        }
    }

    // Flatten param_values to match flattened_fields order
    let flattened = flatten_param_values(&param_values, &event.params);

    // Verify we have the right number of values
    if flattened.len() != event.flattened_fields.len() {
        tracing::debug!(
            "Flattened values count {} doesn't match flattened_fields count {}",
            flattened.len(),
            event.flattened_fields.len()
        );
        return Ok(None);
    }

    Ok(Some(DecodedLogRecord {
        block_number: log.block_number,
        block_timestamp: log.block_timestamp,
        transaction_hash: log.transaction_hash.0,
        log_index: log.log_index,
        contract_address: log.address,
        decoded_values: flattened,
    }))
}

/// Flatten param_values to match the order of flattened_fields
fn flatten_param_values(values: &[DecodedValue], params: &[EventParam]) -> Vec<DecodedValue> {
    let mut result = Vec::new();

    for (value, param) in values.iter().zip(params.iter()) {
        flatten_single_value(value, param, &mut result);
    }

    result
}

/// Recursively flatten a single value based on its param info
fn flatten_single_value(value: &DecodedValue, param: &EventParam, output: &mut Vec<DecodedValue>) {
    // Check if this is an indexed tuple (stored as hash)
    if param.indexed {
        if let Some(TupleFieldInfo::Tuple(_)) = &param.tuple_fields {
            // Indexed tuple - just add the hash directly
            output.push(value.clone());
            return;
        }
    }

    match (&param.tuple_fields, value) {
        (Some(TupleFieldInfo::Tuple(field_infos)), DecodedValue::NamedTuple(named_values)) => {
            // Flatten the named tuple
            if let DynSolType::Tuple(types) = &param.param_type {
                for ((field_name, field_info), field_type) in field_infos.iter().zip(types.iter()) {
                    // Find the matching value
                    let field_value = named_values
                        .iter()
                        .find(|(n, _)| n == field_name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(DecodedValue::Bytes(vec![]));

                    // Create sub-param for recursion
                    let sub_param = EventParam {
                        name: field_name.clone(),
                        param_type: field_type.clone(),
                        type_string: String::new(),
                        indexed: param.indexed,
                        tuple_fields: Some(field_info.clone()),
                    };

                    flatten_single_value(&field_value, &sub_param, output);
                }
            }
        }
        _ => {
            // Leaf value - add directly
            output.push(value.clone());
        }
    }
}

/// Decode a value from a topic
fn decode_topic(topic: &[u8; 32], param_type: &DynSolType) -> Result<DecodedValue, LogDecodingError> {
    match param_type {
        DynSolType::Address => {
            let mut addr = [0u8; 20];
            addr.copy_from_slice(&topic[12..32]);
            Ok(DecodedValue::Address(addr))
        }
        DynSolType::Uint(bits) => {
            let val = U256::from_be_bytes(*topic);
            if *bits <= 64 {
                Ok(DecodedValue::Uint64(val.try_into().unwrap_or(u64::MAX)))
            } else {
                Ok(DecodedValue::Uint256(val))
            }
        }
        DynSolType::Int(bits) => {
            let val = I256::from_be_bytes(*topic);
            if *bits <= 64 {
                Ok(DecodedValue::Int64(val.try_into().unwrap_or(i64::MAX)))
            } else {
                Ok(DecodedValue::Int256(val))
            }
        }
        DynSolType::Bool => Ok(DecodedValue::Bool(topic[31] != 0)),
        DynSolType::FixedBytes(32) => Ok(DecodedValue::Bytes32(*topic)),
        _ => {
            // For complex indexed types, they're stored as keccak256 hash
            Ok(DecodedValue::Bytes32(*topic))
        }
    }
}

/// Convert DynSolValue to DecodedValue, handling tuples with field info
fn convert_dyn_sol_value_with_tuple_info(
    value: &DynSolValue,
    tuple_fields: &Option<TupleFieldInfo>,
) -> Result<DecodedValue, LogDecodingError> {
    match (value, tuple_fields) {
        (DynSolValue::Tuple(values), Some(TupleFieldInfo::Tuple(field_infos))) => {
            // Named tuple - create named values
            let mut named_values = Vec::new();
            for ((field_name, field_info), val) in field_infos.iter().zip(values.iter()) {
                let decoded = convert_dyn_sol_value_with_tuple_info(val, &Some(field_info.clone()))?;
                named_values.push((field_name.clone(), decoded));
            }
            Ok(DecodedValue::NamedTuple(named_values))
        }
        _ => convert_dyn_sol_value(value),
    }
}

/// Convert DynSolValue to DecodedValue (for simple types)
fn convert_dyn_sol_value(value: &DynSolValue) -> Result<DecodedValue, LogDecodingError> {
    match value {
        DynSolValue::Address(addr) => Ok(DecodedValue::Address(addr.0 .0)),
        DynSolValue::Uint(val, bits) => {
            if *bits <= 8 {
                Ok(DecodedValue::Uint8((*val).try_into().unwrap_or(u8::MAX)))
            } else if *bits <= 64 {
                Ok(DecodedValue::Uint64((*val).try_into().unwrap_or(u64::MAX)))
            } else {
                Ok(DecodedValue::Uint256(*val))
            }
        }
        DynSolValue::Int(val, bits) => {
            if *bits <= 8 {
                Ok(DecodedValue::Int8((*val).try_into().unwrap_or(i8::MAX)))
            } else if *bits <= 64 {
                Ok(DecodedValue::Int64((*val).try_into().unwrap_or(i64::MAX)))
            } else {
                Ok(DecodedValue::Int256(*val))
            }
        }
        DynSolValue::Bool(b) => Ok(DecodedValue::Bool(*b)),
        DynSolValue::FixedBytes(bytes, 32) => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes[..]);
            Ok(DecodedValue::Bytes32(arr))
        }
        DynSolValue::FixedBytes(bytes, _) => Ok(DecodedValue::Bytes(bytes.to_vec())),
        DynSolValue::Bytes(bytes) => Ok(DecodedValue::Bytes(bytes.clone())),
        DynSolValue::String(s) => Ok(DecodedValue::String(s.clone())),
        DynSolValue::Tuple(values) => {
            // Unnamed tuple - flatten directly
            // This shouldn't happen for our named tuple signatures
            let named = values
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let decoded = convert_dyn_sol_value(v)?;
                    Ok((format!("field_{}", i), decoded))
                })
                .collect::<Result<Vec<_>, LogDecodingError>>()?;
            Ok(DecodedValue::NamedTuple(named))
        }
        _ => Err(LogDecodingError::Decode(format!(
            "Unsupported value type: {:?}",
            value
        ))),
    }
}

/// Write decoded logs to parquet using flattened fields
fn write_decoded_logs_to_parquet(
    records: &[DecodedLogRecord],
    event: &ParsedEvent,
    output_path: &Path,
) -> Result<(), LogDecodingError> {
    // Build schema using flattened fields
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("transaction_hash", DataType::FixedSizeBinary(32), false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("contract_address", DataType::FixedSizeBinary(20), false),
    ];

    // Add fields from flattened_fields
    for flattened in &event.flattened_fields {
        fields.push(Field::new(
            &flattened.full_name,
            flattened.arrow_type.clone(),
            true,
        ));
    }

    let schema = Arc::new(Schema::new(fields));

    // Build arrays
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // transaction_hash
    if records.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(32).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.transaction_hash.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    // log_index
    let arr: UInt32Array = records.iter().map(|r| Some(r.log_index)).collect();
    arrays.push(Arc::new(arr));

    // contract_address
    if records.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    // Flattened decoded parameters
    for (field_idx, flattened) in event.flattened_fields.iter().enumerate() {
        let arr = build_flattened_field_array(records, field_idx, &flattened.leaf_type)?;
        arrays.push(arr);
    }

    // Write to parquet
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

/// Build an Arrow array for a flattened field
fn build_flattened_field_array(
    records: &[DecodedLogRecord],
    field_idx: usize,
    leaf_type: &DynSolType,
) -> Result<ArrayRef, LogDecodingError> {
    match leaf_type {
        DynSolType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Address(addr)) => addr.as_slice(),
                        _ => &[0u8; 20][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        DynSolType::Uint(bits) => {
            if *bits <= 8 {
                let arr: arrow::array::UInt8Array = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Uint8(v)) => Some(*v),
                        Some(DecodedValue::Uint64(v)) => Some(*v as u8),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(arr))
            } else if *bits <= 64 {
                let arr: UInt64Array = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Uint64(v)) => Some(*v),
                        Some(DecodedValue::Uint8(v)) => Some(*v as u64),
                        Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(arr))
            } else {
                let arr: StringArray = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Uint256(v)) => Some(v.to_string()),
                        Some(DecodedValue::Uint64(v)) => Some(v.to_string()),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(arr))
            }
        }
        DynSolType::Int(bits) => {
            if *bits <= 8 {
                let arr: Int8Array = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Int8(v)) => Some(*v),
                        Some(DecodedValue::Int64(v)) => Some(*v as i8),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(arr))
            } else if *bits <= 64 {
                let arr: Int64Array = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Int64(v)) => Some(*v),
                        Some(DecodedValue::Int8(v)) => Some(*v as i64),
                        Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(arr))
            } else {
                let arr: StringArray = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Int256(v)) => Some(v.to_string()),
                        Some(DecodedValue::Int64(v)) => Some(v.to_string()),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(arr))
            }
        }
        DynSolType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| match r.decoded_values.get(field_idx) {
                    Some(DecodedValue::Bool(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DynSolType::FixedBytes(32) => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Bytes32(b)) => b.as_slice(),
                        _ => &[0u8; 32][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        DynSolType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| match r.decoded_values.get(field_idx) {
                    Some(DecodedValue::String(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        _ => {
            // Fallback: Binary
            let arr: BinaryArray = records
                .iter()
                .map(|r| match r.decoded_values.get(field_idx) {
                    Some(DecodedValue::Bytes(b)) => Some(b.as_slice()),
                    Some(DecodedValue::Bytes32(b)) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
    }
}
