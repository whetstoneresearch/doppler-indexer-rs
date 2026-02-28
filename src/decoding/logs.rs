use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{I256, U256};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Int64Array, Int8Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

use super::event_parsing::{EventParam, ParsedEvent, TupleFieldInfo};
use super::types::DecoderMessage;
use crate::raw_data::historical::factories::RecollectRequest;
use crate::raw_data::historical::receipts::LogData;
use crate::transformations::{
    DecodedEvent as TransformDecodedEvent, DecodedEventsMessage,
    DecodedValue as TransformDecodedValue, RangeCompleteMessage,
};
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
#[derive(Debug, Clone)]
pub(crate) struct EventMatcher {
    /// Contract or collection name (used for output directory)
    pub name: String,
    /// Event name for output directory (from config name field, or parsed from signature)
    pub event_name: String,
    /// Parsed event definition
    pub event: ParsedEvent,
    /// Addresses to match (for regular contracts)
    pub addresses: HashSet<[u8; 20]>,
    /// If true, this is a factory event (addresses come from factory discovery)
    pub is_factory: bool,
    /// Start block for this contract (events before this block are skipped)
    pub start_block: Option<u64>,
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
    transform_tx: Option<Sender<DecodedEventsMessage>>,
    recollect_tx: Option<Sender<RecollectRequest>>,
    complete_tx: Option<Sender<RangeCompleteMessage>>,
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

    // =========================================================================
    // Catchup phase: Process existing raw log files
    // =========================================================================
    let raw_logs_dir = PathBuf::from(format!("data/raw/{}/logs", chain.name));
    if raw_logs_dir.exists() {
        super::catchup::catchup_decode_logs(
            &raw_logs_dir,
            &output_base,
            range_size,
            &regular_matchers,
            &factory_matchers,
            chain,
            raw_data_config,
            transform_tx.as_ref(),
            recollect_tx.as_ref(),
        )
        .await?;
    }

    tracing::info!("Log decoding catchup complete for chain {}", chain.name);

    // =========================================================================
    // Live phase: Process new data as it arrives
    // =========================================================================
    super::current::decode_logs_live(
        &mut decoder_rx,
        &regular_matchers,
        &factory_matchers,
        &output_base,
        transform_tx.as_ref(),
        complete_tx.as_ref(),
    )
    .await?;

    tracing::info!("Log decoding complete for chain {}", chain.name);
    Ok(())
}

/// Build event matchers from contract configurations
pub(crate) fn build_event_matchers(
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
            let start_block = contract.start_block.map(|u| u.to::<u64>());
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
                    start_block,
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
                                    // Factory events use discovery block, not start_block
                                    start_block: None,
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

/// Process logs and write decoded data
pub(crate) async fn process_logs(
    logs: &[LogData],
    range_start: u64,
    range_end: u64,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    factory_addresses: &HashMap<String, HashSet<[u8; 20]>>,
    output_base: &Path,
    transform_tx: Option<&Sender<DecodedEventsMessage>>,
    complete_tx: Option<&Sender<RangeCompleteMessage>>,
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
            // Skip if block is before contract's start_block
            if let Some(sb) = matcher.start_block {
                if log.block_number < sb {
                    continue;
                }
            }
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
        // Skip if entire range is before contract's start_block
        if let Some(sb) = matcher.start_block {
            if range_end <= sb {
                continue;
            }
        }

        let output_dir = output_base.join(&matcher.name).join(&matcher.event_name);
        let output_path = output_dir.join(&file_name);

        if !output_path.exists() {
            std::fs::create_dir_all(&output_dir)?;
            write_decoded_logs_to_parquet(&[], &matcher.event, &output_path)?;
        }
    }

    for (_, matchers) in factory_matchers {
        for matcher in matchers {
            // Skip if entire range is before contract's start_block (for factories, start_block is None)
            if let Some(sb) = matcher.start_block {
                if range_end <= sb {
                    continue;
                }
            }

            let output_dir = output_base.join(&matcher.name).join(&matcher.event_name);
            let output_path = output_dir.join(&file_name);

            if !output_path.exists() {
                std::fs::create_dir_all(&output_dir)?;
                write_decoded_logs_to_parquet(&[], &matcher.event, &output_path)?;
            }
        }
    }

    // Send to transformation channel if enabled
    if let Some(tx) = transform_tx {
        // Re-decode and send to transformation engine (we need to iterate again to build the transform messages)
        // Group decoded logs by (contract_name, event_name) for sending
        let mut transform_events_by_type: HashMap<(String, String), Vec<TransformDecodedEvent>> =
            HashMap::new();

        for log in logs {
            if log.topics.is_empty() {
                continue;
            }
            let topic0 = log.topics[0];

            // Try regular matchers
            for matcher in regular_matchers {
                // Skip if block is before contract's start_block
                if let Some(sb) = matcher.start_block {
                    if log.block_number < sb {
                        continue;
                    }
                }
                if !matcher.addresses.contains(&log.address) {
                    continue;
                }
                if topic0 != matcher.event.topic0 {
                    continue;
                }

                if let Some(decoded) = decode_log(log, &matcher.event)? {
                    let transform_event = convert_to_transform_event(
                        &decoded,
                        &matcher.event,
                        &matcher.name,
                        &matcher.event_name,
                    );
                    let key = (matcher.name.clone(), matcher.event_name.clone());
                    transform_events_by_type.entry(key).or_default().push(transform_event);
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
                        let transform_event = convert_to_transform_event(
                            &decoded,
                            &matcher.event,
                            collection_name,
                            &matcher.event_name,
                        );
                        let key = (collection_name.clone(), matcher.event_name.clone());
                        transform_events_by_type.entry(key).or_default().push(transform_event);
                    }
                }
            }
        }

        // Send each event type as a message
        for ((source_name, event_name), events) in transform_events_by_type {
            if events.is_empty() {
                continue;
            }
            let msg = DecodedEventsMessage {
                range_start,
                range_end,
                source_name,
                event_name,
                events,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!("Failed to send decoded events to transformation channel: {}", e);
            }
        }
    }

    // Signal that all events for this range have been sent
    if let Some(tx) = complete_tx {
        let msg = RangeCompleteMessage { range_start, range_end };
        if let Err(e) = tx.send(msg).await {
            tracing::warn!("Failed to send range complete: {}", e);
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

/// Convert internal DecodedValue to transformation DecodedValue
fn convert_decoded_value(value: &DecodedValue) -> TransformDecodedValue {
    match value {
        DecodedValue::Address(a) => TransformDecodedValue::Address(*a),
        DecodedValue::Uint256(v) => TransformDecodedValue::Uint256(*v),
        DecodedValue::Int256(v) => TransformDecodedValue::Int256(*v),
        DecodedValue::Uint64(v) => TransformDecodedValue::Uint64(*v),
        DecodedValue::Int64(v) => TransformDecodedValue::Int64(*v),
        DecodedValue::Uint8(v) => TransformDecodedValue::Uint8(*v),
        DecodedValue::Int8(v) => TransformDecodedValue::Int8(*v),
        DecodedValue::Bool(v) => TransformDecodedValue::Bool(*v),
        DecodedValue::Bytes32(b) => TransformDecodedValue::Bytes32(*b),
        DecodedValue::Bytes(b) => TransformDecodedValue::Bytes(b.clone()),
        DecodedValue::String(s) => TransformDecodedValue::String(s.clone()),
        DecodedValue::NamedTuple(fields) => {
            let converted: Vec<(String, TransformDecodedValue)> = fields
                .iter()
                .map(|(name, val)| (name.clone(), convert_decoded_value(val)))
                .collect();
            TransformDecodedValue::NamedTuple(converted)
        }
    }
}

/// Convert a DecodedLogRecord to a TransformDecodedEvent
fn convert_to_transform_event(
    record: &DecodedLogRecord,
    parsed_event: &ParsedEvent,
    source_name: &str,
    event_name: &str,
) -> TransformDecodedEvent {
    // Build params HashMap from flattened values
    let mut params = HashMap::new();
    for (idx, flattened) in parsed_event.flattened_fields.iter().enumerate() {
        if let Some(value) = record.decoded_values.get(idx) {
            params.insert(flattened.full_name.clone(), convert_decoded_value(value));
        }
    }

    TransformDecodedEvent {
        block_number: record.block_number,
        block_timestamp: record.block_timestamp,
        transaction_hash: record.transaction_hash,
        log_index: record.log_index,
        contract_address: record.contract_address,
        source_name: source_name.to_string(),
        event_name: event_name.to_string(),
        event_signature: parsed_event.signature.clone(),
        params,
    }
}
