use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{I256, U256};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array,
    Int8Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

use super::event_parsing::{EventParam, ParsedEvent, TupleFieldInfo};
use super::types::{BuiltMatchers, DecoderMessage, LogDecoderOutputs, LogMatcherConfig};
use crate::live::{LiveDecodedLog, LiveStorage};
use crate::raw_data::historical::factories::RecollectRequest;
use crate::raw_data::historical::receipts::LogData;
use crate::storage::contract_index::{
    range_key, read_contract_index, update_contract_index, write_contract_index, ExpectedContracts,
};
use crate::transformations::{
    DecodedEvent as TransformDecodedEvent, DecodedEventsMessage, RangeCompleteKind,
    RangeCompleteMessage,
};
use crate::types::chain::{ChainAddress, LogPosition, TxId};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{
    resolve_factory_config, AddressOrAddresses, Contracts, FactoryCollections,
};
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::decoded::DecodedValue;

#[derive(Debug, Error)]
pub enum LogDecodingError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet read error: {0}")]
    ParquetRead(#[from] crate::storage::parquet_readers::ParquetReadError),

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
    pub _is_factory: bool,
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

pub async fn decode_logs(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    mut decoder_rx: Receiver<DecoderMessage>,
    transform_tx: Option<Sender<DecodedEventsMessage>>,
    recollect_tx: Option<Sender<RecollectRequest>>,
    complete_tx: Option<Sender<RangeCompleteMessage>>,
    skip_catchup: bool,
) -> Result<(), LogDecodingError> {
    let output_base = crate::storage::paths::decoded_logs_dir(&chain.name);
    std::fs::create_dir_all(&output_base)?;

    // Build event matchers from contract configs
    tracing::debug!("Building event matchers for chain {}", chain.name);
    let (regular_matchers, factory_matchers) =
        match build_event_matchers(&chain.contracts, &chain.factory_collections) {
            Ok(matchers) => matchers,
            Err(e) => {
                tracing::error!(
                    "Failed to build event matchers for chain {}: {}",
                    chain.name,
                    e
                );
                return Err(e);
            }
        };

    if regular_matchers.is_empty() && factory_matchers.is_empty() {
        tracing::info!("No events configured for decoding on chain {}", chain.name);
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
    let matchers = LogMatcherConfig {
        regular_matchers: &regular_matchers,
        factory_matchers: &factory_matchers,
    };

    if !skip_catchup {
        let raw_logs_dir = crate::storage::paths::raw_logs_dir(&chain.name);
        if raw_logs_dir.exists() {
            super::catchup::catchup_decode_logs(
                &raw_logs_dir,
                &output_base,
                &matchers,
                chain,
                raw_data_config,
                transform_tx.as_ref(),
                recollect_tx.as_ref(),
            )
            .await?;
        }

        tracing::info!("Log decoding catchup complete for chain {}", chain.name);
    }

    // Recollect is only used during catchup (to re-fetch corrupt raw files).
    // Drop the sender so recollect_rx in the receipts collector can close,
    // allowing it to send AllRangesComplete and unblock shutdown.
    drop(recollect_tx);

    // =========================================================================
    // Live phase: Process new data as it arrives
    // =========================================================================
    let log_outputs = LogDecoderOutputs {
        transform_tx: transform_tx.as_ref(),
        complete_tx: complete_tx.as_ref(),
    };
    super::current::decode_logs_live(
        &mut decoder_rx,
        &matchers,
        &output_base,
        &chain.name,
        &log_outputs,
        Some(&chain.contracts),
    )
    .await?;

    tracing::info!("Log decoding complete for chain {}", chain.name);
    Ok(())
}

/// Build event matchers from contract configurations
pub(crate) fn build_event_matchers(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> Result<BuiltMatchers, LogDecodingError> {
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
                    _is_factory: false,
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
                                    _is_factory: true,
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
    matchers: &LogMatcherConfig<'_>,
    factory_addresses: &HashMap<String, HashSet<[u8; 20]>>,
    output_base: &Path,
    outputs: &LogDecoderOutputs<'_>,
    expected_by_collection: Option<&HashMap<String, ExpectedContracts>>,
) -> Result<(), LogDecodingError> {
    let regular_matchers = matchers.regular_matchers;
    let factory_matchers = matchers.factory_matchers;
    let build_transform = outputs.transform_tx.is_some();

    // Build topic0 index for O(1) matcher lookup instead of O(matchers) linear scan per log.
    // This is the key optimization for catchup with many configured events.
    let mut regular_by_topic: HashMap<[u8; 32], Vec<&EventMatcher>> = HashMap::new();
    for matcher in regular_matchers {
        regular_by_topic
            .entry(matcher.event.topic0)
            .or_default()
            .push(matcher);
    }

    let mut factory_by_topic: HashMap<[u8; 32], Vec<(&str, &EventMatcher)>> = HashMap::new();
    for (collection_name, matchers) in factory_matchers {
        for matcher in matchers {
            factory_by_topic
                .entry(matcher.event.topic0)
                .or_default()
                .push((collection_name.as_str(), matcher));
        }
    }

    // Group decoded logs by (contract_name, event_name) — for parquet storage
    let mut decoded_by_event: HashMap<(String, String), (Vec<DecodedLogRecord>, &ParsedEvent)> =
        HashMap::new();
    // Also build transform events in the same pass when the channel is present
    let mut transform_events_by_type: HashMap<(String, String), Vec<TransformDecodedEvent>> =
        HashMap::new();

    for log in logs {
        // Skip if no topic0
        if log.topics.is_empty() {
            continue;
        }
        let topic0 = log.topics[0];

        // Regular matchers via topic0 index
        if let Some(matchers) = regular_by_topic.get(&topic0) {
            for matcher in matchers {
                if let Some(sb) = matcher.start_block {
                    if log.block_number < sb {
                        continue;
                    }
                }
                if !matcher.addresses.contains(&log.address) {
                    continue;
                }

                if let Some(decoded) = decode_log(log, &matcher.event)? {
                    if build_transform {
                        let transform_event = convert_to_transform_event(
                            &decoded,
                            &matcher.event,
                            &matcher.name,
                            &matcher.event_name,
                        );
                        let key = (matcher.name.clone(), matcher.event_name.clone());
                        transform_events_by_type
                            .entry(key)
                            .or_default()
                            .push(transform_event);
                    }

                    let key = (matcher.name.clone(), matcher.event_name.clone());
                    decoded_by_event
                        .entry(key)
                        .or_insert_with(|| (Vec::new(), &matcher.event))
                        .0
                        .push(decoded);
                }
            }
        }

        // Factory matchers via topic0 index
        if let Some(entries) = factory_by_topic.get(&topic0) {
            for (collection_name, matcher) in entries {
                let addrs = match factory_addresses.get(*collection_name) {
                    Some(addrs) => addrs,
                    None => continue,
                };

                if !addrs.contains(&log.address) {
                    continue;
                }

                if let Some(decoded) = decode_log(log, &matcher.event)? {
                    if build_transform {
                        let transform_event = convert_to_transform_event(
                            &decoded,
                            &matcher.event,
                            collection_name,
                            &matcher.event_name,
                        );
                        let key = (collection_name.to_string(), matcher.event_name.clone());
                        transform_events_by_type
                            .entry(key)
                            .or_default()
                            .push(transform_event);
                    }

                    let key = (collection_name.to_string(), matcher.event_name.clone());
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

    // Send transform events to the transformation channel FIRST (before parquet I/O)
    // to unblock the transformation engine while parquet writes happen.
    if let Some(tx) = outputs.transform_tx {
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
                tracing::warn!(
                    "Failed to send decoded events to transformation channel: {}",
                    e
                );
            }
        }
    }

    // Signal that all events for this range have been sent
    if let Some(tx) = outputs.complete_tx {
        let msg = RangeCompleteMessage {
            range_start,
            range_end,
            kind: RangeCompleteKind::Logs,
        };
        if let Err(e) = tx.send(msg).await {
            tracing::warn!("Failed to send range complete: {}", e);
        }
    }

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

    for matchers in factory_matchers.values() {
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

    // Update contract index for factory collections so that catchup can skip
    // ranges already processed with the current set of contracts/addresses.
    if let Some(expected_map) = expected_by_collection {
        let rk = range_key(range_start, range_end - 1);

        // Collect unique (collection, event_name) pairs from factory matchers
        let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
        for (collection, matchers) in factory_matchers {
            for matcher in matchers {
                seen_pairs.insert((collection.clone(), matcher.event_name.clone()));
            }
        }

        for (collection, event_name) in seen_pairs {
            if let Some(expected) = expected_map.get(&collection) {
                let dir = output_base.join(&collection).join(&event_name);
                let mut index = read_contract_index(&dir);
                update_contract_index(&mut index, &rk, expected);
                write_contract_index(&dir, &index)?;
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

    // Preserve original ABI parameter order. Indexed params are decoded from
    // topics while non-indexed params come from data, but flattening expects
    // values aligned with event.params, not grouped by storage location.
    let mut param_values: Vec<Option<DecodedValue>> = vec![None; event.params.len()];
    let indexed_positions: Vec<usize> = event
        .params
        .iter()
        .enumerate()
        .filter_map(|(idx, param)| param.indexed.then_some(idx))
        .collect();
    let data_positions: Vec<usize> = event
        .params
        .iter()
        .enumerate()
        .filter_map(|(idx, param)| (!param.indexed).then_some(idx))
        .collect();

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
                param_values[indexed_positions[i]] = Some(DecodedValue::Bytes32(*topic));
                continue;
            }
        }

        let value = decode_topic(topic, &param.param_type)?;
        param_values[indexed_positions[i]] = Some(value);
    }

    // Decode non-indexed params from data
    if !data_params.is_empty() {
        let data_types: Vec<DynSolType> =
            data_params.iter().map(|p| p.param_type.clone()).collect();

        let tuple_type = DynSolType::Tuple(data_types);

        match tuple_type.abi_decode(&log.data) {
            Ok(DynSolValue::Tuple(values)) => {
                for ((value, param), param_idx) in values
                    .iter()
                    .zip(data_params.iter())
                    .zip(data_positions.iter())
                {
                    let decoded =
                        convert_dyn_sol_value_with_tuple_info(value, &param.tuple_fields)?;
                    param_values[*param_idx] = Some(decoded);
                }
            }
            Ok(_) => return Ok(None),
            Err(e) => {
                tracing::debug!("Failed to decode log data: {}", e);
                return Ok(None);
            }
        }
    }

    let param_values: Vec<DecodedValue> = match param_values.into_iter().collect() {
        Some(values) => values,
        None => {
            tracing::debug!(
                "Decoded param count/order mismatch for event {} at block {} log_index {}",
                event.name,
                log.block_number,
                log.log_index
            );
            return Ok(None);
        }
    };

    // Flatten param_values to match flattened_fields order
    // Returns None if any tuple field is missing (C1 fix: avoid silent data loss)
    let flattened = match flatten_param_values(&param_values, &event.params) {
        Some(f) => f,
        None => return Ok(None), // Missing tuple field logged in flatten_param_values
    };

    // Verify we have the right number of values
    if flattened.len() != event.flattened_fields.len() {
        tracing::debug!(
            "Flattened values count {} doesn't match flattened_fields count {} for event {}",
            flattened.len(),
            event.flattened_fields.len(),
            event.name
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
/// Returns None if any tuple field is missing (to avoid corrupt data)
fn flatten_param_values(
    values: &[DecodedValue],
    params: &[EventParam],
) -> Option<Vec<DecodedValue>> {
    let mut result = Vec::new();

    for (value, param) in values.iter().zip(params.iter()) {
        match flatten_single_value(value, param, &mut result) {
            FlattenResult::Ok => {}
            FlattenResult::MissingField {
                field_name,
                expected_fields,
                actual_fields,
            } => {
                tracing::warn!(
                    "Missing field '{}' in tuple decode. Expected: {:?}, got: {:?}",
                    field_name,
                    expected_fields,
                    actual_fields
                );
                return None; // Skip log rather than produce corrupt data
            }
        }
    }

    Some(result)
}

/// Recursively flatten a single value based on its param info
/// Result type for flatten_single_value to propagate missing field errors
enum FlattenResult {
    Ok,
    MissingField {
        field_name: String,
        expected_fields: Vec<String>,
        actual_fields: Vec<String>,
    },
}

fn flatten_single_value(
    value: &DecodedValue,
    param: &EventParam,
    output: &mut Vec<DecodedValue>,
) -> FlattenResult {
    // Check if this is an indexed tuple (stored as hash)
    if param.indexed {
        if let Some(TupleFieldInfo::Tuple(_)) = &param.tuple_fields {
            // Indexed tuple - just add the hash directly
            output.push(value.clone());
            return FlattenResult::Ok;
        }
    }

    match (&param.tuple_fields, value) {
        (Some(TupleFieldInfo::Tuple(field_infos)), DecodedValue::NamedTuple(named_values)) => {
            // Flatten the named tuple
            if let DynSolType::Tuple(types) = &param.param_type {
                for ((field_name, field_info), field_type) in field_infos.iter().zip(types.iter()) {
                    // Find the matching value
                    let field_value = match named_values.iter().find(|(n, _)| n == field_name) {
                        Some((_, v)) => v.clone(),
                        None => {
                            let expected_fields: Vec<String> =
                                field_infos.iter().map(|(n, _)| n.clone()).collect();
                            let actual_fields: Vec<String> =
                                named_values.iter().map(|(n, _)| n.clone()).collect();
                            return FlattenResult::MissingField {
                                field_name: field_name.clone(),
                                expected_fields,
                                actual_fields,
                            };
                        }
                    };

                    // Create sub-param for recursion
                    let sub_param = EventParam {
                        name: field_name.clone(),
                        param_type: field_type.clone(),
                        type_string: String::new(),
                        indexed: param.indexed,
                        tuple_fields: Some(field_info.clone()),
                    };

                    if let result @ FlattenResult::MissingField { .. } =
                        flatten_single_value(&field_value, &sub_param, output)
                    {
                        return result;
                    }
                }
            }
            FlattenResult::Ok
        }
        _ => {
            // Leaf value - add directly
            output.push(value.clone());
            FlattenResult::Ok
        }
    }
}

/// Decode a value from a topic
fn decode_topic(
    topic: &[u8; 32],
    param_type: &DynSolType,
) -> Result<DecodedValue, LogDecodingError> {
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
                let decoded =
                    convert_dyn_sol_value_with_tuple_info(val, &Some(field_info.clone()))?;
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
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
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
                let values: Vec<Option<&[u8]>> = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Address(addr)) => Some(addr.as_slice()),
                        _ => None,
                    })
                    .collect();
                let arr =
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 20)?;
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
                let values: Vec<Option<&[u8]>> = records
                    .iter()
                    .map(|r| match r.decoded_values.get(field_idx) {
                        Some(DecodedValue::Bytes32(b)) => Some(b.as_slice()),
                        _ => None,
                    })
                    .collect();
                let arr =
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 32)?;
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
            params.insert(flattened.full_name.clone(), value.clone());
        }
    }

    TransformDecodedEvent {
        block_number: record.block_number,
        block_timestamp: record.block_timestamp,
        transaction_id: TxId::Evm(record.transaction_hash),
        position: LogPosition::Evm {
            log_index: record.log_index,
        },
        contract_address: ChainAddress::Evm(record.contract_address),
        source_name: source_name.to_string(),
        event_name: event_name.to_string(),
        event_signature: parsed_event.signature.clone(),
        params,
    }
}

// =========================================================================
// Live mode support
// =========================================================================

/// Convert DecodedLogRecord to LiveDecodedLog for bincode storage.
fn convert_to_live_decoded_log(record: &DecodedLogRecord) -> LiveDecodedLog {
    LiveDecodedLog {
        block_number: record.block_number,
        block_timestamp: record.block_timestamp,
        transaction_hash: record.transaction_hash,
        log_index: record.log_index,
        contract_address: record.contract_address,
        decoded_values: record.decoded_values.clone(),
    }
}

/// Process logs in live mode - writes to bincode storage.
pub(crate) async fn process_logs_live(
    logs: &[LogData],
    block_number: u64,
    matchers: &LogMatcherConfig<'_>,
    factory_addresses: &HashMap<String, HashSet<[u8; 20]>>,
    storage: &LiveStorage,
    outputs: &LogDecoderOutputs<'_>,
) -> Result<(), LogDecodingError> {
    let regular_matchers = matchers.regular_matchers;
    let factory_matchers = matchers.factory_matchers;
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
        "Live log decoding block {}: {} logs in file, {} decoded events across {} event types",
        block_number,
        logs.len(),
        total_decoded,
        decoded_by_event.len()
    );

    // Write decoded data to bincode files
    for ((contract_name, event_name), (records, _parsed_event)) in &decoded_by_event {
        let live_records: Vec<LiveDecodedLog> =
            records.iter().map(convert_to_live_decoded_log).collect();

        storage
            .write_decoded_logs(block_number, contract_name, event_name, &live_records)
            .map_err(|e| LogDecodingError::Io(std::io::Error::other(e.to_string())))?;

        tracing::debug!(
            "Wrote {} decoded {} events to live storage for block {}",
            live_records.len(),
            event_name,
            block_number
        );
    }

    // Send to transformation channel if enabled
    if let Some(tx) = outputs.transform_tx {
        let mut transform_events_by_type: HashMap<(String, String), Vec<TransformDecodedEvent>> =
            HashMap::new();

        for log in logs {
            if log.topics.is_empty() {
                continue;
            }
            let topic0 = log.topics[0];

            // Try regular matchers
            for matcher in regular_matchers {
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
                    transform_events_by_type
                        .entry(key)
                        .or_default()
                        .push(transform_event);
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
                        transform_events_by_type
                            .entry(key)
                            .or_default()
                            .push(transform_event);
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
                range_start: block_number,
                range_end: block_number + 1,
                source_name,
                event_name,
                events,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!(
                    "Failed to send decoded events to transformation channel: {}",
                    e
                );
            }
        }
    }

    // Update block status to mark log decoding complete
    match storage.update_status_atomic(block_number, |status| {
        status.logs_decoded = true;
    }) {
        Ok(()) => {
            tracing::info!("Block {} logs decoded", block_number);
        }
        Err(e) => {
            tracing::warn!("Failed to update block status after log decoding: {}", e);
        }
    }

    // Signal that all events for this block have been sent
    if let Some(tx) = outputs.complete_tx {
        let msg = RangeCompleteMessage {
            range_start: block_number,
            range_end: block_number + 1,
            kind: RangeCompleteKind::Logs,
        };
        if let Err(e) = tx.send(msg).await {
            tracing::warn!("Failed to send range complete: {}", e);
        }
    }

    Ok(())
}

/// Delete decoded log data for orphaned blocks (reorg cleanup).
pub(crate) fn delete_decoded_logs_for_blocks(
    storage: &LiveStorage,
    blocks: &[u64],
) -> Result<(), LogDecodingError> {
    for &block_number in blocks {
        storage
            .delete_all_decoded_logs(block_number)
            .map_err(|e| LogDecodingError::Io(std::io::Error::other(e.to_string())))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::dyn_abi::DynSolValue;
    use alloy::primitives::{Address, B256, U256};

    use super::decode_log;
    use crate::decoding::event_parsing::ParsedEvent;
    use crate::raw_data::historical::receipts::LogData;
    use crate::types::decoded::DecodedValue;

    fn encode_address_topic(address: [u8; 20]) -> [u8; 32] {
        let mut topic = [0u8; 32];
        topic[12..].copy_from_slice(&address);
        topic
    }

    fn encode_int_topic(value: i32) -> [u8; 32] {
        alloy::primitives::I256::try_from(value)
            .expect("valid i32")
            .to_be_bytes::<32>()
    }

    #[test]
    fn decode_log_keeps_interleaved_indexed_params_in_signature_order() {
        let parsed = ParsedEvent::from_signature(
            "Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)",
        )
        .unwrap();

        let sender = [0x11u8; 20];
        let owner = [0x22u8; 20];
        let amount = 1234u128;
        let amount0 = U256::from(5678u64);
        let amount1 = U256::from(9012u64);

        let log = LogData {
            block_number: 1,
            block_timestamp: 2,
            transaction_hash: B256::ZERO,
            log_index: 3,
            address: [0x33u8; 20],
            topics: vec![
                parsed.topic0,
                encode_address_topic(owner),
                encode_int_topic(-120),
                encode_int_topic(240),
            ],
            data: DynSolValue::Tuple(vec![
                DynSolValue::Address(Address::from_slice(&sender)),
                DynSolValue::Uint(U256::from(amount), 128),
                DynSolValue::Uint(amount0, 256),
                DynSolValue::Uint(amount1, 256),
            ])
            .abi_encode_params(),
        };

        let decoded = decode_log(&log, &parsed).unwrap().unwrap();

        assert!(matches!(
            decoded.decoded_values.first(),
            Some(DecodedValue::Address(value)) if *value == sender
        ));
        assert!(matches!(
            decoded.decoded_values.get(1),
            Some(DecodedValue::Address(value)) if *value == owner
        ));
        assert!(matches!(
            decoded.decoded_values.get(2),
            Some(DecodedValue::Int64(value)) if *value == -120
        ));
        assert!(matches!(
            decoded.decoded_values.get(3),
            Some(DecodedValue::Int64(value)) if *value == 240
        ));
    }
}
