//! Event-triggered eth_call configuration, processing, and parquet output.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryBuilder, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::config::compute_function_selector;
use super::helpers::parse_function_name;
use super::types::{
    EthCallCollectionError, EthCallContext, EventCallKey, EventCallResult, EventTriggeredCallConfig,
};
use super::{execute_multicalls_generic, BlockMulticall, EventCallMeta, MulticallSlotGeneric};
use crate::decoding::{DecoderMessage, EventCallResult as DecoderEventCallResult};
use crate::raw_data::historical::receipts::EventTriggerData;
use crate::storage::contract_index::{
    build_expected_factory_contracts_for_range, range_key, read_contract_index,
    update_contract_index, write_contract_index,
};
use crate::storage::{upload_parquet_to_s3, upload_sidecar_to_s3};
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::eth_call::{
    encode_call_with_params, EventFieldLocation, EvmType, ParamConfig,
};
use crate::types::uniswap::v4::PoolKey;

/// Pending write tasks spawned by process functions.
/// Returned to the caller so writes can be awaited after releasing concurrency permits.
pub type PendingWrites = tokio::task::JoinSet<Result<(), EthCallCollectionError>>;

/// A trigger that was skipped because its factory address wasn't known yet.
/// Buffered for replay when factory addresses arrive via IncrementalAddresses.
#[derive(Debug, Clone)]
pub struct SkippedFactoryTrigger {
    pub trigger: EventTriggerData,
}

/// An event-triggered call matched with its config and encoded parameters,
/// ready to be grouped by output key.
pub(super) struct PreparedEventCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub config: Arc<EventTriggeredCallConfig>,
    pub target_address: Address,
    pub decoded_values: Vec<DynSolValue>,
    pub encoded_params: Arc<Vec<Vec<u8>>>,
}

/// A fully-built eth_call ready for RPC batch execution.
#[derive(Clone)]
pub(super) struct PendingEventCall {
    pub request: TransactionRequest,
    pub block_id: BlockId,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: Address,
    pub encoded_params: Arc<Vec<Vec<u8>>>,
}

/// Build event-triggered call configs from contracts configuration
/// Returns a map from (source_name, event_signature_hash) -> Vec<EventTriggeredCallConfig>
pub fn build_event_triggered_call_configs(
    contracts: &Contracts,
) -> HashMap<EventCallKey, Vec<EventTriggeredCallConfig>> {
    let mut configs: HashMap<EventCallKey, Vec<EventTriggeredCallConfig>> = HashMap::new();

    for (contract_name, contract) in contracts {
        // Get contract addresses
        let addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };
        let start_block = contract.start_block.map(|u| u.to::<u64>());

        // Check contract-level calls for on_events frequency
        if let Some(calls) = &contract.calls {
            for call in calls {
                // Iterate over all event trigger configs (supports single or multiple events)
                for trigger_config in call.frequency.event_configs() {
                    let event_hash = compute_event_signature_hash(&trigger_config.event);
                    let key = (trigger_config.source.clone(), event_hash);

                    let selector = compute_function_selector(&call.function);
                    let function_name = parse_function_name(&call.function);

                    // Resolve target override if specified, otherwise use contract addresses
                    let target_addrs = if let Some(target) = &call.target {
                        match target.resolve_all(contracts) {
                            Some(addrs) => addrs,
                            None => {
                                tracing::warn!(
                                    "Could not resolve target for event-triggered call {} on contract {}, skipping",
                                    call.function, contract_name
                                );
                                continue;
                            }
                        }
                    } else {
                        addresses.clone()
                    };

                    // For each target address, create a config
                    for address in &target_addrs {
                        configs
                            .entry(key.clone())
                            .or_default()
                            .push(EventTriggeredCallConfig {
                                contract_name: contract_name.clone(),
                                target_address: Some(*address),
                                function_name: function_name.clone(),
                                function_selector: selector,
                                params: call.params.clone(),
                                is_factory: false,
                                start_block,
                            });
                    }
                }
            }
        }

        // Check factory calls for on_events frequency
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if let Some(calls) = &factory.calls {
                    for call in calls {
                        // Iterate over all event trigger configs (supports single or multiple events)
                        for trigger_config in call.frequency.event_configs() {
                            let event_hash = compute_event_signature_hash(&trigger_config.event);
                            let key = (trigger_config.source.clone(), event_hash);

                            let selector = compute_function_selector(&call.function);
                            let function_name = parse_function_name(&call.function);

                            // If target is specified, use resolved target; otherwise use event emitter at runtime
                            let target_address = call.target.as_ref().and_then(|t| {
                                let resolved = t.resolve(contracts);
                                if resolved.is_none() {
                                    tracing::warn!(
                                        "Could not resolve target for factory event-triggered call {} on collection {}, will use event emitter",
                                        call.function, factory.collection
                                    );
                                }
                                resolved
                            });

                            configs.entry(key.clone()).or_default().push(
                                EventTriggeredCallConfig {
                                    contract_name: factory.collection.clone(),
                                    target_address,
                                    function_name: function_name.clone(),
                                    function_selector: selector,
                                    params: call.params.clone(),
                                    is_factory: true,
                                    // Factory events use discovery block, not start_block
                                    start_block: None,
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    configs
}

/// Compute the keccak256 hash of an event signature
pub(crate) fn compute_event_signature_hash(signature: &str) -> [u8; 32] {
    use alloy::primitives::keccak256;
    keccak256(signature.as_bytes()).0
}

/// Extract a parameter value from event data (topics or data fields)
pub(crate) fn extract_param_from_event(
    trigger: &EventTriggerData,
    from_event: &EventFieldLocation,
    param_type: &EvmType,
) -> Result<(DynSolValue, Vec<u8>), EthCallCollectionError> {
    match from_event {
        EventFieldLocation::Address => {
            // Validate that the expected type is Address
            if !matches!(param_type, EvmType::Address) {
                return Err(EthCallCollectionError::EventParamExtraction(format!(
                    "from_event: \"address\" requires param type to be address, got {:?}",
                    param_type
                )));
            }
            let addr = Address::from(trigger.emitter_address);
            let encoded = DynSolValue::Address(addr).abi_encode();
            Ok((DynSolValue::Address(addr), encoded))
        }
        EventFieldLocation::Topic(idx) => {
            let idx = *idx;
            if idx >= trigger.topics.len() {
                return Err(EthCallCollectionError::EventParamExtraction(format!(
                    "Topic index {} out of bounds (event has {} topics)",
                    idx,
                    trigger.topics.len()
                )));
            }

            let topic_bytes = &trigger.topics[idx];
            extract_value_from_32_bytes(topic_bytes, param_type)
        }
        EventFieldLocation::Data(idx) => {
            let idx = *idx;
            // Data is stored as 32-byte words
            let start = idx * 32;
            let end = start + 32;

            if end > trigger.data.len() {
                return Err(EthCallCollectionError::EventParamExtraction(format!(
                    "Data index {} out of bounds (event data is {} bytes)",
                    idx,
                    trigger.data.len()
                )));
            }

            let mut word = [0u8; 32];
            word.copy_from_slice(&trigger.data[start..end]);
            extract_value_from_32_bytes(&word, param_type)
        }
        EventFieldLocation::V4PoolIdFromKeyData => {
            extract_v4_pool_id_from_key_data(trigger, param_type)
        }
        EventFieldLocation::V4PoolKeyTupleFromData(offset) => {
            extract_v4_pool_key_tuple_from_data(trigger, param_type, *offset)
        }
    }
}

fn extract_v4_pool_id_from_key_data(
    trigger: &EventTriggerData,
    param_type: &EvmType,
) -> Result<(DynSolValue, Vec<u8>), EthCallCollectionError> {
    if !matches!(param_type, EvmType::Bytes32) {
        return Err(EthCallCollectionError::EventParamExtraction(format!(
            "from_event: \"v4_pool_id_from_key_data\" requires param type to be bytes32, got {:?}",
            param_type
        )));
    }

    if trigger.data.len() < 5 * 32 {
        return Err(EthCallCollectionError::EventParamExtraction(format!(
            "v4_pool_id_from_key_data requires at least 160 bytes of event data, got {}",
            trigger.data.len()
        )));
    }

    let word = |idx: usize| -> [u8; 32] {
        let start = idx * 32;
        let mut out = [0u8; 32];
        out.copy_from_slice(&trigger.data[start..start + 32]);
        out
    };

    let pool_id = PoolKey {
        currency0: Address::from(address_from_word(&word(0))),
        currency1: Address::from(address_from_word(&word(1))),
        fee: uint24_from_word(&word(2)),
        tick_spacing: int24_from_word(&word(3)),
        hooks: Address::from(address_from_word(&word(4))),
    }
    .pool_id();

    let encoded = DynSolValue::FixedBytes(pool_id, 32).abi_encode();
    Ok((DynSolValue::FixedBytes(pool_id, 32), encoded))
}

/// Extract a V4 PoolKey tuple (5 consecutive 32-byte words) from event data
/// starting at the given word offset, and return it as a `DynSolValue::Tuple`
/// with its ABI encoding.
///
/// The 5 words are:
///   0: currency0 (address)
///   1: currency1 (address)
///   2: fee (uint24)
///   3: tickSpacing (int24)
///   4: hooks (address)
fn extract_v4_pool_key_tuple_from_data(
    trigger: &EventTriggerData,
    _param_type: &EvmType,
    offset: usize,
) -> Result<(DynSolValue, Vec<u8>), EthCallCollectionError> {
    let required_bytes = (offset + 5) * 32;
    if trigger.data.len() < required_bytes {
        return Err(EthCallCollectionError::EventParamExtraction(format!(
            "v4_pool_key_tuple_from_data requires at least {} bytes of event data (offset={}, need 5 words), got {}",
            required_bytes, offset, trigger.data.len()
        )));
    }

    let word = |idx: usize| -> [u8; 32] {
        let start = (offset + idx) * 32;
        let mut out = [0u8; 32];
        out.copy_from_slice(&trigger.data[start..start + 32]);
        out
    };

    use alloy::primitives::{I256, U256};

    let currency0 = Address::from(address_from_word(&word(0)));
    let currency1 = Address::from(address_from_word(&word(1)));
    let fee = uint24_from_word(&word(2));
    let tick_spacing = int24_from_word(&word(3));
    let hooks = Address::from(address_from_word(&word(4)));

    let tuple = DynSolValue::Tuple(vec![
        DynSolValue::Address(currency0),
        DynSolValue::Address(currency1),
        DynSolValue::Uint(U256::from(fee), 24),
        DynSolValue::Int(I256::try_from(tick_spacing).unwrap_or_default(), 24),
        DynSolValue::Address(hooks),
    ]);

    let encoded = tuple.abi_encode();
    Ok((tuple, encoded))
}

fn address_from_word(word: &[u8; 32]) -> [u8; 20] {
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&word[12..32]);
    addr
}

fn uint24_from_word(word: &[u8; 32]) -> u32 {
    ((word[29] as u32) << 16) | ((word[30] as u32) << 8) | (word[31] as u32)
}

fn int24_from_word(word: &[u8; 32]) -> i32 {
    let raw = ((word[29] as i32) << 16) | ((word[30] as i32) << 8) | (word[31] as i32);
    if (raw & 0x80_0000) != 0 {
        raw | !0x00FF_FFFF
    } else {
        raw
    }
}

/// Extract a typed value from a 32-byte word
pub(crate) fn extract_value_from_32_bytes(
    bytes: &[u8; 32],
    param_type: &EvmType,
) -> Result<(DynSolValue, Vec<u8>), EthCallCollectionError> {
    use alloy::primitives::{I256, U256};

    match param_type {
        EvmType::Address => {
            // Address is stored in the last 20 bytes of the 32-byte word
            let mut addr_bytes = [0u8; 20];
            addr_bytes.copy_from_slice(&bytes[12..32]);
            let addr = Address::from(addr_bytes);
            let encoded = DynSolValue::Address(addr).abi_encode();
            Ok((DynSolValue::Address(addr), encoded))
        }
        EvmType::Uint256 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 256).abi_encode();
            Ok((DynSolValue::Uint(val, 256), encoded))
        }
        EvmType::Uint128 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 128).abi_encode();
            Ok((DynSolValue::Uint(val, 128), encoded))
        }
        EvmType::Uint64 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 64).abi_encode();
            Ok((DynSolValue::Uint(val, 64), encoded))
        }
        EvmType::Uint32 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 32).abi_encode();
            Ok((DynSolValue::Uint(val, 32), encoded))
        }
        EvmType::Uint24 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 24).abi_encode();
            Ok((DynSolValue::Uint(val, 24), encoded))
        }
        EvmType::Uint16 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 16).abi_encode();
            Ok((DynSolValue::Uint(val, 16), encoded))
        }
        EvmType::Uint8 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 8).abi_encode();
            Ok((DynSolValue::Uint(val, 8), encoded))
        }
        EvmType::Uint80 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 80).abi_encode();
            Ok((DynSolValue::Uint(val, 80), encoded))
        }
        EvmType::Int256 => {
            let val = I256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Int(val, 256).abi_encode();
            Ok((DynSolValue::Int(val, 256), encoded))
        }
        EvmType::Int128 => {
            let val = I256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Int(val, 128).abi_encode();
            Ok((DynSolValue::Int(val, 128), encoded))
        }
        EvmType::Int64 => {
            let val = I256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Int(val, 64).abi_encode();
            Ok((DynSolValue::Int(val, 64), encoded))
        }
        EvmType::Int32 => {
            let val = I256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Int(val, 32).abi_encode();
            Ok((DynSolValue::Int(val, 32), encoded))
        }
        EvmType::Int24 => {
            let val = I256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Int(val, 24).abi_encode();
            Ok((DynSolValue::Int(val, 24), encoded))
        }
        EvmType::Int16 => {
            let val = I256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Int(val, 16).abi_encode();
            Ok((DynSolValue::Int(val, 16), encoded))
        }
        EvmType::Int8 => {
            let val = I256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Int(val, 8).abi_encode();
            Ok((DynSolValue::Int(val, 8), encoded))
        }
        EvmType::Bool => {
            let val = bytes[31] != 0;
            let encoded = DynSolValue::Bool(val).abi_encode();
            Ok((DynSolValue::Bool(val), encoded))
        }
        EvmType::Bytes32 => {
            let b256 = alloy::primitives::B256::from(*bytes);
            let encoded = DynSolValue::FixedBytes(b256, 32).abi_encode();
            Ok((DynSolValue::FixedBytes(b256, 32), encoded))
        }
        EvmType::Uint160 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 160).abi_encode();
            Ok((DynSolValue::Uint(val, 160), encoded))
        }
        EvmType::Uint96 => {
            let val = U256::from_be_bytes(*bytes);
            let encoded = DynSolValue::Uint(val, 96).abi_encode();
            Ok((DynSolValue::Uint(val, 96), encoded))
        }
        EvmType::Bytes | EvmType::String => {
            Err(EthCallCollectionError::EventParamExtraction(format!(
                "Dynamic types ({:?}) cannot be extracted from event data directly",
                param_type
            )))
        }
        // Named types delegate to inner type
        EvmType::Named { inner, .. } => extract_value_from_32_bytes(bytes, inner),
        // NamedTuple not supported for event param extraction
        EvmType::NamedTuple(_) => Err(EthCallCollectionError::EventParamExtraction(
            "NamedTuple cannot be used as a parameter type".to_string(),
        )),
        EvmType::UnnamedTuple(_) => Err(EthCallCollectionError::EventParamExtraction(
            "UnnamedTuple cannot be used as a parameter type".to_string(),
        )),
        EvmType::Array(_) => Err(EthCallCollectionError::EventParamExtraction(
            "Array cannot be used as a parameter type".to_string(),
        )),
    }
}

/// Build parameters for an event-triggered call
/// Encode calldata for "once" calls that have self-address params
pub fn encode_once_call_params(
    function_selector: [u8; 4],
    param_configs: &[ParamConfig],
    self_address: Address,
) -> Result<Bytes, EthCallCollectionError> {
    let mut params = Vec::with_capacity(param_configs.len());

    for param in param_configs {
        match param {
            ParamConfig::Static { param_type, values } => {
                if let Some(value) = values.first() {
                    let dyn_val = param_type.parse_value(value)?;
                    params.push(dyn_val);
                } else {
                    return Err(EthCallCollectionError::EventParamExtraction(
                        "Static param has no values".to_string(),
                    ));
                }
            }
            ParamConfig::SelfAddress { param_type, source } => {
                if source == "self" {
                    let dyn_val = match param_type {
                        EvmType::Address => DynSolValue::Address(self_address),
                        _ => {
                            return Err(EthCallCollectionError::EventParamExtraction(format!(
                                "source: \"self\" can only be used with address type, got {:?}",
                                param_type
                            )));
                        }
                    };
                    params.push(dyn_val);
                } else {
                    return Err(EthCallCollectionError::EventParamExtraction(format!(
                        "Unknown source: {} (expected \"self\")",
                        source
                    )));
                }
            }
            ParamConfig::FromEvent { .. } => {
                return Err(EthCallCollectionError::EventParamExtraction(
                    "from_event params are not supported for 'once' frequency calls".to_string(),
                ));
            }
        }
    }

    Ok(encode_call_with_params(function_selector, &params))
}

pub fn build_event_call_params(
    trigger: &EventTriggerData,
    param_configs: &[ParamConfig],
) -> Result<(Vec<DynSolValue>, Vec<Vec<u8>>), EthCallCollectionError> {
    let mut params = Vec::with_capacity(param_configs.len());
    let mut encoded_params = Vec::with_capacity(param_configs.len());

    for param in param_configs {
        match param {
            ParamConfig::Static { param_type, values } => {
                // Use the first static value (event-triggered calls use first value only)
                if let Some(value) = values.first() {
                    let dyn_val = param_type.parse_value(value)?;
                    let encoded = dyn_val.abi_encode();
                    params.push(dyn_val);
                    encoded_params.push(encoded);
                } else {
                    return Err(EthCallCollectionError::EventParamExtraction(
                        "Static param has no values".to_string(),
                    ));
                }
            }
            ParamConfig::FromEvent {
                param_type,
                from_event,
            } => {
                let (dyn_val, encoded) = extract_param_from_event(trigger, from_event, param_type)?;
                params.push(dyn_val);
                encoded_params.push(encoded);
            }
            ParamConfig::SelfAddress { param_type, source } => {
                if source == "self" {
                    // Use the event emitter address
                    let addr = Address::from(trigger.emitter_address);
                    let dyn_val = match param_type {
                        EvmType::Address => DynSolValue::Address(addr),
                        _ => {
                            return Err(EthCallCollectionError::EventParamExtraction(format!(
                                "source: \"self\" can only be used with address type, got {:?}",
                                param_type
                            )));
                        }
                    };
                    let encoded = dyn_val.abi_encode();
                    params.push(dyn_val);
                    encoded_params.push(encoded);
                } else {
                    return Err(EthCallCollectionError::EventParamExtraction(format!(
                        "Unknown source: {} (expected \"self\")",
                        source
                    )));
                }
            }
        }
    }

    Ok((params, encoded_params))
}

/// Build expected per-output trigger key counts for event-triggered calls without
/// performing any RPC. This mirrors the filtering logic in `process_event_triggers`
/// closely enough to audit whether an existing on-events parquet contains the
/// right triggering `(block_number, log_index)` rows for each output file.
pub(crate) fn expected_event_call_key_counts_by_output(
    triggers: &[EventTriggerData],
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
) -> HashMap<(String, String), HashMap<(u64, u32), usize>> {
    let mut counts: HashMap<(String, String), HashMap<(u64, u32), usize>> = HashMap::new();

    for trigger in triggers {
        let key = (trigger.source_name.clone(), trigger.event_signature);

        if let Some(configs) = event_call_configs.get(&key) {
            for config in configs {
                if let Some(sb) = config.start_block {
                    if trigger.block_number < sb {
                        continue;
                    }
                }

                if config.target_address.is_none() {
                    let emitter = Address::from(trigger.emitter_address);
                    let Some(known_addresses) = factory_addresses.get(&config.contract_name) else {
                        continue;
                    };
                    if !known_addresses.contains(&emitter) {
                        continue;
                    }
                }

                if build_event_call_params(trigger, &config.params).is_err() {
                    continue;
                }

                let output_key = (config.contract_name.clone(), config.function_name.clone());
                let trigger_key = (trigger.block_number, trigger.log_index);
                *counts
                    .entry(output_key)
                    .or_default()
                    .entry(trigger_key)
                    .or_insert(0) += 1;
            }
        }
    }

    counts
}

async fn process_trigger_chunk<'a>(
    trigger_chunk: Vec<EventTriggerData>,
    event_call_configs: &'a HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &'a HashMap<String, HashSet<Address>>,
    ctx: &'a EthCallContext<'_>,
    range_start: u64,
    range_end: u64,
) -> Result<
    (
        Vec<SkippedFactoryTrigger>,
        HashMap<(String, String), (Vec<EventCallResult>, usize)>,
        HashSet<(String, String)>,
    ),
    EthCallCollectionError,
> {
    struct TaggedCall {
        output_key: (String, String),
        pending: PendingEventCall,
        max_params: usize,
    }

    let mut skipped: Vec<SkippedFactoryTrigger> = Vec::new();
    let mut results: HashMap<(String, String), (Vec<EventCallResult>, usize)> = HashMap::new();
    let mut written: HashSet<(String, String)> = HashSet::new();
    let mut calls_by_output: HashMap<(String, String), Vec<PreparedEventCall>> = HashMap::new();

    for trigger in &trigger_chunk {
        let key = (trigger.source_name.clone(), trigger.event_signature);

        if let Some(configs) = event_call_configs.get(&key) {
            for config in configs {
                if let Some(sb) = config.start_block {
                    if trigger.block_number < sb {
                        continue;
                    }
                }

                let target_address = if let Some(addr) = config.target_address {
                    addr
                } else {
                    let emitter = Address::from(trigger.emitter_address);
                    match factory_addresses.get(&config.contract_name) {
                        Some(known_addresses) => {
                            if !known_addresses.contains(&emitter) {
                                skipped.push(SkippedFactoryTrigger {
                                    trigger: trigger.clone(),
                                });
                                tracing::debug!(
                                    "Buffering event trigger: emitter {:?} not yet in known addresses for {}",
                                    emitter,
                                    config.contract_name
                                );
                                continue;
                            }
                            emitter
                        }
                        None => {
                            skipped.push(SkippedFactoryTrigger {
                                trigger: trigger.clone(),
                            });
                            tracing::debug!(
                                "Buffering event trigger for {}: no factory addresses loaded yet",
                                config.contract_name
                            );
                            continue;
                        }
                    }
                };

                let (params, encoded_params) =
                    match build_event_call_params(trigger, &config.params) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(
                                "Failed to build params for {}.{} from event: {}",
                                config.contract_name,
                                config.function_name,
                                e
                            );
                            continue;
                        }
                    };

                let output_key = (config.contract_name.clone(), config.function_name.clone());
                let config_arc = Arc::new(config.clone());
                calls_by_output
                    .entry(output_key)
                    .or_default()
                    .push(PreparedEventCall {
                        block_number: trigger.block_number,
                        block_timestamp: trigger.block_timestamp,
                        log_index: trigger.log_index,
                        config: config_arc,
                        target_address,
                        decoded_values: params,
                        encoded_params: Arc::new(encoded_params),
                    });
            }
        }
    }

    let mut all_tagged: Vec<TaggedCall> = Vec::new();
    for ((contract_name, function_name), calls) in &calls_by_output {
        if calls.is_empty() {
            continue;
        }
        written.insert((contract_name.clone(), function_name.clone()));

        let max_params = calls
            .iter()
            .map(|c| c.encoded_params.len())
            .max()
            .unwrap_or(0);

        for call in calls {
            let calldata =
                encode_call_with_params(call.config.function_selector, &call.decoded_values);
            let tx = TransactionRequest::default()
                .to(call.target_address)
                .input(calldata.into());
            let block_id = BlockId::Number(BlockNumberOrTag::Number(call.block_number));
            all_tagged.push(TaggedCall {
                output_key: (contract_name.clone(), function_name.clone()),
                pending: PendingEventCall {
                    request: tx,
                    block_id,
                    block_number: call.block_number,
                    block_timestamp: call.block_timestamp,
                    log_index: call.log_index,
                    target_address: call.target_address,
                    encoded_params: call.encoded_params.clone(),
                },
                max_params,
            });
        }
    }

    if all_tagged.is_empty() {
        return Ok((skipped, results, written));
    }

    let batch_calls: Vec<(TransactionRequest, BlockId)> = all_tagged
        .iter()
        .map(|t| (t.pending.request.clone(), t.pending.block_id))
        .collect();

    tracing::info!(
        "Submitting {} event-triggered eth_calls in batch for blocks {}-{}",
        batch_calls.len(),
        range_start,
        range_end
    );

    let rpc_results = ctx.client.call_batch(batch_calls).await?;

    for (i, result) in rpc_results.into_iter().enumerate() {
        let tagged = &all_tagged[i];
        let call = &tagged.pending;

        let event_result = match result {
            Ok(bytes) => EventCallResult {
                block_number: call.block_number,
                block_timestamp: call.block_timestamp,
                log_index: call.log_index,
                target_address: call.target_address.0 .0,
                value_bytes: bytes.to_vec(),
                param_values: (*call.encoded_params).clone(),
                is_reverted: false,
                revert_reason: None,
            },
            Err(e) => {
                let reason = e.to_string();
                tracing::warn!(
                    "Event-triggered eth_call reverted for {}.{} at block {} (address {}): {}",
                    tagged.output_key.0,
                    tagged.output_key.1,
                    call.block_number,
                    call.target_address,
                    reason
                );
                EventCallResult {
                    block_number: call.block_number,
                    block_timestamp: call.block_timestamp,
                    log_index: call.log_index,
                    target_address: call.target_address.0 .0,
                    value_bytes: Vec::new(),
                    param_values: (*call.encoded_params).clone(),
                    is_reverted: true,
                    revert_reason: Some(reason),
                }
            }
        };

        let entry = results
            .entry(tagged.output_key.clone())
            .or_insert_with(|| (Vec::new(), tagged.max_params));
        entry.0.push(event_result);
    }

    Ok((skipped, results, written))
}

/// Process event triggers and make eth_calls
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_event_triggers(
    triggers: Vec<EventTriggerData>,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
    ctx: &EthCallContext<'_>,
    range_start: u64,
    range_end: u64,
    contracts: &Contracts,
    skip_contract_index: bool,
    trigger_batch_size: usize,
) -> Result<(Vec<SkippedFactoryTrigger>, PendingWrites), EthCallCollectionError> {
    let mut skipped_factory_triggers: Vec<SkippedFactoryTrigger> = Vec::new();
    let empty_writes = PendingWrites::new();

    // Collect all configured (contract_name, function_name) pairs so we can write empty files
    // for pairs with no matching events (prevents catchup from retrying)
    let configured_pairs: HashSet<(String, String)> = event_call_configs
        .values()
        .flatten()
        .map(|c| (c.contract_name.clone(), c.function_name.clone()))
        .collect();

    if triggers.is_empty() {
        // Write empty files for all configured pairs
        for (contract_name, function_name) in &configured_pairs {
            write_empty_event_call_file(
                ctx.output_dir,
                contract_name,
                function_name,
                range_start,
                range_end,
            )
            .await?;
            if !skip_contract_index {
                let sub_dir = ctx
                    .output_dir
                    .join(contract_name)
                    .join(function_name)
                    .join("on_events");
                let expected_for_range =
                    build_expected_factory_contracts_for_range(contracts, range_end + 1);
                if let Some(expected) = expected_for_range.get(contract_name.as_str()) {
                    let rk = range_key(range_start, range_end);
                    let mut ci = read_contract_index(&sub_dir);
                    update_contract_index(&mut ci, &rk, expected);
                    if let Err(e) = write_contract_index(&sub_dir, &ci) {
                        tracing::warn!("Failed to write contract index (empty on_events): {}", e);
                    }
                }
            }
        }
        return Ok((skipped_factory_triggers, empty_writes));
    }

    let mut written_pairs: HashSet<(String, String)> = HashSet::new();
    let mut group_results: HashMap<(String, String), (Vec<EventCallResult>, usize)> =
        HashMap::new();

    let chunk_size = if trigger_batch_size == 0 {
        usize::MAX
    } else {
        trigger_batch_size
    };

    let chunks: Vec<Vec<EventTriggerData>> = if chunk_size == usize::MAX {
        vec![triggers]
    } else {
        triggers.chunks(chunk_size).map(|c| c.to_vec()).collect()
    };

    let all_chunk_results = futures::future::try_join_all(chunks.into_iter().map(|chunk| {
        process_trigger_chunk(
            chunk,
            event_call_configs,
            factory_addresses,
            ctx,
            range_start,
            range_end,
        )
    }))
    .await?;

    for (chunk_skipped, chunk_results, chunk_written) in all_chunk_results {
        skipped_factory_triggers.extend(chunk_skipped);
        written_pairs.extend(chunk_written);
        for (key, (results, max_params)) in chunk_results {
            let entry = group_results
                .entry(key)
                .or_insert_with(|| (Vec::new(), max_params));
            entry.0.extend(results);
            entry.1 = entry.1.max(max_params);
        }
    }

    if !group_results.is_empty() {
        // Phase 2: Write all groups concurrently
        let mut write_set: tokio::task::JoinSet<Result<(), EthCallCollectionError>> =
            tokio::task::JoinSet::new();

        for ((contract_name, function_name), (mut results, max_params)) in group_results {
            results.sort_by_key(|r| (r.block_number, r.log_index, r.target_address));

            let sub_dir = ctx
                .output_dir
                .join(&contract_name)
                .join(&function_name)
                .join("on_events");
            let output_path = sub_dir.join(format!("{}-{}.parquet", range_start, range_end));
            let result_count = results.len();

            let decoder_results: Option<Vec<DecoderEventCallResult>> = if ctx.decoder_tx.is_some() {
                Some(
                    results
                        .iter()
                        .map(|r| DecoderEventCallResult {
                            block_number: r.block_number,
                            block_timestamp: r.block_timestamp,
                            log_index: r.log_index,
                            target_address: r.target_address,
                            value: r.value_bytes.clone(),
                            is_reverted: r.is_reverted,
                            revert_reason: r.revert_reason.clone(),
                        })
                        .collect(),
                )
            } else {
                None
            };

            let storage_manager = ctx.storage_manager.cloned();
            let chain_name = ctx.chain_name.to_string();
            let decoder_tx = ctx.decoder_tx.clone();
            let skip_ci = skip_contract_index;
            let contracts = contracts.clone();

            write_set.spawn(async move {
                tokio::fs::create_dir_all(&sub_dir).await?;

                write_event_call_results_to_parquet_async(
                    results,
                    output_path.clone(),
                    max_params,
                )
                .await?;

                tracing::info!(
                    "Wrote {} event-triggered eth_call results to {} (blocks {}-{})",
                    result_count,
                    output_path.display(),
                    range_start,
                    range_end,
                );

                if let Some(ref sm) = storage_manager {
                    let data_type = format!(
                        "raw/eth_calls/{}/{}/on_events",
                        contract_name, function_name
                    );
                    upload_parquet_to_s3(sm, &output_path, &chain_name, &data_type, range_start, range_end)
                        .await
                        .map_err(|e| {
                            EthCallCollectionError::Io(std::io::Error::other(e.to_string()))
                        })?;
                }

                if !skip_ci {
                    let expected_for_range =
                        build_expected_factory_contracts_for_range(&contracts, range_end + 1);
                    if let Some(expected) = expected_for_range.get(&contract_name) {
                        let rk = range_key(range_start, range_end);
                        let mut ci = read_contract_index(&sub_dir);
                        update_contract_index(&mut ci, &rk, expected);
                        if let Err(e) = write_contract_index(&sub_dir, &ci) {
                            tracing::warn!(
                                "Failed to write contract index for {}.{}/on_events: {}",
                                contract_name, function_name, e
                            );
                        } else if let Some(ref sm) = storage_manager {
                            let index_path = sub_dir.join("contract_index.json");
                            if let Err(e) = upload_sidecar_to_s3(sm, &index_path).await {
                                tracing::warn!(
                                    "Failed to upload contract index sidecar for {}.{}/on_events: {}",
                                    contract_name, function_name, e
                                );
                            }
                        }
                    }
                }

                if let Some(ref tx) = decoder_tx {
                    if let Some(decoder_results) = decoder_results {
                        let _ = tx
                            .send(DecoderMessage::EventCallsReady {
                                range_start,
                                range_end: range_end + 1,
                                contract_name: contract_name.clone(),
                                function_name: function_name.clone(),
                                results: decoder_results,
                                live_mode: false,
                                retry_transform_after_decode: false,
                            })
                            .await;
                    }
                }

                Ok(())
            });
        }

        // Spawn empty-pair writes into the same write_set
        for (contract_name, function_name) in configured_pairs {
            if !written_pairs.contains(&(contract_name.clone(), function_name.clone())) {
                let output_dir = ctx.output_dir.to_path_buf();
                let skip_ci = skip_contract_index;
                let contracts = contracts.clone();
                write_set.spawn(async move {
                    write_empty_event_call_file(
                        &output_dir,
                        &contract_name,
                        &function_name,
                        range_start,
                        range_end,
                    )
                    .await?;
                    if !skip_ci {
                        let sub_dir = output_dir
                            .join(&contract_name)
                            .join(&function_name)
                            .join("on_events");
                        let expected_for_range =
                            build_expected_factory_contracts_for_range(&contracts, range_end + 1);
                        if let Some(expected) = expected_for_range.get(contract_name.as_str()) {
                            let rk = range_key(range_start, range_end);
                            let mut ci = read_contract_index(&sub_dir);
                            update_contract_index(&mut ci, &rk, expected);
                            if let Err(e) = write_contract_index(&sub_dir, &ci) {
                                tracing::warn!(
                                    "Failed to write contract index (empty on_events): {}",
                                    e
                                );
                            }
                        }
                    }
                    Ok(())
                });
            }
        }

        return Ok((skipped_factory_triggers, write_set));
    }

    Ok((skipped_factory_triggers, empty_writes))
}

/// Write an empty parquet file for event-triggered calls when no events matched
async fn write_empty_event_call_file(
    output_dir: &Path,
    contract_name: &str,
    function_name: &str,
    range_start: u64,
    range_end: u64,
) -> Result<(), EthCallCollectionError> {
    let sub_dir = output_dir
        .join(contract_name)
        .join(function_name)
        .join("on_events");
    tokio::fs::create_dir_all(&sub_dir).await?;

    let file_name = format!("{}-{}.parquet", range_start, range_end);
    let output_path = sub_dir.join(&file_name);

    // Write empty parquet with 0 params
    write_event_call_results_to_parquet_async(vec![], output_path.clone(), 0).await?;

    tracing::debug!(
        "Wrote empty event-triggered eth_call file to {} (no matching events)",
        output_path.display()
    );

    Ok(())
}

async fn process_trigger_chunk_multicall<'a>(
    trigger_chunk: Vec<EventTriggerData>,
    event_call_configs: &'a HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &'a HashMap<String, HashSet<Address>>,
    ctx: &'a EthCallContext<'_>,
    multicall3_address: Address,
    range_start: u64,
    range_end: u64,
) -> Result<
    (
        Vec<SkippedFactoryTrigger>,
        HashMap<(String, String), Vec<EventCallResult>>,
        HashSet<(String, String)>,
    ),
    EthCallCollectionError,
> {
    let mut skipped: Vec<SkippedFactoryTrigger> = Vec::new();
    let mut results: HashMap<(String, String), Vec<EventCallResult>> = HashMap::new();
    let mut written: HashSet<(String, String)> = HashSet::new();
    let mut calls_by_output: HashMap<(String, String), Vec<PreparedEventCall>> = HashMap::new();

    for trigger in &trigger_chunk {
        let key = (trigger.source_name.clone(), trigger.event_signature);

        if let Some(configs) = event_call_configs.get(&key) {
            for config in configs {
                if let Some(sb) = config.start_block {
                    if trigger.block_number < sb {
                        continue;
                    }
                }

                let target_address = if let Some(addr) = config.target_address {
                    addr
                } else {
                    let emitter = Address::from(trigger.emitter_address);
                    match factory_addresses.get(&config.contract_name) {
                        Some(known_addresses) => {
                            if !known_addresses.contains(&emitter) {
                                skipped.push(SkippedFactoryTrigger {
                                    trigger: trigger.clone(),
                                });
                                tracing::debug!(
                                    "Buffering event trigger: emitter {:?} not yet in known addresses for {}",
                                    emitter,
                                    config.contract_name
                                );
                                continue;
                            }
                            emitter
                        }
                        None => {
                            skipped.push(SkippedFactoryTrigger {
                                trigger: trigger.clone(),
                            });
                            tracing::debug!(
                                "Buffering event trigger for {}: no factory addresses loaded yet",
                                config.contract_name
                            );
                            continue;
                        }
                    }
                };

                let (params, encoded_params) =
                    match build_event_call_params(trigger, &config.params) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(
                                "Failed to build params for {}.{} from event: {}",
                                config.contract_name,
                                config.function_name,
                                e
                            );
                            continue;
                        }
                    };

                let output_key = (config.contract_name.clone(), config.function_name.clone());
                let config_arc = Arc::new(config.clone());
                calls_by_output
                    .entry(output_key)
                    .or_default()
                    .push(PreparedEventCall {
                        block_number: trigger.block_number,
                        block_timestamp: trigger.block_timestamp,
                        log_index: trigger.log_index,
                        config: config_arc,
                        target_address,
                        decoded_values: params,
                        encoded_params: Arc::new(encoded_params),
                    });
            }
        }
    }

    if calls_by_output.is_empty() {
        return Ok((skipped, results, written));
    }

    for key in calls_by_output.keys() {
        written.insert(key.clone());
    }

    let mut calls_by_block: HashMap<u64, Vec<PreparedEventCall>> = HashMap::new();
    for ((_contract_name, _function_name), calls) in calls_by_output {
        for call in calls {
            let block_number = call.block_number;
            calls_by_block.entry(block_number).or_default().push(call);
        }
    }

    let mut sorted_blocks: Vec<u64> = calls_by_block.keys().copied().collect();
    sorted_blocks.sort_unstable();

    let mut block_multicalls: Vec<BlockMulticall<(EventCallMeta, u64, u64)>> = Vec::new();

    for block_number in sorted_blocks {
        if let Some(calls) = calls_by_block.get(&block_number) {
            let mut slots: Vec<MulticallSlotGeneric<(EventCallMeta, u64, u64)>> = Vec::new();

            for call_info in calls {
                let calldata = encode_call_with_params(
                    call_info.config.function_selector,
                    &call_info.decoded_values,
                );
                slots.push(MulticallSlotGeneric {
                    block_number,
                    block_timestamp: call_info.block_timestamp,
                    target_address: call_info.target_address,
                    encoded_calldata: calldata,
                    metadata: (
                        EventCallMeta {
                            contract_name: call_info.config.contract_name.clone(),
                            function_name: call_info.config.function_name.clone(),
                            target_address: call_info.target_address,
                            log_index: call_info.log_index,
                            param_values: call_info.encoded_params.clone(),
                        },
                        block_number,
                        call_info.block_timestamp,
                    ),
                });
            }

            if !slots.is_empty() {
                block_multicalls.push(BlockMulticall {
                    block_number,
                    block_id: BlockId::Number(BlockNumberOrTag::Number(block_number)),
                    slots,
                });
            }
        }
    }

    if block_multicalls.is_empty() {
        return Ok((skipped, results, written));
    }

    const MAX_SUBCALLS_PER_MULTICALL: usize = 25;
    let total_subcalls: usize = block_multicalls.iter().map(|bm| bm.slots.len()).sum();
    let max_subcalls = block_multicalls
        .iter()
        .map(|bm| bm.slots.len())
        .max()
        .unwrap_or(0);

    let min_block = block_multicalls.iter().map(|bm| bm.block_number).min().unwrap();
    let max_block = block_multicalls.iter().map(|bm| bm.block_number).max().unwrap();

    let block_multicalls: Vec<BlockMulticall<(EventCallMeta, u64, u64)>> =
        if max_subcalls > MAX_SUBCALLS_PER_MULTICALL {
            let mut split = Vec::with_capacity(block_multicalls.len() * 2);
            for bm in block_multicalls {
                if bm.slots.len() <= MAX_SUBCALLS_PER_MULTICALL {
                    split.push(bm);
                } else {
                    for chunk in bm.slots.chunks(MAX_SUBCALLS_PER_MULTICALL) {
                        split.push(BlockMulticall {
                            block_number: bm.block_number,
                            block_id: bm.block_id,
                            slots: chunk.to_vec(),
                        });
                    }
                }
            }
            split
        } else {
            block_multicalls
        };

    tracing::info!(
        "Executing {} multicalls ({} sub-calls, max {}/block) for event-triggered calls in blocks {}-{}",
        block_multicalls.len(),
        total_subcalls,
        max_subcalls,
        min_block,
        max_block
    );

    let rpc_results = execute_multicalls_generic(
        ctx.client,
        multicall3_address,
        block_multicalls,
        ctx.chain_name,
    )
    .await?;

    for ((meta, block_number, block_timestamp), return_data, success) in rpc_results {
        let key = (meta.contract_name.clone(), meta.function_name.clone());
        let is_reverted = !success;
        let revert_reason = if is_reverted {
            Some(format!(
                "Multicall3 reported failure for {}.{} at block {}",
                meta.contract_name, meta.function_name, block_number
            ))
        } else {
            None
        };
        if is_reverted {
            tracing::warn!(
                "Multicall event-triggered eth_call reverted for {}.{} at block {} (address {}, log_index {})",
                meta.contract_name,
                meta.function_name,
                block_number,
                meta.target_address,
                meta.log_index
            );
        }
        results.entry(key).or_default().push(EventCallResult {
            block_number,
            block_timestamp,
            log_index: meta.log_index,
            target_address: meta.target_address.0 .0,
            value_bytes: if is_reverted { Vec::new() } else { return_data },
            param_values: (*meta.param_values).clone(),
            is_reverted,
            revert_reason,
        });
    }

    Ok((skipped, results, written))
}

/// Process event triggers using Multicall3 aggregate3 to batch all calls per block
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_event_triggers_multicall(
    triggers: Vec<EventTriggerData>,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
    ctx: &EthCallContext<'_>,
    multicall3_address: Address,
    range_start: u64,
    range_end: u64,
    contracts: &Contracts,
    skip_contract_index: bool,
    trigger_batch_size: usize,
) -> Result<(Vec<SkippedFactoryTrigger>, PendingWrites), EthCallCollectionError> {
    let mut skipped_factory_triggers: Vec<SkippedFactoryTrigger> = Vec::new();
    let empty_writes = PendingWrites::new();

    // Collect all configured (contract_name, function_name) pairs so we can write empty files
    // for pairs with no matching events (prevents catchup from retrying)
    let configured_pairs: HashSet<(String, String)> = event_call_configs
        .values()
        .flatten()
        .map(|c| (c.contract_name.clone(), c.function_name.clone()))
        .collect();

    if triggers.is_empty() {
        // Write empty files for all configured pairs
        for (contract_name, function_name) in &configured_pairs {
            write_empty_event_call_file(
                ctx.output_dir,
                contract_name,
                function_name,
                range_start,
                range_end,
            )
            .await?;
            if !skip_contract_index {
                let sub_dir = ctx
                    .output_dir
                    .join(contract_name)
                    .join(function_name)
                    .join("on_events");
                let expected_for_range =
                    build_expected_factory_contracts_for_range(contracts, range_end + 1);
                if let Some(expected) = expected_for_range.get(contract_name.as_str()) {
                    let rk = range_key(range_start, range_end);
                    let mut ci = read_contract_index(&sub_dir);
                    update_contract_index(&mut ci, &rk, expected);
                    if let Err(e) = write_contract_index(&sub_dir, &ci) {
                        tracing::warn!("Failed to write contract index (empty on_events): {}", e);
                    }
                }
            }
        }
        return Ok((skipped_factory_triggers, empty_writes));
    }

    let mut group_results: HashMap<(String, String), Vec<EventCallResult>> = HashMap::new();
    let mut written_pairs: HashSet<(String, String)> = HashSet::new();

    let chunk_size = if trigger_batch_size == 0 {
        usize::MAX
    } else {
        trigger_batch_size
    };

    let chunks: Vec<Vec<EventTriggerData>> = if chunk_size == usize::MAX {
        vec![triggers]
    } else {
        triggers.chunks(chunk_size).map(|c| c.to_vec()).collect()
    };

    let all_chunk_results = futures::future::try_join_all(chunks.into_iter().map(|chunk| {
        process_trigger_chunk_multicall(
            chunk,
            event_call_configs,
            factory_addresses,
            ctx,
            multicall3_address,
            range_start,
            range_end,
        )
    }))
    .await?;

    for (chunk_skipped, chunk_results, chunk_written) in all_chunk_results {
        skipped_factory_triggers.extend(chunk_skipped);
        written_pairs.extend(chunk_written);
        for (key, results) in chunk_results {
            group_results.entry(key).or_default().extend(results);
        }
    }

    // Write parquet, upload S3, and send to decoder for all groups CONCURRENTLY.
    // Each group writes to a different file, so there's no conflict.
    let mut write_set: tokio::task::JoinSet<Result<(), EthCallCollectionError>> =
        tokio::task::JoinSet::new();

    for ((contract_name, function_name), mut results) in group_results {
        if results.is_empty() {
            continue;
        }

        results.sort_by_key(|r| (r.block_number, r.log_index, r.target_address));

        let sub_dir = ctx
            .output_dir
            .join(&contract_name)
            .join(&function_name)
            .join("on_events");

        let output_path = sub_dir.join(format!("{}-{}.parquet", range_start, range_end));

        let result_count = results.len();
        let max_params = results
            .iter()
            .map(|r| r.param_values.len())
            .max()
            .unwrap_or(0);

        let decoder_results: Option<Vec<DecoderEventCallResult>> = if ctx.decoder_tx.is_some() {
            Some(
                results
                    .iter()
                    .map(|r| DecoderEventCallResult {
                        block_number: r.block_number,
                        block_timestamp: r.block_timestamp,
                        log_index: r.log_index,
                        target_address: r.target_address,
                        value: r.value_bytes.clone(),
                        is_reverted: r.is_reverted,
                        revert_reason: r.revert_reason.clone(),
                    })
                    .collect(),
            )
        } else {
            None
        };

        // Clone what the spawned write task needs
        let storage_manager = ctx.storage_manager.cloned();
        let chain_name = ctx.chain_name.to_string();
        let decoder_tx = ctx.decoder_tx.clone();
        let skip_ci = skip_contract_index;
        let contracts = contracts.clone();

        write_set.spawn(async move {
            tokio::fs::create_dir_all(&sub_dir).await?;

            write_event_call_results_to_parquet_async(results, output_path.clone(), max_params)
                .await?;

            tracing::info!(
                "Wrote {} multicall event-triggered eth_call results to {} (blocks {}-{})",
                result_count,
                output_path.display(),
                range_start,
                range_end,
            );

            if let Some(ref sm) = storage_manager {
                let data_type = format!(
                    "raw/eth_calls/{}/{}/on_events",
                    contract_name, function_name
                );
                upload_parquet_to_s3(
                    sm,
                    &output_path,
                    &chain_name,
                    &data_type,
                    range_start,
                    range_end,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            if !skip_ci {
                let expected_for_range =
                    build_expected_factory_contracts_for_range(&contracts, range_end + 1);
                if let Some(expected) = expected_for_range.get(&contract_name) {
                    let rk = range_key(range_start, range_end);
                    let mut ci = read_contract_index(&sub_dir);
                    update_contract_index(&mut ci, &rk, expected);
                    if let Err(e) = write_contract_index(&sub_dir, &ci) {
                        tracing::warn!(
                            "Failed to write contract index for {}.{}/on_events: {}",
                            contract_name,
                            function_name,
                            e
                        );
                    } else if let Some(ref sm) = storage_manager {
                        let index_path = sub_dir.join("contract_index.json");
                        if let Err(e) = upload_sidecar_to_s3(sm, &index_path).await {
                            tracing::warn!(
                                "Failed to upload contract index sidecar for {}.{}/on_events: {}",
                                contract_name,
                                function_name,
                                e
                            );
                        }
                    }
                }
            }

            if let Some(ref tx) = decoder_tx {
                if let Some(decoder_results) = decoder_results {
                    let _ = tx
                        .send(DecoderMessage::EventCallsReady {
                            range_start,
                            range_end: range_end + 1,
                            contract_name: contract_name.clone(),
                            function_name: function_name.clone(),
                            results: decoder_results,
                            live_mode: false,
                            retry_transform_after_decode: false,
                        })
                        .await;
                }
            }

            Ok(())
        });
    }

    // Spawn empty-pair writes into the same write_set
    for (contract_name, function_name) in configured_pairs {
        if !written_pairs.contains(&(contract_name.clone(), function_name.clone())) {
            let output_dir = ctx.output_dir.to_path_buf();
            let skip_ci = skip_contract_index;
            let contracts = contracts.clone();
            write_set.spawn(async move {
                write_empty_event_call_file(
                    &output_dir,
                    &contract_name,
                    &function_name,
                    range_start,
                    range_end,
                )
                .await?;
                if !skip_ci {
                    let sub_dir = output_dir
                        .join(&contract_name)
                        .join(&function_name)
                        .join("on_events");
                    let expected_for_range =
                        build_expected_factory_contracts_for_range(&contracts, range_end + 1);
                    if let Some(expected) = expected_for_range.get(contract_name.as_str()) {
                        let rk = range_key(range_start, range_end);
                        let mut ci = read_contract_index(&sub_dir);
                        update_contract_index(&mut ci, &rk, expected);
                        if let Err(e) = write_contract_index(&sub_dir, &ci) {
                            tracing::warn!(
                                "Failed to write contract index (empty on_events): {}",
                                e
                            );
                        }
                    }
                }
                Ok(())
            });
        }
    }

    Ok((skipped_factory_triggers, write_set))
}

/// Write event-triggered call results to parquet
pub(crate) fn write_event_call_results_to_parquet(
    results: &[EventCallResult],
    output_path: &Path,
    num_params: usize,
) -> Result<(), EthCallCollectionError> {
    let schema = build_event_call_schema(num_params);
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = results.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = results.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // log_index
    let arr: UInt32Array = results.iter().map(|r| Some(r.log_index)).collect();
    arrays.push(Arc::new(arr));

    // target_address - use FixedSizeBinaryBuilder for empty case compatibility
    let mut builder = FixedSizeBinaryBuilder::new(20);
    for r in results {
        builder.append_value(r.target_address.as_slice())?;
    }
    arrays.push(Arc::new(builder.finish()));

    // value
    let arr: BinaryArray = results
        .iter()
        .map(|r| Some(r.value_bytes.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    // is_reverted
    let arr: BooleanArray = results.iter().map(|r| Some(r.is_reverted)).collect();
    arrays.push(Arc::new(arr));

    // revert_reason
    let arr: StringArray = results.iter().map(|r| r.revert_reason.as_deref()).collect();
    arrays.push(Arc::new(arr));

    // param columns
    for i in 0..num_params {
        let arr: BinaryArray = results
            .iter()
            .map(|r| r.param_values.get(i).map(|v| v.as_slice()))
            .collect();
        arrays.push(Arc::new(arr));
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    crate::storage::atomic_write_parquet_fast(&batch, output_path)?;
    Ok(())
}

/// Async wrapper for write_event_call_results_to_parquet
pub(crate) async fn write_event_call_results_to_parquet_async(
    results: Vec<EventCallResult>,
    output_path: PathBuf,
    num_params: usize,
) -> Result<(), EthCallCollectionError> {
    tokio::task::spawn_blocking(move || {
        write_event_call_results_to_parquet(&results, &output_path, num_params)
    })
    .await
    .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))?
}

/// Build schema for event-triggered call results
pub(crate) fn build_event_call_schema(num_params: usize) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("target_address", DataType::FixedSizeBinary(20), false),
        Field::new("value", DataType::Binary, false),
        Field::new("is_reverted", DataType::Boolean, false),
        Field::new("revert_reason", DataType::Utf8, true),
    ];

    for i in 0..num_params {
        fields.push(Field::new(format!("param_{}", i), DataType::Binary, true));
    }

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::extract_param_from_event;
    use crate::raw_data::historical::receipts::EventTriggerData;
    use crate::types::config::eth_call::{EventFieldLocation, EvmType};
    use crate::types::uniswap::v4::PoolKey;
    use alloy::dyn_abi::DynSolValue;
    use alloy::primitives::{Address, I256, U256};
    use alloy::sol;
    use alloy::sol_types::SolValue;

    sol! {
        struct TestPoolKey {
            address currency0;
            address currency1;
            uint24 fee;
            int24 tickSpacing;
            address hooks;
        }
    }

    #[test]
    fn test_extract_param_from_event_v4_pool_id_from_key_data() {
        let currency0 = Address::from([0x11; 20]);
        let currency1 = Address::from([0x22; 20]);
        let hooks = Address::from([0x33; 20]);
        let fee = 3000u32;
        let tick_spacing = -60i32;

        let trigger = EventTriggerData {
            block_number: 100,
            block_timestamp: 200,
            log_index: 0,
            emitter_address: [0u8; 20],
            source_name: "test".to_string(),
            event_signature: [0u8; 32],
            topics: vec![[0u8; 32]],
            data: TestPoolKey {
                currency0,
                currency1,
                fee: fee.try_into().unwrap(),
                tickSpacing: tick_spacing.try_into().unwrap(),
                hooks,
            }
            .abi_encode(),
        };

        let (value, _encoded) = extract_param_from_event(
            &trigger,
            &EventFieldLocation::V4PoolIdFromKeyData,
            &EvmType::Bytes32,
        )
        .expect("pool id extraction should succeed");

        let expected = PoolKey {
            currency0,
            currency1,
            fee,
            tick_spacing,
            hooks,
        }
        .pool_id();

        assert_eq!(value, DynSolValue::FixedBytes(expected, 32));
    }

    #[test]
    fn test_extract_param_from_event_v4_pool_key_tuple_from_data_with_offset() {
        let currency0 = Address::from([0x11; 20]);
        let currency1 = Address::from([0x22; 20]);
        let hooks = Address::from([0x33; 20]);
        let fee = 3000u32;
        let tick_spacing = -60i32;

        let mut data = vec![0xAA; 32];
        data.extend(
            TestPoolKey {
                currency0,
                currency1,
                fee: fee.try_into().unwrap(),
                tickSpacing: tick_spacing.try_into().unwrap(),
                hooks,
            }
            .abi_encode(),
        );

        let trigger = EventTriggerData {
            block_number: 100,
            block_timestamp: 200,
            log_index: 0,
            emitter_address: [0u8; 20],
            source_name: "test".to_string(),
            event_signature: [0u8; 32],
            topics: vec![[0u8; 32]],
            data,
        };

        let (value, encoded) = extract_param_from_event(
            &trigger,
            &EventFieldLocation::V4PoolKeyTupleFromData(1),
            &EvmType::Address,
        )
        .expect("pool key tuple extraction should succeed");

        let expected = DynSolValue::Tuple(vec![
            DynSolValue::Address(currency0),
            DynSolValue::Address(currency1),
            DynSolValue::Uint(U256::from(fee), 24),
            DynSolValue::Int(I256::try_from(tick_spacing).unwrap_or_default(), 24),
            DynSolValue::Address(hooks),
        ]);

        assert_eq!(value, expected);
        assert_eq!(encoded, expected.abi_encode());
    }

    #[test]
    fn test_extract_param_from_event_v4_pool_key_tuple_from_data_short_data_errors() {
        let trigger = EventTriggerData {
            block_number: 100,
            block_timestamp: 200,
            log_index: 0,
            emitter_address: [0u8; 20],
            source_name: "test".to_string(),
            event_signature: [0u8; 32],
            topics: vec![[0u8; 32]],
            data: vec![0u8; 4 * 32],
        };

        let err = extract_param_from_event(
            &trigger,
            &EventFieldLocation::V4PoolKeyTupleFromData(0),
            &EvmType::Address,
        )
        .expect_err("short event data should fail");

        assert!(
            err.to_string().contains("requires at least"),
            "error should mention required data length: {err}"
        );
    }
}
