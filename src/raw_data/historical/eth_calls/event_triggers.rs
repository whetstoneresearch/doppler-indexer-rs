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

/// A trigger that was skipped because its factory address wasn't known yet.
/// Buffered for replay when factory addresses arrive via IncrementalAddresses.
#[derive(Debug, Clone)]
pub struct SkippedFactoryTrigger {
    pub trigger: EventTriggerData,
}

/// An event-triggered call matched with its config and encoded parameters,
/// ready to be grouped by output key.
pub(super) struct PreparedEventCall {
    pub trigger: EventTriggerData,
    pub config: EventTriggeredCallConfig,
    pub target_address: Address,
    pub decoded_values: Vec<DynSolValue>,
    pub encoded_params: Vec<Vec<u8>>,
}

/// A fully-built eth_call ready for RPC batch execution.
#[derive(Clone)]
pub(super) struct PendingEventCall {
    pub request: TransactionRequest,
    pub block_id: BlockId,
    pub trigger: EventTriggerData,
    pub target_address: Address,
    pub encoded_params: Vec<Vec<u8>>,
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
                    let function_name = call
                        .function
                        .split('(')
                        .next()
                        .unwrap_or(&call.function)
                        .to_string();

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
                            let function_name = call
                                .function
                                .split('(')
                                .next()
                                .unwrap_or(&call.function)
                                .to_string();

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

/// Process event triggers and make eth_calls
pub(crate) async fn process_event_triggers(
    triggers: Vec<EventTriggerData>,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
    ctx: &EthCallContext<'_>,
    range_start: u64,
    range_end: u64,
    contracts: &Contracts,
) -> Result<Vec<SkippedFactoryTrigger>, EthCallCollectionError> {
    let mut skipped_factory_triggers: Vec<SkippedFactoryTrigger> = Vec::new();

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
            let sub_dir = ctx.output_dir.join(contract_name).join(function_name).join("on_events");
            let expected_for_range =
                build_expected_factory_contracts_for_range(contracts, range_end + 1);
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
        return Ok(skipped_factory_triggers);
    }

    // Group triggers by (source_name, event_signature) and then by (contract_name, function_name)
    // to batch calls together
    let mut calls_by_output: HashMap<(String, String), Vec<PreparedEventCall>> = HashMap::new();

    for trigger in triggers {
        let key = (trigger.source_name.clone(), trigger.event_signature);

        if let Some(configs) = event_call_configs.get(&key) {
            for config in configs {
                // Skip if trigger block is before contract's start_block
                if let Some(sb) = config.start_block {
                    if trigger.block_number < sb {
                        continue;
                    }
                }

                // Determine target address
                let target_address = if let Some(addr) = config.target_address {
                    addr
                } else {
                    // Factory collection - use event emitter, but ONLY if it's a known factory address
                    let emitter = Address::from(trigger.emitter_address);
                    match factory_addresses.get(&config.contract_name) {
                        Some(known_addresses) => {
                            if !known_addresses.contains(&emitter) {
                                // Emitter not in known addresses — could be not-yet-discovered.
                                // Buffer for replay; factory RangeComplete will drop if
                                // this is a legitimate miss.
                                skipped_factory_triggers.push(SkippedFactoryTrigger {
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
                            // No factory addresses known yet for this collection
                            // Buffer the trigger for replay when addresses arrive
                            skipped_factory_triggers.push(SkippedFactoryTrigger {
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

                // Build parameters
                let (params, encoded_params) =
                    match build_event_call_params(&trigger, &config.params) {
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
                calls_by_output
                    .entry(output_key)
                    .or_default()
                    .push(PreparedEventCall {
                        trigger: trigger.clone(),
                        config: config.clone(),
                        target_address,
                        decoded_values: params,
                        encoded_params,
                    });
            }
        }
    }

    // Track which (contract_name, function_name) pairs got results
    let mut written_pairs: HashSet<(String, String)> = HashSet::new();

    // Process each group of calls
    for ((contract_name, function_name), calls) in calls_by_output {
        if calls.is_empty() {
            continue;
        }

        // Mark this pair as having results
        written_pairs.insert((contract_name.clone(), function_name.clone()));

        // Get block range for output file naming
        let min_block = calls.iter().map(|c| c.trigger.block_number).min().unwrap();
        let max_block = calls.iter().map(|c| c.trigger.block_number).max().unwrap();

        tracing::info!(
            "Processing {} event-triggered eth_calls for {}.{} (blocks {}-{})",
            calls.len(),
            contract_name,
            function_name,
            min_block,
            max_block
        );

        // Build RPC calls (owned triggers to avoid lifetime issues with concurrent chunks)
        let mut pending_calls: Vec<PendingEventCall> = Vec::new();

        for call in &calls {
            let calldata =
                encode_call_with_params(call.config.function_selector, &call.decoded_values);
            let tx = TransactionRequest::default()
                .to(call.target_address)
                .input(calldata.into());
            let block_id = BlockId::Number(BlockNumberOrTag::Number(call.trigger.block_number));
            pending_calls.push(PendingEventCall {
                request: tx,
                block_id,
                trigger: call.trigger.clone(),
                target_address: call.target_address,
                encoded_params: call.encoded_params.clone(),
            });
        }

        // Execute all calls in a single batch
        let max_params = calls
            .iter()
            .map(|c| c.encoded_params.len())
            .max()
            .unwrap_or(0);

        let batch_calls: Vec<(TransactionRequest, BlockId)> = pending_calls
            .iter()
            .map(|c| (c.request.clone(), c.block_id))
            .collect();

        let results = ctx.client.call_batch(batch_calls).await?;

        let mut all_results: Vec<EventCallResult> = Vec::with_capacity(results.len());
        for (i, result) in results.into_iter().enumerate() {
            let call = &pending_calls[i];

            match result {
                Ok(bytes) => {
                    all_results.push(EventCallResult {
                        block_number: call.trigger.block_number,
                        block_timestamp: call.trigger.block_timestamp,
                        log_index: call.trigger.log_index,
                        target_address: call.target_address.0 .0,
                        value_bytes: bytes.to_vec(),
                        param_values: call.encoded_params.clone(),
                        is_reverted: false,
                        revert_reason: None,
                    });
                }
                Err(e) => {
                    let reason = e.to_string();
                    let params_hex: Vec<String> = call
                        .encoded_params
                        .iter()
                        .map(|p| format!("0x{}", hex::encode(p)))
                        .collect();
                    tracing::warn!(
                        "Event-triggered eth_call reverted for {}.{} at block {} (address {}, params {:?}): {}",
                        contract_name,
                        function_name,
                        call.trigger.block_number,
                        call.target_address,
                        params_hex,
                        reason
                    );
                    all_results.push(EventCallResult {
                        block_number: call.trigger.block_number,
                        block_timestamp: call.trigger.block_timestamp,
                        log_index: call.trigger.log_index,
                        target_address: call.target_address.0 .0,
                        value_bytes: Vec::new(),
                        param_values: call.encoded_params.clone(),
                        is_reverted: true,
                        revert_reason: Some(reason),
                    });
                }
            }
        }

        // Sort by block number, log index
        all_results.sort_by_key(|r| (r.block_number, r.log_index, r.target_address));

        // Write to parquet and send to decoder
        if !all_results.is_empty() {
            let sub_dir = ctx
                .output_dir
                .join(&contract_name)
                .join(&function_name)
                .join("on_events");
            tokio::fs::create_dir_all(&sub_dir).await?;

            let file_name = format!("{}-{}.parquet", range_start, range_end);
            let output_path = sub_dir.join(&file_name);

            let result_count = all_results.len();

            // Build decoder results before moving all_results into the async write
            let decoder_results: Option<Vec<DecoderEventCallResult>> = if ctx.decoder_tx.is_some() {
                Some(
                    all_results
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

            write_event_call_results_to_parquet_async(all_results, output_path.clone(), max_params)
                .await?;

            tracing::info!(
                "Wrote {} event-triggered eth_call results to {} (event blocks {}-{})",
                result_count,
                output_path.display(),
                min_block,
                max_block,
            );

            // Upload to S3 if configured
            if let Some(sm) = ctx.storage_manager {
                let data_type = format!(
                    "raw/eth_calls/{}/{}/on_events",
                    contract_name, function_name
                );
                upload_parquet_to_s3(
                    sm,
                    &output_path,
                    ctx.chain_name,
                    &data_type,
                    range_start,
                    range_end,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            // Update contract index for on_events
            let expected_for_range =
                build_expected_factory_contracts_for_range(contracts, range_end + 1);
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
                } else if let Some(sm) = ctx.storage_manager {
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

            // Send to decoder for decoding (range_end + 1 for exclusive convention)
            if let Some(tx) = ctx.decoder_tx {
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
        }
    }

    // Write empty files for configured pairs that didn't have any matching events
    for (contract_name, function_name) in configured_pairs {
        if !written_pairs.contains(&(contract_name.clone(), function_name.clone())) {
            write_empty_event_call_file(
                ctx.output_dir,
                &contract_name,
                &function_name,
                range_start,
                range_end,
            )
            .await?;
            let sub_dir =
                ctx.output_dir.join(&contract_name).join(&function_name).join("on_events");
            let expected_for_range =
                build_expected_factory_contracts_for_range(contracts, range_end + 1);
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
    }

    Ok(skipped_factory_triggers)
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

/// Process event triggers using Multicall3 aggregate3 to batch all calls per block
pub(crate) async fn process_event_triggers_multicall(
    triggers: Vec<EventTriggerData>,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
    ctx: &EthCallContext<'_>,
    multicall3_address: Address,
    range_start: u64,
    range_end: u64,
    contracts: &Contracts,
) -> Result<Vec<SkippedFactoryTrigger>, EthCallCollectionError> {
    let mut skipped_factory_triggers: Vec<SkippedFactoryTrigger> = Vec::new();

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
            let sub_dir = ctx.output_dir.join(contract_name).join(function_name).join("on_events");
            let expected_for_range =
                build_expected_factory_contracts_for_range(contracts, range_end + 1);
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
        return Ok(skipped_factory_triggers);
    }

    // Group triggers by (source_name, event_signature) and prepare call info
    // Key: (contract_name, function_name)
    let mut calls_by_output: HashMap<(String, String), Vec<PreparedEventCall>> = HashMap::new();

    for trigger in triggers {
        let key = (trigger.source_name.clone(), trigger.event_signature);

        if let Some(configs) = event_call_configs.get(&key) {
            for config in configs {
                // Skip if trigger block is before contract's start_block
                if let Some(sb) = config.start_block {
                    if trigger.block_number < sb {
                        continue;
                    }
                }

                // Determine target address
                let target_address = if let Some(addr) = config.target_address {
                    addr
                } else {
                    // Factory collection - use event emitter, but ONLY if it's a known factory address
                    let emitter = Address::from(trigger.emitter_address);
                    match factory_addresses.get(&config.contract_name) {
                        Some(known_addresses) => {
                            if !known_addresses.contains(&emitter) {
                                // Emitter not in known addresses — could be not-yet-discovered.
                                // Buffer for replay; factory RangeComplete will drop if
                                // this is a legitimate miss.
                                skipped_factory_triggers.push(SkippedFactoryTrigger {
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
                            // No factory addresses known yet for this collection
                            // Buffer the trigger for replay when addresses arrive
                            skipped_factory_triggers.push(SkippedFactoryTrigger {
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

                // Build parameters
                let (params, encoded_params) =
                    match build_event_call_params(&trigger, &config.params) {
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
                calls_by_output
                    .entry(output_key)
                    .or_default()
                    .push(PreparedEventCall {
                        trigger: trigger.clone(),
                        config: config.clone(),
                        target_address,
                        decoded_values: params,
                        encoded_params,
                    });
            }
        }
    }

    if calls_by_output.is_empty() {
        return Ok(skipped_factory_triggers);
    }

    // Group all calls by block number for multicall batching
    let mut calls_by_block: HashMap<u64, Vec<PreparedEventCall>> = HashMap::new();

    for ((_contract_name, _function_name), calls) in calls_by_output {
        for call in calls {
            let block_number = call.trigger.block_number;
            calls_by_block.entry(block_number).or_default().push(call);
        }
    }

    // Build per-block multicalls with block info included in metadata
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
                    block_timestamp: call_info.trigger.block_timestamp,
                    target_address: call_info.target_address,
                    encoded_calldata: calldata,
                    metadata: (
                        EventCallMeta {
                            contract_name: call_info.config.contract_name.clone(),
                            function_name: call_info.config.function_name.clone(),
                            target_address: call_info.target_address,
                            log_index: call_info.trigger.log_index,
                            param_values: call_info.encoded_params.clone(),
                        },
                        block_number,
                        call_info.trigger.block_timestamp,
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
        return Ok(skipped_factory_triggers);
    }

    // Get min/max blocks for logging and file naming
    let min_block = block_multicalls
        .iter()
        .map(|bm| bm.block_number)
        .min()
        .unwrap();
    let max_block = block_multicalls
        .iter()
        .map(|bm| bm.block_number)
        .max()
        .unwrap();

    tracing::info!(
        "Executing {} multicalls for event-triggered calls in blocks {}-{}",
        block_multicalls.len(),
        min_block,
        max_block
    );

    // Execute all multicalls
    let results =
        execute_multicalls_generic(ctx.client, multicall3_address, block_multicalls).await?;

    // Distribute results back to groups
    let mut group_results: HashMap<(String, String), Vec<EventCallResult>> = HashMap::new();

    for ((meta, block_number, block_timestamp), return_data, success) in results {
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
        group_results.entry(key).or_default().push(EventCallResult {
            block_number,
            block_timestamp,
            log_index: meta.log_index,
            target_address: meta.target_address.0 .0,
            value_bytes: if is_reverted { Vec::new() } else { return_data },
            param_values: meta.param_values,
            is_reverted,
            revert_reason,
        });
    }

    // Track which (contract_name, function_name) pairs got results
    let mut written_pairs: HashSet<(String, String)> = HashSet::new();

    // Write parquet and send to decoder for each group
    for ((contract_name, function_name), mut results) in group_results {
        if results.is_empty() {
            continue;
        }

        // Mark this pair as having results
        written_pairs.insert((contract_name.clone(), function_name.clone()));

        // Sort by block number, log index
        results.sort_by_key(|r| (r.block_number, r.log_index, r.target_address));

        let sub_dir = ctx
            .output_dir
            .join(&contract_name)
            .join(&function_name)
            .join("on_events");
        tokio::fs::create_dir_all(&sub_dir).await?;

        let file_name = format!("{}-{}.parquet", range_start, range_end);
        let output_path = sub_dir.join(&file_name);

        let result_count = results.len();
        let max_params = results
            .iter()
            .map(|r| r.param_values.len())
            .max()
            .unwrap_or(0);

        // Build decoder results before moving results into the async write
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

        write_event_call_results_to_parquet_async(results, output_path.clone(), max_params).await?;

        tracing::info!(
            "Wrote {} multicall event-triggered eth_call results to {} (event blocks {}-{})",
            result_count,
            output_path.display(),
            min_block,
            max_block,
        );

        // Upload to S3 if configured
        if let Some(sm) = ctx.storage_manager {
            let data_type = format!(
                "raw/eth_calls/{}/{}/on_events",
                contract_name, function_name
            );
            upload_parquet_to_s3(
                sm,
                &output_path,
                ctx.chain_name,
                &data_type,
                range_start,
                range_end,
            )
            .await
            .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }

        // Update contract index for on_events
        let expected_for_range =
            build_expected_factory_contracts_for_range(contracts, range_end + 1);
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
            } else if let Some(sm) = ctx.storage_manager {
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

        // Send to decoder (range_end + 1 for exclusive convention)
        if let Some(tx) = ctx.decoder_tx {
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
    }

    // Write empty files for configured pairs that didn't have any matching events
    for (contract_name, function_name) in configured_pairs {
        if !written_pairs.contains(&(contract_name.clone(), function_name.clone())) {
            write_empty_event_call_file(
                ctx.output_dir,
                &contract_name,
                &function_name,
                range_start,
                range_end,
            )
            .await?;
            let sub_dir =
                ctx.output_dir.join(&contract_name).join(&function_name).join("on_events");
            let expected_for_range =
                build_expected_factory_contracts_for_range(contracts, range_end + 1);
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
    }

    Ok(skipped_factory_triggers)
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
    crate::storage::atomic_write_parquet(&batch, output_path)?;
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
