//! Live mode eth_call collector.
//!
//! Collects eth_calls for a single block in live mode, supporting:
//! - Regular frequency-based calls
//! - Factory calls for known factory addresses
//! - Once calls for newly discovered addresses
//! - Event-triggered calls based on matching log events

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};
use tokio::sync::mpsc;

use super::error::LiveError;
use super::storage::LiveStorage;
use super::types::{LiveEthCall, LiveFactoryAddresses, LiveLog};
use crate::decoding::{DecoderMessage, EthCallResult, EventCallResult, OnceCallResult};
use crate::raw_data::historical::eth_calls::{
    build_call_configs, build_event_call_params, build_event_triggered_call_configs,
    build_factory_once_call_configs, build_once_call_configs, encode_once_call_params, CallConfig,
    EventCallKey, EventTriggeredCallConfig, FrequencyState, OnceCallConfig,
};
use crate::raw_data::historical::receipts::EventTriggerData;
use crate::types::config::eth_call::encode_call_with_params;
use crate::raw_data::historical::factories::get_factory_call_configs;
use crate::rpc::UnifiedRpcClient;
use crate::types::config::chain::ChainConfig;
use crate::types::config::eth_call::{EthCallConfig, Frequency, ParamConfig};

// Re-export LiveError as LiveEthCallError for backwards compatibility
pub use super::error::LiveEthCallError;

/// Collects eth_calls for live mode blocks.
pub struct LiveEthCallCollector {
    chain_name: String,
    http_client: Arc<UnifiedRpcClient>,
    storage: LiveStorage,
    // Call configurations
    call_configs: Vec<CallConfig>,
    once_configs: HashMap<String, Vec<OnceCallConfig>>,
    factory_call_configs: HashMap<String, Vec<EthCallConfig>>,
    factory_once_configs: HashMap<String, Vec<OnceCallConfig>>,
    event_call_configs: HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    // Runtime state
    factory_addresses: HashMap<String, HashSet<Address>>,
    once_called_addresses: HashSet<[u8; 20]>,
    frequency_state: FrequencyState,
    multicall3_address: Option<Address>,
    rpc_batch_size: usize,
}

impl LiveEthCallCollector {
    /// Create a new LiveEthCallCollector from chain configuration.
    pub fn new(
        chain: &ChainConfig,
        http_client: Arc<UnifiedRpcClient>,
        multicall3_address: Option<Address>,
        rpc_batch_size: usize,
    ) -> Self {
        let storage = LiveStorage::new(&chain.name);

        // Build call configs from contracts
        let call_configs = build_call_configs(&chain.contracts).unwrap_or_default();
        let once_configs = build_once_call_configs(&chain.contracts);
        let factory_call_configs =
            get_factory_call_configs(&chain.contracts, &chain.factory_collections);
        let factory_once_configs =
            build_factory_once_call_configs(&factory_call_configs, &chain.contracts);
        let event_call_configs = build_event_triggered_call_configs(&chain.contracts);

        tracing::info!(
            "LiveEthCallCollector initialized: {} regular calls, {} once configs, {} factory configs, {} event configs",
            call_configs.len(),
            once_configs.len(),
            factory_call_configs.len(),
            event_call_configs.len()
        );

        Self {
            chain_name: chain.name.clone(),
            http_client,
            storage,
            call_configs,
            once_configs,
            factory_call_configs,
            factory_once_configs,
            event_call_configs,
            factory_addresses: HashMap::new(),
            once_called_addresses: HashSet::new(),
            frequency_state: FrequencyState {
                last_call_times: HashMap::new(),
            },
            multicall3_address,
            rpc_batch_size,
        }
    }

    /// Check if this collector has any eth_call configurations.
    pub fn has_calls(&self) -> bool {
        !self.call_configs.is_empty()
            || !self.once_configs.is_empty()
            || !self.factory_call_configs.is_empty()
            || !self.event_call_configs.is_empty()
    }

    /// Update factory addresses with newly discovered ones.
    pub fn update_factory_addresses(&mut self, factory_addrs: &LiveFactoryAddresses) {
        for (collection_name, addresses) in &factory_addrs.addresses_by_collection {
            let entry = self
                .factory_addresses
                .entry(collection_name.clone())
                .or_default();
            for (_, addr) in addresses {
                entry.insert(Address::from(*addr));
            }
        }
    }

    /// Collect all eth_calls for a single block.
    ///
    /// Returns the collected calls and sends messages to the decoder channel.
    pub async fn collect_for_block(
        &mut self,
        block_number: u64,
        block_timestamp: u64,
        logs: &[LiveLog],
        factory_addrs: &LiveFactoryAddresses,
        decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<Vec<LiveEthCall>, LiveError> {
        let mut all_calls = Vec::new();

        // 1. Regular frequency-based calls
        let regular_calls = self
            .collect_regular_calls(block_number, block_timestamp, decoder_tx)
            .await?;
        all_calls.extend(regular_calls);

        // 2. Factory calls for known addresses
        let factory_calls = self
            .collect_factory_calls(block_number, block_timestamp, decoder_tx)
            .await?;
        all_calls.extend(factory_calls);

        // 3. Once calls for newly discovered addresses
        let once_calls = self
            .collect_once_calls(block_number, block_timestamp, factory_addrs, decoder_tx)
            .await?;
        all_calls.extend(once_calls);

        // 4. Event-triggered calls
        let event_calls = self
            .collect_event_triggered_calls(block_number, block_timestamp, logs, decoder_tx)
            .await?;
        all_calls.extend(event_calls);

        if !all_calls.is_empty() {
            tracing::debug!(
                "Collected {} eth_calls for block {}",
                all_calls.len(),
                block_number
            );
        }

        Ok(all_calls)
    }

    /// Collect regular frequency-based calls.
    async fn collect_regular_calls(
        &mut self,
        block_number: u64,
        block_timestamp: u64,
        decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<Vec<LiveEthCall>, LiveError> {
        if self.call_configs.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));

        // Group by contract/function for decoder messages
        let mut grouped_configs: HashMap<(String, String), Vec<&CallConfig>> = HashMap::new();
        for config in &self.call_configs {
            // Skip if block is before contract's start_block
            if let Some(sb) = config.start_block {
                if block_number < sb {
                    continue;
                }
            }

            // Check frequency
            if !self.should_call_for_frequency(
                &config.contract_name,
                &config.function_name,
                block_timestamp,
                &config.frequency,
            ) {
                continue;
            }

            grouped_configs
                .entry((config.contract_name.clone(), config.function_name.clone()))
                .or_default()
                .push(config);
        }

        for ((contract_name, function_name), configs) in &grouped_configs {
            // Build batch of calls
            let calls: Vec<(TransactionRequest, BlockId)> = configs
                .iter()
                .map(|config| {
                    let tx = TransactionRequest::default()
                        .to(config.address)
                        .input(config.encoded_calldata.clone().into());
                    (tx, block_id)
                })
                .collect();

            if calls.is_empty() {
                continue;
            }

            // Execute batch
            let rpc_results = self.http_client.call_batch(calls).await?;

            let mut decoder_results = Vec::new();

            for (i, result) in rpc_results.into_iter().enumerate() {
                let config = configs[i];
                let result_bytes = match result {
                    Ok(bytes) => bytes.to_vec(),
                    Err(e) => {
                        tracing::warn!(
                            "eth_call failed for {}.{} at {} block {}: calldata=0x{}, error={}",
                            contract_name,
                            function_name,
                            config.address,
                            block_number,
                            hex::encode(&config.encoded_calldata),
                            e
                        );
                        continue; // Skip reverted calls
                    }
                };

                results.push(LiveEthCall {
                    block_number,
                    block_timestamp,
                    contract_name: contract_name.clone(),
                    contract_address: config.address.0 .0,
                    function_name: function_name.clone(),
                    result: result_bytes.clone(),
                });

                decoder_results.push(EthCallResult {
                    block_number,
                    block_timestamp,
                    contract_address: config.address.0 .0,
                    value: result_bytes,
                });
            }

            // Update frequency state
            let state_key = (contract_name.clone(), function_name.clone());
            self.frequency_state
                .last_call_times
                .insert(state_key, block_timestamp);

            // Send to decoder
            if let Some(tx) = decoder_tx {
                if !decoder_results.is_empty() {
                    let _ = tx
                        .send(DecoderMessage::EthCallsReady {
                            range_start: block_number,
                            range_end: block_number + 1,
                            contract_name: contract_name.clone(),
                            function_name: function_name.clone(),
                            results: decoder_results,
                            live_mode: true,
                        })
                        .await;
                }
            }
        }

        Ok(results)
    }

    /// Collect factory calls for known factory addresses.
    async fn collect_factory_calls(
        &self,
        block_number: u64,
        block_timestamp: u64,
        decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<Vec<LiveEthCall>, LiveError> {
        if self.factory_call_configs.is_empty() || self.factory_addresses.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));

        for (collection_name, call_configs) in &self.factory_call_configs {
            let Some(addresses) = self.factory_addresses.get(collection_name) else {
                continue;
            };

            for call_config in call_configs {
                // Skip "once" frequency calls - they're handled in collect_once_calls
                if call_config.frequency == Frequency::Once {
                    continue;
                }

                let function_name = &call_config.function;

                // Build calls for all known addresses
                let mut calls = Vec::new();
                let mut call_addresses = Vec::new();

                for address in addresses {
                    let encoded = encode_call_simple(&call_config.function, &call_config.params);
                    let tx = TransactionRequest::default()
                        .to(*address)
                        .input(encoded.into());
                    calls.push((tx, block_id));
                    call_addresses.push(*address);
                }

                if calls.is_empty() {
                    continue;
                }

                // Execute in batches
                let mut decoder_results = Vec::new();

                for chunk_start in (0..calls.len()).step_by(self.rpc_batch_size) {
                    let chunk_end = (chunk_start + self.rpc_batch_size).min(calls.len());
                    let chunk: Vec<_> = calls[chunk_start..chunk_end].to_vec();

                    let rpc_results = self.http_client.call_batch(chunk).await?;

                    for (i, result) in rpc_results.into_iter().enumerate() {
                        let address = call_addresses[chunk_start + i];
                        let result_bytes = match result {
                            Ok(bytes) => bytes.to_vec(),
                            Err(e) => {
                                let encoded = encode_call_simple(&call_config.function, &call_config.params);
                                tracing::warn!(
                                    "Factory eth_call failed for {}.{} at {} block {}: calldata=0x{}, error={}",
                                    collection_name,
                                    function_name,
                                    address,
                                    block_number,
                                    hex::encode(&encoded),
                                    e
                                );
                                continue; // Skip reverted calls
                            }
                        };

                        results.push(LiveEthCall {
                            block_number,
                            block_timestamp,
                            contract_name: collection_name.clone(),
                            contract_address: address.0 .0,
                            function_name: function_name.clone(),
                            result: result_bytes.clone(),
                        });

                        decoder_results.push(EthCallResult {
                            block_number,
                            block_timestamp,
                            contract_address: address.0 .0,
                            value: result_bytes,
                        });
                    }
                }

                // Send to decoder
                if let Some(tx) = decoder_tx {
                    if !decoder_results.is_empty() {
                        let _ = tx
                            .send(DecoderMessage::EthCallsReady {
                                range_start: block_number,
                                range_end: block_number + 1,
                                contract_name: collection_name.clone(),
                                function_name: function_name.clone(),
                                results: decoder_results,
                                live_mode: true,
                            })
                            .await;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Collect once calls for newly discovered addresses.
    async fn collect_once_calls(
        &mut self,
        block_number: u64,
        block_timestamp: u64,
        factory_addrs: &LiveFactoryAddresses,
        decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<Vec<LiveEthCall>, LiveError> {
        let mut results = Vec::new();
        let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));

        // Process factory once calls for newly discovered addresses
        for (collection_name, addresses) in &factory_addrs.addresses_by_collection {
            let Some(once_configs) = self.factory_once_configs.get(collection_name) else {
                continue;
            };

            // Filter to addresses we haven't called yet
            let new_addresses: Vec<[u8; 20]> = addresses
                .iter()
                .filter(|(_, addr)| !self.once_called_addresses.contains(addr))
                .map(|(_, addr)| *addr)
                .collect();

            if new_addresses.is_empty() {
                continue;
            }

            for address in &new_addresses {
                // Build calls for all once functions
                let mut calls = Vec::new();
                let mut function_names = Vec::new();
                let mut calldatas = Vec::new();
                let mut targets = Vec::new();

                for config in once_configs {
                    let calldata = if let Some(ref preencoded) = config.preencoded_calldata {
                        preencoded.clone()
                    } else {
                        // Need to encode with self-address param
                        encode_call_with_self_address(&config.function_selector, &config.params, address)
                    };

                    let target = if let Some(ref target_addrs) = config.target_addresses {
                        // Use configured target address
                        target_addrs.first().copied().unwrap_or(Address::from(*address))
                    } else {
                        Address::from(*address)
                    };

                    let tx = TransactionRequest::default()
                        .to(target)
                        .input(calldata.clone().into());
                    calls.push((tx, block_id));
                    function_names.push(config.function_name.clone());
                    calldatas.push(calldata);
                    targets.push(target);
                }

                if calls.is_empty() {
                    continue;
                }

                let rpc_results = self.http_client.call_batch(calls).await?;

                let mut function_results = HashMap::new();

                for (i, result) in rpc_results.into_iter().enumerate() {
                    let function_name = &function_names[i];
                    let result_bytes = match result {
                        Ok(bytes) => bytes.to_vec(),
                        Err(e) => {
                            tracing::warn!(
                                "Once call failed for {}.{} at {} block {}: calldata=0x{}, target={}, error={}",
                                collection_name,
                                function_name,
                                Address::from(*address),
                                block_number,
                                hex::encode(&calldatas[i]),
                                targets[i],
                                e
                            );
                            continue; // Skip reverted calls
                        }
                    };

                    results.push(LiveEthCall {
                        block_number,
                        block_timestamp,
                        contract_name: collection_name.clone(),
                        contract_address: *address,
                        function_name: function_name.clone(),
                        result: result_bytes.clone(),
                    });

                    function_results.insert(function_name.clone(), result_bytes);
                }

                // Mark address as called
                self.once_called_addresses.insert(*address);

                // Send to decoder as OnceCallsReady
                if let Some(tx) = decoder_tx {
                    if !function_results.is_empty() {
                        let _ = tx
                            .send(DecoderMessage::OnceCallsReady {
                                range_start: block_number,
                                range_end: block_number + 1,
                                contract_name: collection_name.clone(),
                                results: vec![OnceCallResult {
                                    block_number,
                                    block_timestamp,
                                    contract_address: *address,
                                    results: function_results,
                                }],
                                live_mode: true,
                            })
                            .await;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Collect event-triggered calls.
    async fn collect_event_triggered_calls(
        &self,
        block_number: u64,
        block_timestamp: u64,
        logs: &[LiveLog],
        decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<Vec<LiveEthCall>, LiveError> {
        if self.event_call_configs.is_empty() || logs.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));

        // Group calls by (contract_name, function_name) for decoder messages
        let mut grouped_calls: HashMap<
            (String, String),
            Vec<(Address, u32, Bytes, &EventTriggeredCallConfig)>,
        > = HashMap::new();

        for log in logs {
            if log.topics.is_empty() {
                continue;
            }

            let topic0 = log.topics[0];

            // Check each event trigger config
            for ((source_name, event_hash), configs) in &self.event_call_configs {
                if topic0 != *event_hash {
                    continue;
                }

                for config in configs {
                    // Skip if block is before contract's start_block
                    if let Some(sb) = config.start_block {
                        if block_number < sb {
                            continue;
                        }
                    }

                    // Determine target address
                    let target_address = if config.is_factory {
                        // For factory calls, check if emitter is a known factory address
                        let emitter = Address::from(log.address);
                        if let Some(addresses) = self.factory_addresses.get(source_name) {
                            if !addresses.contains(&emitter) {
                                continue;
                            }
                        }
                        emitter
                    } else if let Some(addr) = config.target_address {
                        addr
                    } else {
                        // Use event emitter
                        Address::from(log.address)
                    };

                    // Build calldata from params
                    let calldata = build_calldata_from_event(
                        &config.function_selector,
                        &config.params,
                        log,
                    );

                    let key = (config.contract_name.clone(), config.function_name.clone());
                    grouped_calls
                        .entry(key)
                        .or_default()
                        .push((target_address, log.log_index, calldata, config));
                }
            }
        }

        for ((contract_name, function_name), pending_calls) in &grouped_calls {
            // Build batch
            let calls: Vec<(TransactionRequest, BlockId)> = pending_calls
                .iter()
                .map(|(addr, _, calldata, _)| {
                    let tx = TransactionRequest::default()
                        .to(*addr)
                        .input(calldata.clone().into());
                    (tx, block_id)
                })
                .collect();

            if calls.is_empty() {
                continue;
            }

            // Execute in batches
            let mut decoder_results = Vec::new();

            for chunk_start in (0..calls.len()).step_by(self.rpc_batch_size) {
                let chunk_end = (chunk_start + self.rpc_batch_size).min(calls.len());
                let chunk: Vec<_> = calls[chunk_start..chunk_end].to_vec();

                let rpc_results = self.http_client.call_batch(chunk).await?;

                for (i, result) in rpc_results.into_iter().enumerate() {
                    let (target_address, log_index, calldata, _) = &pending_calls[chunk_start + i];
                    let result_bytes = match result {
                        Ok(bytes) => bytes.to_vec(),
                        Err(e) => {
                            tracing::warn!(
                                "Event-triggered call failed for {}.{} at {} block {}: calldata=0x{}, log_index={}, error={}",
                                contract_name,
                                function_name,
                                target_address,
                                block_number,
                                hex::encode(calldata),
                                log_index,
                                e
                            );
                            continue; // Skip reverted calls
                        }
                    };

                    results.push(LiveEthCall {
                        block_number,
                        block_timestamp,
                        contract_name: contract_name.clone(),
                        contract_address: target_address.0 .0,
                        function_name: function_name.clone(),
                        result: result_bytes.clone(),
                    });

                    decoder_results.push(EventCallResult {
                        block_number,
                        block_timestamp,
                        log_index: *log_index,
                        target_address: target_address.0 .0,
                        value: result_bytes,
                    });
                }
            }

            // Send to decoder
            if let Some(tx) = decoder_tx {
                if !decoder_results.is_empty() {
                    let _ = tx
                        .send(DecoderMessage::EventCallsReady {
                            range_start: block_number,
                            range_end: block_number + 1,
                            contract_name: contract_name.clone(),
                            function_name: function_name.clone(),
                            results: decoder_results,
                            live_mode: true,
                        })
                        .await;
                }
            }
        }

        Ok(results)
    }

    /// Check if a call should be made based on frequency.
    fn should_call_for_frequency(
        &self,
        contract_name: &str,
        function_name: &str,
        block_timestamp: u64,
        frequency: &Frequency,
    ) -> bool {
        match frequency {
            Frequency::EveryBlock => true,
            Frequency::Once => false, // Handled separately
            Frequency::EveryNBlocks(_) => true, // Simplified - always call in live mode
            Frequency::Duration(interval) => {
                let state_key = (contract_name.to_string(), function_name.to_string());
                match self.frequency_state.last_call_times.get(&state_key) {
                    Some(last_ts) => block_timestamp >= last_ts + interval,
                    None => true,
                }
            }
            Frequency::OnEvents(_) => false, // Handled separately in event-triggered calls
        }
    }
}

/// Simple call encoding for functions without parameters.
fn encode_call_simple(function_name: &str, params: &Vec<crate::types::config::eth_call::ParamConfig>) -> Bytes {
    use alloy::primitives::keccak256;

    // For now, assume simple calls without params - just use function selector
    // Full implementation would handle params
    let sig = if params.is_empty() {
        format!("{}()", function_name)
    } else {
        // Would need to format param types
        format!("{}()", function_name)
    };

    let hash = keccak256(sig.as_bytes());
    Bytes::copy_from_slice(&hash[..4])
}

/// Encode calldata with self-address parameter.
fn encode_call_with_self_address(
    selector: &[u8; 4],
    params: &[ParamConfig],
    address: &[u8; 20],
) -> Bytes {
    match encode_once_call_params(*selector, params, Address::from(*address)) {
        Ok(calldata) => calldata,
        Err(e) => {
            tracing::warn!("Failed to encode once call params: {}", e);
            Bytes::copy_from_slice(selector)
        }
    }
}

/// Build calldata from event parameters.
fn build_calldata_from_event(
    selector: &[u8; 4],
    params: &[ParamConfig],
    log: &LiveLog,
) -> Bytes {
    let trigger = EventTriggerData {
        block_number: 0,
        block_timestamp: 0,
        log_index: log.log_index,
        emitter_address: log.address,
        source_name: String::new(),
        event_signature: log.topics.first().copied().unwrap_or([0u8; 32]),
        topics: log.topics.clone(),
        data: log.data.clone(),
    };

    match build_event_call_params(&trigger, params) {
        Ok((dyn_vals, _)) => encode_call_with_params(*selector, &dyn_vals),
        Err(e) => {
            tracing::warn!("Failed to build event call params: {}", e);
            Bytes::copy_from_slice(selector)
        }
    }
}

impl std::fmt::Debug for LiveEthCallCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveEthCallCollector")
            .field("chain_name", &self.chain_name)
            .field("call_configs", &self.call_configs.len())
            .field("factory_addresses", &self.factory_addresses.len())
            .finish()
    }
}
