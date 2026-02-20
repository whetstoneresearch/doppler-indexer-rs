use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "bench")]
use std::time::Instant;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};
use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::raw_data::decoding::{DecoderMessage, EventCallResult as DecoderEventCallResult};
use crate::raw_data::historical::blocks::{get_existing_block_ranges, read_block_info_from_parquet};
use crate::raw_data::historical::factories::{get_factory_call_configs, FactoryAddressData, FactoryMessage};
use crate::raw_data::historical::receipts::{
    build_event_trigger_matchers, extract_event_triggers, EventTriggerData, EventTriggerMessage,
    LogData,
};
use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::eth_call::{encode_call_with_params, EthCallConfig, EvmType, Frequency, ParamConfig, ParamError, ParamValue};
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::config::tokens::{AddressOrPoolId, PoolType, Tokens};

#[derive(Debug, Error)]
pub enum EthCallCollectionError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parameter error: {0}")]
    Param(#[from] ParamError),

    #[error("Task join error: {0}")]
    JoinError(String),

    #[error("Event parameter extraction error: {0}")]
    EventParamExtraction(String),
}

#[derive(Debug, Clone)]
struct BlockRange {
    start: u64,
    end: u64,
}

impl BlockRange {
    fn file_name(&self) -> String {
        format!("{}-{}.parquet", self.start, self.end - 1)
    }
}

#[derive(Debug, Clone)]
struct BlockInfo {
    block_number: u64,
    timestamp: u64,
}

#[derive(Debug, Clone)]
struct CallConfig {
    contract_name: String,
    address: Address,
    function_name: String,
    encoded_calldata: Bytes,
    param_values: Vec<Vec<u8>>,
    frequency: Frequency,
}

#[derive(Debug, Clone)]
struct OnceCallConfig {
    function_name: String,
    function_selector: [u8; 4],
    /// Pre-encoded calldata if no self-address params
    preencoded_calldata: Option<Bytes>,
    /// Param configs needed when preencoded_calldata is None
    params: Vec<ParamConfig>,
    /// Optional target address override (resolved from CallTarget)
    target_addresses: Option<Vec<Address>>,
}

#[derive(Debug)]
struct OnceCallResult {
    block_number: u64,
    block_timestamp: u64,
    contract_address: [u8; 20],
    function_results: HashMap<String, Vec<u8>>,
}

struct FrequencyState {
    last_call_times: HashMap<(String, String), u64>,
}

/// Configuration for an event-triggered call
#[derive(Debug, Clone)]
struct EventTriggeredCallConfig {
    /// Contract name or factory collection name
    contract_name: String,
    /// Target address for the call (None for factory collections - use event emitter)
    target_address: Option<Address>,
    /// Function name (without params)
    function_name: String,
    /// Function selector (first 4 bytes of keccak256 of signature)
    function_selector: [u8; 4],
    /// Parameter configurations
    params: Vec<ParamConfig>,
    /// Whether this is for a factory collection
    is_factory: bool,
}

/// Result from an event-triggered call
#[derive(Debug)]
struct EventCallResult {
    block_number: u64,
    block_timestamp: u64,
    log_index: u32,
    target_address: [u8; 20],
    value_bytes: Vec<u8>,
    param_values: Vec<Vec<u8>>,
}

/// Key for grouping event-triggered calls: (source_name, event_signature_hash)
type EventCallKey = (String, [u8; 32]);

#[derive(Debug)]
struct CallResult {
    block_number: u64,
    block_timestamp: u64,
    contract_address: [u8; 20],
    value_bytes: Vec<u8>,
    param_values: Vec<Vec<u8>>,
}

pub async fn collect_eth_calls(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    mut block_rx: Receiver<(u64, u64)>,
    mut factory_rx: Option<Receiver<FactoryMessage>>,
    mut event_trigger_rx: Option<Receiver<EventTriggerMessage>>,
    decoder_tx: Option<Sender<DecoderMessage>>,
) -> Result<(), EthCallCollectionError> {
    let base_output_dir = PathBuf::from(format!("data/raw/{}/eth_calls", chain.name));
    std::fs::create_dir_all(&base_output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;

    let call_configs = build_call_configs(&chain.contracts)?;
    let factory_call_configs = get_factory_call_configs(&chain.contracts, &chain.factory_collections);
    let event_call_configs = build_event_triggered_call_configs(&chain.contracts);
    let token_call_configs = build_token_call_configs(&chain.tokens, &chain.contracts)?;

    let multicall3_address: Option<Address> = chain.contracts.get("Multicall3").and_then(|c| {
        match &c.address {
            AddressOrAddresses::Single(addr) => Some(*addr),
            AddressOrAddresses::Multiple(addrs) => addrs.first().copied(),
        }
    });

    if multicall3_address.is_some() {
        tracing::info!(
            "Multicall3 found for chain {}, will batch all eth_calls",
            chain.name
        );
    }

    let once_configs = build_once_call_configs(&chain.contracts);
    let factory_once_configs = build_factory_once_call_configs(&factory_call_configs, &chain.contracts);

    let has_regular_calls = !call_configs.is_empty();
    let has_once_calls = !once_configs.is_empty();
    let has_factory_calls = !factory_call_configs.is_empty() && factory_rx.is_some();
    let has_factory_once_calls = !factory_once_configs.is_empty() && factory_rx.is_some();
    let has_event_triggered_calls = !event_call_configs.is_empty() && event_trigger_rx.is_some();
    let has_token_calls = !token_call_configs.is_empty();

    if !has_regular_calls && !has_once_calls && !has_factory_calls && !has_factory_once_calls && !has_event_triggered_calls && !has_token_calls {
        tracing::info!("No eth_calls configured for chain {}", chain.name);
        while block_rx.recv().await.is_some() {}
        return Ok(());
    }

    // Track known factory addresses for filtering event triggers
    let mut factory_addresses: HashMap<String, HashSet<Address>> = HashMap::new();

    let max_params = call_configs
        .iter()
        .map(|c| c.param_values.len())
        .max()
        .unwrap_or(0);

    let factory_max_params = factory_call_configs
        .values()
        .flat_map(|configs| configs.iter().map(|c| c.params.len()))
        .max()
        .unwrap_or(0);

    let mut frequency_state = FrequencyState {
        last_call_times: HashMap::new(),
    };

    tracing::info!(
        "Starting eth_call collection for chain {} with {} regular configs, {} once configs, {} factory collections, {} factory once configs, {} event trigger configs, {} token pool configs",
        chain.name,
        call_configs.len(),
        once_configs.len(),
        factory_call_configs.len(),
        factory_once_configs.len(),
        event_call_configs.len(),
        token_call_configs.len()
    );

    let existing_files = scan_existing_parquet_files(&base_output_dir);

    let mut range_data: HashMap<u64, Vec<BlockInfo>> = HashMap::new();
    let mut range_factory_data: HashMap<u64, FactoryAddressData> = HashMap::new();
    let mut range_regular_done: HashSet<u64> = HashSet::new();
    let mut range_factory_done: HashSet<u64> = HashSet::new();

    if has_regular_calls || has_token_calls || has_once_calls {
        let block_ranges = get_existing_block_ranges(&chain.name);
        tracing::info!(
            "eth_calls catchup: checking {} block ranges (regular={}, token={}, once={})",
            block_ranges.len(),
            has_regular_calls,
            has_token_calls,
            has_once_calls
        );

        // Pre-load or build column indexes for all once directories
        let mut once_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for contract_name in once_configs.keys() {
            let once_dir = base_output_dir.join(contract_name).join("once");
            let index = load_or_build_once_column_index(&once_dir);
            once_column_indexes.insert(contract_name.clone(), index);
        }

        let mut catchup_count = 0;

        for block_range in &block_ranges {
            let range = BlockRange {
                start: block_range.start,
                end: block_range.end,
            };

            // Check if all regular call files exist for this range
            let regular_calls_done = !has_regular_calls || call_configs.iter().all(|config| {
                let rel_path = format!("{}/{}/{}", config.contract_name, config.function_name, range.file_name());
                existing_files.contains(&rel_path)
            });

            // Check if all token pool call files exist for this range
            let token_calls_done = !has_token_calls || token_call_configs.iter().all(|config| {
                let rel_path = format!("{}_pool/{}/{}", config.token_name, config.function_name, range.file_name());
                existing_files.contains(&rel_path)
            });

            // Check if all once call files exist AND have all expected columns for this range
            let once_calls_done = !has_once_calls || once_configs.iter().all(|(contract_name, configs)| {
                let rel_path = format!("{}/once/{}", contract_name, range.file_name());
                let expected: HashSet<&str> = configs
                    .iter()
                    .map(|c| c.function_name.as_str())
                    .collect();

                if !existing_files.contains(&rel_path) {
                    tracing::info!(
                        "Once file missing for {} range {}-{}, will collect {} functions",
                        contract_name,
                        range.start,
                        range.end - 1,
                        expected.len()
                    );
                    return false;
                }

                // Use pre-loaded index (which was built from parquet schemas if index file didn't exist)
                let index = once_column_indexes.get(contract_name).unwrap();
                match index.get(&range.file_name()) {
                    Some(cols) => {
                        let missing: Vec<_> = expected.iter().filter(|f| !cols.contains(&f.to_string())).collect();
                        if !missing.is_empty() {
                            tracing::info!(
                                "Once file {} for {} exists but missing columns: {:?} (has: {:?})",
                                range.file_name(),
                                contract_name,
                                missing,
                                cols
                            );
                            false
                        } else {
                            tracing::debug!(
                                "Once file {} for {} complete with all {} columns",
                                range.file_name(),
                                contract_name,
                                cols.len()
                            );
                            true
                        }
                    }
                    None => {
                        // File exists but wasn't found by index builder - shouldn't happen but handle it
                        tracing::warn!(
                            "Once file {} for {} exists but not in pre-built index, will collect",
                            range.file_name(),
                            contract_name
                        );
                        false
                    }
                }
            });

            // Skip this range only if ALL call types have their files
            if regular_calls_done && token_calls_done && once_calls_done {
                range_regular_done.insert(range.start);
                continue;
            }

            let block_infos = match read_block_info_from_parquet(&block_range.file_path) {
                Ok(infos) => infos,
                Err(e) => {
                    tracing::warn!(
                        "Failed to read block info from {}: {}",
                        block_range.file_path.display(),
                        e
                    );
                    continue;
                }
            };

            if block_infos.is_empty() {
                continue;
            }

            tracing::info!(
                "Catchup: processing eth_calls for blocks {}-{} from existing block file",
                range.start,
                range.end - 1
            );

            let blocks: Vec<BlockInfo> = block_infos
                .into_iter()
                .map(|info| BlockInfo {
                    block_number: info.block_number,
                    timestamp: info.timestamp,
                })
                .collect();

            range_data.insert(range.start, blocks.clone());

            if has_regular_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_range_multicall(
                        &range,
                        blocks.clone(),
                        client,
                        &call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        max_params,
                        &mut frequency_state,
                        multicall_addr,
                    )
                    .await?;
                } else {
                    process_range(
                        &range,
                        blocks.clone(),
                        client,
                        &call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        max_params,
                        &mut frequency_state,
                    )
                    .await?;
                }
            }

            if has_token_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_token_range_multicall(
                        &range,
                        blocks.clone(),
                        client,
                        &token_call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        &mut frequency_state,
                        multicall_addr,
                    )
                    .await?;
                } else {
                    process_token_range(
                        &range,
                        blocks.clone(),
                        client,
                        &token_call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        &mut frequency_state,
                    )
                    .await?;
                }
            }

            if has_once_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_once_calls_multicall(
                        &range,
                        &blocks,
                        client,
                        &once_configs,
                        &chain.contracts,
                        &base_output_dir,
                        &existing_files,
                        multicall_addr,
                        rpc_batch_size,
                    )
                    .await?;
                } else {
                    process_once_calls_regular(
                        &range,
                        &blocks,
                        client,
                        &once_configs,
                        &chain.contracts,
                        &base_output_dir,
                        &existing_files,
                    )
                    .await?;
                }
            }

            range_regular_done.insert(range.start);
            catchup_count += 1;
        }

        if catchup_count > 0 {
            tracing::info!(
                "Catchup complete: processed {} eth_call ranges for chain {}",
                catchup_count,
                chain.name
            );
        }
    }

    // =========================================================================
    // Catchup phase for factory once calls: Check for missing columns in existing files
    // =========================================================================
    if has_factory_once_calls {
        tracing::info!(
            "Factory once calls catchup: checking {} factory collections for missing columns",
            factory_once_configs.len()
        );

        // Pre-load or build column indexes for all factory once directories
        let mut factory_once_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for collection_name in factory_once_configs.keys() {
            let once_dir = base_output_dir.join(collection_name).join("once");
            let index = load_or_build_once_column_index(&once_dir);
            factory_once_column_indexes.insert(collection_name.clone(), index);
        }

        // Load factory address data from existing parquet files
        let factory_catchup_data = load_factory_addresses_for_once_catchup(&chain.name, &factory_once_configs);

        if !factory_catchup_data.is_empty() {
            let block_ranges = get_existing_block_ranges(&chain.name);
            let mut factory_once_catchup_count = 0;

            for block_range in &block_ranges {
                let range = BlockRange {
                    start: block_range.start,
                    end: block_range.end,
                };

                // Build FactoryAddressData for this range from loaded data
                let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> = HashMap::new();
                for (collection_name, addr_data) in &factory_catchup_data {
                    for (addr, block_num, timestamp) in addr_data {
                        if *block_num >= range.start && *block_num < range.end {
                            addresses_by_block
                                .entry(*block_num)
                                .or_default()
                                .push((*timestamp, *addr, collection_name.clone()));
                        }
                    }
                }

                if addresses_by_block.is_empty() {
                    continue;
                }

                let factory_data = FactoryAddressData {
                    range_start: range.start,
                    range_end: range.end,
                    addresses_by_block,
                };

                process_factory_once_calls(
                    &range,
                    client,
                    &factory_data,
                    &factory_once_configs,
                    &base_output_dir,
                    &existing_files,
                    &factory_once_column_indexes,
                    &decoder_tx,
                )
                .await?;

                factory_once_catchup_count += 1;
            }

            if factory_once_catchup_count > 0 {
                tracing::info!(
                    "Factory once calls catchup complete: checked {} ranges for chain {}",
                    factory_once_catchup_count,
                    chain.name
                );
            }
        }
    }

    // =========================================================================
    // Catchup phase for event-triggered calls: Read from existing log parquet files
    // =========================================================================
    if has_event_triggered_calls {
        // CRITICAL: Load historical factory addresses BEFORE processing event triggers
        // This ensures we can properly filter events from factory-created contracts
        let historical_factory_addrs = load_historical_factory_addresses(&chain.name, &event_call_configs);
        for (collection_name, addrs) in historical_factory_addrs {
            tracing::info!(
                "Loaded {} historical factory addresses for collection {}",
                addrs.len(),
                collection_name
            );
            factory_addresses
                .entry(collection_name)
                .or_default()
                .extend(addrs);
        }

        let log_ranges = get_existing_log_ranges(&chain.name);
        let event_matchers = build_event_trigger_matchers(&chain.contracts);
        let mut event_catchup_count = 0;

        tracing::info!(
            "Event-triggered calls catchup: checking {} log ranges for chain {}",
            log_ranges.len(),
            chain.name
        );

        for log_range in &log_ranges {
            // Check if output already exists for all event-triggered call configs
            let mut needs_processing = false;
            for (_, configs) in &event_call_configs {
                for config in configs {
                    if !event_output_exists(
                        &base_output_dir,
                        &config.contract_name,
                        &config.function_name,
                        log_range.start,
                        log_range.end,
                    ) {
                        needs_processing = true;
                        break;
                    }
                }
                if needs_processing {
                    break;
                }
            }

            if !needs_processing {
                tracing::debug!(
                    "Skipping event-triggered calls for blocks {}-{} (already exists)",
                    log_range.start,
                    log_range.end - 1
                );
                continue;
            }

            // Read logs from parquet file
            let logs = match read_logs_from_parquet(&log_range.file_path) {
                Ok(logs) => logs,
                Err(e) => {
                    tracing::warn!(
                        "Failed to read logs from {}: {}",
                        log_range.file_path.display(),
                        e
                    );
                    continue;
                }
            };

            if logs.is_empty() {
                continue;
            }

            tracing::info!(
                "Catchup: processing event-triggered calls for blocks {}-{} ({} logs)",
                log_range.start,
                log_range.end - 1,
                logs.len()
            );

            // Extract event triggers from logs
            let triggers = extract_event_triggers(&logs, &event_matchers);

            if !triggers.is_empty() {
                tracing::info!(
                    "Extracted {} event triggers from {} logs for blocks {}-{}",
                    triggers.len(),
                    logs.len(),
                    log_range.start,
                    log_range.end - 1
                );

                // Process the triggers
                if let Some(multicall_addr) = multicall3_address {
                    process_event_triggers_multicall(
                        triggers,
                        &event_call_configs,
                        &factory_addresses,
                        client,
                        &base_output_dir,
                        rpc_batch_size,
                        &decoder_tx,
                        multicall_addr,
                    )
                    .await?;
                } else {
                    process_event_triggers(
                        triggers,
                        &event_call_configs,
                        &factory_addresses,
                        client,
                        &base_output_dir,
                        rpc_batch_size,
                        &decoder_tx,
                    )
                    .await?;
                }

                event_catchup_count += 1;
            }
        }

        if event_catchup_count > 0 {
            tracing::info!(
                "Event-triggered calls catchup complete: processed {} ranges for chain {}",
                event_catchup_count,
                chain.name
            );
        }
    }

    let mut block_rx_closed = false;
    let mut event_trigger_rx_closed = !has_event_triggered_calls;

    loop {
        if block_rx_closed {
            // Check if we should still wait for factory or event trigger data
            let waiting_for_factory = has_factory_calls && factory_rx.is_some();
            let waiting_for_events = has_event_triggered_calls && !event_trigger_rx_closed;

            if !waiting_for_factory && !waiting_for_events {
                break;
            }

            // Process remaining factory and event data
            tokio::select! {
                factory_result = async {
                    match &mut factory_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match factory_result {
                        Some(FactoryMessage::IncrementalAddresses(factory_data)) => {
                            let range_start = factory_data.range_start;

                            // Update factory addresses for event trigger filtering
                            for (_block, addrs) in &factory_data.addresses_by_block {
                                for (_, addr, collection_name) in addrs {
                                    factory_addresses
                                        .entry(collection_name.clone())
                                        .or_default()
                                        .insert(*addr);
                                }
                            }

                            // Merge into existing range_factory_data
                            let existing = range_factory_data.entry(range_start).or_insert_with(|| {
                                FactoryAddressData {
                                    range_start: factory_data.range_start,
                                    range_end: factory_data.range_end,
                                    addresses_by_block: HashMap::new(),
                                }
                            });
                            for (block, addrs) in factory_data.addresses_by_block {
                                existing.addresses_by_block.entry(block).or_default().extend(addrs);
                            }
                        }
                        Some(FactoryMessage::RangeComplete { range_start, range_end }) => {
                            if range_regular_done.contains(&range_start) {
                                if !range_factory_done.contains(&range_start) {
                                    let range = BlockRange {
                                        start: range_start,
                                        end: range_end,
                                    };

                                    if let (Some(blocks), Some(factory_data)) = (range_data.get(&range_start), range_factory_data.get(&range_start)) {
                                        if let Some(multicall_addr) = multicall3_address {
                                            process_factory_range_multicall(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &factory_call_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                rpc_batch_size,
                                                factory_max_params,
                                                &mut frequency_state,
                                                multicall_addr,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_range(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &factory_call_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                rpc_batch_size,
                                                factory_max_params,
                                                &mut frequency_state,
                                            )
                                            .await?;
                                        }

                                        if has_factory_once_calls {
                                            let empty_index = HashMap::new();
                                            if let Some(multicall_addr) = multicall3_address {
                                                process_factory_once_calls_multicall(
                                                    &range,
                                                    client,
                                                    factory_data,
                                                    &factory_once_configs,
                                                    &base_output_dir,
                                                    &existing_files,
                                                    &empty_index,
                                                    multicall_addr,
                                                    rpc_batch_size,
                                                )
                                                .await?;
                                            } else {
                                                process_factory_once_calls(
                                                    &range,
                                                    client,
                                                    factory_data,
                                                    &factory_once_configs,
                                                    &base_output_dir,
                                                    &existing_files,
                                                    &empty_index,
                                                    &decoder_tx,
                                                )
                                                .await?;
                                            }
                                        }
                                    }
                                    range_factory_done.insert(range_start);
                                }

                                if range_regular_done.contains(&range_start)
                                    && range_factory_done.contains(&range_start)
                                {
                                    range_data.remove(&range_start);
                                    range_factory_data.remove(&range_start);
                                }
                            }
                        }
                        Some(FactoryMessage::AllComplete) | None => {
                            tracing::debug!("eth_calls: factory channel closed");
                            factory_rx = None;
                        }
                    }
                }

                event_result = async {
                    match &mut event_trigger_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match event_result {
                        Some(EventTriggerMessage::Triggers(triggers)) => {
                            if let Some(multicall_addr) = multicall3_address {
                                process_event_triggers_multicall(
                                    triggers,
                                    &event_call_configs,
                                    &factory_addresses,
                                    client,
                                    &base_output_dir,
                                    rpc_batch_size,
                                    &decoder_tx,
                                    multicall_addr,
                                )
                                .await?;
                            } else {
                                process_event_triggers(
                                    triggers,
                                    &event_call_configs,
                                    &factory_addresses,
                                    client,
                                    &base_output_dir,
                                    rpc_batch_size,
                                    &decoder_tx,
                                )
                                .await?;
                            }
                        }
                        Some(EventTriggerMessage::RangeComplete { .. }) => {
                            // Range complete - no action needed
                        }
                        Some(EventTriggerMessage::AllComplete) | None => {
                            tracing::debug!("eth_calls: event trigger channel closed");
                            event_trigger_rx_closed = true;
                        }
                    }
                }
            }
            continue;
        }

        tokio::select! {
            block_result = block_rx.recv() => {
                match block_result {
                    Some((block_number, timestamp)) => {
                        let range_start = (block_number / range_size) * range_size;

                        range_data.entry(range_start).or_default().push(BlockInfo {
                            block_number,
                            timestamp,
                        });

                        if let Some(blocks) = range_data.get(&range_start) {
                            let expected_blocks: HashSet<u64> =
                                (range_start..range_start + range_size).collect();
                            let received_blocks: HashSet<u64> =
                                blocks.iter().map(|b| b.block_number).collect();

                            if expected_blocks.is_subset(&received_blocks)
                                && !range_regular_done.contains(&range_start)
                            {
                                let range = BlockRange {
                                    start: range_start,
                                    end: range_start + range_size,
                                };

                                if has_regular_calls {
                                    if let Some(multicall_addr) = multicall3_address {
                                        process_range_multicall(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &call_configs,
                                            &base_output_dir,
                                            &existing_files,
                                            rpc_batch_size,
                                            max_params,
                                            &mut frequency_state,
                                            multicall_addr,
                                        )
                                        .await?;
                                    } else {
                                        process_range(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &call_configs,
                                            &base_output_dir,
                                            &existing_files,
                                            rpc_batch_size,
                                            max_params,
                                            &mut frequency_state,
                                        )
                                        .await?;
                                    }
                                }

                                if has_token_calls {
                                    if let Some(multicall_addr) = multicall3_address {
                                        process_token_range_multicall(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &token_call_configs,
                                            &base_output_dir,
                                            &existing_files,
                                            rpc_batch_size,
                                            &mut frequency_state,
                                            multicall_addr,
                                        )
                                        .await?;
                                    } else {
                                        process_token_range(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &token_call_configs,
                                            &base_output_dir,
                                            &existing_files,
                                            rpc_batch_size,
                                            &mut frequency_state,
                                        )
                                        .await?;
                                    }
                                }

                                if has_once_calls {
                                    if let Some(multicall_addr) = multicall3_address {
                                        process_once_calls_multicall(
                                            &range,
                                            blocks,
                                            client,
                                            &once_configs,
                                            &chain.contracts,
                                            &base_output_dir,
                                            &existing_files,
                                            multicall_addr,
                                            rpc_batch_size,
                                        )
                                        .await?;
                                    } else {
                                        process_once_calls_regular(
                                            &range,
                                            blocks,
                                            client,
                                            &once_configs,
                                            &chain.contracts,
                                            &base_output_dir,
                                            &existing_files,
                                        )
                                        .await?;
                                    }
                                }
                                range_regular_done.insert(range_start);

                                if let Some(factory_data) = range_factory_data.get(&range_start) {
                                    if has_factory_calls && !range_factory_done.contains(&range_start) {
                                        if let Some(multicall_addr) = multicall3_address {
                                            process_factory_range_multicall(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &factory_call_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                rpc_batch_size,
                                                factory_max_params,
                                                &mut frequency_state,
                                                multicall_addr,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_range(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &factory_call_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                rpc_batch_size,
                                                factory_max_params,
                                                &mut frequency_state,
                                            )
                                            .await?;
                                        }
                                    }

                                    if has_factory_once_calls {
                                        let empty_index = HashMap::new();
                                        if let Some(multicall_addr) = multicall3_address {
                                            process_factory_once_calls_multicall(
                                                &range,
                                                client,
                                                factory_data,
                                                &factory_once_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                &empty_index,
                                                multicall_addr,
                                                rpc_batch_size,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_once_calls(
                                                &range,
                                                client,
                                                factory_data,
                                                &factory_once_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                &empty_index,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        }
                                    }
                                    range_factory_done.insert(range_start);
                                }

                                if range_regular_done.contains(&range_start)
                                    && (!has_factory_calls || range_factory_done.contains(&range_start))
                                {
                                    range_data.remove(&range_start);
                                    range_factory_data.remove(&range_start);
                                }
                            }
                        }
                    }
                    None => {
                        block_rx_closed = true;
                    }
                }
            }

            factory_result = async {
                match &mut factory_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match factory_result {
                    Some(FactoryMessage::IncrementalAddresses(factory_data)) => {
                        let range_start = factory_data.range_start;

                        // Update factory addresses for event trigger filtering
                        for (_block, addrs) in &factory_data.addresses_by_block {
                            for (_, addr, collection_name) in addrs {
                                factory_addresses
                                    .entry(collection_name.clone())
                                    .or_default()
                                    .insert(*addr);
                            }
                        }

                        // Merge into existing range_factory_data
                        let existing = range_factory_data.entry(range_start).or_insert_with(|| {
                            FactoryAddressData {
                                range_start: factory_data.range_start,
                                range_end: factory_data.range_end,
                                addresses_by_block: HashMap::new(),
                            }
                        });
                        for (block, addrs) in factory_data.addresses_by_block {
                            existing.addresses_by_block.entry(block).or_default().extend(addrs);
                        }
                    }
                    Some(FactoryMessage::RangeComplete { range_start, range_end }) => {
                        if range_regular_done.contains(&range_start) {
                            if has_factory_calls && !range_factory_done.contains(&range_start) {
                                let range = BlockRange {
                                    start: range_start,
                                    end: range_end,
                                };

                                if let (Some(blocks), Some(factory_data)) = (range_data.get(&range_start), range_factory_data.get(&range_start)) {
                                    if let Some(multicall_addr) = multicall3_address {
                                        process_factory_range_multicall(
                                            &range,
                                            blocks,
                                            client,
                                            factory_data,
                                            &factory_call_configs,
                                            &base_output_dir,
                                            &existing_files,
                                            rpc_batch_size,
                                            factory_max_params,
                                            &mut frequency_state,
                                            multicall_addr,
                                        )
                                        .await?;
                                    } else {
                                        process_factory_range(
                                            &range,
                                            blocks,
                                            client,
                                            factory_data,
                                            &factory_call_configs,
                                            &base_output_dir,
                                            &existing_files,
                                            rpc_batch_size,
                                            factory_max_params,
                                            &mut frequency_state,
                                        )
                                        .await?;
                                    }

                                    if has_factory_once_calls {
                                        let empty_index = HashMap::new();
                                        if let Some(multicall_addr) = multicall3_address {
                                            process_factory_once_calls_multicall(
                                                &range,
                                                client,
                                                factory_data,
                                                &factory_once_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                &empty_index,
                                                multicall_addr,
                                                rpc_batch_size,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_once_calls(
                                                &range,
                                                client,
                                                factory_data,
                                                &factory_once_configs,
                                                &base_output_dir,
                                                &existing_files,
                                                &empty_index,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        }
                                    }
                                }
                                range_factory_done.insert(range_start);
                            }

                            if range_regular_done.contains(&range_start)
                                && (!has_factory_calls || range_factory_done.contains(&range_start))
                            {
                                range_data.remove(&range_start);
                                range_factory_data.remove(&range_start);
                            }
                        }
                    }
                    Some(FactoryMessage::AllComplete) | None => {
                        factory_rx = None;
                    }
                }
            }

            event_result = async {
                match &mut event_trigger_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match event_result {
                    Some(EventTriggerMessage::Triggers(triggers)) => {
                        if let Some(multicall_addr) = multicall3_address {
                            process_event_triggers_multicall(
                                triggers,
                                &event_call_configs,
                                &factory_addresses,
                                client,
                                &base_output_dir,
                                rpc_batch_size,
                                &decoder_tx,
                                multicall_addr,
                            )
                            .await?;
                        } else {
                            process_event_triggers(
                                triggers,
                                &event_call_configs,
                                &factory_addresses,
                                client,
                                &base_output_dir,
                                rpc_batch_size,
                                &decoder_tx,
                            )
                            .await?;
                        }
                    }
                    Some(EventTriggerMessage::RangeComplete { .. }) => {
                        // Range complete - no action needed for event triggers
                    }
                    Some(EventTriggerMessage::AllComplete) | None => {
                        tracing::debug!("eth_calls: event trigger channel closed");
                        event_trigger_rx_closed = true;
                    }
                }
            }
        }
    }

    for (range_start, blocks) in range_data {
        if blocks.is_empty() {
            continue;
        }

        let max_block = blocks
            .iter()
            .map(|b| b.block_number)
            .max()
            .unwrap_or(range_start);
        let range = BlockRange {
            start: range_start,
            end: max_block + 1,
        };

        if has_regular_calls && !range_regular_done.contains(&range_start) {
            if let Some(multicall_addr) = multicall3_address {
                process_range_multicall(
                    &range,
                    blocks.clone(),
                    client,
                    &call_configs,
                    &base_output_dir,
                    &existing_files,
                    rpc_batch_size,
                    max_params,
                    &mut frequency_state,
                    multicall_addr,
                )
                .await?;
            } else {
                process_range(
                    &range,
                    blocks.clone(),
                    client,
                    &call_configs,
                    &base_output_dir,
                    &existing_files,
                    rpc_batch_size,
                    max_params,
                    &mut frequency_state,
                )
                .await?;
            }
        }

        if has_token_calls && !range_regular_done.contains(&range_start) {
            if let Some(multicall_addr) = multicall3_address {
                process_token_range_multicall(
                    &range,
                    blocks.clone(),
                    client,
                    &token_call_configs,
                    &base_output_dir,
                    &existing_files,
                    rpc_batch_size,
                    &mut frequency_state,
                    multicall_addr,
                )
                .await?;
            } else {
                process_token_range(
                    &range,
                    blocks.clone(),
                    client,
                    &token_call_configs,
                    &base_output_dir,
                    &existing_files,
                    rpc_batch_size,
                    &mut frequency_state,
                )
                .await?;
            }
        }

        if has_once_calls && !range_regular_done.contains(&range_start) {
            if let Some(multicall_addr) = multicall3_address {
                process_once_calls_multicall(
                    &range,
                    &blocks,
                    client,
                    &once_configs,
                    &chain.contracts,
                    &base_output_dir,
                    &existing_files,
                    multicall_addr,
                    rpc_batch_size,
                )
                .await?;
            } else {
                process_once_calls_regular(
                    &range,
                    &blocks,
                    client,
                    &once_configs,
                    &chain.contracts,
                    &base_output_dir,
                    &existing_files,
                )
                .await?;
            }
        }

        if has_factory_calls {
            if let Some(factory_data) = range_factory_data.get(&range_start) {
                if !range_factory_done.contains(&range_start) {
                    if let Some(multicall_addr) = multicall3_address {
                        process_factory_range_multicall(
                            &range,
                            &blocks,
                            client,
                            factory_data,
                            &factory_call_configs,
                            &base_output_dir,
                            &existing_files,
                            rpc_batch_size,
                            factory_max_params,
                            &mut frequency_state,
                            multicall_addr,
                        )
                        .await?;
                    } else {
                        process_factory_range(
                            &range,
                            &blocks,
                            client,
                            factory_data,
                            &factory_call_configs,
                            &base_output_dir,
                            &existing_files,
                            rpc_batch_size,
                            factory_max_params,
                            &mut frequency_state,
                        )
                        .await?;
                    }

                    if has_factory_once_calls {
                        let empty_index = HashMap::new();
                        if let Some(multicall_addr) = multicall3_address {
                            process_factory_once_calls_multicall(
                                &range,
                                client,
                                factory_data,
                                &factory_once_configs,
                                &base_output_dir,
                                &existing_files,
                                &empty_index,
                                multicall_addr,
                                rpc_batch_size,
                            )
                            .await?;
                        } else {
                            process_factory_once_calls(
                                &range,
                                client,
                                factory_data,
                                &factory_once_configs,
                                &base_output_dir,
                                &existing_files,
                                &empty_index,
                                &decoder_tx,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }

    // Signal decoder that all ranges are complete
    if let Some(tx) = decoder_tx {
        let _ = tx.send(DecoderMessage::AllComplete).await;
    }

    tracing::info!("Eth_call collection complete for chain {}", chain.name);
    Ok(())
}

// NOTE: build_factory_call_configs was removed - use get_factory_call_configs from factories.rs instead

/// Build event-triggered call configs from contracts configuration
/// Returns a map from (source_name, event_signature_hash) -> Vec<EventTriggeredCallConfig>
fn build_event_triggered_call_configs(
    contracts: &Contracts,
) -> HashMap<EventCallKey, Vec<EventTriggeredCallConfig>> {
    let mut configs: HashMap<EventCallKey, Vec<EventTriggeredCallConfig>> = HashMap::new();

    for (contract_name, contract) in contracts {
        // Get contract addresses
        let addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };

        // Check contract-level calls for on_events frequency
        if let Some(calls) = &contract.calls {
            for call in calls {
                // Iterate over all event trigger configs (supports single or multiple events)
                for trigger_config in call.frequency.event_configs() {
                    let event_hash = compute_event_signature_hash(&trigger_config.event);
                    let key = (trigger_config.source.clone(), event_hash);

                    let selector = compute_function_selector(&call.function);
                    let function_name = call.function
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
                        configs.entry(key.clone()).or_default().push(EventTriggeredCallConfig {
                            contract_name: contract_name.clone(),
                            target_address: Some(*address),
                            function_name: function_name.clone(),
                            function_selector: selector,
                            params: call.params.clone(),
                            is_factory: false,
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
                            let function_name = call.function
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

                            configs.entry(key.clone()).or_default().push(EventTriggeredCallConfig {
                                contract_name: factory.collection.clone(),
                                target_address,
                                function_name: function_name.clone(),
                                function_selector: selector,
                                params: call.params.clone(),
                                is_factory: true,
                            });
                        }
                    }
                }
            }
        }
    }

    configs
}

/// Compute the keccak256 hash of an event signature
fn compute_event_signature_hash(signature: &str) -> [u8; 32] {
    use alloy::primitives::keccak256;
    keccak256(signature.as_bytes()).0
}

/// Extract a parameter value from event data (topics or data fields)
fn extract_param_from_event(
    trigger: &EventTriggerData,
    from_event: &str,
    param_type: &EvmType,
) -> Result<(DynSolValue, Vec<u8>), EthCallCollectionError> {
    let from_event = from_event.trim();

    // Handle "address" - extract the event emitter address
    if from_event == "address" {
        // Validate that the expected type is Address
        if !matches!(param_type, EvmType::Address) {
            return Err(EthCallCollectionError::EventParamExtraction(format!(
                "from_event: \"address\" requires param type to be address, got {:?}",
                param_type
            )));
        }
        let addr = Address::from(trigger.emitter_address);
        let encoded = DynSolValue::Address(addr).abi_encode();
        return Ok((DynSolValue::Address(addr), encoded));
    }

    if let Some(rest) = from_event.strip_prefix("topics[") {
        // Extract topic index
        let idx_str = rest.strip_suffix(']').ok_or_else(|| {
            EthCallCollectionError::EventParamExtraction(format!(
                "Invalid from_event format: {}",
                from_event
            ))
        })?;
        let idx: usize = idx_str.parse().map_err(|_| {
            EthCallCollectionError::EventParamExtraction(format!(
                "Invalid topic index: {}",
                idx_str
            ))
        })?;

        if idx >= trigger.topics.len() {
            return Err(EthCallCollectionError::EventParamExtraction(format!(
                "Topic index {} out of bounds (event has {} topics)",
                idx,
                trigger.topics.len()
            )));
        }

        let topic_bytes = &trigger.topics[idx];
        extract_value_from_32_bytes(topic_bytes, param_type)
    } else if let Some(rest) = from_event.strip_prefix("data[") {
        // Extract data word index
        let idx_str = rest.strip_suffix(']').ok_or_else(|| {
            EthCallCollectionError::EventParamExtraction(format!(
                "Invalid from_event format: {}",
                from_event
            ))
        })?;
        let idx: usize = idx_str.parse().map_err(|_| {
            EthCallCollectionError::EventParamExtraction(format!(
                "Invalid data index: {}",
                idx_str
            ))
        })?;

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
    } else {
        Err(EthCallCollectionError::EventParamExtraction(format!(
            "Unknown from_event format: {} (expected \"address\", \"topics[N]\", or \"data[N]\")",
            from_event
        )))
    }
}

/// Extract a typed value from a 32-byte word
fn extract_value_from_32_bytes(
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
        EvmType::NamedTuple(_) => {
            Err(EthCallCollectionError::EventParamExtraction(
                "NamedTuple cannot be used as a parameter type".to_string(),
            ))
        }
    }
}

/// Build parameters for an event-triggered call
/// Encode calldata for "once" calls that have self-address params
fn encode_once_call_params(
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

fn build_event_call_params(
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
            ParamConfig::FromEvent { param_type, from_event } => {
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
async fn process_event_triggers(
    triggers: Vec<EventTriggerData>,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
    client: &UnifiedRpcClient,
    output_dir: &Path,
    rpc_batch_size: usize,
    decoder_tx: &Option<Sender<DecoderMessage>>,
) -> Result<(), EthCallCollectionError> {
    if triggers.is_empty() {
        return Ok(());
    }

    // Group triggers by (source_name, event_signature) and then by (contract_name, function_name)
    // to batch calls together
    let mut calls_by_output: HashMap<(String, String), Vec<(EventTriggerData, EventTriggeredCallConfig, Address, Vec<DynSolValue>, Vec<Vec<u8>>)>> = HashMap::new();

    for trigger in triggers {
        let key = (trigger.source_name.clone(), trigger.event_signature);

        if let Some(configs) = event_call_configs.get(&key) {
            for config in configs {
                // Determine target address
                let target_address = if let Some(addr) = config.target_address {
                    addr
                } else {
                    // Factory collection - use event emitter, but ONLY if it's a known factory address
                    let emitter = Address::from(trigger.emitter_address);
                    match factory_addresses.get(&config.contract_name) {
                        Some(known_addresses) => {
                            if !known_addresses.contains(&emitter) {
                                // Skip - event emitter is not a known factory address
                                tracing::trace!(
                                    "Skipping event trigger: emitter {:?} not in known addresses for {}",
                                    emitter,
                                    config.contract_name
                                );
                                continue;
                            }
                            emitter
                        }
                        None => {
                            // No factory addresses known yet for this collection
                            // Skip this trigger - it will be caught up later when addresses are loaded
                            // This prevents making calls to random addresses during the race window
                            tracing::debug!(
                                "Skipping event trigger for {}: no factory addresses loaded yet (will be processed during catchup)",
                                config.contract_name
                            );
                            continue;
                        }
                    }
                };

                // Build parameters
                let (params, encoded_params) = match build_event_call_params(&trigger, &config.params) {
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
                    .push((trigger.clone(), config.clone(), target_address, params, encoded_params));
            }
        }
    }

    // Process each group of calls
    for ((contract_name, function_name), calls) in calls_by_output {
        if calls.is_empty() {
            continue;
        }

        // Get block range for output file naming
        let min_block = calls.iter().map(|(t, _, _, _, _)| t.block_number).min().unwrap();
        let max_block = calls.iter().map(|(t, _, _, _, _)| t.block_number).max().unwrap();

        tracing::info!(
            "Processing {} event-triggered eth_calls for {}.{} (blocks {}-{})",
            calls.len(),
            contract_name,
            function_name,
            min_block,
            max_block
        );

        // Build RPC calls
        let mut pending_calls: Vec<(TransactionRequest, BlockId, &EventTriggerData, Address, Vec<Vec<u8>>)> = Vec::new();

        for (trigger, config, target_address, params, encoded_params) in &calls {
            let calldata = encode_call_with_params(config.function_selector, params);
            let tx = TransactionRequest::default()
                .to(*target_address)
                .input(calldata.into());
            let block_id = BlockId::Number(BlockNumberOrTag::Number(trigger.block_number));
            pending_calls.push((tx, block_id, trigger, *target_address, encoded_params.clone()));
        }

        // Execute calls in batches
        let mut all_results: Vec<EventCallResult> = Vec::new();
        let max_params = calls.iter().map(|(_, _, _, _, p)| p.len()).max().unwrap_or(0);

        for chunk in pending_calls.chunks(rpc_batch_size) {
            let batch_calls: Vec<(TransactionRequest, BlockId)> = chunk
                .iter()
                .map(|(tx, block_id, _, _, _)| (tx.clone(), *block_id))
                .collect();

            let results = client.call_batch(batch_calls).await?;

            for (i, result) in results.into_iter().enumerate() {
                let (_, _, trigger, target_address, encoded_params) = &chunk[i];

                match result {
                    Ok(bytes) => {
                        all_results.push(EventCallResult {
                            block_number: trigger.block_number,
                            block_timestamp: trigger.block_timestamp,
                            log_index: trigger.log_index,
                            target_address: target_address.0.0,
                            value_bytes: bytes.to_vec(),
                            param_values: encoded_params.clone(),
                        });
                    }
                    Err(e) => {
                        let params_hex: Vec<String> = encoded_params.iter().map(|p| format!("0x{}", hex::encode(p))).collect();
                        tracing::warn!(
                            "Event-triggered eth_call failed for {}.{} at block {} (address {}, params {:?}): {}",
                            contract_name,
                            function_name,
                            trigger.block_number,
                            target_address,
                            params_hex,
                            e
                        );
                        // Store empty result to maintain consistency
                        all_results.push(EventCallResult {
                            block_number: trigger.block_number,
                            block_timestamp: trigger.block_timestamp,
                            log_index: trigger.log_index,
                            target_address: target_address.0.0,
                            value_bytes: Vec::new(),
                            param_values: encoded_params.clone(),
                        });
                    }
                }
            }
        }

        // Sort by block number, log index
        all_results.sort_by_key(|r| (r.block_number, r.log_index, r.target_address));

        // Write to parquet and send to decoder
        if !all_results.is_empty() {
            let sub_dir = output_dir.join(&contract_name).join(&function_name).join("on_events");
            std::fs::create_dir_all(&sub_dir)?;

            let file_name = format!("{}-{}.parquet", min_block, max_block);
            let output_path = sub_dir.join(&file_name);

            let result_count = all_results.len();
            write_event_call_results_to_parquet(&all_results, &output_path, max_params)?;

            tracing::info!(
                "Wrote {} event-triggered eth_call results to {}",
                result_count,
                output_path.display()
            );

            // Send to decoder for decoding
            if let Some(tx) = decoder_tx {
                let decoder_results: Vec<DecoderEventCallResult> = all_results
                    .iter()
                    .map(|r| DecoderEventCallResult {
                        block_number: r.block_number,
                        block_timestamp: r.block_timestamp,
                        log_index: r.log_index,
                        target_address: r.target_address,
                        value: r.value_bytes.clone(),
                    })
                    .collect();

                let _ = tx.send(DecoderMessage::EventCallsReady {
                    range_start: min_block,
                    range_end: max_block,
                    contract_name: contract_name.clone(),
                    function_name: function_name.clone(),
                    results: decoder_results,
                }).await;
            }
        }
    }

    Ok(())
}

/// Process event triggers using Multicall3 aggregate3 to batch all calls per block
async fn process_event_triggers_multicall(
    triggers: Vec<EventTriggerData>,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
    client: &UnifiedRpcClient,
    output_dir: &Path,
    rpc_batch_size: usize,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    multicall3_address: Address,
) -> Result<(), EthCallCollectionError> {
    if triggers.is_empty() {
        return Ok(());
    }

    // Group triggers by (source_name, event_signature) and prepare call info
    // Key: (contract_name, function_name)
    // Value: Vec<(EventTriggerData, EventTriggeredCallConfig, Address, params, encoded_params)>
    let mut calls_by_output: HashMap<(String, String), Vec<(EventTriggerData, EventTriggeredCallConfig, Address, Vec<DynSolValue>, Vec<Vec<u8>>)>> = HashMap::new();

    for trigger in triggers {
        let key = (trigger.source_name.clone(), trigger.event_signature);

        if let Some(configs) = event_call_configs.get(&key) {
            for config in configs {
                // Determine target address
                let target_address = if let Some(addr) = config.target_address {
                    addr
                } else {
                    // Factory collection - use event emitter, but ONLY if it's a known factory address
                    let emitter = Address::from(trigger.emitter_address);
                    match factory_addresses.get(&config.contract_name) {
                        Some(known_addresses) => {
                            if !known_addresses.contains(&emitter) {
                                continue;
                            }
                            emitter
                        }
                        None => {
                            continue;
                        }
                    }
                };

                // Build parameters
                let (params, encoded_params) = match build_event_call_params(&trigger, &config.params) {
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
                    .push((trigger.clone(), config.clone(), target_address, params, encoded_params));
            }
        }
    }

    if calls_by_output.is_empty() {
        return Ok(());
    }

    // Group all calls by block number for multicall batching
    struct EventCallInfo {
        trigger: EventTriggerData,
        config: EventTriggeredCallConfig,
        target_address: Address,
        params: Vec<DynSolValue>,
        encoded_params: Vec<Vec<u8>>,
    }

    let mut calls_by_block: HashMap<u64, Vec<EventCallInfo>> = HashMap::new();

    for ((_contract_name, _function_name), calls) in &calls_by_output {
        for (trigger, config, target_address, params, encoded_params) in calls {
            calls_by_block
                .entry(trigger.block_number)
                .or_default()
                .push(EventCallInfo {
                    trigger: trigger.clone(),
                    config: config.clone(),
                    target_address: *target_address,
                    params: params.clone(),
                    encoded_params: encoded_params.clone(),
                });
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
                let calldata = encode_call_with_params(call_info.config.function_selector, &call_info.params);
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
        return Ok(());
    }

    // Get min/max blocks for logging and file naming
    let min_block = block_multicalls.iter().map(|bm| bm.block_number).min().unwrap();
    let max_block = block_multicalls.iter().map(|bm| bm.block_number).max().unwrap();

    tracing::info!(
        "Executing {} multicalls for event-triggered calls in blocks {}-{}",
        block_multicalls.len(),
        min_block,
        max_block
    );

    // Execute all multicalls
    let results = execute_multicalls_generic(
        client,
        multicall3_address,
        block_multicalls,
        rpc_batch_size,
    )
    .await?;

    // Distribute results back to groups
    let mut group_results: HashMap<(String, String), Vec<EventCallResult>> = HashMap::new();

    for ((meta, block_number, block_timestamp), return_data, _success) in results {
        let key = (meta.contract_name.clone(), meta.function_name.clone());
        group_results
            .entry(key)
            .or_default()
            .push(EventCallResult {
                block_number,
                block_timestamp,
                log_index: meta.log_index,
                target_address: meta.target_address.0 .0,
                value_bytes: return_data,
                param_values: meta.param_values,
            });
    }

    // Write parquet and send to decoder for each group
    for ((contract_name, function_name), mut results) in group_results {
        if results.is_empty() {
            continue;
        }

        // Sort by block number, log index
        results.sort_by_key(|r| (r.block_number, r.log_index, r.target_address));

        let sub_dir = output_dir.join(&contract_name).join(&function_name).join("on_events");
        std::fs::create_dir_all(&sub_dir)?;

        let file_name = format!("{}-{}.parquet", min_block, max_block);
        let output_path = sub_dir.join(&file_name);

        let result_count = results.len();
        let max_params = results.iter().map(|r| r.param_values.len()).max().unwrap_or(0);
        write_event_call_results_to_parquet(&results, &output_path, max_params)?;

        tracing::info!(
            "Wrote {} multicall event-triggered eth_call results to {}",
            result_count,
            output_path.display()
        );

        // Send to decoder
        if let Some(tx) = decoder_tx {
            let decoder_results: Vec<DecoderEventCallResult> = results
                .iter()
                .map(|r| DecoderEventCallResult {
                    block_number: r.block_number,
                    block_timestamp: r.block_timestamp,
                    log_index: r.log_index,
                    target_address: r.target_address,
                    value: r.value_bytes.clone(),
                })
                .collect();

            let _ = tx.send(DecoderMessage::EventCallsReady {
                range_start: min_block,
                range_end: max_block,
                contract_name: contract_name.clone(),
                function_name: function_name.clone(),
                results: decoder_results,
            }).await;
        }
    }

    Ok(())
}

/// Write event-triggered call results to parquet
fn write_event_call_results_to_parquet(
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

    // target_address
    let arr = FixedSizeBinaryArray::try_from_iter(
        results.iter().map(|r| r.target_address.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    // value
    let arr: BinaryArray = results
        .iter()
        .map(|r| Some(r.value_bytes.as_slice()))
        .collect();
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

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Build schema for event-triggered call results
fn build_event_call_schema(num_params: usize) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("target_address", DataType::FixedSizeBinary(20), false),
        Field::new("value", DataType::Binary, false),
    ];

    for i in 0..num_params {
        fields.push(Field::new(
            &format!("param_{}", i),
            DataType::Binary,
            true,
        ));
    }

    Arc::new(Schema::new(fields))
}

/// Load historical factory addresses from parquet files for event trigger catchup
/// This ensures factory addresses are known before processing historical event triggers
fn load_historical_factory_addresses(
    chain_name: &str,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
) -> HashMap<String, HashSet<Address>> {
    let mut result: HashMap<String, HashSet<Address>> = HashMap::new();

    // Get collection names that need factory addresses (is_factory = true)
    let factory_collections: HashSet<String> = event_call_configs
        .values()
        .flatten()
        .filter(|c| c.is_factory)
        .map(|c| c.contract_name.clone())
        .collect();

    if factory_collections.is_empty() {
        return result;
    }

    let factories_dir = PathBuf::from(format!("data/derived/{}/factories", chain_name));
    if !factories_dir.exists() {
        tracing::debug!(
            "No factories directory found at {}, skipping historical factory address loading",
            factories_dir.display()
        );
        return result;
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

            // Only load if this collection is needed for event triggers
            if !factory_collections.contains(&collection_name) {
                continue;
            }

            // Read parquet files in this collection
            if let Ok(files) = std::fs::read_dir(&path) {
                for file_entry in files.flatten() {
                    let file_path = file_entry.path();
                    if !file_path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        continue;
                    }

                    if let Ok(addresses) = read_factory_addresses_from_parquet(&file_path) {
                        result
                            .entry(collection_name.clone())
                            .or_default()
                            .extend(addresses.into_iter().map(Address::from));
                    }
                }
            }
        }
    }

    if !result.is_empty() {
        tracing::info!(
            "Loaded historical factory addresses for {} collections",
            result.len()
        );
        for (name, addrs) in &result {
            tracing::debug!("  {} - {} addresses", name, addrs.len());
        }
    }

    result
}

/// Load factory addresses with block numbers and timestamps for once-call catchup.
/// Returns a map of collection_name -> Vec<(address, block_number, timestamp)>
fn load_factory_addresses_for_once_catchup(
    chain_name: &str,
    factory_once_configs: &HashMap<String, Vec<OnceCallConfig>>,
) -> HashMap<String, Vec<(Address, u64, u64)>> {
    let mut result: HashMap<String, Vec<(Address, u64, u64)>> = HashMap::new();

    let collections_needed: HashSet<&String> = factory_once_configs.keys().collect();
    if collections_needed.is_empty() {
        return result;
    }

    let factories_dir = PathBuf::from(format!("data/derived/{}/factories", chain_name));
    if !factories_dir.exists() {
        tracing::debug!(
            "No factories directory at {}, skipping factory once catchup",
            factories_dir.display()
        );
        return result;
    }

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

            if !collections_needed.contains(&collection_name) {
                continue;
            }

            // Read parquet files in this collection
            if let Ok(files) = std::fs::read_dir(&path) {
                for file_entry in files.flatten() {
                    let file_path = file_entry.path();
                    if !file_path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        continue;
                    }

                    if let Ok(addresses) = read_factory_addresses_with_blocks(&file_path) {
                        result
                            .entry(collection_name.clone())
                            .or_default()
                            .extend(addresses);
                    }
                }
            }
        }
    }

    if !result.is_empty() {
        tracing::info!(
            "Loaded factory addresses for once-call catchup: {} collections",
            result.len()
        );
        for (name, addrs) in &result {
            tracing::info!("  {} - {} addresses", name, addrs.len());
        }
    }

    result
}

/// Read factory addresses with block number and timestamp from a parquet file
fn read_factory_addresses_with_blocks(path: &Path) -> Result<Vec<(Address, u64, u64)>, std::io::Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(builder) => match builder.build() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to build parquet reader for {}: {}", path.display(), e);
                return Ok(Vec::new());
            }
        },
        Err(e) => {
            tracing::warn!("Failed to create parquet reader for {}: {}", path.display(), e);
            return Ok(Vec::new());
        }
    };

    let mut addresses = Vec::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                continue;
            }
        };

        let addr_col_idx = batch.schema().index_of("factory_address").ok();
        let block_col_idx = batch.schema().index_of("block_number").ok();
        let ts_col_idx = batch.schema().index_of("block_timestamp").ok();

        if addr_col_idx.is_none() || block_col_idx.is_none() {
            continue;
        }

        let addr_col = batch.column(addr_col_idx.unwrap());
        let block_col = batch.column(block_col_idx.unwrap());
        let ts_col = ts_col_idx.map(|i| batch.column(i));

        let addr_array = match addr_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(a) => a,
            None => continue,
        };
        let block_array = match block_col.as_any().downcast_ref::<UInt64Array>() {
            Some(a) => a,
            None => continue,
        };
        let ts_array = ts_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        for i in 0..addr_array.len() {
            if addr_array.is_null(i) || block_array.is_null(i) {
                continue;
            }
            let bytes = addr_array.value(i);
            if bytes.len() != 20 {
                continue;
            }
            let mut addr_bytes = [0u8; 20];
            addr_bytes.copy_from_slice(bytes);
            let block_num = block_array.value(i);
            let timestamp = ts_array.map(|a| a.value(i)).unwrap_or(0);
            addresses.push((Address::from(addr_bytes), block_num, timestamp));
        }
    }

    Ok(addresses)
}

/// Read factory addresses from a parquet file
fn read_factory_addresses_from_parquet(path: &Path) -> Result<HashSet<[u8; 20]>, std::io::Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(builder) => match builder.build() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to build parquet reader for {}: {}", path.display(), e);
                return Ok(HashSet::new());
            }
        },
        Err(e) => {
            tracing::warn!("Failed to create parquet reader for {}: {}", path.display(), e);
            return Ok(HashSet::new());
        }
    };

    let mut addresses = HashSet::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                continue;
            }
        };

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

/// Existing log range info for catchup
#[derive(Debug, Clone)]
struct ExistingLogRange {
    start: u64,
    end: u64,
    file_path: PathBuf,
}

/// Get existing log file ranges for catchup
fn get_existing_log_ranges(chain_name: &str) -> Vec<ExistingLogRange> {
    let logs_dir = PathBuf::from(format!("data/raw/{}/logs", chain_name));
    let mut ranges = Vec::new();

    let entries = match std::fs::read_dir(&logs_dir) {
        Ok(entries) => entries,
        Err(_) => return ranges,
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

        // Parse "logs_START-END" format
        if !file_name.starts_with("logs_") {
            continue;
        }

        let range_part = &file_name[5..]; // Skip "logs_"
        let range_parts: Vec<&str> = range_part.split('-').collect();
        if range_parts.len() != 2 {
            continue;
        }

        let start: u64 = match range_parts[0].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let end: u64 = match range_parts[1].parse::<u64>() {
            Ok(v) => v + 1, // Convert inclusive end to exclusive
            Err(_) => continue,
        };

        ranges.push(ExistingLogRange {
            start,
            end,
            file_path: path,
        });
    }

    ranges.sort_by_key(|r| r.start);
    ranges
}

/// Read logs from a parquet file for catchup
fn read_logs_from_parquet(file_path: &Path) -> Result<Vec<LogData>, EthCallCollectionError> {
    use arrow::array::{Array, ListArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .build()
        .map_err(|e| EthCallCollectionError::Parquet(e))?;

    let mut logs = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(EthCallCollectionError::Arrow)?;

        let block_numbers = batch
            .column_by_name("block_number")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let block_timestamps = batch
            .column_by_name("block_timestamp")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let tx_hashes = batch
            .column_by_name("transaction_hash")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        let log_indices = batch
            .column_by_name("log_index")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>());

        let addresses = batch
            .column_by_name("address")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        let topics_col = batch.column_by_name("topics");

        let data_col = batch
            .column_by_name("data")
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());

        let (Some(block_numbers), Some(block_timestamps), Some(tx_hashes), Some(log_indices), Some(addresses), Some(data_col)) =
            (block_numbers, block_timestamps, tx_hashes, log_indices, addresses, data_col)
        else {
            tracing::warn!("Missing required columns in log parquet file: {:?}", file_path);
            continue;
        };

        for i in 0..batch.num_rows() {
            let block_number = block_numbers.value(i);
            let block_timestamp = block_timestamps.value(i);

            let mut tx_hash_bytes = [0u8; 32];
            tx_hash_bytes.copy_from_slice(tx_hashes.value(i));
            let transaction_hash = alloy::primitives::B256::from(tx_hash_bytes);

            let log_index = log_indices.value(i);

            let mut address = [0u8; 20];
            address.copy_from_slice(addresses.value(i));

            // Extract topics from the list column
            let topics: Vec<[u8; 32]> = if let Some(col) = topics_col {
                if let Some(list_array) = col.as_any().downcast_ref::<ListArray>() {
                    let values = list_array.value(i);
                    if let Some(fixed_array) = values.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                        (0..fixed_array.len())
                            .map(|j| {
                                let mut topic = [0u8; 32];
                                topic.copy_from_slice(fixed_array.value(j));
                                topic
                            })
                            .collect()
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            let data = data_col.value(i).to_vec();

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

/// Check if on_events output already exists for a range
fn event_output_exists(
    output_dir: &Path,
    contract_name: &str,
    function_name: &str,
    range_start: u64,
    range_end: u64,
) -> bool {
    let sub_dir = output_dir.join(contract_name).join(function_name).join("on_events");
    if !sub_dir.exists() {
        return false;
    }

    // Check for any file that overlaps with this range
    // Note: range_end is EXCLUSIVE (e.g., 200 means up to block 199)
    // Output files are named {min_block}-{max_block} where both are INCLUSIVE
    if let Ok(entries) = std::fs::read_dir(&sub_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.extension().map(|e| e == "parquet").unwrap_or(false) {
                continue;
            }

            if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                let parts: Vec<&str> = file_stem.split('-').collect();
                if parts.len() == 2 {
                    if let (Ok(file_start), Ok(file_end_inclusive)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                        // Convert to exclusive end for consistent comparison
                        // Ranges overlap if: start1 < end2 AND start2 < end1 (using exclusive ends)
                        let file_end_exclusive = file_end_inclusive + 1;
                        if range_start < file_end_exclusive && file_start < range_end {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

async fn process_factory_range(
    range: &BlockRange,
    blocks: &[BlockInfo],
    client: &UnifiedRpcClient,
    factory_data: &FactoryAddressData,
    factory_call_configs: &HashMap<String, Vec<EthCallConfig>>,
    output_dir: &Path,
    existing_files: &HashSet<String>,
    rpc_batch_size: usize,
    max_params: usize,
    frequency_state: &mut FrequencyState,
) -> Result<(), EthCallCollectionError> {
    let mut addresses_by_collection: HashMap<String, HashSet<Address>> = HashMap::new();
    for (_block, addrs) in &factory_data.addresses_by_block {
        for (_, addr, collection_name) in addrs {
            addresses_by_collection
                .entry(collection_name.clone())
                .or_default()
                .insert(*addr);
        }
    }

    for (collection_name, call_configs) in factory_call_configs {
        let addresses = match addresses_by_collection.get(collection_name) {
            Some(addrs) if !addrs.is_empty() => addrs,
            _ => continue,
        };

        for call_config in call_configs {
            if call_config.frequency.is_once() {
                continue;
            }

            let selector = compute_function_selector(&call_config.function);
            let function_name = call_config
                .function
                .split('(')
                .next()
                .unwrap_or(&call_config.function)
                .to_string();

            let file_name = range.file_name();
            let rel_path = format!("{}/{}/{}", collection_name, function_name, file_name);

            if existing_files.contains(&rel_path) {
                tracing::debug!(
                    "Skipping factory eth_calls for {}.{} blocks {}-{} (already exists)",
                    collection_name,
                    function_name,
                    range.start,
                    range.end - 1
                );
                continue;
            }

            let param_combinations = generate_param_combinations(&call_config.params)?;

            let mut configs: Vec<CallConfig> = Vec::new();
            for address in addresses {
                for param_combo in &param_combinations {
                    let dyn_values: Vec<DynSolValue> = param_combo
                        .iter()
                        .map(|(param_type, value, _)| param_type.parse_value(value))
                        .collect::<Result<_, _>>()?;

                    let encoded_calldata = encode_call_with_params(selector, &dyn_values);
                    let param_values: Vec<Vec<u8>> =
                        param_combo.iter().map(|(_, _, encoded)| encoded.clone()).collect();

                    configs.push(CallConfig {
                        contract_name: collection_name.clone(),
                        address: *address,
                        function_name: function_name.clone(),
                        encoded_calldata,
                        param_values,
                        frequency: call_config.frequency.clone(),
                    });
                }
            }

            if configs.is_empty() {
                continue;
            }

            let state_key = (collection_name.clone(), function_name.clone());
            let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();

            let filtered_blocks = filter_blocks_for_frequency(blocks, &call_config.frequency, last_call_ts);

            if filtered_blocks.is_empty() {
                tracing::debug!(
                    "No blocks match frequency {:?} for factory {}.{} in range {}-{}",
                    call_config.frequency,
                    collection_name,
                    function_name,
                    range.start,
                    range.end - 1
                );
                continue;
            }

            tracing::info!(
                "Fetching factory eth_calls for {}.{} blocks {}-{} ({} addresses, {} blocks after frequency filter)",
                collection_name,
                function_name,
                range.start,
                range.end - 1,
                addresses.len(),
                filtered_blocks.len()
            );

            #[cfg(feature = "bench")]
            let mut rpc_time = std::time::Duration::ZERO;
            #[cfg(feature = "bench")]
            let mut process_time = std::time::Duration::ZERO;

            let mut all_results: Vec<CallResult> = Vec::new();
            let mut pending_calls: Vec<(TransactionRequest, BlockId, &BlockInfo, &CallConfig)> =
                Vec::new();

            for block in &filtered_blocks {
                for config in &configs {
                    let tx = TransactionRequest::default()
                        .to(config.address)
                        .input(config.encoded_calldata.clone().into());
                    let block_id = BlockId::Number(BlockNumberOrTag::Number(block.block_number));
                    pending_calls.push((tx, block_id, block, config));
                }
            }

            for chunk in pending_calls.chunks(rpc_batch_size) {
                let calls: Vec<(TransactionRequest, BlockId)> = chunk
                    .iter()
                    .map(|(tx, block_id, _, _)| (tx.clone(), *block_id))
                    .collect();

                #[cfg(feature = "bench")]
                let rpc_start = Instant::now();
                let results = client.call_batch(calls).await?;
                #[cfg(feature = "bench")]
                {
                    rpc_time += rpc_start.elapsed();
                }

                #[cfg(feature = "bench")]
                let process_start = Instant::now();
                for (i, result) in results.into_iter().enumerate() {
                    let (_, _, block, config) = &chunk[i];
                    match result {
                        Ok(bytes) => {
                            all_results.push(CallResult {
                                block_number: block.block_number,
                                block_timestamp: block.timestamp,
                                contract_address: config.address.0.0,
                                value_bytes: bytes.to_vec(),
                                param_values: config.param_values.clone(),
                            });
                        }
                        Err(e) => {
                            let params_hex: Vec<String> = config.param_values.iter().map(|p| format!("0x{}", hex::encode(p))).collect();
                            tracing::warn!(
                                "Factory eth_call failed for {}.{} at block {} (address {}, params {:?}): {}",
                                collection_name,
                                function_name,
                                block.block_number,
                                config.address,
                                params_hex,
                                e
                            );
                            all_results.push(CallResult {
                                block_number: block.block_number,
                                block_timestamp: block.timestamp,
                                contract_address: config.address.0.0,
                                value_bytes: Vec::new(),
                                param_values: config.param_values.clone(),
                            });
                        }
                    }
                }
                #[cfg(feature = "bench")]
                {
                    process_time += process_start.elapsed();
                }
            }

            all_results
                .sort_by_key(|r| (r.block_number, r.contract_address, r.param_values.clone()));

            let result_count = all_results.len();
            let sub_dir = output_dir.join(collection_name).join(&function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);
            #[cfg(feature = "bench")]
            let write_start = Instant::now();
            tokio::task::spawn_blocking(move || {
                write_results_to_parquet(&all_results, &output_path, max_params)
            })
            .await
            .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;
            #[cfg(feature = "bench")]
            {
                let write_time = write_start.elapsed();
                crate::bench::record(
                    &format!("eth_calls_{}.{}", collection_name, function_name),
                    range.start,
                    range.end,
                    result_count,
                    rpc_time,
                    process_time,
                    write_time,
                );
            }

            tracing::info!(
                "Wrote {} factory eth_call results to {}",
                result_count,
                sub_dir.join(&file_name).display()
            );

            if let Frequency::Duration(_) = call_config.frequency {
                if let Some(last_block) = filtered_blocks.last() {
                    frequency_state.last_call_times.insert(
                        (collection_name.clone(), function_name.clone()),
                        last_block.timestamp,
                    );
                }
            }
        }
    }

    Ok(())
}

/// Process factory calls using Multicall3 aggregate3 to batch all calls per block
async fn process_factory_range_multicall(
    range: &BlockRange,
    blocks: &[BlockInfo],
    client: &UnifiedRpcClient,
    factory_data: &FactoryAddressData,
    factory_call_configs: &HashMap<String, Vec<EthCallConfig>>,
    output_dir: &Path,
    existing_files: &HashSet<String>,
    rpc_batch_size: usize,
    max_params: usize,
    frequency_state: &mut FrequencyState,
    multicall3_address: Address,
) -> Result<(), EthCallCollectionError> {
    // Collect factory addresses by collection
    let mut addresses_by_collection: HashMap<String, HashSet<Address>> = HashMap::new();
    for (_block, addrs) in &factory_data.addresses_by_block {
        for (_, addr, collection_name) in addrs {
            addresses_by_collection
                .entry(collection_name.clone())
                .or_default()
                .insert(*addr);
        }
    }

    // Build per-group info for active groups
    struct FactoryGroupInfo {
        collection_name: String,
        function_name: String,
        configs: Vec<CallConfig>,
        filtered_blocks: Vec<BlockInfo>,
        frequency: Frequency,
    }

    let mut active_groups: Vec<FactoryGroupInfo> = Vec::new();

    for (collection_name, call_configs) in factory_call_configs {
        let addresses = match addresses_by_collection.get(collection_name) {
            Some(addrs) if !addrs.is_empty() => addrs,
            _ => continue,
        };

        for call_config in call_configs {
            if call_config.frequency.is_once() {
                continue;
            }

            let selector = compute_function_selector(&call_config.function);
            let function_name = call_config
                .function
                .split('(')
                .next()
                .unwrap_or(&call_config.function)
                .to_string();

            let file_name = range.file_name();
            let rel_path = format!("{}/{}/{}", collection_name, function_name, file_name);

            if existing_files.contains(&rel_path) {
                tracing::debug!(
                    "Skipping factory eth_calls for {}.{} blocks {}-{} (already exists)",
                    collection_name,
                    function_name,
                    range.start,
                    range.end - 1
                );
                continue;
            }

            let param_combinations = generate_param_combinations(&call_config.params)?;

            let mut configs: Vec<CallConfig> = Vec::new();
            for address in addresses {
                for param_combo in &param_combinations {
                    let dyn_values: Vec<DynSolValue> = param_combo
                        .iter()
                        .map(|(param_type, value, _)| param_type.parse_value(value))
                        .collect::<Result<_, _>>()?;

                    let encoded_calldata = encode_call_with_params(selector, &dyn_values);
                    let param_values: Vec<Vec<u8>> =
                        param_combo.iter().map(|(_, _, encoded)| encoded.clone()).collect();

                    configs.push(CallConfig {
                        contract_name: collection_name.clone(),
                        address: *address,
                        function_name: function_name.clone(),
                        encoded_calldata,
                        param_values,
                        frequency: call_config.frequency.clone(),
                    });
                }
            }

            if configs.is_empty() {
                continue;
            }

            let state_key = (collection_name.clone(), function_name.clone());
            let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();
            let filtered_blocks = filter_blocks_for_frequency(blocks, &call_config.frequency, last_call_ts);

            if filtered_blocks.is_empty() {
                continue;
            }

            active_groups.push(FactoryGroupInfo {
                collection_name: collection_name.clone(),
                function_name,
                configs,
                filtered_blocks: filtered_blocks.into_iter().cloned().collect(),
                frequency: call_config.frequency.clone(),
            });
        }
    }

    if active_groups.is_empty() {
        return Ok(());
    }

    // Collect all unique blocks across all groups
    let mut all_block_numbers: HashSet<u64> = HashSet::new();
    for group in &active_groups {
        for block in &group.filtered_blocks {
            all_block_numbers.insert(block.block_number);
        }
    }
    let mut sorted_blocks: Vec<u64> = all_block_numbers.into_iter().collect();
    sorted_blocks.sort_unstable();

    // Build block_number -> BlockInfo lookup
    let mut block_info_map: HashMap<u64, &BlockInfo> = HashMap::new();
    for group in &active_groups {
        for block in &group.filtered_blocks {
            block_info_map.entry(block.block_number).or_insert(block);
        }
    }

    // Track which blocks each group needs
    let group_block_sets: Vec<HashSet<u64>> = active_groups
        .iter()
        .map(|g| g.filtered_blocks.iter().map(|b| b.block_number).collect())
        .collect();

    // Build per-block multicalls with block info included in metadata
    let mut block_multicalls: Vec<BlockMulticall<(FactoryCallMeta, u64, u64)>> = Vec::new();

    for &block_number in &sorted_blocks {
        let mut slots: Vec<MulticallSlotGeneric<(FactoryCallMeta, u64, u64)>> = Vec::new();
        let block_info = block_info_map[&block_number];

        for (group_idx, group) in active_groups.iter().enumerate() {
            if !group_block_sets[group_idx].contains(&block_number) {
                continue;
            }
            for config in &group.configs {
                slots.push(MulticallSlotGeneric {
                    block_number,
                    block_timestamp: block_info.timestamp,
                    target_address: config.address,
                    encoded_calldata: config.encoded_calldata.clone(),
                    metadata: (
                        FactoryCallMeta {
                            collection_name: group.collection_name.clone(),
                            function_name: group.function_name.clone(),
                            contract_address: config.address,
                            param_values: config.param_values.clone(),
                        },
                        block_number,
                        block_info.timestamp,
                    ),
                });
            }
        }

        if !slots.is_empty() {
            block_multicalls.push(BlockMulticall {
                block_number,
                block_id: BlockId::Number(BlockNumberOrTag::Number(block_number)),
                slots,
            });
        }
    }

    tracing::info!(
        "Executing {} multicalls for {} factory call groups in blocks {}-{}",
        block_multicalls.len(),
        active_groups.len(),
        range.start,
        range.end - 1
    );

    // Execute all multicalls
    let results = execute_multicalls_generic(
        client,
        multicall3_address,
        block_multicalls,
        rpc_batch_size,
    )
    .await?;

    // Distribute results back to groups
    let mut group_results: HashMap<(String, String), Vec<CallResult>> = HashMap::new();
    for group in &active_groups {
        group_results.insert(
            (group.collection_name.clone(), group.function_name.clone()),
            Vec::new(),
        );
    }

    for ((meta, block_number, block_timestamp), return_data, _success) in results {
        let key = (meta.collection_name.clone(), meta.function_name.clone());
        if let Some(results) = group_results.get_mut(&key) {
            results.push(CallResult {
                block_number,
                block_timestamp,
                contract_address: meta.contract_address.0 .0,
                value_bytes: return_data,
                param_values: meta.param_values,
            });
        }
    }

    // Write parquet for each group
    for group in &active_groups {
        let key = (group.collection_name.clone(), group.function_name.clone());
        if let Some(results) = group_results.get_mut(&key) {
            results.sort_by_key(|r| (r.block_number, r.contract_address, r.param_values.clone()));

            let result_count = results.len();
            let file_name = range.file_name();
            let sub_dir = output_dir.join(&group.collection_name).join(&group.function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            let results_owned = std::mem::take(results);
            tokio::task::spawn_blocking(move || {
                write_results_to_parquet(&results_owned, &output_path, max_params)
            })
            .await
            .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

            tracing::info!(
                "Wrote {} multicall factory eth_call results to {}",
                result_count,
                sub_dir.join(&file_name).display()
            );

            if let Frequency::Duration(_) = &group.frequency {
                if let Some(last_block) = group.filtered_blocks.last() {
                    frequency_state
                        .last_call_times
                        .insert(key.clone(), last_block.timestamp);
                }
            }
        }
    }

    Ok(())
}

fn generate_param_combinations(params: &[ParamConfig]) -> Result<Vec<Vec<(EvmType, ParamValue, Vec<u8>)>>, ParamError> {
    if params.is_empty() {
        return Ok(vec![vec![]]);
    }

    let mut result = vec![vec![]];

    for param in params {
        // Only Static params are supported for block-based frequency calls
        // FromEvent and SelfAddress params are for on_events frequency only
        let values = match param.values() {
            Some(values) => values,
            None => {
                return Err(ParamError::TypeMismatch {
                    expected: "static param with values".to_string(),
                    got: "from_event or source param (only valid for on_events frequency)".to_string(),
                });
            }
        };

        let param_type = param.param_type();
        let mut new_result = Vec::new();
        for existing in &result {
            for value in values {
                let mut combo = existing.clone();
                let dyn_val = param_type.parse_value(value)?;
                let encoded = dyn_val.abi_encode();
                combo.push((param_type.clone(), value.clone(), encoded));
                new_result.push(combo);
            }
        }
        result = new_result;
    }

    Ok(result)
}

fn build_call_configs(contracts: &Contracts) -> Result<Vec<CallConfig>, EthCallCollectionError> {
    let mut configs = Vec::new();

    for (contract_name, contract) in contracts {
        let default_addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };

        if let Some(calls) = &contract.calls {
            for call in calls {
                // Skip once and on_events calls - they're handled separately
                if call.frequency.is_once() || call.frequency.is_on_events() {
                    continue;
                }

                // Resolve target addresses: use target override if specified, otherwise contract addresses
                let addresses = if let Some(target) = &call.target {
                    match target.resolve_all(contracts) {
                        Some(addrs) => addrs,
                        None => {
                            tracing::warn!(
                                "Could not resolve target for call {} on contract {}, skipping",
                                call.function, contract_name
                            );
                            continue;
                        }
                    }
                } else {
                    default_addresses.clone()
                };

                let selector = compute_function_selector(&call.function);
                let function_name = call.function
                    .split('(')
                    .next()
                    .unwrap_or(&call.function)
                    .to_string();

                let param_combinations = generate_param_combinations(&call.params)?;

                for address in &addresses {
                    for param_combo in &param_combinations {
                        let dyn_values: Vec<DynSolValue> = param_combo
                            .iter()
                            .map(|(param_type, value, _)| param_type.parse_value(value))
                            .collect::<Result<_, _>>()?;

                        let encoded_calldata = encode_call_with_params(selector, &dyn_values);

                        let param_values: Vec<Vec<u8>> = param_combo
                            .iter()
                            .map(|(_, _, encoded)| encoded.clone())
                            .collect();

                        configs.push(CallConfig {
                            contract_name: contract_name.clone(),
                            address: *address,
                            function_name: function_name.clone(),
                            encoded_calldata,
                            param_values,
                            frequency: call.frequency.clone(),
                        });
                    }
                }
            }
        }
    }

    Ok(configs)
}

/// Configuration for a token pool call
#[derive(Debug, Clone)]
struct TokenCallConfig {
    /// Token name (used for output directory naming as {token_name}_pool)
    token_name: String,
    /// Pool type (v2, v3, v4)
    pool_type: PoolType,
    /// Target address for the call (pool address for v2/v3, StateView for v4)
    target_address: Address,
    /// Function name (e.g., "slot0")
    function_name: String,
    /// Encoded calldata including selector and any params
    encoded_calldata: Bytes,
    /// Call frequency
    frequency: Frequency,
    /// Output type for decoding
    output_type: EvmType,
}

/// Build call configs from token pool configurations
fn build_token_call_configs(
    tokens: &Tokens,
    contracts: &Contracts,
) -> Result<Vec<TokenCallConfig>, EthCallCollectionError> {
    let mut configs = Vec::new();

    // Look up UniswapV4StateView address from contracts config
    let state_view_address = contracts
        .get("UniswapV4StateView")
        .and_then(|c| match &c.address {
            AddressOrAddresses::Single(addr) => Some(*addr),
            AddressOrAddresses::Multiple(addrs) => addrs.first().copied(),
        });

    for (token_name, token_config) in tokens {
        if let Some(pool) = &token_config.pool {
            if let Some(calls) = &pool.calls {
                for call in calls {
                    // Skip once and on_events calls for now
                    if call.frequency.is_once() || call.frequency.is_on_events() {
                        continue;
                    }

                    let selector = compute_function_selector(&call.function);
                    let function_name = call
                        .function
                        .split('(')
                        .next()
                        .unwrap_or(&call.function)
                        .to_string();

                    let (target_address, encoded_calldata) = match pool.pool_type {
                        PoolType::V2 | PoolType::V3 => {
                            // Target the pool address directly
                            let addr = match &pool.address {
                                AddressOrPoolId::Address(a) => *a,
                                AddressOrPoolId::PoolId(_) => {
                                    tracing::warn!(
                                        "V2/V3 pool {} has PoolId instead of Address, skipping",
                                        token_name
                                    );
                                    continue;
                                }
                            };
                            let calldata = Bytes::copy_from_slice(&selector);
                            (addr, calldata)
                        }
                        PoolType::V4 => {
                            // Target StateView with pool ID as parameter
                            let state_view = match state_view_address {
                                Some(addr) => addr,
                                None => {
                                    tracing::warn!(
                                        "No UniswapV4StateView configured, skipping V4 pool calls for {}",
                                        token_name
                                    );
                                    continue;
                                }
                            };
                            let pool_id_bytes = match &pool.address {
                                AddressOrPoolId::PoolId(id) => *id,
                                AddressOrPoolId::Address(_) => {
                                    tracing::warn!(
                                        "V4 pool {} has Address instead of PoolId, skipping",
                                        token_name
                                    );
                                    continue;
                                }
                            };
                            // Encode call: selector + abi-encoded pool_id (bytes32)
                            let pool_id_value = DynSolValue::FixedBytes(pool_id_bytes, 32);
                            let calldata = encode_call_with_params(selector, &[pool_id_value]);
                            (state_view, calldata)
                        }
                    };

                    configs.push(TokenCallConfig {
                        token_name: token_name.clone(),
                        pool_type: pool.pool_type.clone(),
                        target_address,
                        function_name,
                        encoded_calldata,
                        frequency: call.frequency.clone(),
                        output_type: call.output_type.clone(),
                    });
                }
            }
        }
    }

    Ok(configs)
}

fn build_once_call_configs(contracts: &Contracts) -> HashMap<String, Vec<OnceCallConfig>> {
    let mut configs: HashMap<String, Vec<OnceCallConfig>> = HashMap::new();

    for (contract_name, contract) in contracts {
        if let Some(calls) = &contract.calls {
            for call in calls {
                if call.frequency.is_once() {
                    let selector = compute_function_selector(&call.function);
                    let function_name = call.function
                        .split('(')
                        .next()
                        .unwrap_or(&call.function)
                        .to_string();

                    // Check if call has self-address params (requires dynamic encoding per address)
                    let (preencoded_calldata, params) = if call.has_self_address_param() {
                        // Need to encode dynamically per address
                        (None, call.params.clone())
                    } else if call.params.is_empty() {
                        // No params - just the selector
                        (Some(Bytes::copy_from_slice(&selector)), vec![])
                    } else {
                        // Static params only - pre-encode now
                        // Note: This uses first value from each static param
                        let mut dyn_values = Vec::new();
                        let mut all_static = true;
                        for param in &call.params {
                            match param {
                                ParamConfig::Static { param_type, values } => {
                                    if let Some(value) = values.first() {
                                        if let Ok(dyn_val) = param_type.parse_value(value) {
                                            dyn_values.push(dyn_val);
                                        } else {
                                            all_static = false;
                                            break;
                                        }
                                    } else {
                                        all_static = false;
                                        break;
                                    }
                                }
                                _ => {
                                    all_static = false;
                                    break;
                                }
                            }
                        }
                        if all_static {
                            (Some(encode_call_with_params(selector, &dyn_values)), vec![])
                        } else {
                            // Fallback: store params for dynamic encoding
                            (None, call.params.clone())
                        }
                    };

                    // Resolve target override addresses if specified
                    let target_addresses = call.target.as_ref().and_then(|t| {
                        let resolved = t.resolve_all(contracts);
                        if resolved.is_none() {
                            tracing::warn!(
                                "Could not resolve target for once call {} on contract {}, will use contract addresses",
                                call.function, contract_name
                            );
                        }
                        resolved
                    });

                    configs.entry(contract_name.clone()).or_default().push(OnceCallConfig {
                        function_name,
                        function_selector: selector,
                        preencoded_calldata,
                        params,
                        target_addresses,
                    });
                }
            }
        }
    }

    configs
}

fn build_factory_once_call_configs(
    factory_call_configs: &HashMap<String, Vec<EthCallConfig>>,
    contracts: &Contracts,
) -> HashMap<String, Vec<OnceCallConfig>> {
    let mut configs: HashMap<String, Vec<OnceCallConfig>> = HashMap::new();

    for (collection_name, call_configs) in factory_call_configs {
        for call in call_configs {
            if call.frequency.is_once() {
                let selector = compute_function_selector(&call.function);
                let function_name = call.function
                    .split('(')
                    .next()
                    .unwrap_or(&call.function)
                    .to_string();

                // Resolve target override if specified (resolves to first address only for factory calls)
                let target_addresses = call.target.as_ref().and_then(|t| {
                    let resolved = t.resolve(contracts);
                    if resolved.is_none() {
                        tracing::warn!(
                            "Could not resolve target for factory once call {} on collection {}, will use factory addresses",
                            call.function, collection_name
                        );
                    }
                    // For factory once calls, we resolve to a single target address
                    resolved.map(|addr| vec![addr])
                });

                // Check if call has self-address params (requires dynamic encoding per address)
                let (preencoded_calldata, params) = if call.has_self_address_param() {
                    // Need to encode dynamically per address
                    (None, call.params.clone())
                } else if call.params.is_empty() {
                    // No params - just the selector
                    (Some(Bytes::copy_from_slice(&selector)), vec![])
                } else {
                    // Static params only - pre-encode now
                    let mut dyn_values = Vec::new();
                    let mut all_static = true;
                    for param in &call.params {
                        match param {
                            ParamConfig::Static { param_type, values } => {
                                if let Some(value) = values.first() {
                                    if let Ok(dyn_val) = param_type.parse_value(value) {
                                        dyn_values.push(dyn_val);
                                    } else {
                                        all_static = false;
                                        break;
                                    }
                                } else {
                                    all_static = false;
                                    break;
                                }
                            }
                            _ => {
                                all_static = false;
                                break;
                            }
                        }
                    }
                    if all_static {
                        (Some(encode_call_with_params(selector, &dyn_values)), vec![])
                    } else {
                        // Fallback: store params for dynamic encoding
                        (None, call.params.clone())
                    }
                };

                configs.entry(collection_name.clone()).or_default().push(OnceCallConfig {
                    function_name,
                    function_selector: selector,
                    preencoded_calldata,
                    params,
                    target_addresses,
                });
            }
        }
    }

    configs
}

fn filter_blocks_for_frequency<'a>(
    blocks: &'a [BlockInfo],
    frequency: &Frequency,
    last_call_timestamp: Option<u64>,
) -> Vec<&'a BlockInfo> {
    match frequency {
        Frequency::EveryBlock => blocks.iter().collect(),
        Frequency::Once => vec![], // handled separately
        Frequency::OnEvents(_) => vec![], // handled separately via event triggers
        Frequency::EveryNBlocks(n) => blocks.iter().filter(|b| b.block_number % n == 0).collect(),
        Frequency::Duration(secs) => {
            let mut result = vec![];
            let mut last_ts = last_call_timestamp.unwrap_or(0);
            for block in blocks {
                if block.timestamp >= last_ts + secs {
                    result.push(block);
                    last_ts = block.timestamp;
                }
            }
            result
        }
    }
}

async fn process_once_calls_regular(
    range: &BlockRange,
    blocks: &[BlockInfo],
    client: &UnifiedRpcClient,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    contracts: &Contracts,
    output_dir: &Path,
    existing_files: &HashSet<String>,
) -> Result<(), EthCallCollectionError> {
    let first_block = match blocks.first() {
        Some(b) => b,
        None => return Ok(()),
    };

    for (contract_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name();
        let rel_path = format!("{}/once/{}", contract_name, file_name);
        let sub_dir = output_dir.join(contract_name).join("once");
        let output_path = sub_dir.join(&file_name);

        // Determine which function calls are missing from the existing file
        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file) = if existing_files.contains(&rel_path) {
            let index = read_once_column_index(&sub_dir);
            // Use index if file is tracked, otherwise fall back to reading parquet schema
            let existing_cols: HashSet<String> = if let Some(cols) = index.get(&file_name) {
                cols.iter().cloned().collect()
            } else {
                // File exists but not in index - read schema directly
                tracing::debug!(
                    "File {} exists but not in index, reading schema from parquet",
                    output_path.display()
                );
                read_parquet_column_names(&output_path)
            };
            let missing: Vec<String> = all_fn_names
                .iter()
                .filter(|f| !existing_cols.contains(*f))
                .cloned()
                .collect();
            if missing.is_empty() {
                tracing::debug!(
                    "Skipping once eth_calls for {} blocks {}-{} (all columns present)",
                    contract_name,
                    range.start,
                    range.end - 1
                );
                continue;
            }
            tracing::info!(
                "Found {} missing once columns for {} blocks {}-{}: {:?}",
                missing.len(),
                contract_name,
                range.start,
                range.end - 1,
                missing
            );
            (missing, true)
        } else {
            (all_fn_names.clone(), false)
        };

        // Filter call configs to only missing functions
        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        let contract = match contracts.get(contract_name) {
            Some(c) => c,
            None => continue,
        };

        let default_addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };

        let block_id = BlockId::Number(BlockNumberOrTag::Number(first_block.block_number));
        let mut pending_calls: Vec<(TransactionRequest, BlockId, Address, String)> = Vec::new();

        for call_config in &configs_to_call {
            // Use target override addresses if available, otherwise contract addresses
            let addresses = call_config.target_addresses.as_ref().unwrap_or(&default_addresses);

            for address in addresses {
                let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                    preencoded.clone()
                } else {
                    encode_once_call_params(
                        call_config.function_selector,
                        &call_config.params,
                        *address,
                    )?
                };
                let tx = TransactionRequest::default()
                    .to(*address)
                    .input(calldata.into());
                pending_calls.push((tx, block_id, *address, call_config.function_name.clone()));
            }
        }

        if pending_calls.is_empty() {
            continue;
        }

        let batch_calls: Vec<(TransactionRequest, BlockId)> = pending_calls
            .iter()
            .map(|(tx, bid, _, _)| (tx.clone(), *bid))
            .collect();

        let batch_results = client.call_batch(batch_calls).await?;

        let mut results_by_address: HashMap<Address, HashMap<String, Vec<u8>>> = HashMap::new();

        for (i, result) in batch_results.into_iter().enumerate() {
            let (tx, _, address, function_name) = &pending_calls[i];

            let function_results = results_by_address.entry(*address).or_default();

            match result {
                Ok(bytes) => {
                    function_results.insert(function_name.clone(), bytes.to_vec());
                }
                Err(e) => {
                    let calldata = tx.input.input.as_ref().map(|b| format!("0x{}", hex::encode(b))).unwrap_or_default();
                    tracing::warn!(
                        "once eth_call failed for {}.{} at block {} (address {}, calldata {}): {}",
                        contract_name,
                        function_name,
                        first_block.block_number,
                        address,
                        calldata,
                        e
                    );
                    function_results.insert(function_name.clone(), Vec::new());
                }
            }
        }

        std::fs::create_dir_all(&sub_dir)?;

        if has_existing_file {
            // Merge new columns into existing parquet
            let new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = results_by_address
                .into_iter()
                .map(|(addr, fns)| (addr.0.0, fns))
                .collect();

            let existing_batches = read_existing_once_parquet(&output_path)?;
            if !existing_batches.is_empty() {
                let merged = merge_once_columns(&existing_batches, &new_results, &missing_fn_names)?;

                let file = File::create(&output_path)?;
                let props = WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::SNAPPY)
                    .build();
                let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                writer.write(&merged)?;
                writer.close()?;

                tracing::info!(
                    "Merged {} new once columns into {} for {}",
                    missing_fn_names.len(),
                    output_path.display(),
                    contract_name
                );
            }
        } else {
            // Write new file with all results - collect all unique addresses from pending_calls
            let all_addresses: Vec<Address> = pending_calls
                .iter()
                .map(|(_, _, addr, _)| *addr)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            let results: Vec<OnceCallResult> = all_addresses
                .iter()
                .filter_map(|addr| {
                    results_by_address.remove(addr).map(|function_results| OnceCallResult {
                        block_number: first_block.block_number,
                        block_timestamp: first_block.timestamp,
                        contract_address: addr.0.0,
                        function_results,
                    })
                })
                .collect();

            if !results.is_empty() {
                let output_path = sub_dir.join(&file_name);

                tracing::info!(
                    "Writing {} once eth_call results to {}",
                    results.len(),
                    output_path.display()
                );

                write_once_results_to_parquet(&results, &output_path, &all_fn_names)?;
            }
        }

        // Update the column index with columns actually present in the file
        let actual_cols: Vec<String> = read_parquet_column_names(&output_path)
            .into_iter()
            .collect();
        let mut index = read_once_column_index(&sub_dir);
        index.insert(file_name.clone(), actual_cols.clone());
        write_once_column_index(&sub_dir, &index)?;
        tracing::info!(
            "Updated column index for {}: {} now has {} columns: {:?}",
            contract_name,
            file_name,
            actual_cols.len(),
            actual_cols
        );
    }

    Ok(())
}

async fn process_factory_once_calls(
    range: &BlockRange,
    client: &UnifiedRpcClient,
    factory_data: &FactoryAddressData,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    output_dir: &Path,
    existing_files: &HashSet<String>,
    column_indexes: &HashMap<String, HashMap<String, Vec<String>>>,
    decoder_tx: &Option<Sender<DecoderMessage>>,
) -> Result<(), EthCallCollectionError> {
    for (collection_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name();
        let rel_path = format!("{}/once/{}", collection_name, file_name);
        let sub_dir = output_dir.join(collection_name).join("once");
        let output_path = sub_dir.join(&file_name);

        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file) = if existing_files.contains(&rel_path) {
            // Use pre-loaded index
            let index = column_indexes.get(collection_name);
            let existing_cols: HashSet<String> = index
                .and_then(|idx| idx.get(&file_name))
                .map(|cols| cols.iter().cloned().collect())
                .unwrap_or_default();

            let missing: Vec<String> = all_fn_names
                .iter()
                .filter(|f| !existing_cols.contains(*f))
                .cloned()
                .collect();
            if missing.is_empty() {
                tracing::debug!(
                    "Skipping factory once eth_calls for {} blocks {}-{} (all columns present)",
                    collection_name,
                    range.start,
                    range.end - 1
                );
                continue;
            }
            tracing::info!(
                "Found {} missing factory once columns for {} blocks {}-{}: {:?}",
                missing.len(),
                collection_name,
                range.start,
                range.end - 1,
                missing
            );
            (missing, true)
        } else {
            (all_fn_names.clone(), false)
        };

        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        let mut address_discovery: HashMap<Address, (u64, u64)> = HashMap::new();

        for (block_num, addrs) in &factory_data.addresses_by_block {
            for (timestamp, addr, coll_name) in addrs {
                if coll_name == collection_name {
                    address_discovery.entry(*addr).or_insert((*block_num, *timestamp));
                }
            }
        }

        // Write empty file when no factory addresses discovered (no data for this range)
        if address_discovery.is_empty() && !has_existing_file {
            std::fs::create_dir_all(&sub_dir)?;
            write_once_results_to_parquet(&[], &output_path, &all_fn_names)?;
            let mut index = read_once_column_index(&sub_dir);
            index.insert(file_name.clone(), all_fn_names.clone());
            write_once_column_index(&sub_dir, &index)?;
            if let Some(tx) = decoder_tx {
                let _ = tx.send(DecoderMessage::OnceFileBackfilled {
                    range_start: range.start,
                    range_end: range.end,
                    contract_name: collection_name.clone(),
                }).await;
            }
            continue;
        }

        let mut pending_calls: Vec<(TransactionRequest, BlockId, Address, u64, u64, String)> = Vec::new();

        for (discovered_address, (block_number, timestamp)) in &address_discovery {
            let block_id = BlockId::Number(BlockNumberOrTag::Number(*block_number));

            for call_config in &configs_to_call {
                // Determine target: use override if set, otherwise call the discovered address
                let target_address = call_config.target_addresses
                    .as_ref()
                    .and_then(|addrs| addrs.first().copied())
                    .unwrap_or(*discovered_address);

                // For self-address params, use the discovered address (factory instance)
                let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                    preencoded.clone()
                } else {
                    encode_once_call_params(
                        call_config.function_selector,
                        &call_config.params,
                        *discovered_address,
                    )?
                };
                let tx = TransactionRequest::default()
                    .to(target_address)
                    .input(calldata.into());
                // Store discovered_address for output (keyed by factory instance)
                pending_calls.push((
                    tx,
                    block_id,
                    *discovered_address,
                    *block_number,
                    *timestamp,
                    call_config.function_name.clone(),
                ));
            }
        }

        // Skip only if no pending calls AND no existing file to backfill
        if pending_calls.is_empty() && !has_existing_file {
            continue;
        }

        // Execute batch calls only if there are pending calls
        let results_by_address: HashMap<Address, (u64, u64, HashMap<String, Vec<u8>>)> =
            if !pending_calls.is_empty() {
                let batch_calls: Vec<(TransactionRequest, BlockId)> = pending_calls
                    .iter()
                    .map(|(tx, bid, _, _, _, _)| (tx.clone(), *bid))
                    .collect();

                let batch_results = client.call_batch(batch_calls).await?;

                let mut results_map: HashMap<Address, (u64, u64, HashMap<String, Vec<u8>>)> = HashMap::new();

                for (i, result) in batch_results.into_iter().enumerate() {
                    let (tx, _, address, block_number, timestamp, function_name) = &pending_calls[i];

                    let entry = results_map
                        .entry(*address)
                        .or_insert_with(|| (*block_number, *timestamp, HashMap::new()));

                    match result {
                        Ok(bytes) => {
                            entry.2.insert(function_name.clone(), bytes.to_vec());
                        }
                        Err(e) => {
                            let calldata = tx.input.input.as_ref().map(|b| format!("0x{}", hex::encode(b))).unwrap_or_default();
                            tracing::warn!(
                                "factory once eth_call failed for {}.{} at block {} (address {}, calldata {}): {}",
                                collection_name,
                                function_name,
                                block_number,
                                address,
                                calldata,
                                e
                            );
                            entry.2.insert(function_name.clone(), Vec::new());
                        }
                    }
                }
                results_map
            } else {
                HashMap::new()
            };

        std::fs::create_dir_all(&sub_dir)?;

        if has_existing_file {
            // Merge new columns into existing parquet
            let mut new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = results_by_address
                .into_iter()
                .map(|(addr, (_, _, fns))| (addr.0.0, fns))
                .collect();

            let existing_batches = read_existing_once_parquet(&output_path)?;
            if !existing_batches.is_empty() {
                // Check which addresses already have data for the missing functions
                let existing_results = extract_existing_results_from_parquet(&existing_batches, &missing_fn_names);

                // Find addresses in existing parquet that need backfill
                let existing_addresses = extract_addresses_from_once_parquet(&existing_batches);

                // Build list of (address, block, functions_to_call) for backfill
                // Only call functions that don't already have data
                let mut backfill_calls: Vec<(TransactionRequest, BlockId, [u8; 20], String)> = Vec::new();

                for (addr_bytes, (block_num, _timestamp)) in &existing_addresses {
                    // Skip addresses that are in current batch (already have new results)
                    if new_results.contains_key(addr_bytes) {
                        continue;
                    }

                    let discovered_address = Address::from_slice(addr_bytes);
                    let block_id = BlockId::Number(BlockNumberOrTag::Number(*block_num));

                    // Check which functions this address already has data for
                    let existing_fns = existing_results.get(addr_bytes);

                    for call_config in &configs_to_call {
                        // Skip if this address already has data for this function
                        if let Some(fns) = existing_fns {
                            if fns.contains_key(&call_config.function_name) {
                                continue;
                            }
                        }

                        let target_address = call_config.target_addresses
                            .as_ref()
                            .and_then(|addrs| addrs.first().copied())
                            .unwrap_or(discovered_address);

                        let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                            preencoded.clone()
                        } else {
                            encode_once_call_params(
                                call_config.function_selector,
                                &call_config.params,
                                discovered_address,
                            )?
                        };
                        let tx = TransactionRequest::default()
                            .to(target_address)
                            .input(calldata.into());
                        backfill_calls.push((tx, block_id, *addr_bytes, call_config.function_name.clone()));
                    }
                }

                // Merge existing results into new_results (preserve what we already have)
                for (addr_bytes, fns) in existing_results {
                    let entry = new_results.entry(addr_bytes).or_default();
                    for (fn_name, data) in fns {
                        entry.entry(fn_name).or_insert(data);
                    }
                }

                // Make backfill calls for addresses/functions that don't have data
                if !backfill_calls.is_empty() {
                    tracing::info!(
                        "Backfilling {} calls for factory once columns (addresses missing data)",
                        backfill_calls.len()
                    );

                    let batch_calls: Vec<(TransactionRequest, BlockId)> = backfill_calls
                        .iter()
                        .map(|(tx, bid, _, _)| (tx.clone(), *bid))
                        .collect();

                    let batch_results = client.call_batch(batch_calls).await?;

                    for (i, result) in batch_results.into_iter().enumerate() {
                        let (_, _, addr_bytes, function_name) = &backfill_calls[i];
                        let entry = new_results.entry(*addr_bytes).or_default();
                        match result {
                            Ok(bytes) => {
                                entry.insert(function_name.clone(), bytes.to_vec());
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Backfill factory once eth_call failed for {}.{}: {}",
                                    collection_name,
                                    function_name,
                                    e
                                );
                                entry.insert(function_name.clone(), Vec::new());
                            }
                        }
                    }
                }

                let merged = merge_once_columns(&existing_batches, &new_results, &missing_fn_names)?;

                let file = File::create(&output_path)?;
                let props = WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::SNAPPY)
                    .build();
                let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                writer.write(&merged)?;
                writer.close()?;

                tracing::info!(
                    "Merged {} new factory once columns into {} for {}",
                    missing_fn_names.len(),
                    output_path.display(),
                    collection_name
                );
            }
        } else {
            let results: Vec<OnceCallResult> = results_by_address
                .into_iter()
                .map(|(address, (block_number, timestamp, function_results))| OnceCallResult {
                    block_number,
                    block_timestamp: timestamp,
                    contract_address: address.0.0,
                    function_results,
                })
                .collect();

            if !results.is_empty() {
                tracing::info!(
                    "Writing {} factory once eth_call results to {}",
                    results.len(),
                    output_path.display()
                );

                write_once_results_to_parquet(&results, &output_path, &all_fn_names)?;
            }
        }

        // Update the column index with columns actually present in the file
        let actual_cols: Vec<String> = read_parquet_column_names(&output_path)
            .into_iter()
            .collect();
        let mut index = read_once_column_index(&sub_dir);
        index.insert(file_name.clone(), actual_cols.clone());
        write_once_column_index(&sub_dir, &index)?;
        tracing::info!(
            "Updated column index for factory {}: {} now has {} columns: {:?}",
            collection_name,
            file_name,
            actual_cols.len(),
            actual_cols
        );

        // Notify decoder that this file was updated so it can decode new columns
        if let Some(tx) = decoder_tx {
            let _ = tx
                .send(DecoderMessage::OnceFileBackfilled {
                    range_start: range.start,
                    range_end: range.end,
                    contract_name: collection_name.clone(),
                })
                .await;
        }
    }

    Ok(())
}

/// Process regular once calls using Multicall3 aggregate3
async fn process_once_calls_multicall(
    range: &BlockRange,
    blocks: &[BlockInfo],
    client: &UnifiedRpcClient,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    contracts: &Contracts,
    output_dir: &Path,
    existing_files: &HashSet<String>,
    multicall3_address: Address,
    rpc_batch_size: usize,
) -> Result<(), EthCallCollectionError> {
    let first_block = match blocks.first() {
        Some(b) => b,
        None => return Ok(()),
    };

    // Collect all calls across all contracts into one multicall
    struct OnceCallPending {
        contract_name: String,
        function_name: String,
        address: Address,
    }

    let mut all_slots: Vec<MulticallSlotGeneric<OnceCallMeta>> = Vec::new();
    let mut contracts_to_process: Vec<(String, Vec<String>, Vec<String>, PathBuf, bool)> = Vec::new();

    for (contract_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name();
        let rel_path = format!("{}/once/{}", contract_name, file_name);
        let sub_dir = output_dir.join(contract_name).join("once");
        let output_path = sub_dir.join(&file_name);

        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file) = if existing_files.contains(&rel_path) {
            let index = read_once_column_index(&sub_dir);
            let existing_cols: HashSet<String> = if let Some(cols) = index.get(&file_name) {
                cols.iter().cloned().collect()
            } else {
                read_parquet_column_names(&output_path)
            };
            let missing: Vec<String> = all_fn_names
                .iter()
                .filter(|f| !existing_cols.contains(*f))
                .cloned()
                .collect();
            if missing.is_empty() {
                continue;
            }
            (missing, true)
        } else {
            (all_fn_names.clone(), false)
        };

        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        let contract = match contracts.get(contract_name) {
            Some(c) => c,
            None => continue,
        };

        let default_addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };

        // Build slots for all addresses × functions
        for call_config in &configs_to_call {
            let addresses = call_config.target_addresses.as_ref().unwrap_or(&default_addresses);

            for address in addresses {
                let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                    preencoded.clone()
                } else {
                    encode_once_call_params(
                        call_config.function_selector,
                        &call_config.params,
                        *address,
                    )?
                };

                all_slots.push(MulticallSlotGeneric {
                    block_number: first_block.block_number,
                    block_timestamp: first_block.timestamp,
                    target_address: *address,
                    encoded_calldata: calldata,
                    metadata: OnceCallMeta {
                        contract_name: contract_name.clone(),
                        function_name: call_config.function_name.clone(),
                        contract_address: *address,
                    },
                });
            }
        }

        contracts_to_process.push((
            contract_name.clone(),
            all_fn_names,
            missing_fn_names,
            output_path,
            has_existing_file,
        ));
    }

    if all_slots.is_empty() {
        return Ok(());
    }

    // Build one multicall for all slots (they're all at the same block)
    let block_multicalls = vec![BlockMulticall {
        block_number: first_block.block_number,
        block_id: BlockId::Number(BlockNumberOrTag::Number(first_block.block_number)),
        slots: all_slots,
    }];

    tracing::info!(
        "Executing multicall with {} once calls at block {}",
        block_multicalls[0].slots.len(),
        first_block.block_number
    );

    // Execute multicall
    let results = execute_multicalls_generic(
        client,
        multicall3_address,
        block_multicalls,
        rpc_batch_size,
    )
    .await?;

    // Group results by contract_name -> address -> function_name -> value
    let mut results_by_contract: HashMap<String, HashMap<Address, HashMap<String, Vec<u8>>>> =
        HashMap::new();

    for (meta, return_data, _success) in results {
        results_by_contract
            .entry(meta.contract_name.clone())
            .or_default()
            .entry(meta.contract_address)
            .or_default()
            .insert(meta.function_name, return_data);
    }

    // Write parquet for each contract
    for (contract_name, all_fn_names, missing_fn_names, output_path, has_existing_file) in contracts_to_process {
        let sub_dir = output_path.parent().unwrap();
        std::fs::create_dir_all(sub_dir)?;

        if let Some(results_by_address) = results_by_contract.remove(&contract_name) {
            if has_existing_file {
                let new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = results_by_address
                    .into_iter()
                    .map(|(addr, fns)| (addr.0 .0, fns))
                    .collect();

                let existing_batches = read_existing_once_parquet(&output_path)?;
                if !existing_batches.is_empty() {
                    let merged = merge_once_columns(&existing_batches, &new_results, &missing_fn_names)?;

                    let file = File::create(&output_path)?;
                    let props = WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::SNAPPY)
                        .build();
                    let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                    writer.write(&merged)?;
                    writer.close()?;

                    tracing::info!(
                        "Merged {} new multicall once columns into {} for {}",
                        missing_fn_names.len(),
                        output_path.display(),
                        contract_name
                    );
                }
            } else {
                let results: Vec<OnceCallResult> = results_by_address
                    .into_iter()
                    .map(|(address, function_results)| OnceCallResult {
                        block_number: first_block.block_number,
                        block_timestamp: first_block.timestamp,
                        contract_address: address.0 .0,
                        function_results,
                    })
                    .collect();

                if !results.is_empty() {
                    tracing::info!(
                        "Writing {} multicall once eth_call results to {}",
                        results.len(),
                        output_path.display()
                    );

                    write_once_results_to_parquet(&results, &output_path, &all_fn_names)?;
                }
            }

            // Update column index
            let file_name = output_path.file_name().unwrap().to_string_lossy().to_string();
            let actual_cols: Vec<String> = read_parquet_column_names(&output_path)
                .into_iter()
                .collect();
            let mut index = read_once_column_index(sub_dir);
            index.insert(file_name.clone(), actual_cols.clone());
            write_once_column_index(sub_dir, &index)?;
        }
    }

    Ok(())
}

/// Process factory once calls using Multicall3 aggregate3
async fn process_factory_once_calls_multicall(
    range: &BlockRange,
    client: &UnifiedRpcClient,
    factory_data: &FactoryAddressData,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    output_dir: &Path,
    existing_files: &HashSet<String>,
    column_indexes: &HashMap<String, HashMap<String, Vec<String>>>,
    multicall3_address: Address,
    rpc_batch_size: usize,
) -> Result<(), EthCallCollectionError> {
    // Collect all calls across all collections into one multicall
    #[derive(Clone)]
    struct FactoryOnceSlotMeta {
        collection_name: String,
        function_name: String,
        address: Address,
        block_number: u64,
        block_timestamp: u64,
    }

    let mut all_slots: Vec<MulticallSlotGeneric<FactoryOnceSlotMeta>> = Vec::new();
    // (collection_name, all_fn_names, missing_fn_names, output_path, has_existing_file, configs_to_call)
    let mut collections_to_process: Vec<(String, Vec<String>, Vec<String>, PathBuf, bool, Vec<OnceCallConfig>)> = Vec::new();

    for (collection_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name();
        let rel_path = format!("{}/once/{}", collection_name, file_name);
        let sub_dir = output_dir.join(collection_name).join("once");
        let output_path = sub_dir.join(&file_name);

        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file) = if existing_files.contains(&rel_path) {
            let index = column_indexes.get(collection_name);
            let existing_cols: HashSet<String> = index
                .and_then(|idx| idx.get(&file_name))
                .map(|cols| cols.iter().cloned().collect())
                .unwrap_or_default();

            let missing: Vec<String> = all_fn_names
                .iter()
                .filter(|f| !existing_cols.contains(*f))
                .cloned()
                .collect();
            if missing.is_empty() {
                continue;
            }
            (missing, true)
        } else {
            (all_fn_names.clone(), false)
        };

        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        // Build address discovery map
        let mut address_discovery: HashMap<Address, (u64, u64)> = HashMap::new();
        for (block_num, addrs) in &factory_data.addresses_by_block {
            for (timestamp, addr, coll_name) in addrs {
                if coll_name == collection_name {
                    address_discovery.entry(*addr).or_insert((*block_num, *timestamp));
                }
            }
        }

        // Write empty file when no factory addresses discovered (no data for this range)
        if address_discovery.is_empty() && !has_existing_file {
            std::fs::create_dir_all(&sub_dir)?;
            write_once_results_to_parquet(&[], &output_path, &all_fn_names)?;
            let mut index = read_once_column_index(&sub_dir);
            index.insert(file_name.clone(), all_fn_names.clone());
            write_once_column_index(&sub_dir, &index)?;
            continue;
        }

        // Build slots for all addresses × functions
        // Always iterate over discovered factory addresses, using target override only for RPC call
        for call_config in &configs_to_call {
            for (discovered_address, (block_num, timestamp)) in &address_discovery {
                // Use target override for RPC call if set, otherwise use discovered address
                let target_address = call_config.target_addresses
                    .as_ref()
                    .and_then(|addrs| addrs.first().copied())
                    .unwrap_or(*discovered_address);

                // Encode calldata with DISCOVERED address (for self params)
                let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                    preencoded.clone()
                } else {
                    encode_once_call_params(
                        call_config.function_selector,
                        &call_config.params,
                        *discovered_address,
                    )?
                };

                all_slots.push(MulticallSlotGeneric {
                    block_number: *block_num,
                    block_timestamp: *timestamp,
                    target_address,  // Target for RPC call (may be override)
                    encoded_calldata: calldata,
                    metadata: FactoryOnceSlotMeta {
                        collection_name: collection_name.clone(),
                        function_name: call_config.function_name.clone(),
                        address: *discovered_address,  // Store discovered address
                        block_number: *block_num,
                        block_timestamp: *timestamp,
                    },
                });
            }
        }

        // Clone configs for use in backfill phase
        let configs_for_backfill: Vec<OnceCallConfig> = configs_to_call
            .iter()
            .map(|c| (*c).clone())
            .collect();

        // Always add to collections_to_process - even with empty address_discovery,
        // existing files may need backfill for missing columns
        collections_to_process.push((
            collection_name.clone(),
            all_fn_names,
            missing_fn_names,
            output_path,
            has_existing_file,
            configs_for_backfill,
        ));
    }

    // Skip multicall execution only if there are no slots AND no collections need backfill
    let any_need_backfill = collections_to_process.iter().any(|(_, _, _, _, has_existing, _)| *has_existing);
    if all_slots.is_empty() && !any_need_backfill {
        return Ok(());
    }

    // Group results by collection_name -> address -> (block_num, timestamp, function_results)
    let mut results_by_collection: HashMap<String, HashMap<Address, (u64, u64, HashMap<String, Vec<u8>>)>> =
        HashMap::new();

    // Execute multicalls only if there are slots to process
    if !all_slots.is_empty() {
        // Group slots by block for multicall batching
        let mut slots_by_block: HashMap<u64, Vec<MulticallSlotGeneric<FactoryOnceSlotMeta>>> = HashMap::new();
        for slot in all_slots {
            slots_by_block.entry(slot.block_number).or_default().push(slot);
        }

        let mut block_multicalls: Vec<BlockMulticall<FactoryOnceSlotMeta>> = Vec::new();
        for (block_number, slots) in slots_by_block {
            block_multicalls.push(BlockMulticall {
                block_number,
                block_id: BlockId::Number(BlockNumberOrTag::Number(block_number)),
                slots,
            });
        }

        tracing::info!(
            "Executing {} multicalls for factory once calls in blocks {}-{}",
            block_multicalls.len(),
            range.start,
            range.end - 1
        );

        // Execute multicalls
        let results = execute_multicalls_generic(
            client,
            multicall3_address,
            block_multicalls,
            rpc_batch_size,
        )
        .await?;

        for (meta, return_data, _success) in results {
            let entry = results_by_collection
                .entry(meta.collection_name.clone())
                .or_default()
                .entry(meta.address)
                .or_insert((meta.block_number, meta.block_timestamp, HashMap::new()));
            entry.2.insert(meta.function_name, return_data);
        }
    }

    // Write parquet for each collection
    for (collection_name, all_fn_names, missing_fn_names, output_path, has_existing_file, configs_to_call) in collections_to_process {
        let sub_dir = output_path.parent().unwrap();
        std::fs::create_dir_all(sub_dir)?;

        // Get results for this collection (may be None if no new addresses discovered)
        let results_by_address = results_by_collection.remove(&collection_name);

        // Process if we have results OR if existing file needs backfill
        if results_by_address.is_some() || has_existing_file {
            let results_by_address = results_by_address.unwrap_or_default();

            if has_existing_file {
                let mut new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = results_by_address
                    .into_iter()
                    .map(|(addr, (_, _, fns))| (addr.0 .0, fns))
                    .collect();

                let existing_batches = read_existing_once_parquet(&output_path)?;
                if !existing_batches.is_empty() {
                    // Check which addresses already have data for the missing functions
                    let existing_results = extract_existing_results_from_parquet(&existing_batches, &missing_fn_names);

                    // Find addresses in existing parquet
                    let existing_addresses = extract_addresses_from_once_parquet(&existing_batches);

                    // Build backfill slots only for addresses/functions that don't have data
                    let mut backfill_slots: Vec<MulticallSlotGeneric<FactoryOnceSlotMeta>> = Vec::new();

                    if !configs_to_call.is_empty() {
                        for (addr_bytes, (block_num, timestamp)) in &existing_addresses {
                            // Skip addresses that are in current batch (already have new results)
                            if new_results.contains_key(addr_bytes) {
                                continue;
                            }

                            let discovered_address = Address::from_slice(addr_bytes);

                            // Check which functions this address already has data for
                            let existing_fns = existing_results.get(addr_bytes);

                            for call_config in &configs_to_call {
                                // Skip if this address already has data for this function
                                if let Some(fns) = existing_fns {
                                    if fns.contains_key(&call_config.function_name) {
                                        continue;
                                    }
                                }

                                let target_address = call_config.target_addresses
                                    .as_ref()
                                    .and_then(|addrs| addrs.first().copied())
                                    .unwrap_or(discovered_address);

                                let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                                    preencoded.clone()
                                } else {
                                    encode_once_call_params(
                                        call_config.function_selector,
                                        &call_config.params,
                                        discovered_address,
                                    )?
                                };

                                backfill_slots.push(MulticallSlotGeneric {
                                    block_number: *block_num,
                                    block_timestamp: *timestamp,
                                    target_address,
                                    encoded_calldata: calldata,
                                    metadata: FactoryOnceSlotMeta {
                                        collection_name: collection_name.clone(),
                                        function_name: call_config.function_name.clone(),
                                        address: discovered_address,
                                        block_number: *block_num,
                                        block_timestamp: *timestamp,
                                    },
                                });
                            }
                        }
                    }

                    // Merge existing results into new_results (preserve what we already have)
                    for (addr_bytes, fns) in existing_results {
                        let entry = new_results.entry(addr_bytes).or_default();
                        for (fn_name, data) in fns {
                            entry.entry(fn_name).or_insert(data);
                        }
                    }

                    // Execute backfill multicalls for addresses/functions that don't have data
                    if !backfill_slots.is_empty() {
                        tracing::info!(
                            "Backfilling {} multicall slots for factory once columns (addresses missing data)",
                            backfill_slots.len()
                        );

                        // Group by block for multicall
                        let mut slots_by_block: HashMap<u64, Vec<MulticallSlotGeneric<FactoryOnceSlotMeta>>> = HashMap::new();
                        for slot in backfill_slots {
                            slots_by_block.entry(slot.block_number).or_default().push(slot);
                        }

                        let backfill_multicalls: Vec<BlockMulticall<FactoryOnceSlotMeta>> = slots_by_block
                            .into_iter()
                            .map(|(block_number, slots)| BlockMulticall {
                                block_number,
                                block_id: BlockId::Number(BlockNumberOrTag::Number(block_number)),
                                slots,
                            })
                            .collect();

                        let backfill_results = execute_multicalls_generic(
                            client,
                            multicall3_address,
                            backfill_multicalls,
                            rpc_batch_size,
                        )
                        .await?;

                        for (meta, return_data, _success) in backfill_results {
                            let addr_bytes: [u8; 20] = meta.address.0.0;
                            let entry = new_results.entry(addr_bytes).or_default();
                            entry.insert(meta.function_name, return_data);
                        }
                    }

                    let merged = merge_once_columns(&existing_batches, &new_results, &missing_fn_names)?;

                    let file = File::create(&output_path)?;
                    let props = WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::SNAPPY)
                        .build();
                    let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                    writer.write(&merged)?;
                    writer.close()?;

                    tracing::info!(
                        "Merged {} new multicall factory once columns into {} for {}",
                        missing_fn_names.len(),
                        output_path.display(),
                        collection_name
                    );
                }
            } else {
                let results: Vec<OnceCallResult> = results_by_address
                    .into_iter()
                    .map(|(address, (block_number, timestamp, function_results))| OnceCallResult {
                        block_number,
                        block_timestamp: timestamp,
                        contract_address: address.0 .0,
                        function_results,
                    })
                    .collect();

                if !results.is_empty() {
                    tracing::info!(
                        "Writing {} multicall factory once eth_call results to {}",
                        results.len(),
                        output_path.display()
                    );

                    write_once_results_to_parquet(&results, &output_path, &all_fn_names)?;
                }
            }

            // Update column index
            let file_name = output_path.file_name().unwrap().to_string_lossy().to_string();
            let actual_cols: Vec<String> = read_parquet_column_names(&output_path)
                .into_iter()
                .collect();
            let mut index = read_once_column_index(sub_dir);
            index.insert(file_name.clone(), actual_cols.clone());
            write_once_column_index(sub_dir, &index)?;
        }
    }

    Ok(())
}

fn compute_function_selector(signature: &str) -> [u8; 4] {
    use alloy::primitives::keccak256;
    let hash = keccak256(signature.as_bytes());
    let mut selector = [0u8; 4];
    selector.copy_from_slice(&hash[0..4]);
    selector
}

async fn process_range(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    client: &UnifiedRpcClient,
    call_configs: &[CallConfig],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    rpc_batch_size: usize,
    max_params: usize,
    frequency_state: &mut FrequencyState,
) -> Result<(), EthCallCollectionError> {
    let mut grouped_configs: HashMap<(String, String), Vec<&CallConfig>> = HashMap::new();
    for config in call_configs {
        grouped_configs
            .entry((
                config.contract_name.clone(),
                config.function_name.clone(),
            ))
            .or_default()
            .push(config);
    }

    for ((contract_name, function_name), configs) in &grouped_configs {
        let file_name = range.file_name();
        let rel_path = format!("{}/{}/{}", contract_name, function_name, file_name);

        if existing_files.contains(&rel_path) {
            tracing::debug!(
                "Skipping eth_calls for {}.{} blocks {}-{} (already exists)",
                contract_name,
                function_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        let frequency = &configs[0].frequency;

        let state_key = (contract_name.clone(), function_name.clone());
        let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();

        let filtered_blocks = filter_blocks_for_frequency(&blocks, frequency, last_call_ts);

        if filtered_blocks.is_empty() {
            tracing::debug!(
                "No blocks match frequency {:?} for {}.{} in range {}-{}",
                frequency,
                contract_name,
                function_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        tracing::info!(
            "Fetching eth_calls for {}.{} blocks {}-{} ({} blocks after frequency filter)",
            contract_name,
            function_name,
            range.start,
            range.end - 1,
            filtered_blocks.len()
        );

        #[cfg(feature = "bench")]
        let mut rpc_time = std::time::Duration::ZERO;
        #[cfg(feature = "bench")]
        let mut process_time = std::time::Duration::ZERO;

        let mut all_results: Vec<CallResult> = Vec::new();

        let mut pending_calls: Vec<(TransactionRequest, BlockId, &BlockInfo, &CallConfig)> = Vec::new();

        for block in &filtered_blocks {
            for config in configs {
                let tx = TransactionRequest::default()
                    .to(config.address)
                    .input(config.encoded_calldata.clone().into());
                let block_id = BlockId::Number(BlockNumberOrTag::Number(block.block_number));
                pending_calls.push((tx, block_id, block, config));
            }
        }

        for chunk in pending_calls.chunks(rpc_batch_size) {
            let calls: Vec<(TransactionRequest, BlockId)> = chunk
                .iter()
                .map(|(tx, block_id, _, _)| (tx.clone(), *block_id))
                .collect();

            #[cfg(feature = "bench")]
            let rpc_start = Instant::now();
            let results = client.call_batch(calls).await?;
            #[cfg(feature = "bench")]
            {
                rpc_time += rpc_start.elapsed();
            }

            #[cfg(feature = "bench")]
            let process_start = Instant::now();
            for (i, result) in results.into_iter().enumerate() {
                let (_, _, block, config) = &chunk[i];
                match result {
                    Ok(bytes) => {
                        all_results.push(CallResult {
                            block_number: block.block_number,
                            block_timestamp: block.timestamp,
                            contract_address: config.address.0 .0,
                            value_bytes: bytes.to_vec(),
                            param_values: config.param_values.clone(),
                        });
                    }
                    Err(e) => {
                        let params_hex: Vec<String> = config.param_values.iter().map(|p| format!("0x{}", hex::encode(p))).collect();
                        tracing::warn!(
                            "eth_call failed for {}.{} at block {} (address {}, params {:?}): {}",
                            contract_name,
                            function_name,
                            block.block_number,
                            config.address,
                            params_hex,
                            e
                        );
                        all_results.push(CallResult {
                            block_number: block.block_number,
                            block_timestamp: block.timestamp,
                            contract_address: config.address.0 .0,
                            value_bytes: Vec::new(),
                            param_values: config.param_values.clone(),
                        });
                    }
                }
            }
            #[cfg(feature = "bench")]
            {
                process_time += process_start.elapsed();
            }
        }

        all_results.sort_by_key(|r| (r.block_number, r.contract_address, r.param_values.clone()));

        let result_count = all_results.len();
        let sub_dir = output_dir.join(contract_name).join(function_name);
        std::fs::create_dir_all(&sub_dir)?;
        let output_path = sub_dir.join(&file_name);
        #[cfg(feature = "bench")]
        let write_start = Instant::now();
        tokio::task::spawn_blocking(move || {
            write_results_to_parquet(&all_results, &output_path, max_params)
        })
        .await
        .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;
        #[cfg(feature = "bench")]
        {
            let write_time = write_start.elapsed();
            crate::bench::record(
                &format!("eth_calls_{}.{}", contract_name, function_name),
                range.start,
                range.end,
                result_count,
                rpc_time,
                process_time,
                write_time,
            );
        }

        tracing::info!(
            "Wrote {} eth_call results to {}",
            result_count,
            sub_dir.join(&file_name).display()
        );

        if let Frequency::Duration(_) = frequency {
            if let Some(last_block) = filtered_blocks.last() {
                frequency_state.last_call_times.insert(
                    (contract_name.clone(), function_name.clone()),
                    last_block.timestamp,
                );
            }
        }
    }

    Ok(())
}

/// Process regular calls using Multicall3 aggregate3 to batch all calls per block
async fn process_range_multicall(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    client: &UnifiedRpcClient,
    call_configs: &[CallConfig],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    rpc_batch_size: usize,
    max_params: usize,
    frequency_state: &mut FrequencyState,
    multicall3_address: Address,
) -> Result<(), EthCallCollectionError> {
    // Group configs by (contract_name, function_name)
    let mut grouped_configs: HashMap<(String, String), Vec<&CallConfig>> = HashMap::new();
    for config in call_configs {
        grouped_configs
            .entry((config.contract_name.clone(), config.function_name.clone()))
            .or_default()
            .push(config);
    }

    // Determine which groups actually need processing
    struct GroupInfo<'a> {
        contract_name: String,
        function_name: String,
        configs: Vec<&'a CallConfig>,
        filtered_blocks: Vec<BlockInfo>,
        frequency: Frequency,
    }

    let mut active_groups: Vec<GroupInfo> = Vec::new();

    for ((contract_name, function_name), configs) in &grouped_configs {
        let file_name = range.file_name();
        let rel_path = format!("{}/{}/{}", contract_name, function_name, file_name);

        if existing_files.contains(&rel_path) {
            tracing::debug!(
                "Skipping eth_calls for {}.{} blocks {}-{} (already exists)",
                contract_name,
                function_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        let frequency = &configs[0].frequency;
        let state_key = (contract_name.clone(), function_name.clone());
        let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();
        let filtered_blocks = filter_blocks_for_frequency(&blocks, frequency, last_call_ts);

        if filtered_blocks.is_empty() {
            continue;
        }

        active_groups.push(GroupInfo {
            contract_name: contract_name.clone(),
            function_name: function_name.clone(),
            configs: configs.clone(),
            filtered_blocks: filtered_blocks.into_iter().cloned().collect(),
            frequency: frequency.clone(),
        });
    }

    if active_groups.is_empty() {
        return Ok(());
    }

    // Collect all unique blocks across all groups
    let mut all_block_numbers: HashSet<u64> = HashSet::new();
    for group in &active_groups {
        for block in &group.filtered_blocks {
            all_block_numbers.insert(block.block_number);
        }
    }
    let mut sorted_blocks: Vec<u64> = all_block_numbers.into_iter().collect();
    sorted_blocks.sort_unstable();

    // Build block_number -> BlockInfo lookup
    let mut block_info_map: HashMap<u64, &BlockInfo> = HashMap::new();
    for group in &active_groups {
        for block in &group.filtered_blocks {
            block_info_map.entry(block.block_number).or_insert(block);
        }
    }

    // Track which blocks each group needs
    let group_block_sets: Vec<HashSet<u64>> = active_groups
        .iter()
        .map(|g| g.filtered_blocks.iter().map(|b| b.block_number).collect())
        .collect();

    // Build per-block multicalls with block info included in metadata
    let mut block_multicalls: Vec<BlockMulticall<(RegularCallMeta, u64, u64)>> = Vec::new();

    for &block_number in &sorted_blocks {
        let mut slots: Vec<MulticallSlotGeneric<(RegularCallMeta, u64, u64)>> = Vec::new();
        let block_info = block_info_map[&block_number];

        for (group_idx, group) in active_groups.iter().enumerate() {
            if !group_block_sets[group_idx].contains(&block_number) {
                continue;
            }
            for config in &group.configs {
                slots.push(MulticallSlotGeneric {
                    block_number,
                    block_timestamp: block_info.timestamp,
                    target_address: config.address,
                    encoded_calldata: config.encoded_calldata.clone(),
                    metadata: (
                        RegularCallMeta {
                            contract_name: group.contract_name.clone(),
                            function_name: group.function_name.clone(),
                            contract_address: config.address,
                            param_values: config.param_values.clone(),
                        },
                        block_number,
                        block_info.timestamp,
                    ),
                });
            }
        }

        if !slots.is_empty() {
            block_multicalls.push(BlockMulticall {
                block_number,
                block_id: BlockId::Number(BlockNumberOrTag::Number(block_number)),
                slots,
            });
        }
    }

    tracing::info!(
        "Executing {} multicalls for {} regular call groups in blocks {}-{}",
        block_multicalls.len(),
        active_groups.len(),
        range.start,
        range.end - 1
    );

    // Execute all multicalls
    let results = execute_multicalls_generic(
        client,
        multicall3_address,
        block_multicalls,
        rpc_batch_size,
    )
    .await?;

    // Distribute results back to groups
    let mut group_results: HashMap<(String, String), Vec<CallResult>> = HashMap::new();
    for group in &active_groups {
        group_results.insert(
            (group.contract_name.clone(), group.function_name.clone()),
            Vec::new(),
        );
    }

    for ((meta, block_number, block_timestamp), return_data, _success) in results {
        let key = (meta.contract_name.clone(), meta.function_name.clone());
        if let Some(results) = group_results.get_mut(&key) {
            results.push(CallResult {
                block_number,
                block_timestamp,
                contract_address: meta.contract_address.0 .0,
                value_bytes: return_data,
                param_values: meta.param_values,
            });
        }
    }

    // Write parquet for each group
    for group in &active_groups {
        let key = (group.contract_name.clone(), group.function_name.clone());
        if let Some(results) = group_results.get_mut(&key) {
            results.sort_by_key(|r| (r.block_number, r.contract_address, r.param_values.clone()));

            let result_count = results.len();
            let file_name = range.file_name();
            let sub_dir = output_dir.join(&group.contract_name).join(&group.function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            let results_owned = std::mem::take(results);
            tokio::task::spawn_blocking(move || {
                write_results_to_parquet(&results_owned, &output_path, max_params)
            })
            .await
            .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

            tracing::info!(
                "Wrote {} multicall eth_call results to {}",
                result_count,
                sub_dir.join(&file_name).display()
            );

            if let Frequency::Duration(_) = &group.frequency {
                if let Some(last_block) = group.filtered_blocks.last() {
                    frequency_state
                        .last_call_times
                        .insert(key.clone(), last_block.timestamp);
                }
            }
        }
    }

    Ok(())
}

/// Build aggregate3 calldata for a batch of (target, calldata) pairs.
/// Encodes as: aggregate3((address target, bool allowFailure, bytes callData)[])
fn build_multicall_calldata(calls: &[(Address, &Bytes)]) -> Bytes {
    // aggregate3((address,bool,bytes)[]) selector
    let selector = compute_function_selector("aggregate3((address,bool,bytes)[])");

    // Each element is a tuple of (address, bool, bytes)
    let call_tuples: Vec<DynSolValue> = calls
        .iter()
        .map(|(addr, data)| {
            DynSolValue::Tuple(vec![
                DynSolValue::Address(*addr),
                DynSolValue::Bool(true), // allowFailure
                DynSolValue::Bytes(data.to_vec()),
            ])
        })
        .collect();

    let params = vec![DynSolValue::Array(call_tuples)];
    encode_call_with_params(selector, &params)
}

/// Decode the return data from an aggregate3 call.
/// Returns Vec<(success: bool, returnData: Vec<u8>)>.
fn decode_multicall_results(
    return_data: &[u8],
    expected_count: usize,
) -> Result<Vec<(bool, Vec<u8>)>, String> {
    // Return type is (bool success, bytes returnData)[]
    let result_type = DynSolType::Array(Box::new(DynSolType::Tuple(vec![
        DynSolType::Bool,
        DynSolType::Bytes,
    ])));

    let decoded = result_type
        .abi_decode(return_data)
        .map_err(|e| format!("Failed to decode multicall results: {}", e))?;

    let results_array = match decoded {
        DynSolValue::Array(arr) => arr,
        _ => return Err("Expected array from multicall decode".to_string()),
    };

    if results_array.len() != expected_count {
        return Err(format!(
            "Multicall returned {} results, expected {}",
            results_array.len(),
            expected_count
        ));
    }

    let mut results = Vec::with_capacity(expected_count);
    for item in results_array {
        match item {
            DynSolValue::Tuple(fields) if fields.len() == 2 => {
                let success = match &fields[0] {
                    DynSolValue::Bool(b) => *b,
                    _ => false,
                };
                let data = match &fields[1] {
                    DynSolValue::Bytes(b) => b.clone(),
                    _ => Vec::new(),
                };
                results.push((success, data));
            }
            _ => {
                results.push((false, Vec::new()));
            }
        }
    }

    Ok(results)
}

/// Tracks which group and config index a multicall slot maps back to
struct MulticallSlot<'a> {
    group_key: (String, String), // (token_name, function_name)
    config: &'a TokenCallConfig,
    block: BlockInfo,
}

// =============================================================================
// Generic Multicall Infrastructure
// =============================================================================

/// Generic slot for tracking call metadata through multicall execution
#[derive(Clone)]
struct MulticallSlotGeneric<M> {
    block_number: u64,
    block_timestamp: u64,
    target_address: Address,
    encoded_calldata: Bytes,
    metadata: M,
}

/// Pending multicall for a single block
struct BlockMulticall<M> {
    block_number: u64,
    block_id: BlockId,
    slots: Vec<MulticallSlotGeneric<M>>,
}

/// Metadata for regular calls
#[derive(Clone)]
struct RegularCallMeta {
    contract_name: String,
    function_name: String,
    contract_address: Address,
    param_values: Vec<Vec<u8>>,
}

/// Metadata for factory calls
#[derive(Clone)]
struct FactoryCallMeta {
    collection_name: String,
    function_name: String,
    contract_address: Address,
    param_values: Vec<Vec<u8>>,
}

/// Metadata for event-triggered calls
#[derive(Clone)]
struct EventCallMeta {
    contract_name: String,
    function_name: String,
    target_address: Address,
    log_index: u32,
    param_values: Vec<Vec<u8>>,
}

/// Metadata for once calls
#[derive(Clone)]
struct OnceCallMeta {
    contract_name: String,
    function_name: String,
    contract_address: Address,
}

/// Execute multicalls and return (metadata, return_data, success) for each slot
async fn execute_multicalls_generic<M: Clone>(
    client: &UnifiedRpcClient,
    multicall3_address: Address,
    block_multicalls: Vec<BlockMulticall<M>>,
    rpc_batch_size: usize,
) -> Result<Vec<(M, Vec<u8>, bool)>, EthCallCollectionError> {
    let mut all_results: Vec<(M, Vec<u8>, bool)> = Vec::new();

    for chunk in block_multicalls.chunks(rpc_batch_size) {
        let calls: Vec<(TransactionRequest, BlockId)> = chunk
            .iter()
            .map(|bm| {
                let sub_calls: Vec<(Address, &Bytes)> = bm
                    .slots
                    .iter()
                    .map(|s| (s.target_address, &s.encoded_calldata))
                    .collect();
                let multicall_data = build_multicall_calldata(&sub_calls);
                let tx = TransactionRequest::default()
                    .to(multicall3_address)
                    .input(multicall_data.into());
                (tx, bm.block_id)
            })
            .collect();

        let results = client.call_batch(calls).await?;

        for (i, result) in results.into_iter().enumerate() {
            let bm = &chunk[i];
            let slot_count = bm.slots.len();

            match result {
                Ok(bytes) => {
                    match decode_multicall_results(&bytes, slot_count) {
                        Ok(decoded) => {
                            for (j, (success, return_data)) in decoded.into_iter().enumerate() {
                                let slot = &bm.slots[j];
                                all_results.push((
                                    slot.metadata.clone(),
                                    if success { return_data } else { Vec::new() },
                                    success,
                                ));
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to decode multicall results for block {}: {}",
                                bm.block_number,
                                e
                            );
                            // Treat all sub-calls as failed
                            for slot in &bm.slots {
                                all_results.push((slot.metadata.clone(), Vec::new(), false));
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Multicall RPC failed for block {}: {}",
                        bm.block_number,
                        e
                    );
                    // Treat all sub-calls as failed
                    for slot in &bm.slots {
                        all_results.push((slot.metadata.clone(), Vec::new(), false));
                    }
                }
            }
        }
    }

    Ok(all_results)
}

// =============================================================================
// End Generic Multicall Infrastructure
// =============================================================================

/// Process token pool calls using Multicall3 aggregate3 to batch all calls per block
async fn process_token_range_multicall(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    client: &UnifiedRpcClient,
    token_configs: &[TokenCallConfig],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    rpc_batch_size: usize,
    frequency_state: &mut FrequencyState,
    multicall3_address: Address,
) -> Result<(), EthCallCollectionError> {
    // Group configs by (token_name, function_name) — same as process_token_range
    let mut grouped_configs: HashMap<(String, String), Vec<&TokenCallConfig>> = HashMap::new();
    for config in token_configs {
        grouped_configs
            .entry((config.token_name.clone(), config.function_name.clone()))
            .or_default()
            .push(config);
    }

    // Determine which groups actually need processing (not already on disk)
    // and compute their filtered blocks
    struct GroupInfo<'a> {
        output_name: String,
        function_name: String,
        configs: Vec<&'a TokenCallConfig>,
        filtered_blocks: Vec<BlockInfo>,
        frequency: Frequency,
    }

    let mut active_groups: Vec<GroupInfo> = Vec::new();

    for ((token_name, function_name), configs) in &grouped_configs {
        let output_name = format!("{}_pool", token_name);
        let file_name = range.file_name();
        let rel_path = format!("{}/{}/{}", output_name, function_name, file_name);

        if existing_files.contains(&rel_path) {
            tracing::debug!(
                "Skipping token calls for {}.{} blocks {}-{} (already exists)",
                output_name,
                function_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        let frequency = &configs[0].frequency;
        let state_key = (output_name.clone(), function_name.clone());
        let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();
        let filtered_blocks = filter_blocks_for_frequency(&blocks, frequency, last_call_ts);

        if filtered_blocks.is_empty() {
            continue;
        }

        active_groups.push(GroupInfo {
            output_name,
            function_name: function_name.clone(),
            configs: configs.clone(),
            filtered_blocks: filtered_blocks.into_iter().cloned().collect(),
            frequency: frequency.clone(),
        });
    }

    if active_groups.is_empty() {
        return Ok(());
    }

    // Collect all unique blocks we need to make calls for
    let mut all_block_numbers: HashSet<u64> = HashSet::new();
    for group in &active_groups {
        for block in &group.filtered_blocks {
            all_block_numbers.insert(block.block_number);
        }
    }
    let mut sorted_blocks: Vec<u64> = all_block_numbers.into_iter().collect();
    sorted_blocks.sort_unstable();

    // Build a block_number -> BlockInfo lookup from all groups
    let mut block_info_map: HashMap<u64, &BlockInfo> = HashMap::new();
    for group in &active_groups {
        for block in &group.filtered_blocks {
            block_info_map.entry(block.block_number).or_insert(block);
        }
    }

    // For each group, build a set of which blocks it needs
    let group_block_sets: Vec<HashSet<u64>> = active_groups
        .iter()
        .map(|g| g.filtered_blocks.iter().map(|b| b.block_number).collect())
        .collect();

    // Build per-block multicalls and track slot mappings
    // Each "pending multicall" is one aggregate3 call for one block
    struct PendingMulticall<'a> {
        block_number: u64,
        block_id: BlockId,
        slots: Vec<MulticallSlot<'a>>,
    }

    let mut pending_multicalls: Vec<PendingMulticall> = Vec::new();

    for &block_number in &sorted_blocks {
        let mut sub_calls: Vec<(Address, &Bytes)> = Vec::new();
        let mut slots: Vec<MulticallSlot> = Vec::new();

        let block_info = block_info_map[&block_number];

        for (group_idx, group) in active_groups.iter().enumerate() {
            if !group_block_sets[group_idx].contains(&block_number) {
                continue;
            }
            for config in &group.configs {
                sub_calls.push((config.target_address, &config.encoded_calldata));
                slots.push(MulticallSlot {
                    group_key: (group.output_name.clone(), group.function_name.clone()),
                    config,
                    block: block_info.clone(),
                });
            }
        }

        if !sub_calls.is_empty() {
            let multicall_data = build_multicall_calldata(&sub_calls);
            let tx = TransactionRequest::default()
                .to(multicall3_address)
                .input(multicall_data.into());
            let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));

            // We store the tx temporarily — we'll batch them below
            // But we need to associate slots with each multicall
            pending_multicalls.push(PendingMulticall {
                block_number,
                block_id,
                slots,
            });
        }
    }

    // Per-group results accumulator
    let mut group_results: HashMap<(String, String), Vec<CallResult>> = HashMap::new();
    for group in &active_groups {
        group_results.insert(
            (group.output_name.clone(), group.function_name.clone()),
            Vec::new(),
        );
    }

    // Execute multicalls in RPC batch chunks
    for chunk in pending_multicalls.chunks(rpc_batch_size) {
        let calls: Vec<(TransactionRequest, BlockId)> = chunk
            .iter()
            .map(|pm| {
                // Rebuild the multicall TX for this block
                let sub_calls: Vec<(Address, &Bytes)> = pm
                    .slots
                    .iter()
                    .map(|s| (s.config.target_address, &s.config.encoded_calldata))
                    .collect();
                let multicall_data = build_multicall_calldata(&sub_calls);
                let tx = TransactionRequest::default()
                    .to(multicall3_address)
                    .input(multicall_data.into());
                (tx, pm.block_id)
            })
            .collect();

        let results = client.call_batch(calls).await?;

        for (i, result) in results.into_iter().enumerate() {
            let pm = &chunk[i];
            let slot_count = pm.slots.len();

            match result {
                Ok(bytes) => {
                    match decode_multicall_results(&bytes, slot_count) {
                        Ok(decoded) => {
                            for (j, (success, return_data)) in decoded.into_iter().enumerate() {
                                let slot = &pm.slots[j];
                                let value_bytes = if success {
                                    return_data
                                } else {
                                    Vec::new()
                                };
                                if let Some(results) = group_results.get_mut(&slot.group_key) {
                                    results.push(CallResult {
                                        block_number: slot.block.block_number,
                                        block_timestamp: slot.block.timestamp,
                                        contract_address: slot.config.target_address.0 .0,
                                        value_bytes,
                                        param_values: Vec::new(),
                                    });
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to decode multicall results for block {}: {}",
                                pm.block_number,
                                e
                            );
                            // Treat all sub-calls as failed
                            for slot in &pm.slots {
                                if let Some(results) = group_results.get_mut(&slot.group_key) {
                                    results.push(CallResult {
                                        block_number: slot.block.block_number,
                                        block_timestamp: slot.block.timestamp,
                                        contract_address: slot.config.target_address.0 .0,
                                        value_bytes: Vec::new(),
                                        param_values: Vec::new(),
                                    });
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Multicall RPC failed for block {}: {}",
                        pm.block_number,
                        e
                    );
                    // Treat all sub-calls as failed
                    for slot in &pm.slots {
                        if let Some(results) = group_results.get_mut(&slot.group_key) {
                            results.push(CallResult {
                                block_number: slot.block.block_number,
                                block_timestamp: slot.block.timestamp,
                                contract_address: slot.config.target_address.0 .0,
                                value_bytes: Vec::new(),
                                param_values: Vec::new(),
                            });
                        }
                    }
                }
            }
        }
    }

    // Write parquet for each group — identical to process_token_range
    for group in &active_groups {
        let key = (group.output_name.clone(), group.function_name.clone());
        if let Some(results) = group_results.get_mut(&key) {
            results.sort_by_key(|r| (r.block_number, r.contract_address));

            let result_count = results.len();
            let file_name = range.file_name();
            let sub_dir = output_dir.join(&group.output_name).join(&group.function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            let results_owned = std::mem::take(results);
            tokio::task::spawn_blocking(move || {
                write_results_to_parquet(&results_owned, &output_path, 0)
            })
            .await
            .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

            tracing::info!(
                "Wrote {} multicall token results to {}",
                result_count,
                sub_dir.join(&file_name).display()
            );

            if let Frequency::Duration(_) = &group.frequency {
                if let Some(last_block) = group.filtered_blocks.last() {
                    frequency_state
                        .last_call_times
                        .insert(key.clone(), last_block.timestamp);
                }
            }
        }
    }

    Ok(())
}

/// Process token pool calls for a range of blocks
async fn process_token_range(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    client: &UnifiedRpcClient,
    token_configs: &[TokenCallConfig],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    rpc_batch_size: usize,
    frequency_state: &mut FrequencyState,
) -> Result<(), EthCallCollectionError> {
    // Group configs by (token_name, function_name)
    let mut grouped_configs: HashMap<(String, String), Vec<&TokenCallConfig>> = HashMap::new();
    for config in token_configs {
        grouped_configs
            .entry((config.token_name.clone(), config.function_name.clone()))
            .or_default()
            .push(config);
    }

    for ((token_name, function_name), configs) in &grouped_configs {
        let output_name = format!("{}_pool", token_name);
        let file_name = range.file_name();
        let rel_path = format!("{}/{}/{}", output_name, function_name, file_name);

        if existing_files.contains(&rel_path) {
            tracing::debug!(
                "Skipping token calls for {}.{} blocks {}-{} (already exists)",
                output_name,
                function_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        let frequency = &configs[0].frequency;
        let state_key = (output_name.clone(), function_name.clone());
        let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();
        let filtered_blocks = filter_blocks_for_frequency(&blocks, frequency, last_call_ts);

        if filtered_blocks.is_empty() {
            tracing::debug!(
                "No blocks match frequency {:?} for {}.{} in range {}-{}",
                frequency,
                output_name,
                function_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        tracing::info!(
            "Fetching token calls for {}.{} blocks {}-{} ({} blocks after frequency filter)",
            output_name,
            function_name,
            range.start,
            range.end - 1,
            filtered_blocks.len()
        );

        let mut all_results: Vec<CallResult> = Vec::new();
        let mut pending_calls: Vec<(TransactionRequest, BlockId, &BlockInfo, &TokenCallConfig)> =
            Vec::new();

        for block in &filtered_blocks {
            for config in configs {
                let tx = TransactionRequest::default()
                    .to(config.target_address)
                    .input(config.encoded_calldata.clone().into());
                let block_id = BlockId::Number(BlockNumberOrTag::Number(block.block_number));
                pending_calls.push((tx, block_id, block, config));
            }
        }

        for chunk in pending_calls.chunks(rpc_batch_size) {
            let calls: Vec<(TransactionRequest, BlockId)> = chunk
                .iter()
                .map(|(tx, block_id, _, _)| (tx.clone(), *block_id))
                .collect();

            let results = client.call_batch(calls).await?;

            for (i, result) in results.into_iter().enumerate() {
                let (_, _, block, config) = &chunk[i];
                match result {
                    Ok(bytes) => {
                        all_results.push(CallResult {
                            block_number: block.block_number,
                            block_timestamp: block.timestamp,
                            contract_address: config.target_address.0 .0,
                            value_bytes: bytes.to_vec(),
                            param_values: Vec::new(), // Token calls don't have params stored
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Token call failed for {}.{} at block {}: {}",
                            output_name,
                            function_name,
                            block.block_number,
                            e
                        );
                        all_results.push(CallResult {
                            block_number: block.block_number,
                            block_timestamp: block.timestamp,
                            contract_address: config.target_address.0 .0,
                            value_bytes: Vec::new(),
                            param_values: Vec::new(),
                        });
                    }
                }
            }
        }

        all_results.sort_by_key(|r| (r.block_number, r.contract_address));

        let result_count = all_results.len();
        let sub_dir = output_dir.join(&output_name).join(function_name);
        std::fs::create_dir_all(&sub_dir)?;
        let output_path = sub_dir.join(&file_name);

        tokio::task::spawn_blocking(move || {
            write_results_to_parquet(&all_results, &output_path, 0) // No params
        })
        .await
        .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

        tracing::info!(
            "Wrote {} token call results to {}",
            result_count,
            sub_dir.join(&file_name).display()
        );

        if let Frequency::Duration(_) = frequency {
            if let Some(last_block) = filtered_blocks.last() {
                frequency_state
                    .last_call_times
                    .insert((output_name.clone(), function_name.clone()), last_block.timestamp);
            }
        }
    }

    Ok(())
}

fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();

    // Scan nested directories: dir/contract/function/*.parquet
    if let Ok(contract_entries) = std::fs::read_dir(dir) {
        for contract_entry in contract_entries.flatten() {
            let contract_path = contract_entry.path();
            if !contract_path.is_dir() {
                continue;
            }
            let contract_name = match contract_entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => continue,
            };

            if let Ok(function_entries) = std::fs::read_dir(&contract_path) {
                for function_entry in function_entries.flatten() {
                    let function_path = function_entry.path();
                    if !function_path.is_dir() {
                        continue;
                    }
                    let function_name = match function_entry.file_name().to_str() {
                        Some(name) => name.to_string(),
                        None => continue,
                    };

                    if let Ok(file_entries) = std::fs::read_dir(&function_path) {
                        for file_entry in file_entries.flatten() {
                            if let Some(name) = file_entry.file_name().to_str() {
                                if name.ends_with(".parquet") {
                                    // Store as contract/function/filename
                                    files.insert(format!("{}/{}/{}", contract_name, function_name, name));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    files
}

fn build_schema(num_params: usize) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("contract_address", DataType::FixedSizeBinary(20), false),
        Field::new("value", DataType::Binary, false),
    ];

    for i in 0..num_params {
        fields.push(Field::new(
            &format!("param_{}", i),
            DataType::Binary,
            true,
        ));
    }

    Arc::new(Schema::new(fields))
}

fn write_results_to_parquet(
    results: &[CallResult],
    output_path: &Path,
    num_params: usize,
) -> Result<(), EthCallCollectionError> {
    let schema = build_schema(num_params);
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(
        results.iter().map(|r| r.contract_address.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    let arr: BinaryArray = results
        .iter()
        .map(|r| Some(r.value_bytes.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    for i in 0..num_params {
        let arr: BinaryArray = results
            .iter()
            .map(|r| {
                r.param_values
                    .get(i)
                    .map(|v| v.as_slice())
            })
            .collect();
        arrays.push(Arc::new(arr));
    }

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

fn build_once_schema(function_names: &[String]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("address", DataType::FixedSizeBinary(20), false),
    ];

    for fn_name in function_names {
        fields.push(Field::new(
            &format!("{}_result", fn_name),
            DataType::Binary,
            true,
        ));
    }

    Arc::new(Schema::new(fields))
}

/// Read the column index sidecar file from a `once/` directory.
/// Returns a map of filename -> list of function names whose `{name}_result` columns are present.
fn read_once_column_index(once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> = serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No column index at {} ({}), starting fresh",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Write the column index sidecar file to a `once/` directory.
fn write_once_column_index(
    once_dir: &Path,
    index: &HashMap<String, Vec<String>>,
) -> Result<(), EthCallCollectionError> {
    let index_path = once_dir.join("column_index.json");
    tracing::debug!(
        "Writing column index to {}: {} files tracked",
        index_path.display(),
        index.len()
    );
    let content = serde_json::to_string_pretty(index).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("JSON serialize error: {}", e))
    })?;
    std::fs::write(&index_path, content)?;
    Ok(())
}

/// Load or build the column index for a once/ directory.
/// If the index file exists, load it. Otherwise, scan all parquet files to build it.
fn load_or_build_once_column_index(once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = once_dir.join("column_index.json");

    // Try to load existing index
    if index_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&index_path) {
            if let Ok(index) = serde_json::from_str::<HashMap<String, Vec<String>>>(&content) {
                tracing::debug!(
                    "Loaded column index from {}: {} files tracked",
                    index_path.display(),
                    index.len()
                );
                return index;
            }
        }
    }

    // Index doesn't exist or couldn't be loaded - build from parquet files
    if !once_dir.exists() {
        return HashMap::new();
    }

    let mut index = HashMap::new();
    let parquet_files: Vec<_> = match std::fs::read_dir(once_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|ext| ext == "parquet").unwrap_or(false))
            .collect(),
        Err(_) => return HashMap::new(),
    };

    if parquet_files.is_empty() {
        return HashMap::new();
    }

    tracing::info!(
        "Building column index for {} from {} parquet files",
        once_dir.display(),
        parquet_files.len()
    );

    for entry in parquet_files {
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let cols: Vec<String> = read_parquet_column_names(&path).into_iter().collect();
        if !cols.is_empty() {
            index.insert(file_name, cols);
        }
    }

    // Write the newly built index
    if !index.is_empty() {
        if let Err(e) = write_once_column_index(once_dir, &index) {
            tracing::warn!("Failed to write column index to {}: {}", once_dir.display(), e);
        } else {
            tracing::info!(
                "Built and saved column index for {}: {} files tracked",
                once_dir.display(),
                index.len()
            );
        }
    }

    index
}

/// Read column names from a parquet file's schema (for fallback when index is missing).
/// Returns function names by stripping the `_result` suffix from column names.
fn read_parquet_column_names(path: &Path) -> HashSet<String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    for field in schema.columns() {
        let name = field.name();
        // Extract function name from column name (e.g., "getAssetData_result" -> "getAssetData")
        if let Some(fn_name) = name.strip_suffix("_result") {
            fn_names.insert(fn_name.to_string());
        }
    }

    fn_names
}

/// Read an existing once-call parquet file and return all record batches.
fn read_existing_once_parquet(path: &Path) -> Result<Vec<RecordBatch>, EthCallCollectionError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    tracing::debug!("Reading existing once parquet from {}", path.display());
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let num_cols = batches.first().map(|b| b.num_columns()).unwrap_or(0);
    tracing::debug!(
        "Read {} batches ({} rows, {} columns) from {}",
        batches.len(),
        total_rows,
        num_cols,
        path.display()
    );
    Ok(batches)
}

/// Extract addresses from existing once-call parquet file.
/// Returns a map of address bytes -> (block_number, timestamp).
fn extract_addresses_from_once_parquet(
    batches: &[RecordBatch],
) -> HashMap<[u8; 20], (u64, u64)> {
    let mut addresses = HashMap::new();

    for batch in batches {
        let address_col = match batch.column_by_name("address") {
            Some(col) => col,
            None => continue,
        };
        let address_arr = match address_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(arr) => arr,
            None => continue,
        };

        let block_col = batch.column_by_name("block_number");
        let timestamp_col = batch.column_by_name("block_timestamp");

        let block_arr = block_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let timestamp_arr = timestamp_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        for i in 0..batch.num_rows() {
            let addr_bytes: [u8; 20] = match address_arr.value(i).try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };

            let block_num = block_arr.map(|a| a.value(i)).unwrap_or(0);
            let timestamp = timestamp_arr.map(|a| a.value(i)).unwrap_or(0);

            addresses.entry(addr_bytes).or_insert((block_num, timestamp));
        }
    }

    addresses
}

/// Extract existing non-null results for specific function columns from parquet.
/// Returns a map of address bytes -> function_name -> result bytes.
/// Only includes addresses that have non-null, non-empty data for at least one of the requested columns.
fn extract_existing_results_from_parquet(
    batches: &[RecordBatch],
    fn_names: &[String],
) -> HashMap<[u8; 20], HashMap<String, Vec<u8>>> {
    let mut results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = HashMap::new();

    for batch in batches {
        let address_col = match batch.column_by_name("address") {
            Some(col) => col,
            None => continue,
        };
        let address_arr = match address_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(arr) => arr,
            None => continue,
        };

        // Get result columns for each function
        let result_columns: Vec<(&String, Option<&BinaryArray>)> = fn_names
            .iter()
            .map(|fn_name| {
                let col_name = format!("{}_result", fn_name);
                let col = batch
                    .column_by_name(&col_name)
                    .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());
                (fn_name, col)
            })
            .collect();

        for i in 0..batch.num_rows() {
            let addr_bytes: [u8; 20] = match address_arr.value(i).try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };

            for (fn_name, maybe_col) in &result_columns {
                if let Some(col) = maybe_col {
                    if !col.is_null(i) {
                        let value = col.value(i);
                        if !value.is_empty() {
                            results
                                .entry(addr_bytes)
                                .or_default()
                                .insert((*fn_name).clone(), value.to_vec());
                        }
                    }
                }
            }
        }
    }

    results
}

/// Merge new function result columns into existing once-call record batches.
/// Matches rows by the `address` column (FixedSizeBinary(20)).
/// Returns a single merged RecordBatch.
fn merge_once_columns(
    existing_batches: &[RecordBatch],
    new_results: &HashMap<[u8; 20], HashMap<String, Vec<u8>>>,
    new_fn_names: &[String],
) -> Result<RecordBatch, EthCallCollectionError> {
    tracing::debug!(
        "Merging {} new columns into existing data ({} addresses in new results)",
        new_fn_names.len(),
        new_results.len()
    );

    // Concatenate existing batches into one
    let existing = arrow::compute::concat_batches(
        &existing_batches[0].schema(),
        existing_batches,
    )?;

    let num_rows = existing.num_rows();
    let address_col = existing
        .column_by_name("address")
        .expect("existing once parquet must have address column");
    let address_arr = address_col
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("address column must be FixedSizeBinary(20)");

    tracing::debug!(
        "Existing data has {} rows, {} columns",
        num_rows,
        existing.num_columns()
    );

    // Build new columns by matching on address
    let mut new_columns: Vec<ArrayRef> = Vec::new();
    for fn_name in new_fn_names {
        let mut matched_count = 0;
        let arr: BinaryArray = (0..num_rows)
            .map(|i| {
                let addr_bytes: [u8; 20] = address_arr.value(i).try_into().unwrap();
                let result = new_results
                    .get(&addr_bytes)
                    .and_then(|m| m.get(fn_name))
                    .filter(|v| !v.is_empty())
                    .map(|v| v.as_slice());
                if result.is_some() {
                    matched_count += 1;
                }
                result
            })
            .collect();
        tracing::debug!(
            "Column {}_result: matched {}/{} addresses",
            fn_name,
            matched_count,
            num_rows
        );
        new_columns.push(Arc::new(arr));
    }

    // Build merged schema: existing fields, replacing any that match new columns
    let existing_schema = existing.schema();
    let new_col_names: HashSet<String> = new_fn_names
        .iter()
        .map(|fn_name| format!("{}_result", fn_name))
        .collect();

    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Copy existing columns, skipping any that will be replaced
    for (i, field) in existing_schema.fields().iter().enumerate() {
        if new_col_names.contains(field.name()) {
            tracing::debug!("Replacing existing column: {}", field.name());
            continue;
        }
        fields.push(field.as_ref().clone());
        columns.push(existing.column(i).clone());
    }

    // Add the new/replacement columns
    for (i, fn_name) in new_fn_names.iter().enumerate() {
        fields.push(Field::new(
            &format!("{}_result", fn_name),
            DataType::Binary,
            true,
        ));
        columns.push(new_columns[i].clone());
    }

    let merged_schema = Arc::new(Schema::new(fields));
    let merged = RecordBatch::try_new(merged_schema.clone(), columns)?;
    tracing::debug!(
        "Merged result: {} rows, {} columns (schema: {:?})",
        merged.num_rows(),
        merged.num_columns(),
        merged_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    Ok(merged)
}

fn write_once_results_to_parquet(
    results: &[OnceCallResult],
    output_path: &Path,
    function_names: &[String],
) -> Result<(), EthCallCollectionError> {
    let schema = build_once_schema(function_names);
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = results.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    if results.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            results.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    for fn_name in function_names {
        let arr: BinaryArray = results
            .iter()
            .map(|r| {
                r.function_results
                    .get(fn_name)
                    .filter(|v| !v.is_empty())
                    .map(|v| v.as_slice())
            })
            .collect();
        arrays.push(Arc::new(arr));
    }

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
