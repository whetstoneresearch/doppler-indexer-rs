use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "bench")]
use std::time::Instant;

use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};
use arrow::array::{ArrayRef, BinaryArray, FixedSizeBinaryArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;

use crate::raw_data::historical::blocks::{get_existing_block_ranges, read_block_info_from_parquet};
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::eth_call::{encode_call_with_params, EthCallConfig, EvmType, Frequency, ParamConfig, ParamError, ParamValue};
use crate::types::config::raw_data::RawDataCollectionConfig;

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
    encoded_calldata: Bytes,
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
    mut factory_rx: Option<Receiver<FactoryAddressData>>,
) -> Result<(), EthCallCollectionError> {
    let base_output_dir = PathBuf::from(format!("data/raw/{}/eth_calls", chain.name));
    std::fs::create_dir_all(&base_output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;

    let call_configs = build_call_configs(&chain.contracts)?;
    let factory_call_configs = build_factory_call_configs(&chain.contracts);

    let once_configs = build_once_call_configs(&chain.contracts);
    let factory_once_configs = build_factory_once_call_configs(&factory_call_configs);

    let has_regular_calls = !call_configs.is_empty();
    let has_once_calls = !once_configs.is_empty();
    let has_factory_calls = !factory_call_configs.is_empty() && factory_rx.is_some();
    let has_factory_once_calls = !factory_once_configs.is_empty() && factory_rx.is_some();

    if !has_regular_calls && !has_once_calls && !has_factory_calls && !has_factory_once_calls {
        tracing::info!("No eth_calls configured for chain {}", chain.name);
        while block_rx.recv().await.is_some() {}
        return Ok(());
    }

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
        "Starting eth_call collection for chain {} with {} regular configs, {} once configs, {} factory collections, {} factory once configs",
        chain.name,
        call_configs.len(),
        once_configs.len(),
        factory_call_configs.len(),
        factory_once_configs.len()
    );

    let existing_files = scan_existing_parquet_files(&base_output_dir);

    let mut range_data: HashMap<u64, Vec<BlockInfo>> = HashMap::new();
    let mut range_factory_data: HashMap<u64, FactoryAddressData> = HashMap::new();
    let mut range_regular_done: HashSet<u64> = HashSet::new();
    let mut range_factory_done: HashSet<u64> = HashSet::new();

    if has_regular_calls {
        let block_ranges = get_existing_block_ranges(&chain.name);
        let mut catchup_count = 0;

        for block_range in &block_ranges {
            let range = BlockRange {
                start: block_range.start,
                end: block_range.end,
            };

            let all_exist = call_configs.iter().all(|config| {
                let rel_path = format!("{}/{}/{}", config.contract_name, config.function_name, range.file_name());
                existing_files.contains(&rel_path)
            });

            if all_exist {
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

            if has_once_calls {
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

    let mut block_rx_closed = false;

    loop {
        if block_rx_closed {
            if !has_factory_calls {
                break;
            }

            match &mut factory_rx {
                Some(rx) => {
                    match rx.recv().await {
                        Some(factory_data) => {
                            let range_start = factory_data.range_start;
                            let range_end = factory_data.range_end;

                            range_factory_data.insert(range_start, factory_data.clone());

                            if range_regular_done.contains(&range_start) {
                                if !range_factory_done.contains(&range_start) {
                                    let range = BlockRange {
                                        start: range_start,
                                        end: range_end,
                                    };

                                    if let Some(blocks) = range_data.get(&range_start) {
                                        process_factory_range(
                                            &range,
                                            blocks,
                                            client,
                                            &factory_data,
                                            &factory_call_configs,
                                            &base_output_dir,
                                            &existing_files,
                                            rpc_batch_size,
                                            factory_max_params,
                                            &mut frequency_state,
                                        )
                                        .await?;

                                        if has_factory_once_calls {
                                            process_factory_once_calls(
                                                &range,
                                                client,
                                                &factory_data,
                                                &factory_once_configs,
                                                &base_output_dir,
                                                &existing_files,
                                            )
                                            .await?;
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
                        None => {
                            tracing::debug!("eth_calls: factory channel closed, exiting loop");
                            break;
                        }
                    }
                }
                None => break,
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

                                if has_once_calls {
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
                                range_regular_done.insert(range_start);

                                if let Some(factory_data) = range_factory_data.get(&range_start) {
                                    if has_factory_calls && !range_factory_done.contains(&range_start) {
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
                                        process_factory_once_calls(
                                            &range,
                                            client,
                                            factory_data,
                                            &factory_once_configs,
                                            &base_output_dir,
                                            &existing_files,
                                        )
                                        .await?;
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
                    Some(factory_data) => {
                        let range_start = factory_data.range_start;
                        let range_end = factory_data.range_end;

                        range_factory_data.insert(range_start, factory_data.clone());

                        if range_regular_done.contains(&range_start) {
                            if has_factory_calls && !range_factory_done.contains(&range_start) {
                                let range = BlockRange {
                                    start: range_start,
                                    end: range_end,
                                };

                                if let Some(blocks) = range_data.get(&range_start) {
                                    process_factory_range(
                                        &range,
                                        blocks,
                                        client,
                                        &factory_data,
                                        &factory_call_configs,
                                        &base_output_dir,
                                        &existing_files,
                                        rpc_batch_size,
                                        factory_max_params,
                                        &mut frequency_state,
                                    )
                                    .await?;

                                    if has_factory_once_calls {
                                        process_factory_once_calls(
                                            &range,
                                            client,
                                            &factory_data,
                                            &factory_once_configs,
                                            &base_output_dir,
                                            &existing_files,
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
                    None => {
                        factory_rx = None;
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

        if has_once_calls && !range_regular_done.contains(&range_start) {
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

        if has_factory_calls {
            if let Some(factory_data) = range_factory_data.get(&range_start) {
                if !range_factory_done.contains(&range_start) {
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

                    if has_factory_once_calls {
                        process_factory_once_calls(
                            &range,
                            client,
                            factory_data,
                            &factory_once_configs,
                            &base_output_dir,
                            &existing_files,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    tracing::info!("Eth_call collection complete for chain {}", chain.name);
    Ok(())
}

fn build_factory_call_configs(contracts: &Contracts) -> HashMap<String, Vec<EthCallConfig>> {
    let mut configs: HashMap<String, Vec<EthCallConfig>> = HashMap::new();

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
                            tracing::warn!(
                                "Factory eth_call failed for {}.{} at block {}: {}",
                                collection_name,
                                function_name,
                                block.block_number,
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

fn generate_param_combinations(params: &[ParamConfig]) -> Result<Vec<Vec<(EvmType, ParamValue, Vec<u8>)>>, ParamError> {
    if params.is_empty() {
        return Ok(vec![vec![]]);
    }

    let mut result = vec![vec![]];

    for param in params {
        let mut new_result = Vec::new();
        for existing in &result {
            for value in &param.values {
                let mut combo = existing.clone();                
                let dyn_val = param.param_type.parse_value(value)?;
                let encoded = dyn_val.abi_encode();
                combo.push((param.param_type.clone(), value.clone(), encoded));
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
        let addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };

        if let Some(calls) = &contract.calls {
            for call in calls {
                if call.frequency.is_once() {
                    continue;
                }

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

                    // For "once" calls, we don't support params (they're typically view functions)
                    let encoded_calldata = Bytes::copy_from_slice(&selector);

                    configs.entry(contract_name.clone()).or_default().push(OnceCallConfig {
                        function_name,
                        encoded_calldata,
                    });
                }
            }
        }
    }

    configs
}

fn build_factory_once_call_configs(factory_call_configs: &HashMap<String, Vec<EthCallConfig>>) -> HashMap<String, Vec<OnceCallConfig>> {
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

                let encoded_calldata = Bytes::copy_from_slice(&selector);

                configs.entry(collection_name.clone()).or_default().push(OnceCallConfig {
                    function_name,
                    encoded_calldata,
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

        if existing_files.contains(&rel_path) {
            tracing::debug!(
                "Skipping once eth_calls for {} blocks {}-{} (already exists)",
                contract_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        let contract = match contracts.get(contract_name) {
            Some(c) => c,
            None => continue,
        };

        let addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };

        let block_id = BlockId::Number(BlockNumberOrTag::Number(first_block.block_number));
        let mut pending_calls: Vec<(TransactionRequest, BlockId, Address, String)> = Vec::new();

        for address in &addresses {
            for call_config in call_configs {
                let tx = TransactionRequest::default()
                    .to(*address)
                    .input(call_config.encoded_calldata.clone().into());
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
            let (_, _, address, function_name) = &pending_calls[i];

            let function_results = results_by_address.entry(*address).or_default();

            match result {
                Ok(bytes) => {
                    function_results.insert(function_name.clone(), bytes.to_vec());
                }
                Err(e) => {
                    tracing::warn!(
                        "once eth_call failed for {}.{} at block {}: {}",
                        contract_name,
                        function_name,
                        first_block.block_number,
                        e
                    );
                    function_results.insert(function_name.clone(), Vec::new());
                }
            }
        }

        let results: Vec<OnceCallResult> = addresses
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
            let function_names: Vec<String> = call_configs
                .iter()
                .map(|c| c.function_name.clone())
                .collect();

            let sub_dir = output_dir.join(contract_name).join("once");
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            tracing::info!(
                "Writing {} once eth_call results to {}",
                results.len(),
                output_path.display()
            );

            write_once_results_to_parquet(&results, &output_path, &function_names)?;
        }
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
) -> Result<(), EthCallCollectionError> {
    for (collection_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name();
        let rel_path = format!("{}/once/{}", collection_name, file_name);

        if existing_files.contains(&rel_path) {
            tracing::debug!(
                "Skipping factory once eth_calls for {} blocks {}-{} (already exists)",
                collection_name,
                range.start,
                range.end - 1
            );
            continue;
        }

        let mut address_discovery: HashMap<Address, (u64, u64)> = HashMap::new(); // addr -> (block, timestamp)

        for (block_num, addrs) in &factory_data.addresses_by_block {
            for (timestamp, addr, coll_name) in addrs {
                if coll_name == collection_name {
                    address_discovery.entry(*addr).or_insert((*block_num, *timestamp));
                }
            }
        }

        if address_discovery.is_empty() {
            continue;
        }

        let mut pending_calls: Vec<(TransactionRequest, BlockId, Address, u64, u64, String)> = Vec::new();

        for (address, (block_number, timestamp)) in &address_discovery {
            let block_id = BlockId::Number(BlockNumberOrTag::Number(*block_number));

            for call_config in call_configs {
                let tx = TransactionRequest::default()
                    .to(*address)
                    .input(call_config.encoded_calldata.clone().into());
                pending_calls.push((
                    tx,
                    block_id,
                    *address,
                    *block_number,
                    *timestamp,
                    call_config.function_name.clone(),
                ));
            }
        }

        if pending_calls.is_empty() {
            continue;
        }

        let batch_calls: Vec<(TransactionRequest, BlockId)> = pending_calls
            .iter()
            .map(|(tx, bid, _, _, _, _)| (tx.clone(), *bid))
            .collect();

        let batch_results = client.call_batch(batch_calls).await?;

        let mut results_by_address: HashMap<Address, (u64, u64, HashMap<String, Vec<u8>>)> = HashMap::new();

        for (i, result) in batch_results.into_iter().enumerate() {
            let (_, _, address, block_number, timestamp, function_name) = &pending_calls[i];

            let entry = results_by_address
                .entry(*address)
                .or_insert_with(|| (*block_number, *timestamp, HashMap::new()));

            match result {
                Ok(bytes) => {
                    entry.2.insert(function_name.clone(), bytes.to_vec());
                }
                Err(e) => {
                    tracing::warn!(
                        "factory once eth_call failed for {}.{} at block {}: {}",
                        collection_name,
                        function_name,
                        block_number,
                        e
                    );
                    entry.2.insert(function_name.clone(), Vec::new());
                }
            }
        }

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
            let function_names: Vec<String> = call_configs
                .iter()
                .map(|c| c.function_name.clone())
                .collect();

            let sub_dir = output_dir.join(collection_name).join("once");
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            tracing::info!(
                "Writing {} factory once eth_call results to {}",
                results.len(),
                output_path.display()
            );

            write_once_results_to_parquet(&results, &output_path, &function_names)?;
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
                        tracing::warn!(
                            "eth_call failed for {}.{} at block {}: {}",
                            contract_name,
                            function_name,
                            block.block_number,
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

    let arr = FixedSizeBinaryArray::try_from_iter(
        results.iter().map(|r| r.contract_address.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

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
