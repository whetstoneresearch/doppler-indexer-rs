//! Execution functions for eth_call collection: all process_* functions and multicall infrastructure.

use std::collections::{HashMap, HashSet};
use std::fs::File;
#[cfg(feature = "bench")]
use std::time::Instant;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};
use futures::{stream, StreamExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::config::{compute_function_selector, generate_param_combinations};
use super::event_triggers::encode_once_call_params;
use super::frequency::filter_blocks_for_frequency;
use super::parquet_io::{
    extract_addresses_from_once_parquet, extract_existing_results_from_parquet, find_null_entries,
    merge_once_columns, read_existing_once_parquet, read_once_column_index,
    read_parquet_column_names, write_once_column_index, write_once_results_to_parquet,
    write_results_to_parquet,
};
use super::types::{
    AddressResults, BlockInfo, BlockRange, CallConfig, CallResult, CollectionResults,
    ContractProcessingInfo, EncodedParam, EthCallCollectionError, EthCallContext,
    FactoryContractProcessingInfo, FrequencyState, OnceCallConfig, OnceCallResult, TokenCallConfig,
};
use crate::decoding::{
    DecoderMessage, EthCallResult as DecoderEthCallResult, OnceCallResult as DecoderOnceCallResult,
};
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::rpc::UnifiedRpcClient;
use crate::storage::upload_parquet_to_s3;
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::eth_call::{encode_call_with_params, EthCallConfig, Frequency};

pub(crate) async fn process_factory_range(
    range: &BlockRange,
    blocks: &[BlockInfo],
    ctx: &EthCallContext<'_>,
    factory_data: &FactoryAddressData,
    factory_call_configs: &HashMap<String, Vec<EthCallConfig>>,
    max_params: usize,
    frequency_state: &mut FrequencyState,
) -> Result<(), EthCallCollectionError> {
    let mut addresses_by_collection: HashMap<String, HashSet<Address>> = HashMap::new();
    for addrs in factory_data.addresses_by_block.values() {
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

            let file_name = range.file_name("");
            let rel_path = format!("{}/{}/{}", collection_name, function_name, file_name);

            if ctx.existing_files.contains(&rel_path) {
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
                        .map(|(param_type, value, _): &EncodedParam| {
                            param_type.parse_value(value)
                        })
                        .collect::<Result<_, _>>()?;

                    let encoded_calldata = encode_call_with_params(selector, &dyn_values);
                    let param_values: Vec<Vec<u8>> = param_combo
                        .iter()
                        .map(|(_, _, encoded): &EncodedParam| encoded.clone())
                        .collect();

                    configs.push(CallConfig {
                        contract_name: collection_name.clone(),
                        address: *address,
                        function_name: function_name.clone(),
                        encoded_calldata,
                        param_values,
                        frequency: call_config.frequency.clone(),
                        // Factory calls use discovery block, not start_block
                        start_block: None,
                    });
                }
            }

            if configs.is_empty() {
                continue;
            }

            let state_key = (collection_name.clone(), function_name.clone());
            let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();

            let filtered_blocks =
                filter_blocks_for_frequency(blocks, &call_config.frequency, last_call_ts);

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
                    // Skip if block is before contract's start_block
                    if let Some(sb) = config.start_block {
                        if block.block_number < sb {
                            continue;
                        }
                    }
                    let tx = TransactionRequest::default()
                        .to(config.address)
                        .input(config.encoded_calldata.clone().into());
                    let block_id = BlockId::Number(BlockNumberOrTag::Number(block.block_number));
                    pending_calls.push((tx, block_id, block, config));
                }
            }

            for chunk in pending_calls.chunks(ctx.rpc_batch_size) {
                let calls: Vec<(TransactionRequest, BlockId)> = chunk
                    .iter()
                    .map(|(tx, block_id, _, _)| (tx.clone(), *block_id))
                    .collect();

                #[cfg(feature = "bench")]
                let rpc_start = Instant::now();
                let results = ctx.client.call_batch(calls).await?;
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
                            let params_hex: Vec<String> = config
                                .param_values
                                .iter()
                                .map(|p| format!("0x{}", hex::encode(p)))
                                .collect();
                            tracing::warn!(
                                "Factory eth_call failed for {}.{} at block {} (address {}, params {:?}): {}",
                                collection_name,
                                function_name,
                                block.block_number,
                                config.address,
                                params_hex,
                                e
                            );
                            // Skip reverted calls - don't store empty results
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
            let sub_dir = ctx.output_dir.join(collection_name).join(&function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            let decoder_results: Option<Vec<DecoderEthCallResult>> = if ctx.decoder_tx.is_some() {
                Some(
                    all_results
                        .iter()
                        .map(|r| DecoderEthCallResult {
                            block_number: r.block_number,
                            block_timestamp: r.block_timestamp,
                            contract_address: r.contract_address,
                            value: r.value_bytes.clone(),
                        })
                        .collect(),
                )
            } else {
                None
            };

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

            let output_path_for_upload = sub_dir.join(&file_name);
            tracing::info!(
                "Wrote {} factory eth_call results to {}",
                result_count,
                output_path_for_upload.display()
            );

            // Upload to S3 if configured
            if let Some(sm) = ctx.storage_manager {
                let data_type = format!("raw/eth_calls/{}/{}", collection_name, function_name);
                upload_parquet_to_s3(
                    sm,
                    &output_path_for_upload,
                    ctx.chain_name,
                    &data_type,
                    range.start,
                    range.end - 1,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            if let Some(tx) = ctx.decoder_tx {
                if let Some(results) = decoder_results {
                    let _ = tx
                        .send(DecoderMessage::EthCallsReady {
                            range_start: range.start,
                            range_end: range.end,
                            contract_name: collection_name.clone(),
                            function_name: function_name.clone(),
                            results,
                            live_mode: false,
                            retry_transform_after_decode: false,
                        })
                        .await;
                }
            }

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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_factory_range_multicall(
    range: &BlockRange,
    blocks: &[BlockInfo],
    ctx: &EthCallContext<'_>,
    factory_data: &FactoryAddressData,
    factory_call_configs: &HashMap<String, Vec<EthCallConfig>>,
    max_params: usize,
    frequency_state: &mut FrequencyState,
    multicall3_address: Address,
) -> Result<(), EthCallCollectionError> {
    // Collect factory addresses by collection
    let mut addresses_by_collection: HashMap<String, HashSet<Address>> = HashMap::new();
    for addrs in factory_data.addresses_by_block.values() {
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

            let file_name = range.file_name("");
            let rel_path = format!("{}/{}/{}", collection_name, function_name, file_name);

            if ctx.existing_files.contains(&rel_path) {
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
                        .map(|(param_type, value, _): &EncodedParam| {
                            param_type.parse_value(value)
                        })
                        .collect::<Result<_, _>>()?;

                    let encoded_calldata = encode_call_with_params(selector, &dyn_values);
                    let param_values: Vec<Vec<u8>> = param_combo
                        .iter()
                        .map(|(_, _, encoded): &EncodedParam| encoded.clone())
                        .collect();

                    configs.push(CallConfig {
                        contract_name: collection_name.clone(),
                        address: *address,
                        function_name: function_name.clone(),
                        encoded_calldata,
                        param_values,
                        frequency: call_config.frequency.clone(),
                        // Factory calls use discovery block, not start_block
                        start_block: None,
                    });
                }
            }

            if configs.is_empty() {
                continue;
            }

            let state_key = (collection_name.clone(), function_name.clone());
            let last_call_ts = frequency_state.last_call_times.get(&state_key).copied();
            let filtered_blocks =
                filter_blocks_for_frequency(blocks, &call_config.frequency, last_call_ts);

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
    let results =
        execute_multicalls_generic(ctx.client, multicall3_address, block_multicalls, ctx.rpc_batch_size)
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
            let file_name = range.file_name("");
            let sub_dir = ctx.output_dir
                .join(&group.collection_name)
                .join(&group.function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            let decoder_results: Option<Vec<DecoderEthCallResult>> = if ctx.decoder_tx.is_some() {
                Some(
                    results
                        .iter()
                        .map(|r| DecoderEthCallResult {
                            block_number: r.block_number,
                            block_timestamp: r.block_timestamp,
                            contract_address: r.contract_address,
                            value: r.value_bytes.clone(),
                        })
                        .collect(),
                )
            } else {
                None
            };

            let results_owned = std::mem::take(results);
            tokio::task::spawn_blocking(move || {
                write_results_to_parquet(&results_owned, &output_path, max_params)
            })
            .await
            .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

            let output_path_for_upload = sub_dir.join(&file_name);
            tracing::info!(
                "Wrote {} multicall factory eth_call results to {}",
                result_count,
                output_path_for_upload.display()
            );

            // Upload to S3 if configured
            if let Some(sm) = ctx.storage_manager {
                let data_type = format!(
                    "raw/eth_calls/{}/{}",
                    group.collection_name, group.function_name
                );
                upload_parquet_to_s3(
                    sm,
                    &output_path_for_upload,
                    ctx.chain_name,
                    &data_type,
                    range.start,
                    range.end - 1,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            if let Some(tx) = ctx.decoder_tx {
                if let Some(results) = decoder_results {
                    let _ = tx
                        .send(DecoderMessage::EthCallsReady {
                            range_start: range.start,
                            range_end: range.end,
                            contract_name: group.collection_name.clone(),
                            function_name: group.function_name.clone(),
                            results,
                            live_mode: false,
                            retry_transform_after_decode: false,
                        })
                        .await;
                }
            }

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

pub(crate) async fn process_once_calls_regular(
    range: &BlockRange,
    blocks: &[BlockInfo],
    ctx: &EthCallContext<'_>,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    contracts: &Contracts,
) -> Result<(), EthCallCollectionError> {
    let first_block = match blocks.first() {
        Some(b) => b,
        None => return Ok(()),
    };

    for (contract_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name("");
        let rel_path = format!("{}/once/{}", contract_name, file_name);
        let sub_dir = ctx.output_dir.join(contract_name).join("once");
        let output_path = sub_dir.join(&file_name);

        // Determine which function calls are missing from the existing file
        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file, null_entries) =
            if ctx.existing_files.contains(&rel_path) {
                let index = read_once_column_index(&sub_dir);
                let existing_cols: HashSet<String> = if let Some(cols) = index.get(&file_name) {
                    cols.iter().cloned().collect()
                } else {
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
                // Find addresses with null values for existing columns
                let all_null_entries = find_null_entries(&output_path);
                let null_entries: HashMap<String, HashSet<[u8; 20]>> = all_null_entries
                    .into_iter()
                    .filter(|(k, _)| existing_cols.contains(k) && !missing.contains(k))
                    .collect();
                if missing.is_empty() && null_entries.is_empty() {
                    tracing::debug!(
                    "Skipping once eth_calls for {} blocks {}-{} (all columns present, no nulls)",
                    contract_name,
                    range.start,
                    range.end - 1
                );
                    continue;
                }
                if !missing.is_empty() {
                    tracing::info!(
                        "Found {} missing once columns for {} blocks {}-{}: {:?}",
                        missing.len(),
                        contract_name,
                        range.start,
                        range.end - 1,
                        missing
                    );
                }
                if !null_entries.is_empty() {
                    let null_count: usize = null_entries.values().map(|s| s.len()).sum();
                    tracing::info!(
                    "Found {} null entries across {} columns for {} blocks {}-{}, will re-fetch",
                    null_count,
                    null_entries.len(),
                    contract_name,
                    range.start,
                    range.end - 1,
                );
                }
                (missing, true, null_entries)
            } else {
                (all_fn_names.clone(), false, HashMap::new())
            };

        // Filter call configs to only missing functions
        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        // Configs for partially-null columns (only need to call specific addresses)
        let configs_to_patch: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| null_entries.contains_key(&c.function_name))
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

        // Missing columns: call ALL addresses
        for call_config in &configs_to_call {
            // Skip if first_block is before contract's start_block
            if let Some(sb) = call_config.start_block {
                if first_block.block_number < sb {
                    continue;
                }
            }
            let addresses = call_config
                .target_addresses
                .as_ref()
                .unwrap_or(&default_addresses);

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

        // Partially-null columns: call only addresses with null values
        for call_config in &configs_to_patch {
            // Skip if first_block is before contract's start_block
            if let Some(sb) = call_config.start_block {
                if first_block.block_number < sb {
                    continue;
                }
            }
            let null_addrs = &null_entries[&call_config.function_name];
            let addresses = call_config
                .target_addresses
                .as_ref()
                .unwrap_or(&default_addresses);

            for address in addresses {
                if !null_addrs.contains(&address.0 .0) {
                    continue;
                }
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

        let batch_results = ctx.client.call_batch(batch_calls).await?;

        let mut results_by_address: HashMap<Address, HashMap<String, Vec<u8>>> = HashMap::new();

        for (i, result) in batch_results.into_iter().enumerate() {
            let (tx, _, address, function_name) = &pending_calls[i];

            let function_results = results_by_address.entry(*address).or_default();

            match result {
                Ok(bytes) => {
                    function_results.insert(function_name.clone(), bytes.to_vec());
                }
                Err(e) => {
                    let calldata = tx
                        .input
                        .input
                        .as_ref()
                        .map(|b| format!("0x{}", hex::encode(b)))
                        .unwrap_or_default();
                    tracing::warn!(
                        "once eth_call failed for {}.{} at block {} (address {}, calldata {}): {}",
                        contract_name,
                        function_name,
                        first_block.block_number,
                        address,
                        calldata,
                        e
                    );
                    // Skip reverted calls - don't insert empty results
                }
            }
        }

        std::fs::create_dir_all(&sub_dir)?;

        let decoder_once_results: Option<Vec<DecoderOnceCallResult>> = if ctx.decoder_tx.is_some() {
            Some(
                results_by_address
                    .iter()
                    .map(|(addr, function_results)| DecoderOnceCallResult {
                        block_number: first_block.block_number,
                        block_timestamp: first_block.timestamp,
                        contract_address: addr.0 .0,
                        results: function_results.clone(),
                    })
                    .collect(),
            )
        } else {
            None
        };

        if has_existing_file {
            // Merge new columns into existing parquet
            let new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = results_by_address
                .into_iter()
                .map(|(addr, fns)| (addr.0 .0, fns))
                .collect();

            let existing_batches = read_existing_once_parquet(&output_path)?;
            if !existing_batches.is_empty() {
                // Combine missing + patch function names for merge
                let patch_fn_names: Vec<String> = null_entries.keys().cloned().collect();
                let all_new_fn_names: Vec<String> = missing_fn_names
                    .iter()
                    .chain(patch_fn_names.iter())
                    .cloned()
                    .collect();
                let merged =
                    merge_once_columns(&existing_batches, &new_results, &all_new_fn_names)?;

                let file = File::create(&output_path)?;
                let props = WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::SNAPPY)
                    .build();
                let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                writer.write(&merged)?;
                writer.close()?;

                tracing::info!(
                    "Merged {} new + {} patched once columns into {} for {}",
                    missing_fn_names.len(),
                    patch_fn_names.len(),
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
                    results_by_address
                        .remove(addr)
                        .map(|function_results| OnceCallResult {
                            block_number: first_block.block_number,
                            block_timestamp: first_block.timestamp,
                            contract_address: addr.0 .0,
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

        // Upload to S3 if configured
        if let Some(sm) = ctx.storage_manager {
            let data_type = format!("raw/eth_calls/{}/once", contract_name);
            upload_parquet_to_s3(
                sm,
                &output_path,
                ctx.chain_name,
                &data_type,
                range.start,
                range.end - 1,
            )
            .await
            .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }

        // Update the column index with all columns present in the file
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

        if let Some(tx) = ctx.decoder_tx {
            if let Some(results) = decoder_once_results {
                let _ = tx
                    .send(DecoderMessage::OnceCallsReady {
                        range_start: range.start,
                        range_end: range.end,
                        contract_name: contract_name.clone(),
                        results,
                        live_mode: false,
                        retry_transform_after_decode: false,
                    })
                    .await;
            }
        }
    }

    Ok(())
}

pub(crate) async fn process_factory_once_calls(
    range: &BlockRange,
    ctx: &EthCallContext<'_>,
    factory_data: &FactoryAddressData,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    column_indexes: &HashMap<String, HashMap<String, Vec<String>>>,
) -> Result<(), EthCallCollectionError> {
    for (collection_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name("");
        let rel_path = format!("{}/once/{}", collection_name, file_name);
        let sub_dir = ctx.output_dir.join(collection_name).join("once");
        let output_path = sub_dir.join(&file_name);

        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file, null_entries) = if ctx.existing_files
            .contains(&rel_path)
        {
            let index = column_indexes.get(collection_name);
            let indexed_cols: HashSet<String> = index
                .and_then(|idx| idx.get(&file_name))
                .map(|cols| cols.iter().cloned().collect())
                .unwrap_or_default();

            // Check actual parquet schema to determine truly missing columns
            let parquet_cols = read_parquet_column_names(&output_path);

            // Missing: in config but not in the parquet file
            let missing: Vec<String> = all_fn_names
                .iter()
                .filter(|f| !parquet_cols.contains(*f))
                .cloned()
                .collect();

            // Only try to fill null results if:
            // 1) there is no column index file (indexed_cols is empty)
            // 2) the block range file is not in the column index
            // 3) the column with nulls is not in the column index for that file
            // If a column IS in the index, nulls are considered permanent.
            let null_entries: HashMap<String, HashSet<[u8; 20]>> = {
                let unindexed_in_parquet: Vec<&String> = all_fn_names
                    .iter()
                    .filter(|f| parquet_cols.contains(*f) && !indexed_cols.contains(*f))
                    .collect();
                if unindexed_in_parquet.is_empty() {
                    HashMap::new()
                } else {
                    let all_null_entries = find_null_entries(&output_path);
                    all_null_entries
                        .into_iter()
                        .filter(|(k, _)| !indexed_cols.contains(k))
                        .collect()
                }
            };

            if missing.is_empty() && null_entries.is_empty() {
                tracing::debug!(
                    "Skipping factory once eth_calls for {} blocks {}-{} (all columns present and indexed)",
                    collection_name,
                    range.start,
                    range.end - 1
                );
                continue;
            }
            if !missing.is_empty() {
                tracing::info!(
                    "Found {} missing factory once columns for {} blocks {}-{}: {:?}",
                    missing.len(),
                    collection_name,
                    range.start,
                    range.end - 1,
                    missing
                );
            }
            if !null_entries.is_empty() {
                let null_count: usize = null_entries.values().map(|s| s.len()).sum();
                tracing::info!(
                    "Found {} null entries across {} unindexed factory columns for {} blocks {}-{}, will re-fetch",
                    null_count,
                    null_entries.len(),
                    collection_name,
                    range.start,
                    range.end - 1,
                );
            }
            (missing, true, null_entries)
        } else {
            (all_fn_names.clone(), false, HashMap::new())
        };

        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        // Configs for partially-null columns (only need to call specific addresses)
        let configs_to_patch: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| null_entries.contains_key(&c.function_name))
            .collect();

        let mut address_discovery: HashMap<Address, (u64, u64)> = HashMap::new();

        for (block_num, addrs) in &factory_data.addresses_by_block {
            for (timestamp, addr, coll_name) in addrs {
                if coll_name == collection_name {
                    address_discovery
                        .entry(*addr)
                        .or_insert((*block_num, *timestamp));
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
            if let Some(tx) = ctx.decoder_tx {
                let _ = tx
                    .send(DecoderMessage::OnceFileBackfilled {
                        range_start: range.start,
                        range_end: range.end,
                        contract_name: collection_name.clone(),
                    })
                    .await;
            }
            continue;
        }

        let mut pending_calls: Vec<(TransactionRequest, BlockId, Address, u64, u64, String)> =
            Vec::new();

        // Missing columns: call ALL discovered addresses
        for (discovered_address, (block_number, timestamp)) in &address_discovery {
            let block_id = BlockId::Number(BlockNumberOrTag::Number(*block_number));

            for call_config in &configs_to_call {
                let target_address = call_config
                    .target_addresses
                    .as_ref()
                    .and_then(|addrs| addrs.first().copied())
                    .unwrap_or(*discovered_address);

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

        // Partially-null columns: call only addresses with null values
        for (discovered_address, (block_number, timestamp)) in &address_discovery {
            let block_id = BlockId::Number(BlockNumberOrTag::Number(*block_number));

            for call_config in &configs_to_patch {
                let null_addrs = &null_entries[&call_config.function_name];
                if !null_addrs.contains(&discovered_address.0 .0) {
                    continue;
                }
                let target_address = call_config
                    .target_addresses
                    .as_ref()
                    .and_then(|addrs| addrs.first().copied())
                    .unwrap_or(*discovered_address);

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
        let results_by_address: AddressResults =
            if !pending_calls.is_empty() {
                let batch_calls: Vec<(TransactionRequest, BlockId)> = pending_calls
                    .iter()
                    .map(|(tx, bid, _, _, _, _)| (tx.clone(), *bid))
                    .collect();

                let batch_results = ctx.client.call_batch(batch_calls).await?;

                let mut results_map: AddressResults = HashMap::new();

                for (i, result) in batch_results.into_iter().enumerate() {
                    let (tx, _, address, block_number, timestamp, function_name) =
                        &pending_calls[i];

                    let entry = results_map
                        .entry(*address)
                        .or_insert_with(|| (*block_number, *timestamp, HashMap::new()));

                    match result {
                        Ok(bytes) => {
                            entry.2.insert(function_name.clone(), bytes.to_vec());
                        }
                        Err(e) => {
                            let calldata = tx
                                .input
                                .input
                                .as_ref()
                                .map(|b| format!("0x{}", hex::encode(b)))
                                .unwrap_or_default();
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
                .map(|(addr, (_, _, fns))| (addr.0 .0, fns))
                .collect();

            let existing_batches = read_existing_once_parquet(&output_path)?;
            if !existing_batches.is_empty() {
                // Check which addresses already have data for the missing functions
                let existing_results =
                    extract_existing_results_from_parquet(&existing_batches, &missing_fn_names);

                // Find addresses in existing parquet that need backfill
                let existing_addresses = extract_addresses_from_once_parquet(&existing_batches);

                // Build list of (address, block, functions_to_call) for backfill
                // Only call functions that don't already have data
                let mut backfill_calls: Vec<(TransactionRequest, BlockId, [u8; 20], String)> =
                    Vec::new();

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

                        let target_address = call_config
                            .target_addresses
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
                        backfill_calls.push((
                            tx,
                            block_id,
                            *addr_bytes,
                            call_config.function_name.clone(),
                        ));
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

                    let batch_results = ctx.client.call_batch(batch_calls).await?;

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

                // Combine missing + patch function names for merge
                let patch_fn_names: Vec<String> = null_entries.keys().cloned().collect();
                let all_new_fn_names: Vec<String> = missing_fn_names
                    .iter()
                    .chain(patch_fn_names.iter())
                    .cloned()
                    .collect();
                let merged =
                    merge_once_columns(&existing_batches, &new_results, &all_new_fn_names)?;

                let file = File::create(&output_path)?;
                let props = WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::SNAPPY)
                    .build();
                let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                writer.write(&merged)?;
                writer.close()?;

                tracing::info!(
                    "Merged {} new + {} patched factory once columns into {} for {}",
                    missing_fn_names.len(),
                    patch_fn_names.len(),
                    output_path.display(),
                    collection_name
                );
            }
        } else {
            let results: Vec<OnceCallResult> = results_by_address
                .into_iter()
                .map(
                    |(address, (block_number, timestamp, function_results))| OnceCallResult {
                        block_number,
                        block_timestamp: timestamp,
                        contract_address: address.0 .0,
                        function_results,
                    },
                )
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

        // Upload to S3 if configured
        if let Some(sm) = ctx.storage_manager {
            let data_type = format!("raw/eth_calls/{}/once", collection_name);
            upload_parquet_to_s3(
                sm,
                &output_path,
                ctx.chain_name,
                &data_type,
                range.start,
                range.end - 1,
            )
            .await
            .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }

        // Update the column index with all columns present in the file
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
        if let Some(tx) = ctx.decoder_tx {
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
pub(crate) async fn process_once_calls_multicall(
    range: &BlockRange,
    blocks: &[BlockInfo],
    ctx: &EthCallContext<'_>,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    contracts: &Contracts,
    multicall3_address: Address,
) -> Result<(), EthCallCollectionError> {
    let first_block = match blocks.first() {
        Some(b) => b,
        None => return Ok(()),
    };

    let mut all_slots: Vec<MulticallSlotGeneric<OnceCallMeta>> = Vec::new();
    let mut contracts_to_process: Vec<ContractProcessingInfo> = Vec::new();

    for (contract_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name("");
        let rel_path = format!("{}/once/{}", contract_name, file_name);
        let sub_dir = ctx.output_dir.join(contract_name).join("once");
        let output_path = sub_dir.join(&file_name);

        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file, null_entries) =
            if ctx.existing_files.contains(&rel_path) {
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
                // Find addresses with null values for existing columns
                let all_null_entries = find_null_entries(&output_path);
                let null_entries: HashMap<String, HashSet<[u8; 20]>> = all_null_entries
                    .into_iter()
                    .filter(|(k, _)| existing_cols.contains(k) && !missing.contains(k))
                    .collect();
                if missing.is_empty() && null_entries.is_empty() {
                    continue;
                }
                (missing, true, null_entries)
            } else {
                (all_fn_names.clone(), false, HashMap::new())
            };

        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        let configs_to_patch: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| null_entries.contains_key(&c.function_name))
            .collect();

        let contract = match contracts.get(contract_name) {
            Some(c) => c,
            None => continue,
        };

        // Skip if first_block is before contract's start_block
        if let Some(sb) = contract.start_block {
            let start_block = sb.to::<u64>();
            if first_block.block_number < start_block {
                tracing::debug!(
                    "Skipping once calls for {} at block {} (before start_block {})",
                    contract_name,
                    first_block.block_number,
                    start_block
                );
                continue;
            }
        }

        let default_addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };

        // Missing columns: slots for ALL addresses
        for call_config in &configs_to_call {
            let addresses = call_config
                .target_addresses
                .as_ref()
                .unwrap_or(&default_addresses);

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

        // Partially-null columns: slots only for addresses with null values
        for call_config in &configs_to_patch {
            let null_addrs = &null_entries[&call_config.function_name];
            let addresses = call_config
                .target_addresses
                .as_ref()
                .unwrap_or(&default_addresses);

            for address in addresses {
                if !null_addrs.contains(&address.0 .0) {
                    continue;
                }
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

        let patch_fn_names: Vec<String> = null_entries.keys().cloned().collect();
        contracts_to_process.push(ContractProcessingInfo {
            name: contract_name.clone(),
            all_fn_names,
            missing_fn_names,
            patch_fn_names,
            output_path,
            has_existing_file,
        });
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
    let results =
        execute_multicalls_generic(ctx.client, multicall3_address, block_multicalls, ctx.rpc_batch_size)
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
    for ContractProcessingInfo {
        name: contract_name,
        all_fn_names,
        missing_fn_names,
        patch_fn_names,
        output_path,
        has_existing_file,
    } in contracts_to_process
    {
        let sub_dir = output_path.parent().unwrap();
        std::fs::create_dir_all(sub_dir)?;

        if let Some(results_by_address) = results_by_contract.remove(&contract_name) {
            let decoder_once_results: Option<Vec<DecoderOnceCallResult>> = if ctx.decoder_tx.is_some() {
                Some(
                    results_by_address
                        .iter()
                        .map(|(addr, function_results)| DecoderOnceCallResult {
                            block_number: first_block.block_number,
                            block_timestamp: first_block.timestamp,
                            contract_address: addr.0 .0,
                            results: function_results.clone(),
                        })
                        .collect(),
                )
            } else {
                None
            };

            if has_existing_file {
                let new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = results_by_address
                    .into_iter()
                    .map(|(addr, fns)| (addr.0 .0, fns))
                    .collect();

                let existing_batches = read_existing_once_parquet(&output_path)?;
                if !existing_batches.is_empty() {
                    // Combine missing + patch function names for merge
                    let all_new_fn_names: Vec<String> = missing_fn_names
                        .iter()
                        .chain(patch_fn_names.iter())
                        .cloned()
                        .collect();
                    let merged =
                        merge_once_columns(&existing_batches, &new_results, &all_new_fn_names)?;

                    let file = File::create(&output_path)?;
                    let props = WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::SNAPPY)
                        .build();
                    let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                    writer.write(&merged)?;
                    writer.close()?;

                    tracing::info!(
                        "Merged {} new + {} patched multicall once columns into {} for {}",
                        missing_fn_names.len(),
                        patch_fn_names.len(),
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

            // Upload to S3 if configured
            if let Some(sm) = ctx.storage_manager {
                let data_type = format!("raw/eth_calls/{}/once", contract_name);
                upload_parquet_to_s3(
                    sm,
                    &output_path,
                    ctx.chain_name,
                    &data_type,
                    range.start,
                    range.end - 1,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            // Update column index with all columns present in the file
            let file_name = output_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let actual_cols: Vec<String> = read_parquet_column_names(&output_path)
                .into_iter()
                .collect();
            let mut index = read_once_column_index(sub_dir);
            index.insert(file_name.clone(), actual_cols.clone());
            write_once_column_index(sub_dir, &index)?;

            if let Some(tx) = ctx.decoder_tx {
                if let Some(results) = decoder_once_results {
                    let _ = tx
                        .send(DecoderMessage::OnceCallsReady {
                            range_start: range.start,
                            range_end: range.end,
                            contract_name: contract_name.clone(),
                            results,
                            live_mode: false,
                            retry_transform_after_decode: false,
                        })
                        .await;
                }
            }
        }
    }

    Ok(())
}

/// Process factory once calls using Multicall3 aggregate3
pub(crate) async fn process_factory_once_calls_multicall(
    range: &BlockRange,
    ctx: &EthCallContext<'_>,
    factory_data: &FactoryAddressData,
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    column_indexes: &HashMap<String, HashMap<String, Vec<String>>>,
    multicall3_address: Address,
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
    let mut collections_to_process: Vec<FactoryContractProcessingInfo> = Vec::new();

    for (collection_name, call_configs) in once_configs {
        if call_configs.is_empty() {
            continue;
        }

        let file_name = range.file_name("");
        let rel_path = format!("{}/once/{}", collection_name, file_name);
        let sub_dir = ctx.output_dir.join(collection_name).join("once");
        let output_path = sub_dir.join(&file_name);

        let all_fn_names: Vec<String> = call_configs
            .iter()
            .map(|c| c.function_name.clone())
            .collect();

        let (missing_fn_names, has_existing_file, null_entries) =
            if ctx.existing_files.contains(&rel_path) {
                let index = column_indexes.get(collection_name);
                let indexed_cols: HashSet<String> = index
                    .and_then(|idx| idx.get(&file_name))
                    .map(|cols| cols.iter().cloned().collect())
                    .unwrap_or_default();

                // Check actual parquet schema to determine truly missing columns
                let parquet_cols = read_parquet_column_names(&output_path);

                // Missing: in config but not in the parquet file
                let missing: Vec<String> = all_fn_names
                    .iter()
                    .filter(|f| !parquet_cols.contains(*f))
                    .cloned()
                    .collect();

                // Only try to fill null results if:
                // 1) there is no column index file (indexed_cols is empty)
                // 2) the block range file is not in the column index
                // 3) the column with nulls is not in the column index for that file
                // If a column IS in the index, nulls are considered permanent.
                let null_entries: HashMap<String, HashSet<[u8; 20]>> = {
                    let unindexed_in_parquet: Vec<&String> = all_fn_names
                        .iter()
                        .filter(|f| parquet_cols.contains(*f) && !indexed_cols.contains(*f))
                        .collect();
                    if unindexed_in_parquet.is_empty() {
                        HashMap::new()
                    } else {
                        let all_null_entries = find_null_entries(&output_path);
                        all_null_entries
                            .into_iter()
                            .filter(|(k, _)| !indexed_cols.contains(k))
                            .collect()
                    }
                };

                if missing.is_empty() && null_entries.is_empty() {
                    continue;
                }
                (missing, true, null_entries)
            } else {
                (all_fn_names.clone(), false, HashMap::new())
            };

        let configs_to_call: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| missing_fn_names.contains(&c.function_name))
            .collect();

        let configs_to_patch: Vec<&OnceCallConfig> = call_configs
            .iter()
            .filter(|c| null_entries.contains_key(&c.function_name))
            .collect();

        // Build address discovery map
        let mut address_discovery: HashMap<Address, (u64, u64)> = HashMap::new();
        for (block_num, addrs) in &factory_data.addresses_by_block {
            for (timestamp, addr, coll_name) in addrs {
                if coll_name == collection_name {
                    address_discovery
                        .entry(*addr)
                        .or_insert((*block_num, *timestamp));
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

        // Missing columns: slots for ALL discovered addresses
        for call_config in &configs_to_call {
            for (discovered_address, (block_num, timestamp)) in &address_discovery {
                let target_address = call_config
                    .target_addresses
                    .as_ref()
                    .and_then(|addrs| addrs.first().copied())
                    .unwrap_or(*discovered_address);

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
                    target_address,
                    encoded_calldata: calldata,
                    metadata: FactoryOnceSlotMeta {
                        collection_name: collection_name.clone(),
                        function_name: call_config.function_name.clone(),
                        address: *discovered_address,
                        block_number: *block_num,
                        block_timestamp: *timestamp,
                    },
                });
            }
        }

        // Partially-null columns: slots only for addresses with null values
        for call_config in &configs_to_patch {
            let null_addrs = &null_entries[&call_config.function_name];
            for (discovered_address, (block_num, timestamp)) in &address_discovery {
                if !null_addrs.contains(&discovered_address.0 .0) {
                    continue;
                }
                let target_address = call_config
                    .target_addresses
                    .as_ref()
                    .and_then(|addrs| addrs.first().copied())
                    .unwrap_or(*discovered_address);

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
                    target_address,
                    encoded_calldata: calldata,
                    metadata: FactoryOnceSlotMeta {
                        collection_name: collection_name.clone(),
                        function_name: call_config.function_name.clone(),
                        address: *discovered_address,
                        block_number: *block_num,
                        block_timestamp: *timestamp,
                    },
                });
            }
        }

        // Clone configs for use in backfill phase
        let configs_for_backfill: Vec<OnceCallConfig> =
            configs_to_call.iter().map(|c| (*c).clone()).collect();

        let patch_fn_names: Vec<String> = null_entries.keys().cloned().collect();

        // Always add to collections_to_process - even with empty address_discovery,
        // existing files may need backfill for missing columns
        collections_to_process.push(FactoryContractProcessingInfo {
            name: collection_name.clone(),
            all_fn_names,
            missing_fn_names,
            patch_fn_names,
            output_path,
            has_existing_file,
            once_configs: configs_for_backfill,
        });
    }

    // Skip multicall execution only if there are no slots AND no collections need backfill
    let any_need_backfill = collections_to_process
        .iter()
        .any(|info| info.has_existing_file);
    if all_slots.is_empty() && !any_need_backfill {
        return Ok(());
    }

    // Group results by collection_name -> address -> (block_num, timestamp, function_results)
    let mut results_by_collection: CollectionResults = HashMap::new();

    // Execute multicalls only if there are slots to process
    if !all_slots.is_empty() {
        // Group slots by block for multicall batching
        let mut slots_by_block: HashMap<u64, Vec<MulticallSlotGeneric<FactoryOnceSlotMeta>>> =
            HashMap::new();
        for slot in all_slots {
            slots_by_block
                .entry(slot.block_number)
                .or_default()
                .push(slot);
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
            ctx.client,
            multicall3_address,
            block_multicalls,
            ctx.rpc_batch_size,
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
    for FactoryContractProcessingInfo {
        name: collection_name,
        all_fn_names,
        missing_fn_names,
        patch_fn_names,
        output_path,
        has_existing_file,
        once_configs: configs_to_call,
    } in collections_to_process
    {
        let sub_dir = output_path.parent().unwrap();
        std::fs::create_dir_all(sub_dir)?;

        // Get results for this collection (may be None if no new addresses discovered)
        let results_by_address = results_by_collection.remove(&collection_name);

        // Process if we have results OR if existing file needs backfill
        if results_by_address.is_some() || has_existing_file {
            let results_by_address = results_by_address.unwrap_or_default();

            if has_existing_file {
                let mut new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> =
                    results_by_address
                        .into_iter()
                        .map(|(addr, (_, _, fns))| (addr.0 .0, fns))
                        .collect();

                let existing_batches = read_existing_once_parquet(&output_path)?;
                if !existing_batches.is_empty() {
                    // Check which addresses already have data for the missing functions
                    let existing_results =
                        extract_existing_results_from_parquet(&existing_batches, &missing_fn_names);

                    // Find addresses in existing parquet
                    let existing_addresses = extract_addresses_from_once_parquet(&existing_batches);

                    // Build backfill slots only for addresses/functions that don't have data
                    let mut backfill_slots: Vec<MulticallSlotGeneric<FactoryOnceSlotMeta>> =
                        Vec::new();

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

                                let target_address = call_config
                                    .target_addresses
                                    .as_ref()
                                    .and_then(|addrs| addrs.first().copied())
                                    .unwrap_or(discovered_address);

                                let calldata =
                                    if let Some(preencoded) = &call_config.preencoded_calldata {
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
                        let mut slots_by_block: HashMap<
                            u64,
                            Vec<MulticallSlotGeneric<FactoryOnceSlotMeta>>,
                        > = HashMap::new();
                        for slot in backfill_slots {
                            slots_by_block
                                .entry(slot.block_number)
                                .or_default()
                                .push(slot);
                        }

                        let backfill_multicalls: Vec<BlockMulticall<FactoryOnceSlotMeta>> =
                            slots_by_block
                                .into_iter()
                                .map(|(block_number, slots)| BlockMulticall {
                                    block_number,
                                    block_id: BlockId::Number(BlockNumberOrTag::Number(
                                        block_number,
                                    )),
                                    slots,
                                })
                                .collect();

                        let backfill_results = execute_multicalls_generic(
                            ctx.client,
                            multicall3_address,
                            backfill_multicalls,
                            ctx.rpc_batch_size,
                        )
                        .await?;

                        for (meta, return_data, _success) in backfill_results {
                            let addr_bytes: [u8; 20] = meta.address.0 .0;
                            let entry = new_results.entry(addr_bytes).or_default();
                            entry.insert(meta.function_name, return_data);
                        }
                    }

                    // Combine missing + patch function names for merge
                    let all_new_fn_names: Vec<String> = missing_fn_names
                        .iter()
                        .chain(patch_fn_names.iter())
                        .cloned()
                        .collect();
                    let merged =
                        merge_once_columns(&existing_batches, &new_results, &all_new_fn_names)?;

                    let file = File::create(&output_path)?;
                    let props = WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::SNAPPY)
                        .build();
                    let mut writer = ArrowWriter::try_new(file, merged.schema(), Some(props))?;
                    writer.write(&merged)?;
                    writer.close()?;

                    tracing::info!(
                        "Merged {} new + {} patched multicall factory once columns into {} for {}",
                        missing_fn_names.len(),
                        patch_fn_names.len(),
                        output_path.display(),
                        collection_name
                    );
                }
            } else {
                let results: Vec<OnceCallResult> = results_by_address
                    .into_iter()
                    .map(
                        |(address, (block_number, timestamp, function_results))| OnceCallResult {
                            block_number,
                            block_timestamp: timestamp,
                            contract_address: address.0 .0,
                            function_results,
                        },
                    )
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

            // Upload to S3 if configured
            if let Some(sm) = ctx.storage_manager {
                let data_type = format!("raw/eth_calls/{}/once", collection_name);
                upload_parquet_to_s3(
                    sm,
                    &output_path,
                    ctx.chain_name,
                    &data_type,
                    range.start,
                    range.end - 1,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            // Update column index with all columns present in the file
            let file_name = output_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let actual_cols: Vec<String> = read_parquet_column_names(&output_path)
                .into_iter()
                .collect();
            let mut index = read_once_column_index(sub_dir);
            index.insert(file_name.clone(), actual_cols.clone());
            write_once_column_index(sub_dir, &index)?;

            if let Some(tx) = ctx.decoder_tx {
                let _ = tx
                    .send(DecoderMessage::OnceFileBackfilled {
                        range_start: range.start,
                        range_end: range.end,
                        contract_name: collection_name.clone(),
                    })
                    .await;
            }
        }
    }

    Ok(())
}

pub(crate) async fn process_range(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    ctx: &EthCallContext<'_>,
    call_configs: &[CallConfig],
    max_params: usize,
    frequency_state: &mut FrequencyState,
) -> Result<(), EthCallCollectionError> {
    let mut grouped_configs: HashMap<(String, String), Vec<&CallConfig>> = HashMap::new();
    for config in call_configs {
        grouped_configs
            .entry((config.contract_name.clone(), config.function_name.clone()))
            .or_default()
            .push(config);
    }

    for ((contract_name, function_name), configs) in &grouped_configs {
        let file_name = range.file_name("");
        let rel_path = format!("{}/{}/{}", contract_name, function_name, file_name);

        if ctx.existing_files.contains(&rel_path)
            || ctx.s3_manifest.as_ref().is_some_and(|m| {
                m.has_raw_eth_calls_granular(
                    contract_name,
                    function_name,
                    range.start,
                    range.end - 1,
                )
            })
        {
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

        let mut pending_calls: Vec<(TransactionRequest, BlockId, &BlockInfo, &CallConfig)> =
            Vec::new();

        for block in &filtered_blocks {
            for config in configs {
                // Skip if block is before contract's start_block
                if let Some(sb) = config.start_block {
                    if block.block_number < sb {
                        continue;
                    }
                }
                let tx = TransactionRequest::default()
                    .to(config.address)
                    .input(config.encoded_calldata.clone().into());
                let block_id = BlockId::Number(BlockNumberOrTag::Number(block.block_number));
                pending_calls.push((tx, block_id, block, config));
            }
        }

        for chunk in pending_calls.chunks(ctx.rpc_batch_size) {
            let calls: Vec<(TransactionRequest, BlockId)> = chunk
                .iter()
                .map(|(tx, block_id, _, _)| (tx.clone(), *block_id))
                .collect();

            #[cfg(feature = "bench")]
            let rpc_start = Instant::now();
            let results = ctx.client.call_batch(calls).await?;
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
                        let params_hex: Vec<String> = config
                            .param_values
                            .iter()
                            .map(|p| format!("0x{}", hex::encode(p)))
                            .collect();
                        tracing::warn!(
                            "eth_call failed for {}.{} at block {} (address {}, params {:?}): {}",
                            contract_name,
                            function_name,
                            block.block_number,
                            config.address,
                            params_hex,
                            e
                        );
                        // Skip reverted calls - don't store empty results
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
        let sub_dir = ctx.output_dir.join(contract_name).join(function_name);
        std::fs::create_dir_all(&sub_dir)?;
        let output_path = sub_dir.join(&file_name);

        let decoder_results: Option<Vec<DecoderEthCallResult>> = if ctx.decoder_tx.is_some() {
            Some(
                all_results
                    .iter()
                    .map(|r| DecoderEthCallResult {
                        block_number: r.block_number,
                        block_timestamp: r.block_timestamp,
                        contract_address: r.contract_address,
                        value: r.value_bytes.clone(),
                    })
                    .collect(),
            )
        } else {
            None
        };

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

        let output_path_for_upload = sub_dir.join(&file_name);
        tracing::info!(
            "Wrote {} eth_call results to {}",
            result_count,
            output_path_for_upload.display()
        );

        // Upload to S3 if configured
        if let Some(sm) = ctx.storage_manager {
            let data_type = format!("raw/eth_calls/{}/{}", contract_name, function_name);
            upload_parquet_to_s3(
                sm,
                &output_path_for_upload,
                ctx.chain_name,
                &data_type,
                range.start,
                range.end - 1,
            )
            .await
            .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }

        if let Some(tx) = ctx.decoder_tx {
            if let Some(results) = decoder_results {
                let _ = tx
                    .send(DecoderMessage::EthCallsReady {
                        range_start: range.start,
                        range_end: range.end,
                        contract_name: contract_name.clone(),
                        function_name: function_name.clone(),
                        results,
                        live_mode: false,
                        retry_transform_after_decode: false,
                    })
                    .await;
            }
        }

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
pub(crate) async fn process_range_multicall(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    ctx: &EthCallContext<'_>,
    call_configs: &[CallConfig],
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
        let file_name = range.file_name("");
        let rel_path = format!("{}/{}/{}", contract_name, function_name, file_name);

        if ctx.existing_files.contains(&rel_path)
            || ctx.s3_manifest.as_ref().is_some_and(|m| {
                m.has_raw_eth_calls_granular(
                    contract_name,
                    function_name,
                    range.start,
                    range.end - 1,
                )
            })
        {
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
                // Skip if block is before contract's start_block
                if let Some(sb) = config.start_block {
                    if block_number < sb {
                        continue;
                    }
                }
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
    let results =
        execute_multicalls_generic(ctx.client, multicall3_address, block_multicalls, ctx.rpc_batch_size)
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
            let file_name = range.file_name("");
            let sub_dir = ctx.output_dir
                .join(&group.contract_name)
                .join(&group.function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            let decoder_results: Option<Vec<DecoderEthCallResult>> = if ctx.decoder_tx.is_some() {
                Some(
                    results
                        .iter()
                        .map(|r| DecoderEthCallResult {
                            block_number: r.block_number,
                            block_timestamp: r.block_timestamp,
                            contract_address: r.contract_address,
                            value: r.value_bytes.clone(),
                        })
                        .collect(),
                )
            } else {
                None
            };

            let results_owned = std::mem::take(results);
            tokio::task::spawn_blocking(move || {
                write_results_to_parquet(&results_owned, &output_path, max_params)
            })
            .await
            .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

            let output_path_for_upload = sub_dir.join(&file_name);
            tracing::info!(
                "Wrote {} multicall eth_call results to {}",
                result_count,
                output_path_for_upload.display()
            );

            // Upload to S3 if configured
            if let Some(sm) = ctx.storage_manager {
                let data_type = format!(
                    "raw/eth_calls/{}/{}",
                    group.contract_name, group.function_name
                );
                upload_parquet_to_s3(
                    sm,
                    &output_path_for_upload,
                    ctx.chain_name,
                    &data_type,
                    range.start,
                    range.end - 1,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            if let Some(tx) = ctx.decoder_tx {
                if let Some(results) = decoder_results {
                    let _ = tx
                        .send(DecoderMessage::EthCallsReady {
                            range_start: range.start,
                            range_end: range.end,
                            contract_name: group.contract_name.clone(),
                            function_name: group.function_name.clone(),
                            results,
                            live_mode: false,
                            retry_transform_after_decode: false,
                        })
                        .await;
                }
            }

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
pub(crate) fn build_multicall_calldata(calls: &[(Address, &Bytes)]) -> Bytes {
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
pub(crate) fn decode_multicall_results(
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
pub(crate) struct MulticallSlot<'a> {
    pub(crate) group_key: (String, String), // (token_name, function_name)
    pub(crate) config: &'a TokenCallConfig,
    pub(crate) block: BlockInfo,
}

// =============================================================================
// Generic Multicall Infrastructure
// =============================================================================

/// Generic slot for tracking call metadata through multicall execution
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct MulticallSlotGeneric<M> {
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: u64,
    pub(crate) target_address: Address,
    pub(crate) encoded_calldata: Bytes,
    pub(crate) metadata: M,
}

/// Pending multicall for a single block
#[derive(Clone)]
pub(crate) struct BlockMulticall<M> {
    pub(crate) block_number: u64,
    pub(crate) block_id: BlockId,
    pub(crate) slots: Vec<MulticallSlotGeneric<M>>,
}

/// Metadata for regular calls
#[derive(Clone)]
pub(crate) struct RegularCallMeta {
    pub(crate) contract_name: String,
    pub(crate) function_name: String,
    pub(crate) contract_address: Address,
    pub(crate) param_values: Vec<Vec<u8>>,
}

/// Metadata for factory calls
#[derive(Clone)]
pub(crate) struct FactoryCallMeta {
    pub(crate) collection_name: String,
    pub(crate) function_name: String,
    pub(crate) contract_address: Address,
    pub(crate) param_values: Vec<Vec<u8>>,
}

/// Metadata for event-triggered calls
#[derive(Clone)]
pub(crate) struct EventCallMeta {
    pub(crate) contract_name: String,
    pub(crate) function_name: String,
    pub(crate) target_address: Address,
    pub(crate) log_index: u32,
    pub(crate) param_values: Vec<Vec<u8>>,
}

/// Metadata for once calls
#[derive(Clone)]
pub(crate) struct OnceCallMeta {
    pub(crate) contract_name: String,
    pub(crate) function_name: String,
    pub(crate) contract_address: Address,
}

/// Execute multicalls and return (metadata, return_data, success) for each slot
pub(crate) async fn execute_multicalls_generic<M: Clone + Send + Sync>(
    client: &UnifiedRpcClient,
    multicall3_address: Address,
    block_multicalls: Vec<BlockMulticall<M>>,
    rpc_batch_size: usize,
) -> Result<Vec<(M, Vec<u8>, bool)>, EthCallCollectionError> {
    // Track failed calls with chunk-relative indices
    struct ChunkFailedCall {
        relative_index: usize, // Index within the chunk's results
        target_address: Address,
        calldata: Bytes,
        block_number: u64,
        block_id: BlockId,
    }

    struct ChunkResult<M> {
        results: Vec<(M, Vec<u8>, bool)>,
        failed_calls: Vec<ChunkFailedCall>,
    }

    // Number of chunks to process concurrently
    let chunk_concurrency = 4;

    // Pre-collect chunks into owned vectors to avoid lifetime issues
    let owned_chunks: Vec<Vec<BlockMulticall<M>>> = block_multicalls
        .chunks(rpc_batch_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    // Process chunks concurrently using buffered (maintains order for correct index calculation)
    let chunk_results: Vec<ChunkResult<M>> = stream::iter(owned_chunks)
        .map(|chunk| async move {
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

            let mut chunk_results: Vec<(M, Vec<u8>, bool)> = Vec::new();
            let mut chunk_failed_calls: Vec<ChunkFailedCall> = Vec::new();

            for (i, result) in results.into_iter().enumerate() {
                let bm = &chunk[i];
                let slot_count = bm.slots.len();

                match result {
                    Ok(bytes) => {
                        match decode_multicall_results(&bytes, slot_count) {
                            Ok(decoded) => {
                                for (j, (success, return_data)) in decoded.into_iter().enumerate() {
                                    let slot = &bm.slots[j];
                                    if !success {
                                        tracing::warn!(
                                            "Multicall sub-call failed at block {} targeting {}",
                                            bm.block_number,
                                            slot.target_address
                                        );
                                        chunk_failed_calls.push(ChunkFailedCall {
                                            relative_index: chunk_results.len(),
                                            target_address: slot.target_address,
                                            calldata: slot.encoded_calldata.clone(),
                                            block_number: bm.block_number,
                                            block_id: bm.block_id,
                                        });
                                    }
                                    chunk_results.push((
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
                                    chunk_results.push((slot.metadata.clone(), Vec::new(), false));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Multicall RPC failed for block {}: {}", bm.block_number, e);
                        // Treat all sub-calls as failed
                        for slot in &bm.slots {
                            chunk_results.push((slot.metadata.clone(), Vec::new(), false));
                        }
                    }
                }
            }
            Ok::<_, EthCallCollectionError>(ChunkResult {
                results: chunk_results,
                failed_calls: chunk_failed_calls,
            })
        })
        .buffered(chunk_concurrency)
        .try_collect()
        .await?;

    // Flatten results and compute global indices for failed calls
    struct FailedCall {
        result_index: usize,
        target_address: Address,
        calldata: Bytes,
        block_number: u64,
        block_id: BlockId,
    }

    let mut all_results: Vec<(M, Vec<u8>, bool)> = Vec::new();
    let mut failed_retries: Vec<FailedCall> = Vec::new();

    for chunk_result in chunk_results {
        let base_index = all_results.len();

        // Convert chunk-relative indices to global indices
        for failed in chunk_result.failed_calls {
            failed_retries.push(FailedCall {
                result_index: base_index + failed.relative_index,
                target_address: failed.target_address,
                calldata: failed.calldata,
                block_number: failed.block_number,
                block_id: failed.block_id,
            });
        }

        all_results.extend(chunk_result.results);
    }

    // Retry failed multicall sub-calls individually
    if !failed_retries.is_empty() {
        tracing::info!(
            "Retrying {} failed multicall sub-calls individually",
            failed_retries.len()
        );

        let retry_batch: Vec<(TransactionRequest, BlockId)> = failed_retries
            .iter()
            .map(|f| {
                let tx = TransactionRequest::default()
                    .to(f.target_address)
                    .input(f.calldata.clone().into());
                (tx, f.block_id)
            })
            .collect();

        let retry_results = client.call_batch(retry_batch).await?;

        for (i, result) in retry_results.into_iter().enumerate() {
            let failed = &failed_retries[i];
            match result {
                Ok(bytes) if !bytes.is_empty() => {
                    tracing::info!(
                        "Individual retry succeeded for block {} target {}",
                        failed.block_number,
                        failed.target_address
                    );
                    let entry = &mut all_results[failed.result_index];
                    *entry = (entry.0.clone(), bytes.to_vec(), true);
                }
                Ok(_) => {
                    tracing::warn!(
                        "Individual retry returned empty for block {} target {}",
                        failed.block_number,
                        failed.target_address
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Individual retry also failed for block {} target {}: {}",
                        failed.block_number,
                        failed.target_address,
                        e
                    );
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
pub(crate) async fn process_token_range_multicall(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    ctx: &EthCallContext<'_>,
    token_configs: &[TokenCallConfig],
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
        let file_name = range.file_name("");
        let rel_path = format!("{}/{}/{}", output_name, function_name, file_name);

        if ctx.existing_files.contains(&rel_path) {
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
            let _tx = TransactionRequest::default()
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
    for chunk in pending_multicalls.chunks(ctx.rpc_batch_size) {
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

        let results = ctx.client.call_batch(calls).await?;

        for (i, result) in results.into_iter().enumerate() {
            let pm = &chunk[i];
            let slot_count = pm.slots.len();

            match result {
                Ok(bytes) => {
                    match decode_multicall_results(&bytes, slot_count) {
                        Ok(decoded) => {
                            for (j, (success, return_data)) in decoded.into_iter().enumerate() {
                                let slot = &pm.slots[j];
                                // Skip failed sub-calls - don't store empty results
                                if !success {
                                    continue;
                                }
                                if let Some(results) = group_results.get_mut(&slot.group_key) {
                                    results.push(CallResult {
                                        block_number: slot.block.block_number,
                                        block_timestamp: slot.block.timestamp,
                                        contract_address: slot.config.target_address.0 .0,
                                        value_bytes: return_data,
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
                            // Skip all sub-calls - don't store empty results
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Multicall RPC failed for block {}: {}", pm.block_number, e);
                    // Skip all sub-calls - don't store empty results
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
            let file_name = range.file_name("");
            let sub_dir = ctx.output_dir
                .join(&group.output_name)
                .join(&group.function_name);
            std::fs::create_dir_all(&sub_dir)?;
            let output_path = sub_dir.join(&file_name);

            let decoder_results: Option<Vec<DecoderEthCallResult>> = if ctx.decoder_tx.is_some() {
                Some(
                    results
                        .iter()
                        .map(|r| DecoderEthCallResult {
                            block_number: r.block_number,
                            block_timestamp: r.block_timestamp,
                            contract_address: r.contract_address,
                            value: r.value_bytes.clone(),
                        })
                        .collect(),
                )
            } else {
                None
            };

            let results_owned = std::mem::take(results);
            tokio::task::spawn_blocking(move || {
                write_results_to_parquet(&results_owned, &output_path, 0)
            })
            .await
            .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

            let output_path_for_upload = sub_dir.join(&file_name);
            tracing::info!(
                "Wrote {} multicall token results to {}",
                result_count,
                output_path_for_upload.display()
            );

            // Upload to S3 if configured
            if let Some(sm) = ctx.storage_manager {
                let data_type = format!(
                    "raw/eth_calls/{}/{}",
                    group.output_name, group.function_name
                );
                upload_parquet_to_s3(
                    sm,
                    &output_path_for_upload,
                    ctx.chain_name,
                    &data_type,
                    range.start,
                    range.end - 1,
                )
                .await
                .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }

            if let Some(tx) = ctx.decoder_tx {
                if let Some(results) = decoder_results {
                    let _ = tx
                        .send(DecoderMessage::EthCallsReady {
                            range_start: range.start,
                            range_end: range.end,
                            contract_name: group.output_name.clone(),
                            function_name: group.function_name.clone(),
                            results,
                            live_mode: false,
                            retry_transform_after_decode: false,
                        })
                        .await;
                }
            }

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
pub(crate) async fn process_token_range(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    ctx: &EthCallContext<'_>,
    token_configs: &[TokenCallConfig],
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
        let file_name = range.file_name("");
        let rel_path = format!("{}/{}/{}", output_name, function_name, file_name);

        if ctx.existing_files.contains(&rel_path) {
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

        for chunk in pending_calls.chunks(ctx.rpc_batch_size) {
            let calls: Vec<(TransactionRequest, BlockId)> = chunk
                .iter()
                .map(|(tx, block_id, _, _)| (tx.clone(), *block_id))
                .collect();

            let results = ctx.client.call_batch(calls).await?;

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
                        // Skip reverted calls - don't store empty results
                    }
                }
            }
        }

        all_results.sort_by_key(|r| (r.block_number, r.contract_address));

        let result_count = all_results.len();
        let sub_dir = ctx.output_dir.join(&output_name).join(function_name);
        std::fs::create_dir_all(&sub_dir)?;
        let output_path = sub_dir.join(&file_name);

        let decoder_results: Option<Vec<DecoderEthCallResult>> = if ctx.decoder_tx.is_some() {
            Some(
                all_results
                    .iter()
                    .map(|r| DecoderEthCallResult {
                        block_number: r.block_number,
                        block_timestamp: r.block_timestamp,
                        contract_address: r.contract_address,
                        value: r.value_bytes.clone(),
                    })
                    .collect(),
            )
        } else {
            None
        };

        tokio::task::spawn_blocking(move || {
            write_results_to_parquet(&all_results, &output_path, 0) // No params
        })
        .await
        .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

        let output_path_for_upload = sub_dir.join(&file_name);
        tracing::info!(
            "Wrote {} token call results to {}",
            result_count,
            output_path_for_upload.display()
        );

        // Upload to S3 if configured
        if let Some(sm) = ctx.storage_manager {
            let data_type = format!("raw/eth_calls/{}/{}", output_name, function_name);
            upload_parquet_to_s3(
                sm,
                &output_path_for_upload,
                ctx.chain_name,
                &data_type,
                range.start,
                range.end - 1,
            )
            .await
            .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }

        if let Some(tx) = ctx.decoder_tx {
            if let Some(results) = decoder_results {
                let _ = tx
                    .send(DecoderMessage::EthCallsReady {
                        range_start: range.start,
                        range_end: range.end,
                        contract_name: output_name.clone(),
                        function_name: function_name.clone(),
                        results,
                        live_mode: false,
                        retry_transform_after_decode: false,
                    })
                    .await;
            }
        }

        if let Frequency::Duration(_) = frequency {
            if let Some(last_block) = filtered_blocks.last() {
                frequency_state.last_call_times.insert(
                    (output_name.clone(), function_name.clone()),
                    last_block.timestamp,
                );
            }
        }
    }

    Ok(())
}
