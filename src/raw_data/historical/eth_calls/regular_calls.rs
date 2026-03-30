//! Regular and factory call execution: process_range, process_range_multicall,
//! process_factory_range, process_factory_range_multicall.

use std::collections::{HashMap, HashSet};
#[cfg(feature = "bench")]
use std::time::Instant;

use alloy::dyn_abi::DynSolValue;
use alloy::primitives::Address;
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};

use super::config::{compute_function_selector, generate_param_combinations};
use super::frequency::filter_blocks_for_frequency;
use super::multicall::{
    execute_multicalls_generic, BlockMulticall, FactoryCallMeta, FactoryGroupInfo,
    MulticallSlotGeneric, RegularCallMeta, RegularGroupInfo,
};
use super::postprocessing::{finalize_regular_results, FinalizeRegularParams};
use super::types::{
    BlockInfo, BlockRange, CallConfig, CallResult, EthCallCollectionError, EthCallContext,
    EncodedParam, FrequencyState,
};
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::types::config::eth_call::{encode_call_with_params, EthCallConfig};

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
                        .map(|(param_type, value, _): &EncodedParam| param_type.parse_value(value))
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

            let calls: Vec<(TransactionRequest, BlockId)> = pending_calls
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
                let (_, _, block, config) = &pending_calls[i];
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

            all_results
                .sort_by_key(|r| (r.block_number, r.contract_address, r.param_values.clone()));

            let sub_dir = ctx.output_dir.join(collection_name).join(&function_name);
            tokio::fs::create_dir_all(&sub_dir).await?;

            let last_ts = filtered_blocks.last().map(|b| b.timestamp);

            finalize_regular_results(FinalizeRegularParams {
                results: all_results,
                max_params,
                sub_dir,
                file_name,
                log_label: "factory eth_call".to_string(),
                s3_data_type: format!("raw/eth_calls/{}/{}", collection_name, function_name),
                chain_name: ctx.chain_name,
                range_start: range.start,
                range_end: range.end,
                storage_manager: ctx.storage_manager,
                decoder_tx: ctx.decoder_tx,
                contract_name: collection_name.clone(),
                function_name: function_name.clone(),
                frequency: &call_config.frequency,
                frequency_state,
                last_block_timestamp: last_ts,
                #[cfg(feature = "bench")]
                bench: Some((
                    format!("eth_calls_{}.{}", collection_name, function_name),
                    rpc_time,
                    process_time,
                )),
            })
            .await?;
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
                        .map(|(param_type, value, _): &EncodedParam| param_type.parse_value(value))
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
    let results = execute_multicalls_generic(
        ctx.client,
        multicall3_address,
        block_multicalls,
        ctx.rpc_batch_size,
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

            let file_name = range.file_name("");
            let sub_dir = ctx
                .output_dir
                .join(&group.collection_name)
                .join(&group.function_name);
            tokio::fs::create_dir_all(&sub_dir).await?;

            let last_ts = group.filtered_blocks.last().map(|b| b.timestamp);

            finalize_regular_results(FinalizeRegularParams {
                results: std::mem::take(results),
                max_params,
                sub_dir,
                file_name,
                log_label: "multicall factory eth_call".to_string(),
                s3_data_type: format!(
                    "raw/eth_calls/{}/{}",
                    group.collection_name, group.function_name
                ),
                chain_name: ctx.chain_name,
                range_start: range.start,
                range_end: range.end,
                storage_manager: ctx.storage_manager,
                decoder_tx: ctx.decoder_tx,
                contract_name: group.collection_name.clone(),
                function_name: group.function_name.clone(),
                frequency: &group.frequency,
                frequency_state,
                last_block_timestamp: last_ts,
                #[cfg(feature = "bench")]
                bench: None,
            })
            .await?;
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

        let calls: Vec<(TransactionRequest, BlockId)> = pending_calls
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
            let (_, _, block, config) = &pending_calls[i];
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

        all_results.sort_by_key(|r| (r.block_number, r.contract_address, r.param_values.clone()));

        let sub_dir = ctx.output_dir.join(contract_name).join(function_name);
        tokio::fs::create_dir_all(&sub_dir).await?;

        let last_ts = filtered_blocks.last().map(|b| b.timestamp);

        finalize_regular_results(FinalizeRegularParams {
            results: all_results,
            max_params,
            sub_dir,
            file_name,
            log_label: "eth_call".to_string(),
            s3_data_type: format!("raw/eth_calls/{}/{}", contract_name, function_name),
            chain_name: ctx.chain_name,
            range_start: range.start,
            range_end: range.end,
            storage_manager: ctx.storage_manager,
            decoder_tx: ctx.decoder_tx,
            contract_name: contract_name.clone(),
            function_name: function_name.clone(),
            frequency,
            frequency_state,
            last_block_timestamp: last_ts,
            #[cfg(feature = "bench")]
            bench: Some((
                format!("eth_calls_{}.{}", contract_name, function_name),
                rpc_time,
                process_time,
            )),
        })
        .await?;
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
    let mut active_groups: Vec<RegularGroupInfo> = Vec::new();

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

        active_groups.push(RegularGroupInfo {
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
    let results = execute_multicalls_generic(
        ctx.client,
        multicall3_address,
        block_multicalls,
        ctx.rpc_batch_size,
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

            let file_name = range.file_name("");
            let sub_dir = ctx
                .output_dir
                .join(&group.contract_name)
                .join(&group.function_name);
            tokio::fs::create_dir_all(&sub_dir).await?;

            let last_ts = group.filtered_blocks.last().map(|b| b.timestamp);

            finalize_regular_results(FinalizeRegularParams {
                results: std::mem::take(results),
                max_params,
                sub_dir,
                file_name,
                log_label: "multicall eth_call".to_string(),
                s3_data_type: format!(
                    "raw/eth_calls/{}/{}",
                    group.contract_name, group.function_name
                ),
                chain_name: ctx.chain_name,
                range_start: range.start,
                range_end: range.end,
                storage_manager: ctx.storage_manager,
                decoder_tx: ctx.decoder_tx,
                contract_name: group.contract_name.clone(),
                function_name: group.function_name.clone(),
                frequency: &group.frequency,
                frequency_state,
                last_block_timestamp: last_ts,
                #[cfg(feature = "bench")]
                bench: None,
            })
            .await?;
        }
    }

    Ok(())
}
