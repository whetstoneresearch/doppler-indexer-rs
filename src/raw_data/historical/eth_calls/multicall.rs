//! Multicall3 infrastructure and multicall-based process functions for
//! regular and factory eth_calls.

use std::collections::{HashMap, HashSet};

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};
use futures::{stream, StreamExt, TryStreamExt};

use super::config::{compute_function_selector, generate_param_combinations};
use super::finalization::extract_function_name;
use super::frequency::filter_blocks_for_frequency;
use super::parquet_io::write_results_to_parquet;
use super::types::{
    BlockInfo, BlockRange, CallConfig, CallResult, EthCallCollectionError, EthCallContext,
    EncodedParam, FrequencyState,
};
use crate::decoding::{DecoderMessage, EthCallResult as DecoderEthCallResult};
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::rpc::UnifiedRpcClient;
use crate::storage::upload_parquet_to_s3;
use crate::types::config::eth_call::{encode_call_with_params, EthCallConfig, Frequency};

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

            let result_count = results.len();
            let file_name = range.file_name("");
            let sub_dir = ctx
                .output_dir
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
            let function_name = extract_function_name(&call_config.function).to_string();

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

            let result_count = results.len();
            let file_name = range.file_name("");
            let sub_dir = ctx
                .output_dir
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
