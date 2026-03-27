//! Token pool call execution: `process_token_range` and
//! `process_token_range_multicall`, plus the token-specific `MulticallSlot`.

use std::collections::{HashMap, HashSet};

use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};

use super::frequency::filter_blocks_for_frequency;
use super::multicall::{build_multicall_calldata, decode_multicall_results};
use super::parquet_io::write_results_to_parquet;
use super::types::{
    BlockInfo, BlockRange, CallResult, EthCallCollectionError, EthCallContext, FrequencyState,
    TokenCallConfig,
};
use crate::decoding::{DecoderMessage, EthCallResult as DecoderEthCallResult};
use crate::storage::upload_parquet_to_s3;
use crate::types::config::eth_call::Frequency;

/// Tracks which group and config index a multicall slot maps back to
pub(crate) struct MulticallSlot<'a> {
    pub(crate) group_key: (String, String), // (token_name, function_name)
    pub(crate) config: &'a TokenCallConfig,
    pub(crate) block: BlockInfo,
}

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
            let sub_dir = ctx
                .output_dir
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
