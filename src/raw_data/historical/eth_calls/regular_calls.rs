//! Regular (non-multicall) eth_call execution for configured contracts and
//! factory-discovered contracts.

use std::collections::{HashMap, HashSet};
#[cfg(feature = "bench")]
use std::time::Instant;

use alloy::dyn_abi::DynSolValue;
use alloy::primitives::Address;
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};

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
use crate::storage::upload_parquet_to_s3;
use crate::types::config::eth_call::{encode_call_with_params, EthCallConfig, Frequency};

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
