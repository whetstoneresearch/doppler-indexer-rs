//! Once-call execution: process_once_calls_regular, process_once_calls_multicall,
//! process_factory_once_calls, process_factory_once_calls_multicall.

use std::collections::{HashMap, HashSet};

use alloy::primitives::Address;
use alloy::rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest};

use super::event_triggers::encode_once_call_params;
use super::multicall::{
    execute_multicalls_generic, BlockMulticall, FactoryOnceSlotMeta, MulticallSlotGeneric,
    OnceCallMeta,
};
use super::parquet_io::{
    extract_addresses_from_once_parquet, extract_existing_results_from_parquet,
    find_null_entries_async, merge_once_columns, read_existing_once_parquet_async,
    read_once_column_index_async, read_parquet_column_names_async, write_once_column_index_async,
    write_once_results_to_parquet_async, write_record_batch_to_parquet_async,
};
use super::types::{
    AddressResults, BlockInfo, BlockRange, CollectionResults, ContractProcessingInfo,
    EthCallCollectionError, EthCallContext, FactoryContractProcessingInfo, OnceCallConfig,
    OnceCallResult,
};
use crate::decoding::{DecoderMessage, OnceCallResult as DecoderOnceCallResult};
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::storage::contract_index::{
    get_missing_contracts, range_key, read_contract_index, update_contract_index,
    write_contract_index, ExpectedContracts,
};
use crate::storage::upload_parquet_to_s3;
use crate::types::config::contract::{AddressOrAddresses, Contracts};

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
                let index = read_once_column_index_async(sub_dir.clone()).await;
                let existing_cols: HashSet<String> = if let Some(cols) = index.get(&file_name) {
                    cols.iter().cloned().collect()
                } else {
                    tracing::debug!(
                        "File {} exists but not in index, reading schema from parquet",
                        output_path.display()
                    );
                    read_parquet_column_names_async(output_path.clone()).await
                };
                let missing: Vec<String> = all_fn_names
                    .iter()
                    .filter(|f| !existing_cols.contains(*f))
                    .cloned()
                    .collect();
                // Find addresses with null values for existing columns
                let all_null_entries = find_null_entries_async(output_path.clone()).await;
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

        tokio::fs::create_dir_all(&sub_dir).await?;

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

            let existing_batches = read_existing_once_parquet_async(output_path.clone()).await?;
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

                write_record_batch_to_parquet_async(merged, output_path.clone()).await?;

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

                write_once_results_to_parquet_async(results, output_path, all_fn_names.clone())
                    .await?;
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
        let actual_cols: Vec<String> = read_parquet_column_names_async(output_path.clone())
            .await
            .into_iter()
            .collect();
        let mut index = read_once_column_index_async(sub_dir.clone()).await;
        index.insert(file_name.clone(), actual_cols.clone());
        write_once_column_index_async(sub_dir.to_path_buf(), index).await?;
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
    expected_contracts: Option<&HashMap<String, ExpectedContracts>>,
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

        let (missing_fn_names, has_existing_file, null_entries) = if ctx
            .existing_files
            .contains(&rel_path)
        {
            let index = column_indexes.get(collection_name);
            let indexed_cols: HashSet<String> = index
                .and_then(|idx| idx.get(&file_name))
                .map(|cols| cols.iter().cloned().collect())
                .unwrap_or_default();

            // Check actual parquet schema to determine truly missing columns
            let parquet_cols = read_parquet_column_names_async(output_path.clone()).await;

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
                    let all_null_entries = find_null_entries_async(output_path.clone()).await;
                    all_null_entries
                        .into_iter()
                        .filter(|(k, _)| !indexed_cols.contains(k))
                        .collect()
                }
            };

            if missing.is_empty() && null_entries.is_empty() {
                // Column check passes. Also verify contract-index completeness.
                let index_complete = match expected_contracts.and_then(|m| m.get(collection_name)) {
                    None => true,
                    Some(expected) => {
                        let index = read_contract_index(&sub_dir);
                        let rk = range_key(range.start, range.end - 1);
                        get_missing_contracts(&index, &rk, expected).is_empty()
                    }
                };
                if index_complete {
                    tracing::debug!(
                        "Skipping factory once eth_calls for {} blocks {}-{} (all columns present and indexed)",
                        collection_name,
                        range.start,
                        range.end - 1
                    );
                    continue;
                }
                tracing::info!(
                    "Factory once {} blocks {}-{}: columns complete but contract index incomplete, re-checking",
                    collection_name,
                    range.start,
                    range.end - 1
                );
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
            tokio::fs::create_dir_all(&sub_dir).await?;
            write_once_results_to_parquet_async(vec![], output_path.clone(), all_fn_names.clone())
                .await?;
            let mut index = read_once_column_index_async(sub_dir.clone()).await;
            index.insert(file_name.clone(), all_fn_names.clone());
            write_once_column_index_async(sub_dir.to_path_buf(), index).await?;
            if let Some(expected) = expected_contracts.and_then(|m| m.get(collection_name)) {
                let rk = range_key(range.start, range.end - 1);
                let mut ci = read_contract_index(&sub_dir);
                update_contract_index(&mut ci, &rk, expected);
                if let Err(e) = write_contract_index(&sub_dir, &ci) {
                    tracing::warn!("Failed to write once contract index (empty range) for {}: {}", collection_name, e);
                }
            }
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

        // C. Read parquet early to identify absent addresses
        let (mut existing_batches_opt, existing_addr_set) = if has_existing_file {
            let batches = read_existing_once_parquet_async(output_path.clone()).await?;
            let addrs: HashSet<[u8; 20]> = extract_addresses_from_once_parquet(&batches)
                .into_keys()
                .collect();
            (Some(batches), addrs)
        } else {
            (None, HashSet::new())
        };

        let absent_addresses: Vec<(&Address, &(u64, u64))> = address_discovery
            .iter()
            .filter(|(addr, _)| !existing_addr_set.is_empty() && !existing_addr_set.contains(&addr.0 .0))
            .collect();

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

        // D. Absent addresses: call existing functions for addresses not yet in the parquet
        if !absent_addresses.is_empty() {
            let already_covered_by_global: HashSet<&str> =
                configs_to_call.iter().map(|c| c.function_name.as_str()).collect();

            for (addr, (block_number, timestamp)) in &absent_addresses {
                let block_id = BlockId::Number(BlockNumberOrTag::Number(*block_number));

                for call_config in call_configs.iter() {
                    if already_covered_by_global.contains(call_config.function_name.as_str()) {
                        continue;
                    }
                    let target_address = call_config
                        .target_addresses
                        .as_ref()
                        .and_then(|addrs| addrs.first().copied())
                        .unwrap_or(**addr);

                    let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                        preencoded.clone()
                    } else {
                        encode_once_call_params(
                            call_config.function_selector,
                            &call_config.params,
                            **addr,
                        )?
                    };
                    let tx = TransactionRequest::default()
                        .to(target_address)
                        .input(calldata.into());
                    pending_calls.push((
                        tx,
                        block_id,
                        **addr,
                        *block_number,
                        *timestamp,
                        call_config.function_name.clone(),
                    ));
                }
            }
        }

        // H. No-op optimization: if contract-index gate fell through but no calls needed,
        // just write the missing contract_index.json and skip the parquet rewrite.
        if pending_calls.is_empty() && has_existing_file {
            if let Some(expected) = expected_contracts.and_then(|m| m.get(collection_name)) {
                let rk = range_key(range.start, range.end - 1);
                let mut ci = read_contract_index(&sub_dir);
                update_contract_index(&mut ci, &rk, expected);
                if let Err(e) = write_contract_index(&sub_dir, &ci) {
                    tracing::warn!("Failed to write once contract index for {}: {}", collection_name, e);
                } else {
                    tracing::info!(
                        "Factory once {} blocks {}-{}: wrote missing contract index (no parquet changes needed)",
                        collection_name, range.start, range.end - 1
                    );
                }
            }
            continue;
        }

        // Skip only if no pending calls AND no existing file to backfill
        if pending_calls.is_empty() && !has_existing_file {
            continue;
        }

        // Execute batch calls only if there are pending calls
        let results_by_address: AddressResults = if !pending_calls.is_empty() {
            let batch_calls: Vec<(TransactionRequest, BlockId)> = pending_calls
                .iter()
                .map(|(tx, bid, _, _, _, _)| (tx.clone(), *bid))
                .collect();

            let batch_results = ctx.client.call_batch(batch_calls).await?;

            let mut results_map: AddressResults = HashMap::new();

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

        tokio::fs::create_dir_all(&sub_dir).await?;

        if has_existing_file {
            // Merge new columns into existing parquet
            let mut new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> = results_by_address
                .into_iter()
                .map(|(addr, (_, _, fns))| (addr.0 .0, fns))
                .collect();

            // E. Reuse pre-read batches from step C (or read now if not available)
            let existing_batches = match existing_batches_opt.take() {
                Some(batches) => batches,
                None => read_existing_once_parquet_async(output_path.clone()).await?,
            };
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

                write_record_batch_to_parquet_async(merged, output_path.clone()).await?;

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

                write_once_results_to_parquet_async(
                    results,
                    output_path.clone(),
                    all_fn_names.clone(),
                )
                .await?;
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
        let actual_cols: Vec<String> = read_parquet_column_names_async(output_path.clone())
            .await
            .into_iter()
            .collect();
        let mut index = read_once_column_index_async(sub_dir.clone()).await;
        index.insert(file_name.clone(), actual_cols.clone());
        write_once_column_index_async(sub_dir.to_path_buf(), index).await?;
        tracing::info!(
            "Updated column index for factory {}: {} now has {} columns: {:?}",
            collection_name,
            file_name,
            actual_cols.len(),
            actual_cols
        );

        // F. Write contract index after parquet write
        if let Some(expected) = expected_contracts.and_then(|m| m.get(collection_name)) {
            let rk = range_key(range.start, range.end - 1);
            let mut ci = read_contract_index(&sub_dir);
            update_contract_index(&mut ci, &rk, expected);
            if let Err(e) = write_contract_index(&sub_dir, &ci) {
                tracing::warn!("Failed to write once contract index for {}: {}", collection_name, e);
            }
        }

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
                let index = read_once_column_index_async(sub_dir.clone()).await;
                let existing_cols: HashSet<String> = if let Some(cols) = index.get(&file_name) {
                    cols.iter().cloned().collect()
                } else {
                    read_parquet_column_names_async(output_path.clone()).await
                };
                let missing: Vec<String> = all_fn_names
                    .iter()
                    .filter(|f| !existing_cols.contains(*f))
                    .cloned()
                    .collect();
                // Find addresses with null values for existing columns
                let all_null_entries = find_null_entries_async(output_path.clone()).await;
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
        execute_multicalls_generic(ctx.client, multicall3_address, block_multicalls).await?;

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
        tokio::fs::create_dir_all(sub_dir).await?;

        if let Some(results_by_address) = results_by_contract.remove(&contract_name) {
            let decoder_once_results: Option<Vec<DecoderOnceCallResult>> =
                if ctx.decoder_tx.is_some() {
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

                let existing_batches =
                    read_existing_once_parquet_async(output_path.clone()).await?;
                if !existing_batches.is_empty() {
                    // Combine missing + patch function names for merge
                    let all_new_fn_names: Vec<String> = missing_fn_names
                        .iter()
                        .chain(patch_fn_names.iter())
                        .cloned()
                        .collect();
                    let merged =
                        merge_once_columns(&existing_batches, &new_results, &all_new_fn_names)?;

                    write_record_batch_to_parquet_async(merged, output_path.clone()).await?;

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

                    write_once_results_to_parquet_async(
                        results,
                        output_path.clone(),
                        all_fn_names.clone(),
                    )
                    .await?;
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
            let actual_cols: Vec<String> = read_parquet_column_names_async(output_path.clone())
                .await
                .into_iter()
                .collect();
            let mut index = read_once_column_index_async(sub_dir.to_path_buf()).await;
            index.insert(file_name.clone(), actual_cols.clone());
            write_once_column_index_async(sub_dir.to_path_buf(), index).await?;

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
    expected_contracts: Option<&HashMap<String, ExpectedContracts>>,
) -> Result<(), EthCallCollectionError> {
    // Collect all calls across all collections into one multicall
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
                let parquet_cols = read_parquet_column_names_async(output_path.clone()).await;

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
                        let all_null_entries = find_null_entries_async(output_path.clone()).await;
                        all_null_entries
                            .into_iter()
                            .filter(|(k, _)| !indexed_cols.contains(k))
                            .collect()
                    }
                };

                if missing.is_empty() && null_entries.is_empty() {
                    // Column check passes. Also verify contract-index completeness.
                    let index_complete =
                        match expected_contracts.and_then(|m| m.get(collection_name)) {
                            None => true,
                            Some(expected) => {
                                let index = read_contract_index(&sub_dir);
                                let rk = range_key(range.start, range.end - 1);
                                get_missing_contracts(&index, &rk, expected).is_empty()
                            }
                        };
                    if index_complete {
                        continue;
                    }
                    tracing::info!(
                        "Factory once multicall {} blocks {}-{}: columns complete but contract index incomplete, re-checking",
                        collection_name,
                        range.start,
                        range.end - 1
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
            tokio::fs::create_dir_all(&sub_dir).await?;
            write_once_results_to_parquet_async(vec![], output_path.clone(), all_fn_names.clone())
                .await?;
            let mut index = read_once_column_index_async(sub_dir.clone()).await;
            index.insert(file_name.clone(), all_fn_names.clone());
            write_once_column_index_async(sub_dir.to_path_buf(), index).await?;
            // G. Write contract index in empty-file branch
            if let Some(expected) = expected_contracts.and_then(|m| m.get(collection_name)) {
                let rk = range_key(range.start, range.end - 1);
                let mut ci = read_contract_index(&sub_dir);
                update_contract_index(&mut ci, &rk, expected);
                if let Err(e) = write_contract_index(&sub_dir, &ci) {
                    tracing::warn!("Failed to write once contract index (empty range) for {}: {}", collection_name, e);
                }
            }
            continue;
        }

        // C. Read parquet early to identify absent addresses
        let existing_addr_set: HashSet<[u8; 20]> = if has_existing_file {
            let batches = read_existing_once_parquet_async(output_path.clone()).await?;
            extract_addresses_from_once_parquet(&batches)
                .into_keys()
                .collect()
        } else {
            HashSet::new()
        };

        let absent_addresses: Vec<(&Address, &(u64, u64))> = address_discovery
            .iter()
            .filter(|(addr, _)| !existing_addr_set.is_empty() && !existing_addr_set.contains(&addr.0 .0))
            .collect();

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

        // D. Absent addresses: add slots for existing functions for addresses not yet in the parquet
        if !absent_addresses.is_empty() {
            let already_covered_by_global: HashSet<&str> =
                configs_to_call.iter().map(|c| c.function_name.as_str()).collect();

            for (addr, (block_num, timestamp)) in &absent_addresses {
                for call_config in call_configs.iter() {
                    if already_covered_by_global.contains(call_config.function_name.as_str()) {
                        continue;
                    }
                    let target_address = call_config
                        .target_addresses
                        .as_ref()
                        .and_then(|addrs| addrs.first().copied())
                        .unwrap_or(**addr);

                    let calldata = if let Some(preencoded) = &call_config.preencoded_calldata {
                        preencoded.clone()
                    } else {
                        encode_once_call_params(
                            call_config.function_selector,
                            &call_config.params,
                            **addr,
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
                            address: **addr,
                            block_number: *block_num,
                            block_timestamp: *timestamp,
                        },
                    });
                }
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
        let results =
            execute_multicalls_generic(ctx.client, multicall3_address, block_multicalls).await?;

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
        tokio::fs::create_dir_all(sub_dir).await?;

        // Get results for this collection (may be None if no new addresses discovered)
        let results_by_address = results_by_collection.remove(&collection_name);

        // H. No-op optimization: if contract-index gate fell through but no slots were
        // queued for this collection and the parquet already exists, just write the
        // missing contract_index.json and skip the parquet rewrite.
        let has_results = results_by_address.as_ref().map_or(false, |r| !r.is_empty());
        if !has_results && has_existing_file && missing_fn_names.is_empty() && patch_fn_names.is_empty() {
            if let Some(expected) = expected_contracts.and_then(|m| m.get(&collection_name)) {
                let rk = range_key(range.start, range.end - 1);
                let mut ci = read_contract_index(sub_dir);
                update_contract_index(&mut ci, &rk, expected);
                if let Err(e) = write_contract_index(sub_dir, &ci) {
                    tracing::warn!("Failed to write once contract index for {}: {}", collection_name, e);
                } else {
                    tracing::info!(
                        "Factory once multicall {} blocks {}-{}: wrote missing contract index (no parquet changes needed)",
                        collection_name, range.start, range.end - 1
                    );
                }
            }
            continue;
        }

        // Process if we have results OR if existing file needs backfill
        if results_by_address.is_some() || has_existing_file {
            let results_by_address = results_by_address.unwrap_or_default();

            if has_existing_file {
                let mut new_results: HashMap<[u8; 20], HashMap<String, Vec<u8>>> =
                    results_by_address
                        .into_iter()
                        .map(|(addr, (_, _, fns))| (addr.0 .0, fns))
                        .collect();

                let existing_batches =
                    read_existing_once_parquet_async(output_path.clone()).await?;
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

                    write_record_batch_to_parquet_async(merged, output_path.clone()).await?;

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

                    write_once_results_to_parquet_async(
                        results,
                        output_path.clone(),
                        all_fn_names.clone(),
                    )
                    .await?;
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
            let actual_cols: Vec<String> = read_parquet_column_names_async(output_path.clone())
                .await
                .into_iter()
                .collect();
            let mut index = read_once_column_index_async(sub_dir.to_path_buf()).await;
            index.insert(file_name.clone(), actual_cols.clone());
            write_once_column_index_async(sub_dir.to_path_buf(), index).await?;

            // F. Write contract index after parquet write
            if let Some(expected) = expected_contracts.and_then(|m| m.get(&collection_name)) {
                let rk = range_key(range.start, range.end - 1);
                let mut ci = read_contract_index(sub_dir);
                update_contract_index(&mut ci, &rk, expected);
                if let Err(e) = write_contract_index(sub_dir, &ci) {
                    tracing::warn!("Failed to write once contract index for {}: {}", collection_name, e);
                }
            }

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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::storage::contract_index::{
        get_missing_contracts, range_key, read_contract_index, update_contract_index,
        write_contract_index, ExpectedContracts,
    };

    /// When no contract_index.json exists, `read_contract_index` returns an empty
    /// index and `get_missing_contracts` reports everything as missing. The skip
    /// gate must NOT be taken.
    #[test]
    fn test_factory_once_skip_requires_contract_index_completeness() {
        let dir = TempDir::new().unwrap();

        // No contract_index.json written to dir.

        let mut expected = ExpectedContracts::new();
        expected.insert(
            "ContractX".to_string(),
            vec!["0xaaa".to_string(), "0xbbb".to_string()],
        );
        expected.insert("ContractY".to_string(), vec!["0xccc".to_string()]);

        let index = read_contract_index(dir.path());
        assert!(index.is_empty(), "index should be empty when file is absent");

        let rk = range_key(0, 999);
        let missing = get_missing_contracts(&index, &rk, &expected);

        assert!(
            !missing.is_empty(),
            "missing must be non-empty when contract index is absent — skip should NOT be taken"
        );
        // Every expected entry should be reported missing.
        assert!(missing.contains_key("ContractX"));
        assert!(missing.contains_key("ContractY"));
    }

    /// When the contract_index.json contains all expected contracts and addresses
    /// for the range, `get_missing_contracts` returns an empty map. The skip gate
    /// IS taken.
    #[test]
    fn test_factory_once_skip_allowed_when_contract_index_complete() {
        let dir = TempDir::new().unwrap();

        let mut expected = ExpectedContracts::new();
        expected.insert(
            "ContractX".to_string(),
            vec!["0xaaa".to_string(), "0xbbb".to_string()],
        );
        expected.insert("ContractY".to_string(), vec!["0xccc".to_string()]);

        // Write a fully-populated contract index.
        let mut index = read_contract_index(dir.path());
        let rk = range_key(0, 999);
        update_contract_index(&mut index, &rk, &expected);
        write_contract_index(dir.path(), &index).unwrap();

        // Re-read from disk to confirm roundtrip fidelity.
        let index = read_contract_index(dir.path());
        let missing = get_missing_contracts(&index, &rk, &expected);

        assert!(
            missing.is_empty(),
            "missing must be empty when contract index is complete — skip IS taken"
        );
    }

    /// When the contract_index.json only has partial data (one contract missing
    /// entirely, another missing an address), `get_missing_contracts` reports the
    /// gaps. The skip gate must NOT be taken.
    #[test]
    fn test_factory_once_contract_index_partial_not_complete() {
        let dir = TempDir::new().unwrap();

        let mut expected = ExpectedContracts::new();
        expected.insert(
            "ContractX".to_string(),
            vec!["0xaaa".to_string(), "0xbbb".to_string()],
        );
        expected.insert("ContractY".to_string(), vec!["0xccc".to_string()]);

        // Write a partial index: only ContractX with one of its two addresses.
        let rk = range_key(0, 999);
        let mut partial = ExpectedContracts::new();
        partial.insert("ContractX".to_string(), vec!["0xaaa".to_string()]);

        let mut index = read_contract_index(dir.path());
        update_contract_index(&mut index, &rk, &partial);
        write_contract_index(dir.path(), &index).unwrap();

        // Re-read from disk.
        let index = read_contract_index(dir.path());
        let missing = get_missing_contracts(&index, &rk, &expected);

        assert!(
            !missing.is_empty(),
            "missing must be non-empty when contract index is partial — skip should NOT be taken"
        );
        // ContractY is entirely absent.
        assert!(
            missing.contains_key("ContractY"),
            "ContractY should be reported as missing"
        );
        // ContractX is present but missing address 0xbbb.
        let missing_x = missing.get("ContractX").expect("ContractX should have missing addresses");
        assert!(
            missing_x.contains(&"0xbbb".to_string()),
            "0xbbb should be reported as missing for ContractX"
        );
        assert!(
            !missing_x.contains(&"0xaaa".to_string()),
            "0xaaa should NOT be reported as missing for ContractX"
        );
    }
}
