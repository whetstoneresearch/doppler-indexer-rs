use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    process_event_triggers, process_event_triggers_multicall, process_factory_once_calls,
    process_factory_once_calls_multicall, process_factory_range, process_factory_range_multicall,
    process_once_calls_multicall, process_once_calls_regular, process_range,
    process_range_multicall, process_token_range, process_token_range_multicall, BlockInfo,
    BlockRange, EthCallCatchupState, EthCallCollectionError,
};
use crate::raw_data::historical::factories::{FactoryAddressData, FactoryMessage};
use crate::raw_data::historical::receipts::EventTriggerMessage;
use crate::rpc::UnifiedRpcClient;
use crate::types::config::chain::ChainConfig;

pub async fn collect_eth_calls(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    mut block_rx: Receiver<(u64, u64)>,
    mut factory_rx: Option<Receiver<FactoryMessage>>,
    mut event_trigger_rx: Option<Receiver<EventTriggerMessage>>,
    decoder_tx: Option<Sender<DecoderMessage>>,
    mut state: EthCallCatchupState,
) -> Result<(), EthCallCollectionError> {
    // If nothing configured, drain block_rx and return
    if !state.has_regular_calls
        && !state.has_once_calls
        && !state.has_factory_calls
        && !state.has_factory_once_calls
        && !state.has_event_triggered_calls
        && !state.has_token_calls
    {
        while block_rx.recv().await.is_some() {}
        return Ok(());
    }

    let mut block_rx_closed = false;
    let mut event_trigger_rx_closed = !state.has_event_triggered_calls;

    loop {
        if block_rx_closed {
            // Check if we should still wait for factory or event trigger data
            let waiting_for_factory = state.has_factory_calls && factory_rx.is_some();
            let waiting_for_events = state.has_event_triggered_calls && !event_trigger_rx_closed;

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
                                    state.factory_addresses
                                        .entry(collection_name.clone())
                                        .or_default()
                                        .insert(*addr);
                                }
                            }

                            // Merge into existing range_factory_data
                            let existing = state.range_factory_data.entry(range_start).or_insert_with(|| {
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
                            if state.range_regular_done.contains(&range_start) {
                                if !state.range_factory_done.contains(&range_start) {
                                    let range = BlockRange {
                                        start: range_start,
                                        end: range_end,
                                    };

                                    if let (Some(blocks), Some(factory_data)) = (state.range_data.get(&range_start), state.range_factory_data.get(&range_start)) {
                                        if let Some(multicall_addr) = state.multicall3_address {
                                            process_factory_range_multicall(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &state.factory_call_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                state.rpc_batch_size,
                                                state.factory_max_params,
                                                &mut state.frequency_state,
                                                multicall_addr,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_range(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &state.factory_call_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                state.rpc_batch_size,
                                                state.factory_max_params,
                                                &mut state.frequency_state,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        }

                                        if state.has_factory_once_calls {
                                            let empty_index = HashMap::new();
                                            if let Some(multicall_addr) = state.multicall3_address {
                                                process_factory_once_calls_multicall(
                                                    &range,
                                                    client,
                                                    factory_data,
                                                    &state.factory_once_configs,
                                                    &state.base_output_dir,
                                                    &state.existing_files,
                                                    &empty_index,
                                                    multicall_addr,
                                                    state.rpc_batch_size,
                                                    &decoder_tx,
                                                )
                                                .await?;
                                            } else {
                                                process_factory_once_calls(
                                                    &range,
                                                    client,
                                                    factory_data,
                                                    &state.factory_once_configs,
                                                    &state.base_output_dir,
                                                    &state.existing_files,
                                                    &empty_index,
                                                    &decoder_tx,
                                                )
                                                .await?;
                                            }
                                        }
                                    }
                                    state.range_factory_done.insert(range_start);
                                }

                                if state.range_regular_done.contains(&range_start)
                                    && state.range_factory_done.contains(&range_start)
                                {
                                    state.range_data.remove(&range_start);
                                    state.range_factory_data.remove(&range_start);
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
                            // Derive range from trigger block numbers
                            let range_start = triggers.iter().map(|t| t.block_number).min().unwrap_or(0);
                            let range_end = triggers.iter().map(|t| t.block_number).max().unwrap_or(0);

                            if let Some(multicall_addr) = state.multicall3_address {
                                process_event_triggers_multicall(
                                    triggers,
                                    &state.event_call_configs,
                                    &state.factory_addresses,
                                    client,
                                    &state.base_output_dir,
                                    state.rpc_batch_size,
                                    &decoder_tx,
                                    multicall_addr,
                                    range_start,
                                    range_end,
                                )
                                .await?;
                            } else {
                                process_event_triggers(
                                    triggers,
                                    &state.event_call_configs,
                                    &state.factory_addresses,
                                    client,
                                    &state.base_output_dir,
                                    state.rpc_batch_size,
                                    &decoder_tx,
                                    range_start,
                                    range_end,
                                )
                                .await?;
                            }
                        }
                        Some(EventTriggerMessage::RangeComplete { range_start, range_end }) => {
                            // Range complete - write empty files for configured pairs that had no events
                            // Note: This handles the case where no events matched any trigger patterns
                            // The process_event_triggers functions handle empty files for pairs that
                            // had some events but not for all configured pairs
                            let configured_pairs: std::collections::HashSet<(String, String)> = state.event_call_configs
                                .values()
                                .flatten()
                                .map(|c| (c.contract_name.clone(), c.function_name.clone()))
                                .collect();

                            for (contract_name, function_name) in configured_pairs {
                                let sub_dir = state.base_output_dir.join(&contract_name).join(&function_name).join("on_events");
                                let file_name = format!("{}-{}.parquet", range_start, range_end);
                                let output_path = sub_dir.join(&file_name);
                                // Only write if file doesn't exist (don't overwrite if we already wrote results)
                                if !output_path.exists() {
                                    if let Err(e) = std::fs::create_dir_all(&sub_dir) {
                                        tracing::warn!("Failed to create dir for empty event file: {}", e);
                                        continue;
                                    }
                                    if let Err(e) = crate::raw_data::historical::eth_calls::write_event_call_results_to_parquet(&[], &output_path, 0) {
                                        tracing::warn!("Failed to write empty event file: {}", e);
                                    } else {
                                        tracing::debug!("Wrote empty event-triggered eth_call file to {} (no matching events)", output_path.display());
                                    }
                                }
                            }
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
                        let range_start = (block_number / state.range_size) * state.range_size;

                        state.range_data.entry(range_start).or_default().push(BlockInfo {
                            block_number,
                            timestamp,
                        });

                        if let Some(blocks) = state.range_data.get(&range_start) {
                            let expected_blocks: HashSet<u64> =
                                (range_start..range_start + state.range_size).collect();
                            let received_blocks: HashSet<u64> =
                                blocks.iter().map(|b| b.block_number).collect();

                            if expected_blocks.is_subset(&received_blocks)
                                && !state.range_regular_done.contains(&range_start)
                            {
                                let range = BlockRange {
                                    start: range_start,
                                    end: range_start + state.range_size,
                                };

                                if state.has_regular_calls {
                                    if let Some(multicall_addr) = state.multicall3_address {
                                        process_range_multicall(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &state.call_configs,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            state.rpc_batch_size,
                                            state.max_params,
                                            &mut state.frequency_state,
                                            multicall_addr,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    } else {
                                        process_range(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &state.call_configs,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            state.rpc_batch_size,
                                            state.max_params,
                                            &mut state.frequency_state,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    }
                                }

                                if state.has_token_calls {
                                    if let Some(multicall_addr) = state.multicall3_address {
                                        process_token_range_multicall(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &state.token_call_configs,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            state.rpc_batch_size,
                                            &mut state.frequency_state,
                                            multicall_addr,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    } else {
                                        process_token_range(
                                            &range,
                                            blocks.clone(),
                                            client,
                                            &state.token_call_configs,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            state.rpc_batch_size,
                                            &mut state.frequency_state,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    }
                                }

                                if state.has_once_calls {
                                    if let Some(multicall_addr) = state.multicall3_address {
                                        process_once_calls_multicall(
                                            &range,
                                            blocks,
                                            client,
                                            &state.once_configs,
                                            &chain.contracts,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            multicall_addr,
                                            state.rpc_batch_size,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    } else {
                                        process_once_calls_regular(
                                            &range,
                                            blocks,
                                            client,
                                            &state.once_configs,
                                            &chain.contracts,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    }
                                }
                                state.range_regular_done.insert(range_start);

                                if let Some(factory_data) = state.range_factory_data.get(&range_start) {
                                    if state.has_factory_calls && !state.range_factory_done.contains(&range_start) {
                                        if let Some(multicall_addr) = state.multicall3_address {
                                            process_factory_range_multicall(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &state.factory_call_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                state.rpc_batch_size,
                                                state.factory_max_params,
                                                &mut state.frequency_state,
                                                multicall_addr,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_range(
                                                &range,
                                                blocks,
                                                client,
                                                factory_data,
                                                &state.factory_call_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                state.rpc_batch_size,
                                                state.factory_max_params,
                                                &mut state.frequency_state,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        }
                                    }

                                    if state.has_factory_once_calls {
                                        let empty_index = HashMap::new();
                                        if let Some(multicall_addr) = state.multicall3_address {
                                            process_factory_once_calls_multicall(
                                                &range,
                                                client,
                                                factory_data,
                                                &state.factory_once_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                &empty_index,
                                                multicall_addr,
                                                state.rpc_batch_size,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_once_calls(
                                                &range,
                                                client,
                                                factory_data,
                                                &state.factory_once_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                &empty_index,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        }
                                    }
                                    state.range_factory_done.insert(range_start);
                                }

                                if state.range_regular_done.contains(&range_start)
                                    && (!state.has_factory_calls || state.range_factory_done.contains(&range_start))
                                {
                                    state.range_data.remove(&range_start);
                                    state.range_factory_data.remove(&range_start);
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
                                state.factory_addresses
                                    .entry(collection_name.clone())
                                    .or_default()
                                    .insert(*addr);
                            }
                        }

                        // Merge into existing range_factory_data
                        let existing = state.range_factory_data.entry(range_start).or_insert_with(|| {
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
                        if state.range_regular_done.contains(&range_start) {
                            if state.has_factory_calls && !state.range_factory_done.contains(&range_start) {
                                let range = BlockRange {
                                    start: range_start,
                                    end: range_end,
                                };

                                if let (Some(blocks), Some(factory_data)) = (state.range_data.get(&range_start), state.range_factory_data.get(&range_start)) {
                                    if let Some(multicall_addr) = state.multicall3_address {
                                        process_factory_range_multicall(
                                            &range,
                                            blocks,
                                            client,
                                            factory_data,
                                            &state.factory_call_configs,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            state.rpc_batch_size,
                                            state.factory_max_params,
                                            &mut state.frequency_state,
                                            multicall_addr,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    } else {
                                        process_factory_range(
                                            &range,
                                            blocks,
                                            client,
                                            factory_data,
                                            &state.factory_call_configs,
                                            &state.base_output_dir,
                                            &state.existing_files,
                                            state.rpc_batch_size,
                                            state.factory_max_params,
                                            &mut state.frequency_state,
                                            &decoder_tx,
                                        )
                                        .await?;
                                    }

                                    if state.has_factory_once_calls {
                                        let empty_index = HashMap::new();
                                        if let Some(multicall_addr) = state.multicall3_address {
                                            process_factory_once_calls_multicall(
                                                &range,
                                                client,
                                                factory_data,
                                                &state.factory_once_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                &empty_index,
                                                multicall_addr,
                                                state.rpc_batch_size,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        } else {
                                            process_factory_once_calls(
                                                &range,
                                                client,
                                                factory_data,
                                                &state.factory_once_configs,
                                                &state.base_output_dir,
                                                &state.existing_files,
                                                &empty_index,
                                                &decoder_tx,
                                            )
                                            .await?;
                                        }
                                    }
                                }
                                state.range_factory_done.insert(range_start);
                            }

                            if state.range_regular_done.contains(&range_start)
                                && (!state.has_factory_calls || state.range_factory_done.contains(&range_start))
                            {
                                state.range_data.remove(&range_start);
                                state.range_factory_data.remove(&range_start);
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
                        // Derive range from trigger block numbers
                        let range_start = triggers.iter().map(|t| t.block_number).min().unwrap_or(0);
                        let range_end = triggers.iter().map(|t| t.block_number).max().unwrap_or(0);

                        if let Some(multicall_addr) = state.multicall3_address {
                            process_event_triggers_multicall(
                                triggers,
                                &state.event_call_configs,
                                &state.factory_addresses,
                                client,
                                &state.base_output_dir,
                                state.rpc_batch_size,
                                &decoder_tx,
                                multicall_addr,
                                range_start,
                                range_end,
                            )
                            .await?;
                        } else {
                            process_event_triggers(
                                triggers,
                                &state.event_call_configs,
                                &state.factory_addresses,
                                client,
                                &state.base_output_dir,
                                state.rpc_batch_size,
                                &decoder_tx,
                                range_start,
                                range_end,
                            )
                            .await?;
                        }
                    }
                    Some(EventTriggerMessage::RangeComplete { range_start, range_end }) => {
                        // Range complete - write empty files for configured pairs that had no events
                        let configured_pairs: std::collections::HashSet<(String, String)> = state.event_call_configs
                            .values()
                            .flatten()
                            .map(|c| (c.contract_name.clone(), c.function_name.clone()))
                            .collect();

                        for (contract_name, function_name) in configured_pairs {
                            let sub_dir = state.base_output_dir.join(&contract_name).join(&function_name).join("on_events");
                            let file_name = format!("{}-{}.parquet", range_start, range_end);
                            let output_path = sub_dir.join(&file_name);
                            // Only write if file doesn't exist
                            if !output_path.exists() {
                                if let Err(e) = std::fs::create_dir_all(&sub_dir) {
                                    tracing::warn!("Failed to create dir for empty event file: {}", e);
                                    continue;
                                }
                                if let Err(e) = crate::raw_data::historical::eth_calls::write_event_call_results_to_parquet(&[], &output_path, 0) {
                                    tracing::warn!("Failed to write empty event file: {}", e);
                                } else {
                                    tracing::debug!("Wrote empty event-triggered eth_call file to {} (no matching events)", output_path.display());
                                }
                            }
                        }
                    }
                    Some(EventTriggerMessage::AllComplete) | None => {
                        tracing::debug!("eth_calls: event trigger channel closed");
                        event_trigger_rx_closed = true;
                    }
                }
            }
        }
    }

    // Process remaining incomplete ranges
    for (range_start, blocks) in state.range_data {
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

        if state.has_regular_calls && !state.range_regular_done.contains(&range_start) {
            if let Some(multicall_addr) = state.multicall3_address {
                process_range_multicall(
                    &range,
                    blocks.clone(),
                    client,
                    &state.call_configs,
                    &state.base_output_dir,
                    &state.existing_files,
                    state.rpc_batch_size,
                    state.max_params,
                    &mut state.frequency_state,
                    multicall_addr,
                    &decoder_tx,
                )
                .await?;
            } else {
                process_range(
                    &range,
                    blocks.clone(),
                    client,
                    &state.call_configs,
                    &state.base_output_dir,
                    &state.existing_files,
                    state.rpc_batch_size,
                    state.max_params,
                    &mut state.frequency_state,
                    &decoder_tx,
                )
                .await?;
            }
        }

        if state.has_token_calls && !state.range_regular_done.contains(&range_start) {
            if let Some(multicall_addr) = state.multicall3_address {
                process_token_range_multicall(
                    &range,
                    blocks.clone(),
                    client,
                    &state.token_call_configs,
                    &state.base_output_dir,
                    &state.existing_files,
                    state.rpc_batch_size,
                    &mut state.frequency_state,
                    multicall_addr,
                    &decoder_tx,
                )
                .await?;
            } else {
                process_token_range(
                    &range,
                    blocks.clone(),
                    client,
                    &state.token_call_configs,
                    &state.base_output_dir,
                    &state.existing_files,
                    state.rpc_batch_size,
                    &mut state.frequency_state,
                    &decoder_tx,
                )
                .await?;
            }
        }

        if state.has_once_calls && !state.range_regular_done.contains(&range_start) {
            if let Some(multicall_addr) = state.multicall3_address {
                process_once_calls_multicall(
                    &range,
                    &blocks,
                    client,
                    &state.once_configs,
                    &chain.contracts,
                    &state.base_output_dir,
                    &state.existing_files,
                    multicall_addr,
                    state.rpc_batch_size,
                    &decoder_tx,
                )
                .await?;
            } else {
                process_once_calls_regular(
                    &range,
                    &blocks,
                    client,
                    &state.once_configs,
                    &chain.contracts,
                    &state.base_output_dir,
                    &state.existing_files,
                    &decoder_tx,
                )
                .await?;
            }
        }

        if state.has_factory_calls {
            if let Some(factory_data) = state.range_factory_data.get(&range_start) {
                if !state.range_factory_done.contains(&range_start) {
                    if let Some(multicall_addr) = state.multicall3_address {
                        process_factory_range_multicall(
                            &range,
                            &blocks,
                            client,
                            factory_data,
                            &state.factory_call_configs,
                            &state.base_output_dir,
                            &state.existing_files,
                            state.rpc_batch_size,
                            state.factory_max_params,
                            &mut state.frequency_state,
                            multicall_addr,
                            &decoder_tx,
                        )
                        .await?;
                    } else {
                        process_factory_range(
                            &range,
                            &blocks,
                            client,
                            factory_data,
                            &state.factory_call_configs,
                            &state.base_output_dir,
                            &state.existing_files,
                            state.rpc_batch_size,
                            state.factory_max_params,
                            &mut state.frequency_state,
                            &decoder_tx,
                        )
                        .await?;
                    }

                    if state.has_factory_once_calls {
                        let empty_index = HashMap::new();
                        if let Some(multicall_addr) = state.multicall3_address {
                            process_factory_once_calls_multicall(
                                &range,
                                client,
                                factory_data,
                                &state.factory_once_configs,
                                &state.base_output_dir,
                                &state.existing_files,
                                &empty_index,
                                multicall_addr,
                                state.rpc_batch_size,
                                &decoder_tx,
                            )
                            .await?;
                        } else {
                            process_factory_once_calls(
                                &range,
                                client,
                                factory_data,
                                &state.factory_once_configs,
                                &state.base_output_dir,
                                &state.existing_files,
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
