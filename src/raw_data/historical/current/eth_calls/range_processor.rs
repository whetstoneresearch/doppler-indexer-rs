use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    process_factory_once_calls, process_factory_once_calls_multicall, process_factory_range,
    process_factory_range_multicall, process_once_calls_multicall, process_once_calls_regular,
    process_range, process_range_multicall, process_token_range, process_token_range_multicall,
    BlockInfo, BlockRange, EthCallCatchupState, EthCallCollectionError,
};
use crate::rpc::UnifiedRpcClient;
use crate::storage::StorageManager;
use crate::types::config::chain::ChainConfig;

/// Process a complete range (all expected blocks received).
///
/// Runs regular calls, token calls, once calls, then factory + factory-once
/// calls if factory data is already available for this range. Marks the range
/// as done and cleans up state when both regular and factory processing are
/// complete.
pub(super) async fn process_complete_range(
    range_start: u64,
    state: &mut EthCallCatchupState,
    client: &UnifiedRpcClient,
    chain: &ChainConfig,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> Result<(), EthCallCollectionError> {
    let blocks = match state.range_data.get(&range_start) {
        Some(b) => b,
        None => return Ok(()),
    };

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
                &state.s3_manifest,
                state.rpc_batch_size,
                state.max_params,
                &mut state.frequency_state,
                multicall_addr,
                decoder_tx,
                &chain.name,
                storage_manager,
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
                &state.s3_manifest,
                state.rpc_batch_size,
                state.max_params,
                &mut state.frequency_state,
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                    decoder_tx,
                    &chain.name,
                    storage_manager,
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
                    decoder_tx,
                    &chain.name,
                    storage_manager,
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
                    decoder_tx,
                    &chain.name,
                    storage_manager,
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
                    decoder_tx,
                    &chain.name,
                    storage_manager,
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

    Ok(())
}

/// Process an incomplete range at end-of-stream.
///
/// Called during cleanup when `block_rx` closes. The range end is set to
/// `max_block + 1` instead of `range_start + range_size` because not all
/// blocks in the range were received.
pub(super) async fn process_incomplete_range(
    range_start: u64,
    blocks: Vec<BlockInfo>,
    state: &mut EthCallCatchupState,
    client: &UnifiedRpcClient,
    chain: &ChainConfig,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> Result<(), EthCallCollectionError> {
    if blocks.is_empty() {
        return Ok(());
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
                &state.s3_manifest,
                state.rpc_batch_size,
                state.max_params,
                &mut state.frequency_state,
                multicall_addr,
                decoder_tx,
                &chain.name,
                storage_manager,
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
                &state.s3_manifest,
                state.rpc_batch_size,
                state.max_params,
                &mut state.frequency_state,
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                decoder_tx,
                &chain.name,
                storage_manager,
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
                        decoder_tx,
                        &chain.name,
                        storage_manager,
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
                        decoder_tx,
                        &chain.name,
                        storage_manager,
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
                            decoder_tx,
                            &chain.name,
                            storage_manager,
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
                            decoder_tx,
                            &chain.name,
                            storage_manager,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    Ok(())
}
