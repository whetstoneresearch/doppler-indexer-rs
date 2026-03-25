use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use super::events::handle_event_trigger_message;
use super::factory::handle_factory_message;
use super::range_processor::{process_complete_range, process_incomplete_range};
use super::state::CollectorState;
use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    BlockInfo, EthCallCatchupState, EthCallCollectionError,
};
use crate::raw_data::historical::factories::FactoryMessage;
use crate::raw_data::historical::receipts::EventTriggerMessage;
use crate::rpc::UnifiedRpcClient;
use crate::storage::StorageManager;
use crate::types::config::chain::ChainConfig;

pub async fn collect_eth_calls(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    mut block_rx: Receiver<(u64, u64)>,
    mut factory_rx: Option<Receiver<FactoryMessage>>,
    mut event_trigger_rx: Option<Receiver<EventTriggerMessage>>,
    decoder_tx: Option<Sender<DecoderMessage>>,
    mut state: EthCallCatchupState,
    storage_manager: Option<Arc<StorageManager>>,
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

    let mut local_state = CollectorState::new(state.has_event_triggered_calls);

    loop {
        if local_state.block_rx_closed {
            // Check if we should still wait for factory or event trigger data
            let waiting_for_factory = state.has_factory_calls && factory_rx.is_some();
            let waiting_for_events =
                state.has_event_triggered_calls && !local_state.event_trigger_rx_closed;

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
                    handle_factory_message(
                        factory_result,
                        &mut state,
                        client,
                        &mut factory_rx,
                        &decoder_tx,
                        &chain.name,
                        storage_manager.as_ref(),
                    )
                    .await?;
                }

                event_result = async {
                    match &mut event_trigger_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    handle_event_trigger_message(
                        event_result,
                        &mut state,
                        &mut local_state,
                        client,
                        &decoder_tx,
                        &chain.name,
                        storage_manager.as_ref(),
                    )
                    .await?;
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
                                process_complete_range(
                                    range_start,
                                    &mut state,
                                    client,
                                    chain,
                                    &decoder_tx,
                                    storage_manager.as_ref(),
                                )
                                .await?;
                            }
                        }
                    }
                    None => {
                        local_state.block_rx_closed = true;
                    }
                }
            }

            factory_result = async {
                match &mut factory_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                handle_factory_message(
                    factory_result,
                    &mut state,
                    client,
                    &mut factory_rx,
                    &decoder_tx,
                    &chain.name,
                    storage_manager.as_ref(),
                )
                .await?;
            }

            event_result = async {
                match &mut event_trigger_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                handle_event_trigger_message(
                    event_result,
                    &mut state,
                    &mut local_state,
                    client,
                    &decoder_tx,
                    &chain.name,
                    storage_manager.as_ref(),
                )
                .await?;
            }
        }
    }

    // Process remaining incomplete ranges.
    // Take range_data out of state so we can iterate over owned entries
    // while still passing &mut state for the other fields.
    let remaining_ranges: Vec<(u64, Vec<BlockInfo>)> =
        std::mem::take(&mut state.range_data).into_iter().collect();
    for (range_start, blocks) in remaining_ranges {
        process_incomplete_range(
            range_start,
            blocks,
            &mut state,
            client,
            chain,
            &decoder_tx,
            storage_manager.as_ref(),
        )
        .await?;
    }

    // Signal decoder that all ranges are complete
    if let Some(tx) = decoder_tx {
        let _ = tx.send(DecoderMessage::AllComplete).await;
    }

    tracing::info!("Eth_call collection complete for chain {}", chain.name);
    Ok(())
}
