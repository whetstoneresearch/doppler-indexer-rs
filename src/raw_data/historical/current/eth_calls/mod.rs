//! Current-phase eth_calls collector.
//!
//! Receives block notifications, factory addresses, and event triggers via
//! channels, then dispatches eth_call processing for complete ranges.

mod event_handler;
mod factory_handler;
mod range_processor;
mod state;

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    BlockInfo, EthCallCatchupState, EthCallCollectionError,
};
use crate::raw_data::historical::factories::FactoryMessage;
use crate::raw_data::historical::receipts::EventTriggerMessage;
use crate::rpc::UnifiedRpcClient;
use crate::storage::StorageManager;
use crate::types::config::chain::ChainConfig;

use self::event_handler::handle_event_trigger_message;
use self::factory_handler::handle_factory_message;
use self::range_processor::{process_complete_range, process_incomplete_ranges};
use self::state::CollectorState;

/// Receive from an optional channel, or pend forever if `None`.
async fn recv_factory(rx: &mut Option<Receiver<FactoryMessage>>) -> Option<FactoryMessage> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Receive from an optional channel, or pend forever if `None`.
async fn recv_event(rx: &mut Option<Receiver<EventTriggerMessage>>) -> Option<EventTriggerMessage> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

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

    let mut collector = CollectorState::new(!state.has_event_triggered_calls);
    let sm_ref = storage_manager.as_ref();

    loop {
        if collector.block_rx_closed {
            // Check if we should still wait for factory or event trigger data
            let waiting_for_factory = state.has_factory_calls && factory_rx.is_some();
            let waiting_for_events =
                state.has_event_triggered_calls && !collector.event_trigger_rx_closed;

            if !waiting_for_factory && !waiting_for_events {
                break;
            }

            // Process remaining factory and event data
            tokio::select! {
                factory_msg = recv_factory(&mut factory_rx) => {
                    if handle_factory_message(
                        factory_msg, &mut state, client, chain, &decoder_tx, sm_ref,
                    ).await?.is_closed() {
                        factory_rx = None;
                    }
                }
                event_msg = recv_event(&mut event_trigger_rx) => {
                    if handle_event_trigger_message(
                        event_msg, &mut state, &mut collector.pending_event_triggers,
                        client, chain, &decoder_tx, sm_ref,
                    ).await?.is_closed() {
                        collector.event_trigger_rx_closed = true;
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
                                process_complete_range(
                                    range_start, &mut state, client, chain, &decoder_tx, sm_ref,
                                ).await?;
                            }
                        }
                    }
                    None => {
                        collector.block_rx_closed = true;
                    }
                }
            }

            factory_msg = recv_factory(&mut factory_rx) => {
                if handle_factory_message(
                    factory_msg, &mut state, client, chain, &decoder_tx, sm_ref,
                ).await?.is_closed() {
                    factory_rx = None;
                }
            }

            event_msg = recv_event(&mut event_trigger_rx) => {
                if handle_event_trigger_message(
                    event_msg, &mut state, &mut collector.pending_event_triggers,
                    client, chain, &decoder_tx, sm_ref,
                ).await?.is_closed() {
                    collector.event_trigger_rx_closed = true;
                }
            }
        }
    }

    process_incomplete_ranges(&mut state, client, chain, &decoder_tx, sm_ref).await?;

    // Signal decoder that all ranges are complete
    if let Some(tx) = decoder_tx {
        let _ = tx.send(DecoderMessage::AllComplete).await;
    }

    tracing::info!("Eth_call collection complete for chain {}", chain.name);
    Ok(())
}
