use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    process_event_triggers, process_event_triggers_multicall, process_factory_once_calls,
    process_factory_once_calls_multicall, process_factory_range, process_factory_range_multicall,
    BlockRange, EthCallCatchupState, EthCallCollectionError, EthCallContext,
};
use crate::raw_data::historical::factories::{FactoryAddressData, FactoryMessage};
use crate::rpc::UnifiedRpcClient;
use crate::storage::StorageManager;

/// Handle a single `FactoryMessage` received from the factory channel.
///
/// Processes `IncrementalAddresses` (merge into state), `RangeComplete` (run factory
/// and factory-once calls if the regular range is already done), and
/// `AllComplete`/channel-closed (set factory_rx to None).
pub(super) async fn handle_factory_message(
    factory_result: Option<FactoryMessage>,
    state: &mut EthCallCatchupState,
    client: &UnifiedRpcClient,
    factory_rx: &mut Option<Receiver<FactoryMessage>>,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    chain_name: &str,
    storage_manager: Option<&Arc<StorageManager>>,
) -> Result<(), EthCallCollectionError> {
    match factory_result {
        Some(FactoryMessage::IncrementalAddresses(factory_data)) => {
            let range_start = factory_data.range_start;

            // Update factory addresses for event trigger filtering
            for addrs in factory_data.addresses_by_block.values() {
                for (_, addr, collection_name) in addrs {
                    state
                        .factory_addresses
                        .entry(collection_name.clone())
                        .or_default()
                        .insert(*addr);
                }
            }

            // Check if any buffered triggers can now proceed
            if !state.factory_skipped_triggers.is_empty() {
                let mut still_skipped = Vec::new();
                let buffered = std::mem::take(&mut state.factory_skipped_triggers);

                for (skipped_triggers, buf_range_start, buf_range_end) in buffered {
                    // Check if any of the triggers' factory collections are now known
                    let has_new_addresses = skipped_triggers.iter().any(|st| {
                        let key = (st.trigger.source_name.clone(), st.trigger.event_signature);
                        state
                            .event_call_configs
                            .get(&key)
                            .map(|configs| {
                                configs.iter().any(|c| {
                                    c.is_factory
                                        && state.factory_addresses.contains_key(&c.contract_name)
                                })
                            })
                            .unwrap_or(false)
                    });

                    if has_new_addresses {
                        let triggers: Vec<_> = skipped_triggers
                            .iter()
                            .map(|st| st.trigger.clone())
                            .collect();

                        tracing::info!(
                            "Replaying {} previously-skipped triggers for range {}-{} after factory addresses arrived",
                            triggers.len(), buf_range_start, buf_range_end
                        );

                        let ctx = EthCallContext {
                            client,
                            output_dir: &state.base_output_dir,
                            existing_files: &state.existing_files,
                            rpc_batch_size: state.rpc_batch_size,
                            decoder_tx,
                            chain_name,
                            storage_manager,
                            s3_manifest: &state.s3_manifest,
                        };

                        let new_skipped = if let Some(multicall_addr) = state.multicall3_address {
                            process_event_triggers_multicall(
                                triggers,
                                &state.event_call_configs,
                                &state.factory_addresses,
                                &ctx,
                                multicall_addr,
                                buf_range_start,
                                buf_range_end,
                                &state.expected_by_collection,
                            )
                            .await?
                        } else {
                            process_event_triggers(
                                triggers,
                                &state.event_call_configs,
                                &state.factory_addresses,
                                &ctx,
                                buf_range_start,
                                buf_range_end,
                                &state.expected_by_collection,
                            )
                            .await?
                        };

                        if !new_skipped.is_empty() {
                            still_skipped.push((new_skipped, buf_range_start, buf_range_end));
                        }
                    } else {
                        still_skipped.push((skipped_triggers, buf_range_start, buf_range_end));
                    }
                }

                state.factory_skipped_triggers = still_skipped;
            }

            // Merge into existing range_factory_data
            let existing = state
                .range_factory_data
                .entry(range_start)
                .or_insert_with(|| FactoryAddressData {
                    range_start: factory_data.range_start,
                    range_end: factory_data.range_end,
                    addresses_by_block: HashMap::new(),
                });
            for (block, addrs) in factory_data.addresses_by_block {
                existing
                    .addresses_by_block
                    .entry(block)
                    .or_default()
                    .extend(addrs);
            }
        }
        Some(FactoryMessage::RangeComplete {
            range_start,
            range_end,
        }) => {
            if state.range_regular_done.contains(&range_start) {
                if state.has_factory_calls && !state.range_factory_done.contains(&range_start) {
                    let range = BlockRange {
                        start: range_start,
                        end: range_end,
                    };

                    if let (Some(blocks), Some(factory_data)) = (
                        state.range_data.get(&range_start),
                        state.range_factory_data.get(&range_start),
                    ) {
                        let ctx = EthCallContext {
                            client,
                            output_dir: &state.base_output_dir,
                            existing_files: &state.existing_files,
                            rpc_batch_size: state.rpc_batch_size,
                            decoder_tx,
                            chain_name,
                            storage_manager,
                            s3_manifest: &state.s3_manifest,
                        };

                        if let Some(multicall_addr) = state.multicall3_address {
                            process_factory_range_multicall(
                                &range,
                                blocks,
                                &ctx,
                                factory_data,
                                &state.factory_call_configs,
                                state.factory_max_params,
                                &mut state.frequency_state,
                                multicall_addr,
                                None,
                                &state.expected_by_collection,
                            )
                            .await?;
                        } else {
                            process_factory_range(
                                &range,
                                blocks,
                                &ctx,
                                factory_data,
                                &state.factory_call_configs,
                                state.factory_max_params,
                                &mut state.frequency_state,
                                None,
                                &state.expected_by_collection,
                            )
                            .await?;
                        }

                        if state.has_factory_once_calls {
                            let empty_index = HashMap::new();
                            if let Some(multicall_addr) = state.multicall3_address {
                                process_factory_once_calls_multicall(
                                    &range,
                                    &ctx,
                                    factory_data,
                                    &state.factory_once_configs,
                                    &empty_index,
                                    multicall_addr,
                                )
                                .await?;
                            } else {
                                process_factory_once_calls(
                                    &range,
                                    &ctx,
                                    factory_data,
                                    &state.factory_once_configs,
                                    &empty_index,
                                )
                                .await?;
                            }
                        }
                    }
                    state.range_factory_done.insert(range_start);

                    // Factory address discovery is complete for this range.
                    // Any buffered triggers for this range whose emitters still aren't
                    // in factory_addresses are legitimately not factory instances — drop them.
                    if !state.factory_skipped_triggers.is_empty() {
                        let before: usize = state
                            .factory_skipped_triggers
                            .iter()
                            .map(|(t, _, _)| t.len())
                            .sum();
                        state
                            .factory_skipped_triggers
                            .retain(|(_, buf_start, _)| *buf_start != range_start);
                        let after: usize = state
                            .factory_skipped_triggers
                            .iter()
                            .map(|(t, _, _)| t.len())
                            .sum();
                        let dropped = before - after;
                        if dropped > 0 {
                            tracing::debug!(
                                "Dropped {} buffered triggers for range {} — factory discovery complete, addresses not found",
                                dropped, range_start
                            );
                        }
                    }
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
            if !state.factory_skipped_triggers.is_empty() {
                let total: usize = state
                    .factory_skipped_triggers
                    .iter()
                    .map(|(t, _, _)| t.len())
                    .sum();
                tracing::error!(
                    "BUG: {} buffered triggers remain after all factory ranges complete. \
                     These should have been drained by RangeComplete.",
                    total
                );
            }
            tracing::debug!("eth_calls: factory channel closed");
            *factory_rx = None;
        }
    }
    Ok(())
}
