use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    process_factory_once_calls, process_factory_once_calls_multicall, process_factory_range,
    process_factory_range_multicall, BlockRange, EthCallCatchupState, EthCallCollectionError,
    EthCallContext,
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
            tracing::debug!("eth_calls: factory channel closed");
            *factory_rx = None;
        }
    }
    Ok(())
}
