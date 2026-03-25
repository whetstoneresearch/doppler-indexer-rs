//! Deduplicated handler for `EventTriggerMessage` in the eth_calls collector.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    process_event_triggers, process_event_triggers_multicall, EthCallCatchupState,
    EthCallCollectionError,
};
use crate::raw_data::historical::receipts::{EventTriggerData, EventTriggerMessage};
use crate::rpc::UnifiedRpcClient;
use crate::storage::StorageManager;
use crate::types::config::chain::ChainConfig;

use super::factory_handler::HandleResult;

/// Handle a single message (or `None` for channel close) from the event trigger receiver.
///
/// This function is called from both the `block_rx_closed` branch and the main
/// `select!` branch, eliminating ~90 lines of duplication.
pub(super) async fn handle_event_trigger_message(
    msg: Option<EventTriggerMessage>,
    state: &mut EthCallCatchupState,
    pending_triggers: &mut Vec<EventTriggerData>,
    client: &UnifiedRpcClient,
    chain: &ChainConfig,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    storage_manager: Option<&Arc<StorageManager>>,
) -> Result<HandleResult, EthCallCollectionError> {
    match msg {
        Some(EventTriggerMessage::Triggers(triggers)) => {
            // Buffer triggers until RangeComplete arrives with aligned range
            pending_triggers.extend(triggers);
            Ok(HandleResult::Continue)
        }
        Some(EventTriggerMessage::RangeComplete {
            range_start,
            range_end,
        }) => {
            // range_end is exclusive (from BlockRange), convert to inclusive for filenames
            let inclusive_end = range_end - 1;

            // Process all buffered triggers for this range
            if !pending_triggers.is_empty() {
                let triggers = std::mem::take(pending_triggers);
                if let Some(multicall_addr) = state.multicall3_address {
                    process_event_triggers_multicall(
                        triggers,
                        &state.event_call_configs,
                        &state.factory_addresses,
                        client,
                        &state.base_output_dir,
                        state.rpc_batch_size,
                        decoder_tx,
                        multicall_addr,
                        range_start,
                        inclusive_end,
                        &chain.name,
                        storage_manager,
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
                        decoder_tx,
                        range_start,
                        inclusive_end,
                        &chain.name,
                        storage_manager,
                    )
                    .await?;
                }
            } else {
                pending_triggers.clear();
            }

            // Write empty files for configured pairs that had no events
            let configured_pairs: HashSet<(String, String)> = state
                .event_call_configs
                .values()
                .flatten()
                .filter(|c| match c.start_block {
                    Some(sb) => range_end > sb,
                    None => true,
                })
                .map(|c| (c.contract_name.clone(), c.function_name.clone()))
                .collect();

            for (contract_name, function_name) in configured_pairs {
                let sub_dir = state
                    .base_output_dir
                    .join(&contract_name)
                    .join(&function_name)
                    .join("on_events");
                let file_name = format!("{}-{}.parquet", range_start, inclusive_end);
                let output_path = sub_dir.join(&file_name);
                // Only write if file doesn't exist (don't overwrite if we already wrote results)
                if !output_path.exists() {
                    if let Err(e) = std::fs::create_dir_all(&sub_dir) {
                        tracing::warn!("Failed to create dir for empty event file: {}", e);
                        continue;
                    }
                    if let Err(e) =
                        crate::raw_data::historical::eth_calls::write_event_call_results_to_parquet(
                            &[],
                            &output_path,
                            0,
                        )
                    {
                        tracing::warn!("Failed to write empty event file: {}", e);
                    } else {
                        tracing::debug!(
                            "Wrote empty event-triggered eth_call file to {} (no matching events)",
                            output_path.display()
                        );
                    }
                }
            }

            Ok(HandleResult::Continue)
        }
        Some(EventTriggerMessage::AllComplete) | None => {
            tracing::debug!("eth_calls: event trigger channel closed");
            Ok(HandleResult::ChannelClosed)
        }
    }
}
