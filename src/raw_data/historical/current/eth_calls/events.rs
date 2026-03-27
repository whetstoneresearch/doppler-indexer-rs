use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use super::state::CollectorState;
use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    process_event_triggers, process_event_triggers_multicall, EthCallCatchupState,
    EthCallCollectionError, EthCallContext,
};
use crate::raw_data::historical::receipts::EventTriggerMessage;
use crate::rpc::UnifiedRpcClient;
use crate::storage::StorageManager;

/// Handle a single `EventTriggerMessage` received from the event trigger channel.
///
/// Processes `Triggers` (buffer into pending), `RangeComplete` (execute buffered
/// triggers, write empty placeholder files for configured pairs with no events),
/// and `AllComplete`/channel-closed (mark closed).
pub(super) async fn handle_event_trigger_message(
    event_result: Option<EventTriggerMessage>,
    state: &mut EthCallCatchupState,
    local_state: &mut CollectorState,
    client: &UnifiedRpcClient,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    chain_name: &str,
    storage_manager: Option<&Arc<StorageManager>>,
) -> Result<(), EthCallCollectionError> {
    match event_result {
        Some(EventTriggerMessage::Triggers(triggers)) => {
            // Buffer triggers until RangeComplete arrives with aligned range
            local_state.pending_event_triggers.extend(triggers);
        }
        Some(EventTriggerMessage::RangeComplete {
            range_start,
            range_end,
        }) => {
            // range_end is exclusive (from BlockRange), convert to inclusive for filenames
            let inclusive_end = range_end - 1;

            // Process all buffered triggers for this range
            if !local_state.pending_event_triggers.is_empty() {
                let triggers = std::mem::take(&mut local_state.pending_event_triggers);

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

                let skipped = if let Some(multicall_addr) = state.multicall3_address {
                    process_event_triggers_multicall(
                        triggers,
                        &state.event_call_configs,
                        &state.factory_addresses,
                        &ctx,
                        multicall_addr,
                        range_start,
                        inclusive_end,
                    )
                    .await?
                } else {
                    process_event_triggers(
                        triggers,
                        &state.event_call_configs,
                        &state.factory_addresses,
                        &ctx,
                        range_start,
                        inclusive_end,
                    )
                    .await?
                };

                if !skipped.is_empty() {
                    tracing::info!(
                        "Buffered {} triggers skipped due to missing factory addresses for range {}-{}",
                        skipped.len(), range_start, inclusive_end
                    );
                    state
                        .factory_skipped_triggers
                        .push((skipped, range_start, inclusive_end));
                }
            } else {
                local_state.pending_event_triggers.clear();
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
        }
        Some(EventTriggerMessage::AllComplete) | None => {
            tracing::debug!("eth_calls: event trigger channel closed");
            local_state.event_trigger_rx_closed = true;
        }
    }
    Ok(())
}
