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
use crate::storage::contract_index::build_expected_factory_contracts_for_range;
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
                                &state.contracts,
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
                                &state.contracts,
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
                if (state.has_factory_calls || state.has_factory_once_calls)
                    && !state.range_factory_done.contains(&range_start)
                {
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

                        if state.has_factory_calls {
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
                                    &state.contracts,
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
                                    &state.contracts,
                                )
                                .await?;
                            }
                        }

                        if state.has_factory_once_calls {
                            let empty_index = HashMap::new();
                            let expected_once = build_expected_factory_contracts_for_range(
                                &state.contracts,
                                range.end,
                            );
                            if let Some(multicall_addr) = state.multicall3_address {
                                process_factory_once_calls_multicall(
                                    &range,
                                    &ctx,
                                    factory_data,
                                    &state.factory_once_configs,
                                    &empty_index,
                                    multicall_addr,
                                    Some(&expected_once),
                                )
                                .await?;
                            } else {
                                process_factory_once_calls(
                                    &range,
                                    &ctx,
                                    factory_data,
                                    &state.factory_once_configs,
                                    &empty_index,
                                    Some(&expected_once),
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
                    && ((!state.has_factory_calls && !state.has_factory_once_calls)
                        || state.range_factory_done.contains(&range_start))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::raw_data::historical::eth_calls::{
        BlockInfo, EthCallCatchupState, FrequencyState, OnceCallConfig,
    };
    use crate::raw_data::historical::factories::FactoryAddressData;
    use crate::rpc::UnifiedRpcClient;

    fn dummy_client() -> Arc<UnifiedRpcClient> {
        Arc::new(UnifiedRpcClient::from_url("http://127.0.0.1:8545").unwrap())
    }

    fn factory_once_only_state(base_dir: &Path) -> EthCallCatchupState {
        let mut factory_once_configs = HashMap::new();
        factory_once_configs.insert(
            "test_collection".to_string(),
            vec![OnceCallConfig {
                function_name: "testFn".to_string(),
                function_selector: [0u8; 4],
                preencoded_calldata: None,
                params: vec![],
                target_addresses: None,
                start_block: None,
            }],
        );

        EthCallCatchupState {
            base_output_dir: base_dir.to_path_buf(),
            range_size: 100,
            rpc_batch_size: 10,
            multicall3_address: None,
            call_configs: vec![],
            factory_call_configs: HashMap::new(),
            event_call_configs: HashMap::new(),
            once_configs: HashMap::new(),
            factory_once_configs,
            has_regular_calls: false,
            has_once_calls: false,
            has_factory_calls: false,
            has_factory_once_calls: true,
            has_event_triggered_calls: false,
            max_params: 0,
            factory_max_params: 0,
            existing_files: HashSet::new(),
            s3_manifest: None,
            factory_addresses: HashMap::new(),
            frequency_state: FrequencyState {
                last_call_times: HashMap::new(),
            },
            range_data: HashMap::new(),
            range_factory_data: HashMap::new(),
            range_regular_done: HashSet::new(),
            range_factory_done: HashSet::new(),
            factory_skipped_triggers: vec![],
            contracts: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_factory_rangecomplete_runs_factory_once_without_factory_calls() {
        let tmp = TempDir::new().unwrap();
        let client = dummy_client();
        let mut state = factory_once_only_state(tmp.path());

        // Pre-populate: regular processing done, block data available
        state.range_regular_done.insert(0);
        state
            .range_data
            .insert(0, vec![BlockInfo { block_number: 0, timestamp: 100 }]);
        state.range_factory_data.insert(
            0,
            FactoryAddressData {
                range_start: 0,
                range_end: 100,
                addresses_by_block: HashMap::new(),
            },
        );

        let mut factory_rx = None;

        handle_factory_message(
            Some(FactoryMessage::RangeComplete {
                range_start: 0,
                range_end: 100,
            }),
            &mut state,
            &client,
            &mut factory_rx,
            &None,
            "test",
            None,
        )
        .await
        .unwrap();

        // Factory processing marked done
        assert!(
            state.range_factory_done.contains(&0),
            "range_factory_done must be set"
        );
        // Both regular and factory done -> range data cleaned up
        assert!(
            !state.range_data.contains_key(&0),
            "range_data should be cleaned up"
        );

        // Verify empty parquet was written (BlockRange { start: 0, end: 100 } -> 0-99.parquet)
        let parquet_path = tmp.path().join("test_collection/once/0-99.parquet");
        assert!(
            parquet_path.exists(),
            "empty parquet must be written via late-arrival factory-once path"
        );

        // Verify sidecars
        assert!(
            tmp.path()
                .join("test_collection/once/column_index.json")
                .exists(),
            "column_index.json must be written"
        );
    }
}
