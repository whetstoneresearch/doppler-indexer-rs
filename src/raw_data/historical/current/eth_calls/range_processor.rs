use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::{
    process_factory_once_calls, process_factory_once_calls_multicall, process_factory_range,
    process_factory_range_multicall, process_once_calls_multicall, process_once_calls_regular,
    process_range, process_range_multicall, BlockInfo, BlockRange, EthCallCatchupState,
    EthCallCollectionError, EthCallContext,
};
use crate::rpc::UnifiedRpcClient;
use crate::storage::contract_index::build_expected_factory_contracts_for_range;
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

    let ctx = EthCallContext {
        client,
        output_dir: &state.base_output_dir,
        existing_files: &state.existing_files,
        rpc_batch_size: state.rpc_batch_size,
        repair: state.repair,
        decoder_tx,
        chain_name: &chain.name,
        storage_manager,
        s3_manifest: &state.s3_manifest,
    };

    if state.has_regular_calls {
        if let Some(multicall_addr) = state.multicall3_address {
            process_range_multicall(
                &range,
                blocks.clone(),
                &ctx,
                &state.call_configs,
                state.max_params,
                &mut state.frequency_state,
                multicall_addr,
                None,
            )
            .await?;
        } else {
            process_range(
                &range,
                blocks.clone(),
                &ctx,
                &state.call_configs,
                state.max_params,
                &mut state.frequency_state,
                None,
            )
            .await?;
        }
    }

    if state.has_once_calls {
        if let Some(multicall_addr) = state.multicall3_address {
            process_once_calls_multicall(
                &range,
                blocks,
                &ctx,
                &state.once_configs,
                &chain.contracts,
                multicall_addr,
            )
            .await?;
        } else {
            process_once_calls_regular(&range, blocks, &ctx, &state.once_configs, &chain.contracts)
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
            let expected_once =
                build_expected_factory_contracts_for_range(&state.contracts, range.end);
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
        state.range_factory_done.insert(range_start);
    }

    if state.range_regular_done.contains(&range_start)
        && ((!state.has_factory_calls && !state.has_factory_once_calls)
            || state.range_factory_done.contains(&range_start))
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

    let ctx = EthCallContext {
        client,
        output_dir: &state.base_output_dir,
        existing_files: &state.existing_files,
        rpc_batch_size: state.rpc_batch_size,
        repair: state.repair,
        decoder_tx,
        chain_name: &chain.name,
        storage_manager,
        s3_manifest: &state.s3_manifest,
    };

    if state.has_regular_calls && !state.range_regular_done.contains(&range_start) {
        if let Some(multicall_addr) = state.multicall3_address {
            process_range_multicall(
                &range,
                blocks.clone(),
                &ctx,
                &state.call_configs,
                state.max_params,
                &mut state.frequency_state,
                multicall_addr,
                None,
            )
            .await?;
        } else {
            process_range(
                &range,
                blocks.clone(),
                &ctx,
                &state.call_configs,
                state.max_params,
                &mut state.frequency_state,
                None,
            )
            .await?;
        }
    }

    if state.has_once_calls && !state.range_regular_done.contains(&range_start) {
        if let Some(multicall_addr) = state.multicall3_address {
            process_once_calls_multicall(
                &range,
                &blocks,
                &ctx,
                &state.once_configs,
                &chain.contracts,
                multicall_addr,
            )
            .await?;
        } else {
            process_once_calls_regular(
                &range,
                &blocks,
                &ctx,
                &state.once_configs,
                &chain.contracts,
            )
            .await?;
        }
    }

    if state.has_factory_calls || state.has_factory_once_calls {
        if let Some(factory_data) = state.range_factory_data.get(&range_start) {
            if !state.range_factory_done.contains(&range_start) {
                if state.has_factory_calls {
                    if let Some(multicall_addr) = state.multicall3_address {
                        process_factory_range_multicall(
                            &range,
                            &blocks,
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
                            &blocks,
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
                    let expected_once =
                        build_expected_factory_contracts_for_range(&state.contracts, range.end);
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

    use crate::raw_data::historical::eth_calls::{BlockInfo, FrequencyState, OnceCallConfig};
    use crate::raw_data::historical::factories::FactoryAddressData;
    use crate::rpc::UnifiedRpcClient;
    use crate::types::config::chain::ChainConfig;
    use crate::types::config::contract::{
        AddressOrAddresses, ContractConfig, FactoryConfig, FactoryEventConfig,
        FactoryEventConfigOrArray, FactoryParameterLocation,
    };
    use alloy::primitives::{Address, U256};

    fn test_chain() -> ChainConfig {
        ChainConfig {
            name: "test".to_string(),
            chain_id: 1,
            chain_type: crate::types::chain::ChainType::Evm,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            contracts: HashMap::new(),
            block_receipts_method: None,
            factory_collections: HashMap::new(),
            rpc: Default::default(),
            #[cfg(feature = "solana")]
            solana_programs: Default::default(),
            #[cfg(feature = "solana")]
            commitment: Default::default(),
        }
    }

    fn dummy_client() -> Arc<UnifiedRpcClient> {
        Arc::new(UnifiedRpcClient::from_url("http://127.0.0.1:8545").unwrap())
    }

    /// Contracts config with a factory entry for "test_collection" so that
    /// `build_expected_factory_contracts_for_range` returns a non-empty map
    /// and the contract_index.json write path is exercised.
    fn test_contracts() -> HashMap<String, ContractConfig> {
        let mut contracts = HashMap::new();
        contracts.insert(
            "TestFactory".to_string(),
            ContractConfig {
                address: AddressOrAddresses::Single(Address::new([0xaa; 20])),
                start_block: Some(U256::from(0)),
                calls: None,
                factories: Some(vec![FactoryConfig {
                    collection: "test_collection".to_string(),
                    factory_events: FactoryEventConfigOrArray::Single(FactoryEventConfig {
                        name: "Created".to_string(),
                        topics_signature: "Created(address)".to_string(),
                        data_signature: None,
                        factory_parameters: FactoryParameterLocation::Data(vec![0]),
                    }),
                    calls: None,
                    events: None,
                }]),
                events: None,
            },
        );
        contracts
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
            repair: false,
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
            contracts: test_contracts(),
        }
    }

    #[tokio::test]
    async fn test_complete_range_factory_once_only_retains_state_until_factory_arrives() {
        let tmp = TempDir::new().unwrap();
        let client = dummy_client();
        let chain = test_chain();
        let mut state = factory_once_only_state(tmp.path());

        // Pre-populate range_data with one block
        state.range_data.insert(
            0,
            vec![BlockInfo {
                block_number: 0,
                timestamp: 100,
            }],
        );
        // Do NOT insert range_factory_data — factory hasn't arrived yet

        process_complete_range(0, &mut state, &client, &chain, &None, None)
            .await
            .unwrap();

        // Regular processing done
        assert!(state.range_regular_done.contains(&0));
        // Range data retained — factory-once still pending
        assert!(
            state.range_data.contains_key(&0),
            "range_data must not be cleaned up while factory-once is pending"
        );
        // Factory not done yet
        assert!(!state.range_factory_done.contains(&0));
    }

    #[tokio::test]
    async fn test_incomplete_range_flush_runs_factory_once_without_factory_calls() {
        let tmp = TempDir::new().unwrap();
        let client = dummy_client();
        let chain = test_chain();
        let mut state = factory_once_only_state(tmp.path());

        // Pre-populate factory data (empty addresses)
        state.range_factory_data.insert(
            0,
            FactoryAddressData {
                range_start: 0,
                range_end: 50,
                addresses_by_block: HashMap::new(),
            },
        );

        let blocks = vec![BlockInfo {
            block_number: 0,
            timestamp: 100,
        }];

        process_incomplete_range(0, blocks, &mut state, &client, &chain, &None, None)
            .await
            .unwrap();

        // Verify empty parquet was written (range end = max_block + 1 = 1, so file is 0-0.parquet)
        let parquet_path = tmp.path().join("test_collection/once/0-0.parquet");
        assert!(
            parquet_path.exists(),
            "empty parquet must be written for factory-once-only flush"
        );

        // Verify sidecars
        assert!(
            tmp.path()
                .join("test_collection/once/column_index.json")
                .exists(),
            "column_index.json must be written"
        );
        assert!(
            tmp.path()
                .join("test_collection/once/contract_index.json")
                .exists(),
            "contract_index.json must be written when expected_contracts is non-empty"
        );
    }
}
