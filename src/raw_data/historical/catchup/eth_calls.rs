use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use alloy::primitives::Address;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use crate::raw_data::decoding::DecoderMessage;
use crate::raw_data::historical::blocks::{get_existing_block_ranges, read_block_info_from_parquet};
use crate::raw_data::historical::eth_calls::{
    build_call_configs, build_event_triggered_call_configs, build_factory_once_call_configs,
    build_once_call_configs, build_token_call_configs, event_output_exists,
    get_existing_log_ranges, load_factory_addresses_for_once_catchup,
    load_historical_factory_addresses, load_or_build_once_column_index, process_event_triggers,
    read_once_column_index,
    process_event_triggers_multicall, process_factory_once_calls, process_once_calls_multicall,
    process_once_calls_regular, process_range, process_range_multicall, process_token_range,
    process_token_range_multicall, read_logs_from_parquet, scan_existing_parquet_files, BlockInfo,
    BlockRange, EthCallCatchupState, EthCallCollectionError, FrequencyState,
};
use crate::raw_data::historical::factories::{get_factory_call_configs, FactoryAddressData};
use crate::raw_data::historical::receipts::{build_event_trigger_matchers, extract_event_triggers};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::AddressOrAddresses;
use crate::types::config::raw_data::RawDataCollectionConfig;

pub async fn collect_eth_calls(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    has_factory_rx: bool,
    has_event_trigger_rx: bool,
    factory_catchup_done_rx: Option<oneshot::Receiver<()>>,
    eth_calls_catchup_done_tx: Option<oneshot::Sender<()>>,
) -> Result<EthCallCatchupState, EthCallCollectionError> {
    let base_output_dir = PathBuf::from(format!("data/raw/{}/eth_calls", chain.name));
    std::fs::create_dir_all(&base_output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;

    let call_configs = build_call_configs(&chain.contracts)?;
    let factory_call_configs =
        get_factory_call_configs(&chain.contracts, &chain.factory_collections);
    let event_call_configs = build_event_triggered_call_configs(&chain.contracts);
    let token_call_configs = build_token_call_configs(&chain.tokens, &chain.contracts)?;

    let multicall3_address: Option<Address> =
        chain.contracts.get("Multicall3").and_then(|c| match &c.address {
            AddressOrAddresses::Single(addr) => Some(*addr),
            AddressOrAddresses::Multiple(addrs) => addrs.first().copied(),
        });

    if multicall3_address.is_some() {
        tracing::info!(
            "Multicall3 found for chain {}, will batch all eth_calls",
            chain.name
        );
    }

    let once_configs = build_once_call_configs(&chain.contracts);
    let factory_once_configs =
        build_factory_once_call_configs(&factory_call_configs, &chain.contracts);

    let has_regular_calls = !call_configs.is_empty();
    let has_once_calls = !once_configs.is_empty();
    let has_factory_calls = !factory_call_configs.is_empty() && has_factory_rx;
    let has_factory_once_calls = !factory_once_configs.is_empty() && has_factory_rx;
    let has_event_triggered_calls = !event_call_configs.is_empty() && has_event_trigger_rx;
    let has_token_calls = !token_call_configs.is_empty();

    if !has_regular_calls
        && !has_once_calls
        && !has_factory_calls
        && !has_factory_once_calls
        && !has_event_triggered_calls
        && !has_token_calls
    {
        tracing::info!("No eth_calls configured for chain {}", chain.name);
        // Signal catchup done immediately
        if let Some(tx) = eth_calls_catchup_done_tx {
            let _ = tx.send(());
        }
        return Ok(EthCallCatchupState {
            base_output_dir,
            range_size,
            rpc_batch_size,
            multicall3_address,
            call_configs,
            factory_call_configs,
            event_call_configs,
            token_call_configs,
            once_configs,
            factory_once_configs,
            has_regular_calls,
            has_once_calls,
            has_factory_calls,
            has_factory_once_calls,
            has_event_triggered_calls,
            has_token_calls,
            max_params: 0,
            factory_max_params: 0,
            existing_files: HashSet::new(),
            factory_addresses: HashMap::new(),
            frequency_state: FrequencyState {
                last_call_times: HashMap::new(),
            },
            range_data: HashMap::new(),
            range_factory_data: HashMap::new(),
            range_regular_done: HashSet::new(),
            range_factory_done: HashSet::new(),
        });
    }

    // Track known factory addresses for filtering event triggers
    let mut factory_addresses: HashMap<String, HashSet<Address>> = HashMap::new();

    let max_params = call_configs
        .iter()
        .map(|c| c.param_values.len())
        .max()
        .unwrap_or(0);

    let factory_max_params = factory_call_configs
        .values()
        .flat_map(|configs| configs.iter().map(|c| c.params.len()))
        .max()
        .unwrap_or(0);

    let mut frequency_state = FrequencyState {
        last_call_times: HashMap::new(),
    };

    tracing::info!(
        "Starting eth_call collection for chain {} with {} regular configs, {} once configs, {} factory collections, {} factory once configs, {} event trigger configs, {} token pool configs",
        chain.name,
        call_configs.len(),
        once_configs.len(),
        factory_call_configs.len(),
        factory_once_configs.len(),
        event_call_configs.len(),
        token_call_configs.len()
    );

    let existing_files = scan_existing_parquet_files(&base_output_dir);

    let mut range_data: HashMap<u64, Vec<BlockInfo>> = HashMap::new();
    let range_factory_data: HashMap<u64, FactoryAddressData> = HashMap::new();
    let mut range_regular_done: HashSet<u64> = HashSet::new();
    let range_factory_done: HashSet<u64> = HashSet::new();

    if has_regular_calls || has_token_calls || has_once_calls {
        let block_ranges = get_existing_block_ranges(&chain.name);
        tracing::info!(
            "eth_calls catchup: checking {} block ranges (regular={}, token={}, once={})",
            block_ranges.len(),
            has_regular_calls,
            has_token_calls,
            has_once_calls
        );

        // Pre-load or build column indexes for all once directories
        let mut once_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for contract_name in once_configs.keys() {
            let once_dir = base_output_dir.join(contract_name).join("once");
            let index = load_or_build_once_column_index(&once_dir);
            once_column_indexes.insert(contract_name.clone(), index);
        }

        let mut catchup_count = 0;

        for block_range in &block_ranges {
            let range = BlockRange {
                start: block_range.start,
                end: block_range.end,
            };

            // Check if all regular call files exist for this range
            let regular_calls_done = !has_regular_calls
                || call_configs.iter().all(|config| {
                    let rel_path = format!(
                        "{}/{}/{}",
                        config.contract_name,
                        config.function_name,
                        range.file_name()
                    );
                    existing_files.contains(&rel_path)
                });

            // Check if all token pool call files exist for this range
            let token_calls_done = !has_token_calls
                || token_call_configs.iter().all(|config| {
                    let rel_path = format!(
                        "{}_pool/{}/{}",
                        config.token_name,
                        config.function_name,
                        range.file_name()
                    );
                    existing_files.contains(&rel_path)
                });

            // Check if all once call files exist AND have all expected columns for this range
            let once_calls_done = !has_once_calls
                || once_configs.iter().all(|(contract_name, configs)| {
                    let rel_path = format!("{}/once/{}", contract_name, range.file_name());
                    let expected: HashSet<&str> = configs
                        .iter()
                        .map(|c| c.function_name.as_str())
                        .collect();

                    if !existing_files.contains(&rel_path) {
                        tracing::info!(
                            "Once file missing for {} range {}-{}, will collect {} functions",
                            contract_name,
                            range.start,
                            range.end - 1,
                            expected.len()
                        );
                        return false;
                    }

                    // Use pre-loaded index (which was built from parquet schemas if index file didn't exist)
                    let index = once_column_indexes.get(contract_name).unwrap();
                    match index.get(&range.file_name()) {
                        Some(cols) => {
                            let missing: Vec<_> = expected
                                .iter()
                                .filter(|f| !cols.contains(&f.to_string()))
                                .collect();
                            if !missing.is_empty() {
                                tracing::info!(
                                    "Once file {} for {} exists but missing columns: {:?} (has: {:?})",
                                    range.file_name(),
                                    contract_name,
                                    missing,
                                    cols
                                );
                                false
                            } else {
                                tracing::debug!(
                                    "Once file {} for {} complete with all {} columns",
                                    range.file_name(),
                                    contract_name,
                                    cols.len()
                                );
                                true
                            }
                        }
                        None => {
                            // File exists but wasn't found by index builder - shouldn't happen but handle it
                            tracing::warn!(
                                "Once file {} for {} exists but not in pre-built index, will collect",
                                range.file_name(),
                                contract_name
                            );
                            false
                        }
                    }
                });

            // Skip this range only if ALL call types have their files
            if regular_calls_done && token_calls_done && once_calls_done {
                range_regular_done.insert(range.start);
                continue;
            }

            let block_infos = match read_block_info_from_parquet(&block_range.file_path) {
                Ok(infos) => infos,
                Err(e) => {
                    tracing::warn!(
                        "Failed to read block info from {}: {}",
                        block_range.file_path.display(),
                        e
                    );
                    continue;
                }
            };

            if block_infos.is_empty() {
                continue;
            }

            tracing::info!(
                "Catchup: processing eth_calls for blocks {}-{} from existing block file",
                range.start,
                range.end - 1
            );

            let blocks: Vec<BlockInfo> = block_infos
                .into_iter()
                .map(|info| BlockInfo {
                    block_number: info.block_number,
                    timestamp: info.timestamp,
                })
                .collect();

            range_data.insert(range.start, blocks.clone());

            if has_regular_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_range_multicall(
                        &range,
                        blocks.clone(),
                        client,
                        &call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        max_params,
                        &mut frequency_state,
                        multicall_addr,
                        &None,
                    )
                    .await?;
                } else {
                    process_range(
                        &range,
                        blocks.clone(),
                        client,
                        &call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        max_params,
                        &mut frequency_state,
                        &None,
                    )
                    .await?;
                }
            }

            if has_token_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_token_range_multicall(
                        &range,
                        blocks.clone(),
                        client,
                        &token_call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        &mut frequency_state,
                        multicall_addr,
                        &None,
                    )
                    .await?;
                } else {
                    process_token_range(
                        &range,
                        blocks.clone(),
                        client,
                        &token_call_configs,
                        &base_output_dir,
                        &existing_files,
                        rpc_batch_size,
                        &mut frequency_state,
                        &None,
                    )
                    .await?;
                }
            }

            if has_once_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_once_calls_multicall(
                        &range,
                        &blocks,
                        client,
                        &once_configs,
                        &chain.contracts,
                        &base_output_dir,
                        &existing_files,
                        multicall_addr,
                        rpc_batch_size,
                        &None,
                    )
                    .await?;
                } else {
                    process_once_calls_regular(
                        &range,
                        &blocks,
                        client,
                        &once_configs,
                        &chain.contracts,
                        &base_output_dir,
                        &existing_files,
                        &None,
                    )
                    .await?;
                }
            }

            range_regular_done.insert(range.start);
            catchup_count += 1;
        }

        if catchup_count > 0 {
            tracing::info!(
                "Catchup complete: processed {} eth_call ranges for chain {}",
                catchup_count,
                chain.name
            );
        }
    }

    // =========================================================================
    // Wait for factory catchup before factory-dependent catchup phases
    // =========================================================================
    if let Some(rx) = factory_catchup_done_rx {
        tracing::info!(
            "Waiting for factory catchup to complete before factory-dependent catchup..."
        );
        let _ = rx.await;
        tracing::info!("Factory catchup complete, proceeding with factory-dependent catchup");
    }

    // =========================================================================
    // Catchup phase for factory once calls: Check for missing columns in existing files
    // =========================================================================
    if has_factory_once_calls {
        tracing::info!(
            "Factory once calls catchup: checking {} factory collections for missing columns",
            factory_once_configs.len()
        );

        // Read column indexes for all factory once directories (don't auto-build).
        // Using read_once_column_index so we can detect when no index file exists
        // and properly handle null-filling conditions.
        let mut factory_once_column_indexes: HashMap<String, HashMap<String, Vec<String>>> =
            HashMap::new();
        for collection_name in factory_once_configs.keys() {
            let once_dir = base_output_dir.join(collection_name).join("once");
            let index = read_once_column_index(&once_dir);
            factory_once_column_indexes.insert(collection_name.clone(), index);
        }

        // Load factory address data from existing parquet files
        let factory_catchup_data =
            load_factory_addresses_for_once_catchup(&chain.name, &factory_once_configs);

        if !factory_catchup_data.is_empty() {
            let block_ranges = get_existing_block_ranges(&chain.name);
            let mut factory_once_catchup_count = 0;

            for block_range in &block_ranges {
                let range = BlockRange {
                    start: block_range.start,
                    end: block_range.end,
                };

                // Build FactoryAddressData for this range from loaded data
                let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> =
                    HashMap::new();
                for (collection_name, addr_data) in &factory_catchup_data {
                    for (addr, block_num, timestamp) in addr_data {
                        if *block_num >= range.start && *block_num < range.end {
                            addresses_by_block
                                .entry(*block_num)
                                .or_default()
                                .push((*timestamp, *addr, collection_name.clone()));
                        }
                    }
                }

                if addresses_by_block.is_empty() {
                    continue;
                }

                let factory_data = FactoryAddressData {
                    range_start: range.start,
                    range_end: range.end,
                    addresses_by_block,
                };

                process_factory_once_calls(
                    &range,
                    client,
                    &factory_data,
                    &factory_once_configs,
                    &base_output_dir,
                    &existing_files,
                    &factory_once_column_indexes,
                    decoder_tx,
                )
                .await?;

                factory_once_catchup_count += 1;
            }

            if factory_once_catchup_count > 0 {
                tracing::info!(
                    "Factory once calls catchup complete: checked {} ranges for chain {}",
                    factory_once_catchup_count,
                    chain.name
                );
            }
        }
    }

    // =========================================================================
    // Catchup phase for event-triggered calls: Read from existing log parquet files
    // =========================================================================
    if has_event_triggered_calls {
        // CRITICAL: Load historical factory addresses BEFORE processing event triggers
        // This ensures we can properly filter events from factory-created contracts
        let historical_factory_addrs =
            load_historical_factory_addresses(&chain.name, &event_call_configs);
        for (collection_name, addrs) in historical_factory_addrs {
            tracing::info!(
                "Loaded {} historical factory addresses for collection {}",
                addrs.len(),
                collection_name
            );
            factory_addresses
                .entry(collection_name)
                .or_default()
                .extend(addrs);
        }

        let log_ranges = get_existing_log_ranges(&chain.name);
        let event_matchers = build_event_trigger_matchers(&chain.contracts);
        let mut event_catchup_count = 0;

        tracing::info!(
            "Event-triggered calls catchup: checking {} log ranges for chain {}",
            log_ranges.len(),
            chain.name
        );

        for log_range in &log_ranges {
            // Check if output already exists for all event-triggered call configs
            let mut needs_processing = false;
            for (_, configs) in &event_call_configs {
                for config in configs {
                    if !event_output_exists(
                        &base_output_dir,
                        &config.contract_name,
                        &config.function_name,
                        log_range.start,
                        log_range.end,
                    ) {
                        needs_processing = true;
                        break;
                    }
                }
                if needs_processing {
                    break;
                }
            }

            if !needs_processing {
                tracing::debug!(
                    "Skipping event-triggered calls for blocks {}-{} (already exists)",
                    log_range.start,
                    log_range.end - 1
                );
                continue;
            }

            // Read logs from parquet file
            let logs = match read_logs_from_parquet(&log_range.file_path) {
                Ok(logs) => logs,
                Err(e) => {
                    tracing::warn!(
                        "Failed to read logs from {}: {}",
                        log_range.file_path.display(),
                        e
                    );
                    continue;
                }
            };

            if logs.is_empty() {
                continue;
            }

            tracing::info!(
                "Catchup: processing event-triggered calls for blocks {}-{} ({} logs)",
                log_range.start,
                log_range.end - 1,
                logs.len()
            );

            // Extract event triggers from logs
            let triggers = extract_event_triggers(&logs, &event_matchers);

            if !triggers.is_empty() {
                tracing::info!(
                    "Extracted {} event triggers from {} logs for blocks {}-{}",
                    triggers.len(),
                    logs.len(),
                    log_range.start,
                    log_range.end - 1
                );
            }

            // Process the triggers (or write empty files if no triggers)
            let range_start = log_range.start;
            let range_end = log_range.end - 1;
            if let Some(multicall_addr) = multicall3_address {
                process_event_triggers_multicall(
                    triggers,
                    &event_call_configs,
                    &factory_addresses,
                    client,
                    &base_output_dir,
                    rpc_batch_size,
                    decoder_tx,
                    multicall_addr,
                    range_start,
                    range_end,
                )
                .await?;
            } else {
                process_event_triggers(
                    triggers,
                    &event_call_configs,
                    &factory_addresses,
                    client,
                    &base_output_dir,
                    rpc_batch_size,
                    decoder_tx,
                    range_start,
                    range_end,
                )
                .await?;
            }

            event_catchup_count += 1;
        }

        if event_catchup_count > 0 {
            tracing::info!(
                "Event-triggered calls catchup complete: processed {} ranges for chain {}",
                event_catchup_count,
                chain.name
            );
        }
    }

    // Signal that all catchup phases are complete
    if let Some(tx) = eth_calls_catchup_done_tx {
        let _ = tx.send(());
    }

    Ok(EthCallCatchupState {
        base_output_dir,
        range_size,
        rpc_batch_size,
        multicall3_address,
        call_configs,
        factory_call_configs,
        event_call_configs,
        token_call_configs,
        once_configs,
        factory_once_configs,
        has_regular_calls,
        has_once_calls,
        has_factory_calls,
        has_factory_once_calls,
        has_event_triggered_calls,
        has_token_calls,
        max_params,
        factory_max_params,
        existing_files,
        factory_addresses,
        frequency_state,
        range_data,
        range_factory_data,
        range_regular_done,
        range_factory_done,
    })
}
