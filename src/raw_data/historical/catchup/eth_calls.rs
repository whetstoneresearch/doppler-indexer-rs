use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use alloy::primitives::Address;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::blocks::{
    get_existing_block_ranges_async, read_block_info_from_parquet_async,
};
use crate::raw_data::historical::eth_calls::parquet_io::{
    load_or_build_once_column_index_async, read_once_column_index_async,
};
use crate::raw_data::historical::eth_calls::{
    build_call_configs, build_event_triggered_call_configs, build_factory_once_call_configs,
    build_once_call_configs, event_output_exists_async, get_existing_log_ranges_async,
    process_event_triggers, process_event_triggers_multicall, process_factory_once_calls,
    process_once_calls_multicall, process_once_calls_regular, process_range,
    process_range_multicall, read_logs_from_parquet_async, scan_existing_parquet_files_async,
    AbortOnDropHandles, BlockInfo, BlockRange, EthCallCatchupState, EthCallCollectionError,
    EthCallContext, FrequencyState,
};
use crate::raw_data::historical::factories::{get_factory_call_configs, FactoryAddressData};
use crate::storage::contract_index::build_expected_factory_contracts;
use crate::raw_data::historical::receipts::{build_event_trigger_matchers, extract_event_triggers};
use crate::rpc::UnifiedRpcClient;
use crate::storage::factory_data::{
    load_factory_addresses_by_collection, load_factory_addresses_with_metadata,
};
use crate::storage::paths::raw_eth_calls_dir;
use crate::storage::{DataLoader, S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::AddressOrAddresses;
use crate::types::config::raw_data::RawDataCollectionConfig;

#[allow(clippy::too_many_arguments)]
pub async fn collect_eth_calls(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    decoder_tx: &Option<Sender<DecoderMessage>>,
    has_factory_rx: bool,
    has_event_trigger_rx: bool,
    factory_catchup_done_rx: Option<oneshot::Receiver<()>>,
    eth_calls_catchup_done_tx: Option<oneshot::Sender<()>>,
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<EthCallCatchupState, EthCallCollectionError> {
    let base_output_dir = raw_eth_calls_dir(&chain.name);
    tokio::fs::create_dir_all(&base_output_dir).await?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;

    let call_configs = build_call_configs(&chain.contracts)?;
    let factory_call_configs =
        get_factory_call_configs(&chain.contracts, &chain.factory_collections);
    let event_call_configs = build_event_triggered_call_configs(&chain.contracts);
    let multicall3_address: Option<Address> =
        chain
            .contracts
            .get("Multicall3")
            .and_then(|c| match &c.address {
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

    if !has_regular_calls
        && !has_once_calls
        && !has_factory_calls
        && !has_factory_once_calls
        && !has_event_triggered_calls
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
            once_configs,
            factory_once_configs,
            has_regular_calls,
            has_once_calls,
            has_factory_calls,
            has_factory_once_calls,
            has_event_triggered_calls,
            max_params: 0,
            factory_max_params: 0,
            existing_files: HashSet::new(),
            s3_manifest,
            factory_addresses: HashMap::new(),
            frequency_state: FrequencyState {
                last_call_times: HashMap::new(),
            },
            range_data: HashMap::new(),
            range_factory_data: HashMap::new(),
            range_regular_done: HashSet::new(),
            range_factory_done: HashSet::new(),
            factory_skipped_triggers: Vec::new(),
            expected_by_collection: build_expected_factory_contracts(&chain.contracts),
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
        "Starting eth_call collection for chain {} with {} regular configs, {} once configs, {} factory collections, {} factory once configs, {} event trigger configs",
        chain.name,
        call_configs.len(),
        once_configs.len(),
        factory_call_configs.len(),
        factory_once_configs.len(),
        event_call_configs.len()
    );

    let existing_files = scan_existing_parquet_files_async(base_output_dir.clone()).await;

    let mut range_data: HashMap<u64, Vec<BlockInfo>> = HashMap::new();
    let range_factory_data: HashMap<u64, FactoryAddressData> = HashMap::new();
    let mut range_regular_done: HashSet<u64> = HashSet::new();
    let range_factory_done: HashSet<u64> = HashSet::new();

    if has_regular_calls || has_once_calls {
        let block_ranges =
            get_existing_block_ranges_async(chain.name.clone(), s3_manifest.as_ref().cloned())
                .await;
        tracing::info!(
            "eth_calls catchup: checking {} block ranges (regular={}, once={})",
            block_ranges.len(),
            has_regular_calls,
            has_once_calls
        );

        // Pre-load or build column indexes for all once directories
        let mut once_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for contract_name in once_configs.keys() {
            let once_dir = base_output_dir.join(contract_name).join("once");
            let index = load_or_build_once_column_index_async(once_dir).await;
            once_column_indexes.insert(contract_name.clone(), index);
        }

        let mut catchup_count = 0;
        let total_ranges = block_ranges.len();
        let mut pending_writes = AbortOnDropHandles::new();

        for (idx, block_range) in block_ranges.iter().enumerate() {
            // Drain writes from the previous range before starting the next one.
            // This ensures writes from range N complete before range N+1 finishes
            // its RPC work, overlapping I/O with RPC for maximum throughput.
            pending_writes.drain_all().await?;
            let range = BlockRange {
                start: block_range.start,
                end: block_range.end,
            };

            // Check if all regular call files exist for this range
            let regular_calls_done = !has_regular_calls
                || call_configs.iter().all(|config| {
                    // Skip check if range is entirely before contract's start_block (considered done)
                    if let Some(sb) = config.start_block {
                        if range.end <= sb {
                            return true;
                        }
                    }
                    let rel_path = format!(
                        "{}/{}/{}",
                        config.contract_name,
                        config.function_name,
                        range.file_name("")
                    );
                    existing_files.contains(&rel_path)
                });

            // Check if all once call files exist AND have all expected columns for this range
            let once_calls_done = !has_once_calls
                || once_configs.iter().all(|(contract_name, configs)| {
                    // Skip check if range is entirely before contract's start_block (considered done)
                    // All configs for a contract share the same start_block
                    if let Some(sb) = configs.first().and_then(|c| c.start_block) {
                        if range.end <= sb {
                            return true;
                        }
                    }

                    let rel_path = format!("{}/once/{}", contract_name, range.file_name(""));
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
                    match index.get(&range.file_name("")) {
                        Some(cols) => {
                            let missing: Vec<_> = expected
                                .iter()
                                .filter(|f| !cols.contains(&f.to_string()))
                                .collect();
                            if !missing.is_empty() {
                                tracing::info!(
                                    "Once file {} for {} exists but missing columns: {:?} (has: {:?})",
                                    range.file_name(""),
                                    contract_name,
                                    missing,
                                    cols
                                );
                                false
                            } else {
                                tracing::debug!(
                                    "Once file {} for {} complete with all {} columns",
                                    range.file_name(""),
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
                                range.file_name(""),
                                contract_name
                            );
                            false
                        }
                    }
                });

            // Skip this range only if ALL call types have their files
            if regular_calls_done && once_calls_done {
                range_regular_done.insert(range.start);
                continue;
            }

            // Ensure block file is available locally (download from S3 if needed)
            if !block_range.file_path.exists() {
                if let Some(ref sm) = storage_manager {
                    let data_loader =
                        DataLoader::new(Some(sm.clone()), &chain.name, PathBuf::from("data"));
                    match data_loader.ensure_local(&block_range.file_path).await {
                        Ok(true) => {
                            tracing::debug!(
                                "Downloaded block file from S3: {}",
                                block_range.file_path.display()
                            );
                        }
                        Ok(false) => {
                            tracing::warn!(
                                "Block file not found locally or in S3: {} for range {}-{}",
                                block_range.file_path.display(),
                                range.start,
                                range.end - 1
                            );
                            continue;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to download block file from S3: {} - {}",
                                block_range.file_path.display(),
                                e
                            );
                            continue;
                        }
                    }
                } else {
                    tracing::warn!(
                        "Block file not found locally and no S3 configured: {} for range {}-{}",
                        block_range.file_path.display(),
                        range.start,
                        range.end - 1
                    );
                    continue;
                }
            }

            let block_infos =
                match read_block_info_from_parquet_async(block_range.file_path.clone()).await {
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
                "Catchup: processing eth_calls range {}/{} for blocks {}-{} from existing block file",
                idx + 1,
                total_ranges,
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

            let catchup_ctx = EthCallContext {
                client,
                output_dir: &base_output_dir,
                existing_files: &existing_files,
                rpc_batch_size,
                decoder_tx: &None,
                chain_name: &chain.name,
                storage_manager: storage_manager.as_ref(),
                s3_manifest: &s3_manifest,
            };

            if has_regular_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_range_multicall(
                        &range,
                        blocks.clone(),
                        &catchup_ctx,
                        &call_configs,
                        max_params,
                        &mut frequency_state,
                        multicall_addr,
                        Some(&mut pending_writes),
                    )
                    .await?;
                } else {
                    process_range(
                        &range,
                        blocks.clone(),
                        &catchup_ctx,
                        &call_configs,
                        max_params,
                        &mut frequency_state,
                        Some(&mut pending_writes),
                    )
                    .await?;
                }
            }

            if has_once_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_once_calls_multicall(
                        &range,
                        &blocks,
                        &catchup_ctx,
                        &once_configs,
                        &chain.contracts,
                        multicall_addr,
                    )
                    .await?;
                } else {
                    process_once_calls_regular(
                        &range,
                        &blocks,
                        &catchup_ctx,
                        &once_configs,
                        &chain.contracts,
                    )
                    .await?;
                }
            }

            range_regular_done.insert(range.start);
            catchup_count += 1;
        }

        // Drain any remaining writes from the final range
        pending_writes.drain_all().await?;

        if catchup_count > 0 {
            tracing::info!(
                "Catchup complete: processed {} eth_call ranges for chain {}",
                catchup_count,
                chain.name
            );
        } else {
            tracing::info!(
                "eth_calls catchup: all {} block ranges already complete for chain {}",
                total_ranges,
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
            let index = read_once_column_index_async(once_dir).await;
            factory_once_column_indexes.insert(collection_name.clone(), index);
        }

        // Load factory address data from existing parquet files
        let collections_needed: HashSet<String> = factory_once_configs.keys().cloned().collect();
        let factory_catchup_data = load_factory_addresses_with_metadata(
            &chain.name,
            &collections_needed,
            s3_manifest.as_ref(),
            storage_manager.as_ref(),
        )
        .await;

        if !factory_catchup_data.is_empty() {
            let block_ranges =
                get_existing_block_ranges_async(chain.name.clone(), s3_manifest.as_ref().cloned())
                    .await;
            let total_factory_ranges = block_ranges.len();
            let mut factory_once_catchup_count = 0;

            for (idx, block_range) in block_ranges.iter().enumerate() {
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
                            addresses_by_block.entry(*block_num).or_default().push((
                                *timestamp,
                                *addr,
                                collection_name.clone(),
                            ));
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

                let factory_once_ctx = EthCallContext {
                    client,
                    output_dir: &base_output_dir,
                    existing_files: &existing_files,
                    rpc_batch_size,
                    decoder_tx,
                    chain_name: &chain.name,
                    storage_manager: storage_manager.as_ref(),
                    s3_manifest: &s3_manifest,
                };
                process_factory_once_calls(
                    &range,
                    &factory_once_ctx,
                    &factory_data,
                    &factory_once_configs,
                    &factory_once_column_indexes,
                )
                .await?;

                factory_once_catchup_count += 1;
                tracing::debug!(
                    "Factory once calls catchup: processed range {}/{} (blocks {}-{})",
                    idx + 1,
                    total_factory_ranges,
                    range.start,
                    range.end - 1
                );
            }

            if factory_once_catchup_count > 0 {
                tracing::info!(
                    "Factory once calls catchup complete: checked {} ranges for chain {}",
                    factory_once_catchup_count,
                    chain.name
                );
            } else {
                tracing::info!(
                    "Factory once calls catchup: all {} ranges already complete for chain {}",
                    total_factory_ranges,
                    chain.name
                );
            }
        } else {
            tracing::info!("Factory once calls catchup: no factory address data found, skipping");
        }
    }

    // =========================================================================
    // Catchup phase for event-triggered calls: Read from existing log parquet files
    // =========================================================================
    if has_event_triggered_calls {
        // CRITICAL: Load historical factory addresses BEFORE processing event triggers
        // This ensures we can properly filter events from factory-created contracts
        let factory_collections: HashSet<String> = event_call_configs
            .values()
            .flatten()
            .filter(|c| c.is_factory)
            .map(|c| c.contract_name.clone())
            .collect();
        let historical_factory_addrs = load_factory_addresses_by_collection(
            &chain.name,
            &factory_collections,
            s3_manifest.as_ref(),
            storage_manager.as_ref(),
        )
        .await;
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

        let log_ranges =
            get_existing_log_ranges_async(chain.name.clone(), s3_manifest.as_ref().cloned()).await;
        let event_matchers = build_event_trigger_matchers(&chain.contracts);
        let total_log_ranges = log_ranges.len();
        let mut event_catchup_count = 0;
        let s3_manifest_arc = s3_manifest.as_ref().map(|m| Arc::new(m.clone()));
        let event_expected_by_collection = build_expected_factory_contracts(&chain.contracts);

        tracing::info!(
            "Event-triggered calls catchup: checking {} log ranges for chain {}",
            log_ranges.len(),
            chain.name
        );

        for (idx, log_range) in log_ranges.iter().enumerate() {
            // Check if output already exists for all event-triggered call configs
            let mut needs_processing = false;
            for configs in event_call_configs.values() {
                for config in configs {
                    // Range is entirely before this config's start_block — no output needed
                    if let Some(sb) = config.start_block {
                        if log_range.end <= sb {
                            continue;
                        }
                    }
                    // For factory configs, pass expected contracts for contract index checking
                    let expected_for_config = if config.is_factory {
                        event_expected_by_collection.get(&config.contract_name).cloned()
                    } else {
                        None
                    };
                    if !event_output_exists_async(
                        base_output_dir.clone(),
                        config.contract_name.clone(),
                        config.function_name.clone(),
                        log_range.start,
                        log_range.end,
                        s3_manifest_arc.clone(),
                        expected_for_config,
                    )
                    .await?
                    {
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
            let logs = match read_logs_from_parquet_async(log_range.file_path.clone()).await {
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
            let skipped = if let Some(multicall_addr) = multicall3_address {
                let event_ctx = EthCallContext {
                    client,
                    output_dir: &base_output_dir,
                    existing_files: &existing_files,
                    rpc_batch_size,
                    decoder_tx,
                    chain_name: &chain.name,
                    storage_manager: storage_manager.as_ref(),
                    s3_manifest: &s3_manifest,
                };
                process_event_triggers_multicall(
                    triggers,
                    &event_call_configs,
                    &factory_addresses,
                    &event_ctx,
                    multicall_addr,
                    range_start,
                    range_end,
                    &event_expected_by_collection,
                )
                .await?
            } else {
                let event_ctx = EthCallContext {
                    client,
                    output_dir: &base_output_dir,
                    existing_files: &existing_files,
                    rpc_batch_size,
                    decoder_tx,
                    chain_name: &chain.name,
                    storage_manager: storage_manager.as_ref(),
                    s3_manifest: &s3_manifest,
                };
                process_event_triggers(
                    triggers,
                    &event_call_configs,
                    &factory_addresses,
                    &event_ctx,
                    range_start,
                    range_end,
                    &event_expected_by_collection,
                )
                .await?
            };
            if !skipped.is_empty() {
                tracing::warn!(
                    "Unexpected skipped factory triggers during catchup ({} triggers) — factory addresses should be pre-loaded",
                    skipped.len()
                );
            }

            event_catchup_count += 1;
            tracing::debug!(
                "Event-triggered calls catchup: processed range {}/{} (blocks {}-{})",
                idx + 1,
                total_log_ranges,
                log_range.start,
                log_range.end - 1
            );
        }

        if event_catchup_count > 0 {
            tracing::info!(
                "Event-triggered calls catchup complete: processed {} ranges for chain {}",
                event_catchup_count,
                chain.name
            );
        } else {
            tracing::info!(
                "Event-triggered calls catchup: all {} log ranges already complete for chain {}",
                total_log_ranges,
                chain.name
            );
        }
    }

    tracing::info!(
        "Eth_call collection catchup finished for chain {}",
        chain.name
    );

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
        once_configs,
        factory_once_configs,
        has_regular_calls,
        has_once_calls,
        has_factory_calls,
        has_factory_once_calls,
        has_event_triggered_calls,
        max_params,
        factory_max_params,
        existing_files,
        s3_manifest,
        factory_addresses,
        frequency_state,
        range_data,
        range_factory_data,
        range_regular_done,
        range_factory_done,
        factory_skipped_triggers: Vec::new(),
        expected_by_collection: build_expected_factory_contracts(&chain.contracts),
    })
}
