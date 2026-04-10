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
    build_once_call_configs, event_output_exists_async, expected_event_call_key_counts_by_output,
    get_existing_log_ranges_async, process_event_triggers, process_event_triggers_multicall,
    process_factory_once_calls, process_once_calls_multicall, process_once_calls_regular,
    process_range, process_range_multicall, read_event_trigger_log_batches_from_parquet_async,
    scan_existing_parquet_files_async, AbortOnDropHandles, BlockInfo, BlockRange,
    EthCallCatchupState, EthCallCollectionError, EthCallContext, EventCallKey,
    EventTriggeredCallConfig, FrequencyState, OnceCallConfig,
};
use crate::raw_data::historical::factories::{get_factory_call_configs, FactoryAddressData};
use crate::raw_data::historical::receipts::{
    build_event_trigger_matchers, extract_event_triggers, extract_event_triggers_from_batches,
    EventTriggerMatcher,
};
use crate::rpc::UnifiedRpcClient;
use crate::storage::contract_index::{
    build_expected_factory_contracts_for_range, get_missing_contracts, range_key,
    read_contract_index, update_contract_index, write_contract_index,
};
use crate::storage::factory_data::{
    load_factory_addresses_by_collection, load_factory_addresses_with_metadata,
};
use crate::storage::parquet_readers::read_event_calls_from_parquet;
use crate::storage::paths::raw_eth_calls_dir;
use crate::storage::{upload_sidecar_to_s3, DataLoader, S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::shared::repair::RepairScope;

/// Extracted helper: processes factory-once catchup for a single block range.
///
/// Builds a `FactoryAddressData` from the provided `addresses_by_block`, computes
/// expected factory contracts for the range, and delegates to `process_factory_once_calls`.
pub(crate) async fn process_factory_once_catchup_range(
    range: &BlockRange,
    addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
    ctx: &EthCallContext<'_>,
    factory_once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    factory_once_column_indexes: &HashMap<String, HashMap<String, Vec<String>>>,
    contracts: &Contracts,
) -> Result<(), EthCallCollectionError> {
    let factory_data = FactoryAddressData {
        range_start: range.start,
        range_end: range.end,
        addresses_by_block,
    };
    let expected_once = build_expected_factory_contracts_for_range(contracts, range.end);
    process_factory_once_calls(
        range,
        ctx,
        &factory_data,
        factory_once_configs,
        factory_once_column_indexes,
        Some(&expected_once),
    )
    .await
}

fn should_run_event_triggered_catchup(
    has_event_triggered_calls: bool,
    repair: bool,
    repair_only: bool,
) -> bool {
    has_event_triggered_calls && (!repair_only || repair)
}

fn filter_call_configs_for_repair(
    call_configs: &[crate::raw_data::historical::eth_calls::CallConfig],
    repair_scope: Option<&RepairScope>,
) -> Vec<crate::raw_data::historical::eth_calls::CallConfig> {
    match repair_scope {
        Some(scope) => call_configs
            .iter()
            .filter(|config| {
                scope.matches_source_function(&config.contract_name, &config.function_name)
            })
            .cloned()
            .collect(),
        None => call_configs.to_vec(),
    }
}

fn filter_once_configs_for_repair(
    once_configs: &HashMap<String, Vec<OnceCallConfig>>,
    repair_scope: Option<&RepairScope>,
) -> HashMap<String, Vec<OnceCallConfig>> {
    match repair_scope {
        Some(scope) => once_configs
            .iter()
            .filter_map(|(contract_name, configs)| {
                let filtered: Vec<OnceCallConfig> = configs
                    .iter()
                    .filter(|config| {
                        scope.matches_source_function(contract_name, &config.function_name)
                    })
                    .cloned()
                    .collect();
                (!filtered.is_empty()).then(|| (contract_name.clone(), filtered))
            })
            .collect(),
        None => once_configs.clone(),
    }
}

fn filter_event_call_configs_for_repair(
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    repair_scope: Option<&RepairScope>,
) -> HashMap<EventCallKey, Vec<EventTriggeredCallConfig>> {
    match repair_scope {
        Some(scope) => event_call_configs
            .iter()
            .filter_map(|(key, configs)| {
                let filtered: Vec<EventTriggeredCallConfig> = configs
                    .iter()
                    .filter(|config| {
                        scope.matches_source_function(&config.contract_name, &config.function_name)
                    })
                    .cloned()
                    .collect();
                (!filtered.is_empty()).then(|| (key.clone(), filtered))
            })
            .collect(),
        None => event_call_configs.clone(),
    }
}

fn filter_event_trigger_matchers(
    matchers: Vec<EventTriggerMatcher>,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
) -> Vec<EventTriggerMatcher> {
    let active_keys: HashSet<EventCallKey> = event_call_configs.keys().cloned().collect();
    matchers
        .into_iter()
        .filter(|matcher| {
            active_keys.contains(&(matcher.source_name.clone(), matcher.event_topic0))
        })
        .collect()
}

type EventRowKeyCounts = HashMap<(u64, u32), usize>;

fn active_event_output_pairs_for_range(
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    range_end_exclusive: u64,
) -> HashMap<(String, String), bool> {
    let mut pairs = HashMap::new();

    for config in event_call_configs.values().flatten() {
        if let Some(sb) = config.start_block {
            if range_end_exclusive <= sb {
                continue;
            }
        }

        pairs
            .entry((config.contract_name.clone(), config.function_name.clone()))
            .and_modify(|is_factory| *is_factory |= config.is_factory)
            .or_insert(config.is_factory);
    }

    pairs
}

fn build_event_row_key_counts<I>(rows: I) -> EventRowKeyCounts
where
    I: IntoIterator<Item = (u64, u32)>,
{
    let mut counts = HashMap::new();
    for key in rows {
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

async fn read_raw_event_row_key_counts(output_path: PathBuf) -> Result<EventRowKeyCounts, String> {
    tokio::task::spawn_blocking(move || {
        let results = read_event_calls_from_parquet(&output_path).map_err(|e| e.to_string())?;
        Ok::<_, String>(build_event_row_key_counts(
            results.into_iter().map(|r| (r.block_number, r.log_index)),
        ))
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn repair_needs_event_recollection(
    base_output_dir: &std::path::Path,
    event_call_configs: &HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    factory_addresses: &HashMap<String, HashSet<Address>>,
    contracts: &Contracts,
    triggers: &[crate::raw_data::historical::receipts::EventTriggerData],
    range_start: u64,
    range_end_inclusive: u64,
) -> Result<bool, EthCallCollectionError> {
    let range_end_exclusive = range_end_inclusive + 1;
    let active_pairs = active_event_output_pairs_for_range(event_call_configs, range_end_exclusive);
    let expected_counts =
        expected_event_call_key_counts_by_output(triggers, event_call_configs, factory_addresses);
    let expected_factory_contracts =
        build_expected_factory_contracts_for_range(contracts, range_end_exclusive);

    for ((contract_name, function_name), is_factory) in active_pairs {
        let output_path = base_output_dir
            .join(&contract_name)
            .join(&function_name)
            .join("on_events")
            .join(format!("{}-{}.parquet", range_start, range_end_inclusive));

        if !output_path.exists() {
            tracing::info!(
                "Repair flagged missing on_events file for {}.{} blocks {}-{}",
                contract_name,
                function_name,
                range_start,
                range_end_inclusive
            );
            return Ok(true);
        }

        let actual = match read_raw_event_row_key_counts(output_path.clone()).await {
            Ok(counts) => counts,
            Err(err) => {
                tracing::warn!(
                    "Repair flagged unreadable on_events file {}: {}",
                    output_path.display(),
                    err
                );
                return Ok(true);
            }
        };
        let expected = expected_counts
            .get(&(contract_name.clone(), function_name.clone()))
            .cloned()
            .unwrap_or_default();

        if actual != expected {
            tracing::info!(
                "Repair flagged suspicious on_events rows for {}.{} blocks {}-{}: expected {} trigger keys, found {}",
                contract_name,
                function_name,
                range_start,
                range_end_inclusive,
                expected.len(),
                actual.len()
            );
            return Ok(true);
        }

        if is_factory {
            let sub_dir = base_output_dir
                .join(&contract_name)
                .join(&function_name)
                .join("on_events");
            let index = read_contract_index(&sub_dir);
            let rk = range_key(range_start, range_end_inclusive);
            if let Some(expected_contracts) = expected_factory_contracts.get(contract_name.as_str())
            {
                if !get_missing_contracts(&index, &rk, expected_contracts).is_empty() {
                    tracing::info!(
                        "Repair flagged missing contract index coverage for {}.{} blocks {}-{}",
                        contract_name,
                        function_name,
                        range_start,
                        range_end_inclusive
                    );
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

#[allow(clippy::too_many_arguments)]
pub async fn collect_eth_calls(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    repair: bool,
    repair_only: bool,
    repair_scope: Option<RepairScope>,
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
    let repair_scope = repair.then_some(repair_scope.as_ref()).flatten();

    if repair_only {
        tracing::info!(
            "Eth_call collection repair-only mode enabled: running repair passes without normal catchup collection"
        );
    }

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
    let catchup_call_configs = filter_call_configs_for_repair(&call_configs, repair_scope);
    let catchup_once_configs = filter_once_configs_for_repair(&once_configs, repair_scope);
    let catchup_factory_once_configs =
        filter_once_configs_for_repair(&factory_once_configs, repair_scope);
    let catchup_event_call_configs =
        filter_event_call_configs_for_repair(&event_call_configs, repair_scope);

    let has_regular_calls = !call_configs.is_empty();
    let has_once_calls = !once_configs.is_empty();
    let has_factory_calls = !factory_call_configs.is_empty() && has_factory_rx;
    let has_factory_once_calls = !factory_once_configs.is_empty() && has_factory_rx;
    let has_event_triggered_calls = !event_call_configs.is_empty() && has_event_trigger_rx;
    let catchup_has_regular_calls = !catchup_call_configs.is_empty();
    let catchup_has_once_calls = !catchup_once_configs.is_empty();
    let catchup_has_factory_once_calls = !catchup_factory_once_configs.is_empty() && has_factory_rx;
    let catchup_has_event_triggered_calls =
        !catchup_event_call_configs.is_empty() && has_event_trigger_rx;

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
            repair,
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
            contracts: chain.contracts.clone(),
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

    if !repair_only && (catchup_has_regular_calls || catchup_has_once_calls) {
        let block_ranges =
            get_existing_block_ranges_async(chain.name.clone(), s3_manifest.as_ref().cloned())
                .await;
        tracing::info!(
            "eth_calls catchup: checking {} block ranges (regular={}, once={})",
            block_ranges.len(),
            catchup_has_regular_calls,
            catchup_has_once_calls
        );

        // Pre-load or build column indexes for all once directories
        let mut once_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for contract_name in catchup_once_configs.keys() {
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

            if let Some(scope) = repair_scope {
                if !scope.matches_range(range.start, range.end) {
                    continue;
                }
            }

            // Check if all regular call files exist for this range
            let regular_calls_done = !catchup_has_regular_calls
                || catchup_call_configs.iter().all(|config| {
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
            let once_calls_done = !catchup_has_once_calls
                || catchup_once_configs.iter().all(|(contract_name, configs)| {
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
                repair,
                decoder_tx: &None,
                chain_name: &chain.name,
                storage_manager: storage_manager.as_ref(),
                s3_manifest: &s3_manifest,
            };

            if catchup_has_regular_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_range_multicall(
                        &range,
                        blocks.clone(),
                        &catchup_ctx,
                        &catchup_call_configs,
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
                        &catchup_call_configs,
                        max_params,
                        &mut frequency_state,
                        Some(&mut pending_writes),
                    )
                    .await?;
                }
            }

            if catchup_has_once_calls {
                if let Some(multicall_addr) = multicall3_address {
                    process_once_calls_multicall(
                        &range,
                        &blocks,
                        &catchup_ctx,
                        &catchup_once_configs,
                        &chain.contracts,
                        multicall_addr,
                    )
                    .await?;
                } else {
                    process_once_calls_regular(
                        &range,
                        &blocks,
                        &catchup_ctx,
                        &catchup_once_configs,
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
    if catchup_has_factory_once_calls {
        tracing::info!(
            "Factory once calls catchup: checking {} factory collections for missing columns",
            catchup_factory_once_configs.len()
        );

        // Read column indexes for all factory once directories (don't auto-build).
        // Using read_once_column_index so we can detect when no index file exists
        // and properly handle null-filling conditions.
        let mut factory_once_column_indexes: HashMap<String, HashMap<String, Vec<String>>> =
            HashMap::new();
        for collection_name in catchup_factory_once_configs.keys() {
            let once_dir = base_output_dir.join(collection_name).join("once");
            let index = read_once_column_index_async(once_dir).await;
            factory_once_column_indexes.insert(collection_name.clone(), index);
        }

        // Load factory address data from existing parquet files
        let collections_needed: HashSet<String> =
            catchup_factory_once_configs.keys().cloned().collect();
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

                if let Some(scope) = repair_scope {
                    if !scope.matches_range(range.start, range.end) {
                        continue;
                    }
                }

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

                let factory_once_ctx = EthCallContext {
                    client,
                    output_dir: &base_output_dir,
                    existing_files: &existing_files,
                    rpc_batch_size,
                    repair,
                    decoder_tx,
                    chain_name: &chain.name,
                    storage_manager: storage_manager.as_ref(),
                    s3_manifest: &s3_manifest,
                };

                process_factory_once_catchup_range(
                    &range,
                    addresses_by_block,
                    &factory_once_ctx,
                    &catchup_factory_once_configs,
                    &factory_once_column_indexes,
                    &chain.contracts,
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
    if should_run_event_triggered_catchup(catchup_has_event_triggered_calls, repair, repair_only) {
        // CRITICAL: Load historical factory addresses BEFORE processing event triggers
        // This ensures we can properly filter events from factory-created contracts
        let factory_collections: HashSet<String> = catchup_event_call_configs
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
        let total_log_ranges = log_ranges.len();
        let event_call_concurrency = raw_data_config.event_call_concurrency.unwrap_or(4);
        let scoped_log_range_count = log_ranges
            .iter()
            .filter(|log_range| {
                repair_scope.is_none_or(|scope| {
                    scope.matches_range(log_range.start, log_range.end)
                }) && !active_event_output_pairs_for_range(
                    &catchup_event_call_configs,
                    log_range.end,
                )
                .is_empty()
            })
            .count();

        tracing::info!(
            "Event-triggered calls catchup: checking {} scoped log ranges for chain {} (concurrency={})",
            scoped_log_range_count,
            chain.name,
            event_call_concurrency
        );

        // Wrap shared data in Arcs for concurrent task access
        let event_matchers = filter_event_trigger_matchers(
            build_event_trigger_matchers(&chain.contracts),
            &catchup_event_call_configs,
        );
        let event_matchers_arc = Arc::new(event_matchers);
        let event_call_configs_arc = Arc::new(catchup_event_call_configs.clone());
        let factory_addresses_arc = Arc::new(factory_addresses.clone());
        let existing_files_arc = Arc::new(existing_files.clone());
        let contracts_arc = Arc::new(chain.contracts.clone());
        let base_output_dir_arc = Arc::new(base_output_dir.clone());
        let s3_manifest_arc = s3_manifest.as_ref().map(|m| Arc::new(m.clone()));
        let storage_manager_arc = storage_manager.clone();
        let chain_name_arc: Arc<str> = Arc::from(chain.name.as_str());

        // Process ranges concurrently with Semaphore + JoinSet.
        // Contract index writes are deferred to after all ranges complete.
        let mut processed_event_ranges: Vec<(u64, u64)> = Vec::new();
        {
            let semaphore = Arc::new(tokio::sync::Semaphore::new(event_call_concurrency));
            let mut join_set: tokio::task::JoinSet<
                Result<Option<(u64, u64)>, EthCallCollectionError>,
            > = tokio::task::JoinSet::new();

            for (idx, log_range) in log_ranges.iter().enumerate() {
                if let Some(scope) = repair_scope {
                    if !scope.matches_range(log_range.start, log_range.end) {
                        continue;
                    }
                }

                if active_event_output_pairs_for_range(&catchup_event_call_configs, log_range.end)
                    .is_empty()
                {
                    continue;
                }

                let range_start = log_range.start;
                let inclusive_end = log_range.end - 1;

                if repair {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();

                    while let Some(result) = join_set.try_join_next() {
                        match result {
                            Ok(Ok(Some((start, end)))) => {
                                processed_event_ranges.push((start, end));
                            }
                            Ok(Ok(None)) => {}
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(EthCallCollectionError::JoinError(e.to_string()));
                            }
                        }
                    }

                    let event_matchers = event_matchers_arc.clone();
                    let event_call_configs = event_call_configs_arc.clone();
                    let factory_addresses = factory_addresses_arc.clone();
                    let existing_files = existing_files_arc.clone();
                    let contracts = contracts_arc.clone();
                    let base_output_dir = base_output_dir_arc.clone();
                    let s3_manifest = s3_manifest.clone();
                    let storage_manager: Option<Arc<StorageManager>> = storage_manager_arc.clone();
                    let chain_name = chain_name_arc.clone();
                    let client = client.clone();
                    let decoder_tx = decoder_tx.clone();
                    let log_file_path = log_range.file_path.clone();

                    join_set.spawn(async move {
                        let (skipped, mut pending_writes) = {
                            let _permit = permit;

                            let batches = match read_event_trigger_log_batches_from_parquet_async(
                                log_file_path.clone(),
                            )
                            .await
                            {
                                Ok(batches) => batches,
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to read projected logs from {}: {}",
                                        log_file_path.display(),
                                        e
                                    );
                                    return Ok(None);
                                }
                            };

                            let log_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                            if log_count == 0 {
                                return Ok(None);
                            }

                            tracing::info!(
                                "Auditing event-triggered calls for blocks {}-{} ({} logs)",
                                range_start,
                                inclusive_end,
                                log_count
                            );

                            let triggers =
                                extract_event_triggers_from_batches(&batches, &event_matchers);
                            drop(batches);

                            let needs_processing = repair_needs_event_recollection(
                                &base_output_dir,
                                &event_call_configs,
                                &factory_addresses,
                                &contracts,
                                &triggers,
                                range_start,
                                inclusive_end,
                            )
                            .await?;

                            if !needs_processing {
                                tracing::debug!(
                                    "Skipping event-triggered calls for blocks {}-{} (already verified)",
                                    range_start,
                                    inclusive_end
                                );
                                return Ok(None);
                            }

                            if !triggers.is_empty() {
                                tracing::info!(
                                    "Repair recollecting event-triggered calls for blocks {}-{} ({} triggers)",
                                    range_start,
                                    inclusive_end,
                                    triggers.len()
                                );
                            }

                            let event_ctx = EthCallContext {
                                client: &client,
                                output_dir: &base_output_dir,
                                existing_files: &existing_files,
                                rpc_batch_size,
                                repair,
                                decoder_tx: &decoder_tx,
                                chain_name: &chain_name,
                                storage_manager: storage_manager.as_ref(),
                                s3_manifest: &s3_manifest,
                            };
                            if let Some(multicall_addr) = multicall3_address {
                                process_event_triggers_multicall(
                                    triggers,
                                    &event_call_configs,
                                    &factory_addresses,
                                    &event_ctx,
                                    multicall_addr,
                                    range_start,
                                    inclusive_end,
                                    &contracts,
                                    true,
                                )
                                .await?
                            } else {
                                process_event_triggers(
                                    triggers,
                                    &event_call_configs,
                                    &factory_addresses,
                                    &event_ctx,
                                    range_start,
                                    inclusive_end,
                                    &contracts,
                                    true,
                                )
                                .await?
                            }
                        };

                        if !skipped.is_empty() {
                            tracing::warn!(
                                "Unexpected skipped factory triggers during catchup ({} triggers) — factory addresses should be pre-loaded",
                                skipped.len()
                            );
                        }

                        while let Some(result) = pending_writes.join_next().await {
                            match result {
                                Ok(Ok(())) => {}
                                Ok(Err(e)) => return Err(e),
                                Err(e) => {
                                    return Err(EthCallCollectionError::JoinError(e.to_string()))
                                }
                            }
                        }

                        tracing::debug!(
                            "Event-triggered calls catchup: processed range {}/{} (blocks {}-{})",
                            idx + 1,
                            total_log_ranges,
                            range_start,
                            inclusive_end
                        );

                        Ok(Some((range_start, inclusive_end)))
                    });

                    continue;
                }

                let mut needs_processing = false;
                if !repair {
                    let event_expected_for_range =
                        build_expected_factory_contracts_for_range(&chain.contracts, log_range.end);

                    for configs in catchup_event_call_configs.values() {
                        for config in configs {
                            if let Some(sb) = config.start_block {
                                if log_range.end <= sb {
                                    continue;
                                }
                            }
                            let expected_for_config = if config.is_factory {
                                event_expected_for_range.get(&config.contract_name).cloned()
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
                }

                if !repair && !needs_processing {
                    tracing::debug!(
                        "Skipping event-triggered calls for blocks {}-{} (already exists)",
                        log_range.start,
                        log_range.end - 1
                    );
                    continue;
                }

                // --- Sequential phase: read parquet + extract triggers (one file at a time) ---
                // This avoids holding N large log files in memory concurrently.
                let logs =
                    match crate::raw_data::historical::eth_calls::read_logs_from_parquet_async(
                        log_range.file_path.clone(),
                    )
                    .await
                    {
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
                    range_start,
                    inclusive_end,
                    logs.len()
                );

                let triggers = extract_event_triggers(&logs, event_matchers_arc.as_ref());
                // Drop logs immediately — only triggers (much smaller) are kept for RPC work
                drop(logs);

                if repair {
                    needs_processing = repair_needs_event_recollection(
                        &base_output_dir,
                        &catchup_event_call_configs,
                        &factory_addresses,
                        &chain.contracts,
                        &triggers,
                        range_start,
                        inclusive_end,
                    )
                    .await?;
                }

                if !needs_processing {
                    tracing::debug!(
                        "Skipping event-triggered calls for blocks {}-{} (already verified)",
                        range_start,
                        inclusive_end
                    );
                    continue;
                }

                if !triggers.is_empty() {
                    tracing::info!(
                        "Extracted {} event triggers for blocks {}-{}",
                        triggers.len(),
                        range_start,
                        inclusive_end
                    );
                }

                // --- Concurrent phase: acquire permit and spawn RPC work ---
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                // Drain completed tasks to collect results eagerly
                while let Some(result) = join_set.try_join_next() {
                    match result {
                        Ok(Ok(Some((start, end)))) => {
                            processed_event_ranges.push((start, end));
                        }
                        Ok(Ok(None)) => {}
                        Ok(Err(e)) => return Err(e),
                        Err(e) => {
                            return Err(EthCallCollectionError::JoinError(e.to_string()));
                        }
                    }
                }

                let event_call_configs = event_call_configs_arc.clone();
                let factory_addresses = factory_addresses_arc.clone();
                let existing_files = existing_files_arc.clone();
                let contracts = contracts_arc.clone();
                let base_output_dir = base_output_dir_arc.clone();
                let s3_manifest = s3_manifest.clone();
                let storage_manager: Option<Arc<StorageManager>> = storage_manager_arc.clone();
                let chain_name = chain_name_arc.clone();
                let client = client.clone();
                let decoder_tx = decoder_tx.clone();

                join_set.spawn(async move {
                    // RPC phase — hold permit to limit concurrent RPC work
                    let (skipped, mut pending_writes) = {
                        let _permit = permit; // dropped at end of this block

                        let event_ctx = EthCallContext {
                            client: &client,
                            output_dir: &base_output_dir,
                            existing_files: &existing_files,
                            rpc_batch_size,
                            repair,
                            decoder_tx: &decoder_tx,
                            chain_name: &chain_name,
                            storage_manager: storage_manager.as_ref(),
                            s3_manifest: &s3_manifest,
                        };
                        if let Some(multicall_addr) = multicall3_address {
                            process_event_triggers_multicall(
                                triggers,
                                &event_call_configs,
                                &factory_addresses,
                                &event_ctx,
                                multicall_addr,
                                range_start,
                                inclusive_end,
                                &contracts,
                                true,
                            )
                            .await?
                        } else {
                            process_event_triggers(
                                triggers,
                                &event_call_configs,
                                &factory_addresses,
                                &event_ctx,
                                range_start,
                                inclusive_end,
                                &contracts,
                                true,
                            )
                            .await?
                        }
                    }; // permit released — next range's RPC can start immediately

                    if !skipped.is_empty() {
                        tracing::warn!(
                            "Unexpected skipped factory triggers during catchup ({} triggers) — factory addresses should be pre-loaded",
                            skipped.len()
                        );
                    }

                    // Write phase — no permit held, runs concurrently with other ranges' RPC
                    while let Some(result) = pending_writes.join_next().await {
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(EthCallCollectionError::JoinError(e.to_string()))
                            }
                        }
                    }

                    tracing::debug!(
                        "Event-triggered calls catchup: processed range {}/{} (blocks {}-{})",
                        idx + 1,
                        total_log_ranges,
                        range_start,
                        inclusive_end
                    );

                    Ok(Some((range_start, inclusive_end)))
                });
            }

            // Collect results from all spawned tasks
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(Some((start, end)))) => {
                        processed_event_ranges.push((start, end));
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => return Err(e),
                    Err(e) => {
                        return Err(EthCallCollectionError::JoinError(e.to_string()));
                    }
                }
            }
        }
        let event_catchup_count = processed_event_ranges.len();

        // Batch-write contract indexes for all processed ranges (deferred from concurrent phase)
        if !processed_event_ranges.is_empty() {
            let factory_pairs: HashSet<(String, String)> = catchup_event_call_configs
                .values()
                .flatten()
                .filter(|c| c.is_factory)
                .map(|c| (c.contract_name.clone(), c.function_name.clone()))
                .collect();

            for (contract_name, function_name) in &factory_pairs {
                let sub_dir = base_output_dir
                    .join(contract_name)
                    .join(function_name)
                    .join("on_events");
                let mut ci = read_contract_index(&sub_dir);
                let mut wrote_any = false;

                for &(start, end) in &processed_event_ranges {
                    let expected =
                        build_expected_factory_contracts_for_range(&chain.contracts, end + 1);
                    if let Some(exp) = expected.get(contract_name.as_str()) {
                        update_contract_index(&mut ci, &range_key(start, end), exp);
                        wrote_any = true;
                    }
                }

                if wrote_any {
                    if let Err(e) = write_contract_index(&sub_dir, &ci) {
                        tracing::warn!(
                            "Failed to batch-write contract index for {}.{}/on_events: {}",
                            contract_name,
                            function_name,
                            e
                        );
                    } else if let Some(sm) = storage_manager.as_ref() {
                        let index_path = sub_dir.join("contract_index.json");
                        if let Err(e) = upload_sidecar_to_s3(sm, &index_path).await {
                            tracing::warn!(
                                "Failed to upload contract index sidecar for {}.{}/on_events: {}",
                                contract_name,
                                function_name,
                                e
                            );
                        }
                    }
                }
            }
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
        repair,
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
        contracts: chain.contracts.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::raw_data::historical::eth_calls::parquet_io::{
        extract_addresses_from_once_parquet, read_existing_once_parquet, read_parquet_column_names,
    };
    use crate::rpc::UnifiedRpcClient;
    use crate::types::config::contract::{
        AddressOrAddresses, ContractConfig, FactoryConfig, FactoryEventConfig,
        FactoryEventConfigOrArray, FactoryParameterLocation,
    };
    use alloy::primitives::{Address, U256};

    #[tokio::test]
    async fn test_factory_once_catchup_zero_address_range_still_processes() {
        let tmp = TempDir::new().unwrap();
        let client = Arc::new(UnifiedRpcClient::from_url("http://127.0.0.1:8545").unwrap());

        let mut once_configs: HashMap<String, Vec<OnceCallConfig>> = HashMap::new();
        once_configs.insert(
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

        let column_indexes = HashMap::new();
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

        let existing_files = HashSet::new();
        let ctx = EthCallContext {
            client: &client,
            output_dir: tmp.path(),
            existing_files: &existing_files,
            rpc_batch_size: 10,
            repair: false,
            decoder_tx: &None,
            chain_name: "test",
            storage_manager: None,
            s3_manifest: &None,
        };

        let range = BlockRange { start: 0, end: 100 };
        let addresses_by_block = HashMap::new(); // empty = zero-address range

        let result = process_factory_once_catchup_range(
            &range,
            addresses_by_block,
            &ctx,
            &once_configs,
            &column_indexes,
            &contracts,
        )
        .await;

        assert!(result.is_ok(), "processing empty range should succeed");

        // Verify empty parquet was written
        let parquet_path = tmp.path().join("test_collection/once/0-99.parquet");
        assert!(
            parquet_path.exists(),
            "empty parquet must be written for zero-address range"
        );

        // Verify column names include our test function
        let cols = read_parquet_column_names(&parquet_path);
        assert!(
            cols.contains("testFn"),
            "parquet schema must include testFn column"
        );

        // Verify zero rows
        let batches = read_existing_once_parquet(&parquet_path).unwrap();
        let addrs = extract_addresses_from_once_parquet(&batches);
        assert!(addrs.is_empty(), "empty range parquet must have 0 rows");

        // Verify column index was written
        assert!(
            tmp.path()
                .join("test_collection/once/column_index.json")
                .exists(),
            "column_index.json must be written"
        );

        // Verify contract index was written (requires non-empty contracts config)
        assert!(
            tmp.path()
                .join("test_collection/once/contract_index.json")
                .exists(),
            "contract_index.json must be written when expected_contracts is non-empty"
        );
    }

    #[test]
    fn test_event_triggered_catchup_runs_in_repair_only_mode() {
        assert!(should_run_event_triggered_catchup(true, true, true));
        assert!(!should_run_event_triggered_catchup(true, false, true));
        assert!(should_run_event_triggered_catchup(true, false, false));
        assert!(!should_run_event_triggered_catchup(false, true, true));
    }
}
