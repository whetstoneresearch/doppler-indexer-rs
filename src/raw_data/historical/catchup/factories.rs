use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use alloy::primitives::Address;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::factories::{
    build_factory_matchers, get_existing_log_ranges, load_factory_addresses_from_parquet_async,
    process_range_batches, read_log_batches_from_parquet_async, scan_existing_parquet_files,
    FactoryAddressData, FactoryCatchupState, FactoryCollectionError, RecollectRequest,
};
use crate::storage::contract_index::{
    build_address_contract_map, build_expected_factory_contracts_for_range,
    detect_contracts_in_log_parquet, get_missing_contracts, range_key, read_contract_index,
    update_contract_index, write_contract_index, ContractIndex,
};
use crate::storage::paths::factories_dir as factories_dir_path;
use crate::storage::{upload_sidecar_to_s3, DataLoader, S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

#[allow(clippy::too_many_arguments)]
pub async fn collect_factories(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    logs_factory_tx: &Option<Sender<FactoryAddressData>>,
    log_decoder_tx: &Option<Sender<DecoderMessage>>,
    recollect_tx: &Option<Sender<RecollectRequest>>,
    factory_catchup_done_tx: Option<oneshot::Sender<()>>,
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<FactoryCatchupState, FactoryCollectionError> {
    let output_dir = factories_dir_path(&chain.name);
    tokio::fs::create_dir_all(&output_dir).await?;

    let existing_factory_data =
        load_factory_addresses_from_parquet_async(output_dir.clone()).await?;
    if !existing_factory_data.is_empty() {
        tracing::info!(
            "Loaded {} existing factory ranges from parquet for chain {}",
            existing_factory_data.len(),
            chain.name
        );

        for factory_data in existing_factory_data {
            if let Some(ref tx) = logs_factory_tx {
                if tx.send(factory_data.clone()).await.is_err() {
                    tracing::error!(
                        "Failed to send existing factory data for range {}-{} to logs_factory_tx - receiver dropped",
                        factory_data.range_start,
                        factory_data.range_end
                    );
                    return Err(FactoryCollectionError::ChannelSend(format!(
                        "logs_factory_tx (existing data {}-{}) - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    )));
                }
            }

            // Note: eth_calls_factory_tx and call_decoder_tx sends are intentionally
            // skipped during catchup. Both consumers load factory addresses from parquet
            // during their own catchup phases. Sending here would deadlock because those
            // channels (capacity 1000) aren't consumed until after factory catchup completes.
            // The channels are used during the live/current phase instead.

            // Send to log decoder
            let addresses: HashMap<String, Vec<Address>> = factory_data
                .addresses_by_block
                .values()
                .flatten()
                .fold(HashMap::new(), |mut acc, (_, addr, collection)| {
                    acc.entry(collection.clone()).or_default().push(*addr);
                    acc
                });

            if let Some(ref tx) = log_decoder_tx {
                if tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses,
                    })
                    .await
                    .is_err()
                {
                    tracing::error!(
                        "Failed to send factory addresses to log decoder for range {}-{} - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    );
                    return Err(FactoryCollectionError::ChannelSend(format!(
                        "log_decoder_tx (existing factory data {}-{}) - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    )));
                }
            }
        }
    }

    let matchers = build_factory_matchers(&chain.contracts);

    if matchers.is_empty() {
        tracing::info!(
            "No factory matchers configured for chain {}, forwarding empty ranges",
            chain.name
        );

        if let Some(tx) = factory_catchup_done_tx {
            let _ = tx.send(());
        }

        return Ok(FactoryCatchupState {
            matchers: Arc::new(matchers),
            existing_files: Arc::new(HashSet::new()),
            output_dir: Arc::new(output_dir),
            s3_manifest,
            storage_manager,
            chain_name: chain.name.clone(),
        });
    }

    let existing_files = {
        let output_dir_clone = output_dir.clone();
        tokio::task::spawn_blocking(move || scan_existing_parquet_files(&output_dir_clone))
            .await
            .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))?
    };

    // Get the factory collection names from matchers
    let factory_collection_names: HashSet<String> =
        matchers.iter().map(|m| m.collection_name.clone()).collect();

    // Pre-load contract indexes for each collection to avoid repeated disk reads
    let mut contract_indexes: HashMap<String, ContractIndex> = HashMap::new();
    for collection in &factory_collection_names {
        let dir = output_dir.join(collection);
        let idx = {
            let dir_clone = dir.clone();
            tokio::task::spawn_blocking(move || read_contract_index(&dir_clone))
                .await
                .unwrap_or_default()
        };
        contract_indexes.insert(collection.clone(), idx);
    }

    // =========================================================================
    // Migration: build contract_index.json from existing data if missing
    // =========================================================================
    let needs_migration = factory_collection_names
        .iter()
        .any(|c| contract_indexes.get(c).map_or(true, |idx| idx.is_empty()));

    if needs_migration {
        tracing::info!(
            "Contract index migration: scanning log files to build index for existing factory ranges"
        );
        let address_maps = build_address_contract_map(&chain.contracts);
        let log_ranges_for_migration = {
            let chain_name = chain.name.clone();
            let s3_manifest_clone = s3_manifest.clone();
            tokio::task::spawn_blocking(move || {
                get_existing_log_ranges(&chain_name, s3_manifest_clone.as_ref())
            })
            .await
            .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))?
        };

        let mut migrated_count = 0usize;
        for log_range in &log_ranges_for_migration {
            let rk = range_key(log_range.start, log_range.end - 1);

            // Only migrate ranges that have factory parquet but no index entry
            let any_needs_migration = factory_collection_names.iter().any(|collection| {
                let rel_path = format!(
                    "{}/{}-{}.parquet",
                    collection,
                    log_range.start,
                    log_range.end - 1
                );
                let file_exists = existing_files.contains(&rel_path)
                    || s3_manifest.as_ref().is_some_and(|m| {
                        m.has_factories(collection, log_range.start, log_range.end - 1)
                    });
                if !file_exists {
                    return false;
                }
                let index = contract_indexes
                    .get(collection)
                    .cloned()
                    .unwrap_or_default();
                !index.contains_key(&rk)
            });

            if !any_needs_migration {
                continue;
            }

            // Scan the log parquet to detect which contracts were present
            if !log_range.file_path.exists() {
                continue;
            }

            match detect_contracts_in_log_parquet(&log_range.file_path, &address_maps) {
                Ok(detected) => {
                    for (collection, contracts_found) in &detected {
                        let index = contract_indexes.entry(collection.clone()).or_default();
                        if !index.contains_key(&rk) {
                            update_contract_index(index, &rk, contracts_found);
                        }
                    }
                    // Also record collections where no source addresses were found
                    // (the range was processed but had no matching events)
                    for collection in &factory_collection_names {
                        if !detected.contains_key(collection) {
                            let rel_path = format!(
                                "{}/{}-{}.parquet",
                                collection,
                                log_range.start,
                                log_range.end - 1
                            );
                            let file_exists = existing_files.contains(&rel_path)
                                || s3_manifest.as_ref().is_some_and(|m| {
                                    m.has_factories(collection, log_range.start, log_range.end - 1)
                                });
                            if file_exists {
                                let index = contract_indexes.entry(collection.clone()).or_default();
                                if !index.contains_key(&rk) {
                                    // No source addresses matched, but the file exists.
                                    // Record an empty entry so we don't re-scan.
                                    // Use detected (empty for this collection) which is correct:
                                    // no contracts contributed, so empty map.
                                    update_contract_index(
                                        index,
                                        &rk,
                                        detected.get(collection).unwrap_or(&HashMap::new()),
                                    );
                                }
                            }
                        }
                    }
                    migrated_count += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        "Migration: failed to scan log parquet {}: {}",
                        log_range.file_path.display(),
                        e
                    );
                }
            }
        }

        // Write migrated indexes
        if migrated_count > 0 {
            for collection in &factory_collection_names {
                if let Some(index) = contract_indexes.get(collection) {
                    if !index.is_empty() {
                        let dir = output_dir.join(collection);
                        if let Err(e) = write_contract_index(&dir, index) {
                            tracing::warn!(
                                "Migration: failed to write contract index for {}: {}",
                                collection,
                                e
                            );
                        }
                    }
                }
            }
            tracing::info!(
                "Contract index migration complete: built index from {} log ranges",
                migrated_count
            );
        }
    }

    // =========================================================================
    // Catchup phase: Process existing logs files where factory files are missing
    // or where the contract index shows missing contracts/addresses
    // =========================================================================
    let log_ranges = {
        let chain_name = chain.name.clone();
        let s3_manifest_clone = s3_manifest.clone();
        tokio::task::spawn_blocking(move || {
            get_existing_log_ranges(&chain_name, s3_manifest_clone.as_ref())
        })
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))?
    };
    let mut catchup_count = 0;

    let factory_concurrency = raw_data_config.factory_concurrency.unwrap_or(8);
    let matchers = Arc::new(matchers);
    let existing_files = Arc::new(existing_files);
    let output_dir = Arc::new(output_dir);
    let s3_manifest = Arc::new(s3_manifest);
    let storage_manager = Arc::new(storage_manager);
    let chain_name = Arc::new(chain.name.clone());

    // Track which ranges were successfully processed for contract index updates
    let mut processed_ranges: Vec<(u64, u64)> = Vec::new();

    {
        let semaphore = Arc::new(Semaphore::new(factory_concurrency));
        let mut join_set: JoinSet<
            Result<Option<(u64, u64, FactoryAddressData)>, FactoryCollectionError>,
        > = JoinSet::new();

        for log_range in &log_ranges {
            let rk = range_key(log_range.start, log_range.end - 1);

            let range_complete = factory_collection_names.iter().all(|collection| {
                let rel_path = format!(
                    "{}/{}-{}.parquet",
                    collection,
                    log_range.start,
                    log_range.end - 1
                );
                let file_exists = existing_files.contains(&rel_path)
                    || s3_manifest.as_ref().as_ref().is_some_and(|m| {
                        m.has_factories(collection, log_range.start, log_range.end - 1)
                    });

                if !file_exists {
                    return false;
                }

                // File exists — check contract index for missing contracts
                let expected_for_range =
                    build_expected_factory_contracts_for_range(&chain.contracts, log_range.end);
                if let Some(expected) = expected_for_range.get(collection) {
                    let index = contract_indexes
                        .get(collection)
                        .cloned()
                        .unwrap_or_default();
                    get_missing_contracts(&index, &rk, expected).is_empty()
                } else {
                    true
                }
            });

            if range_complete {
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let matchers = matchers.clone();
            let existing_files = existing_files.clone();
            let output_dir = output_dir.clone();
            let s3_manifest = s3_manifest.clone();
            let storage_manager = storage_manager.clone();
            let chain_name = chain_name.clone();
            let file_path = log_range.file_path.clone();
            let start = log_range.start;
            let end = log_range.end;

            let recollect_tx = recollect_tx.clone();
            join_set.spawn(async move {
                let _permit = permit;

                // Ensure file is available locally (download from S3 if needed)
                if !file_path.exists() {
                    if let Some(ref sm) = storage_manager.as_ref() {
                        let data_loader = DataLoader::new(
                            Some((*sm).clone()),
                            &chain_name,
                            PathBuf::from("data"),
                        );
                        match data_loader.ensure_local(&file_path).await {
                            Ok(true) => {
                                tracing::debug!("Downloaded log file from S3: {}", file_path.display());
                            }
                            Ok(false) => {
                                tracing::warn!(
                                    "Log file not found locally or in S3: {} for range {}-{}",
                                    file_path.display(),
                                    start,
                                    end - 1
                                );
                                return Ok(None);
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to download log file from S3: {} - {}",
                                    file_path.display(),
                                    e
                                );
                                return Ok(None);
                            }
                        }
                    } else {
                        tracing::warn!(
                            "Log file not found locally and no S3 configured: {} for range {}-{}",
                            file_path.display(),
                            start,
                            end - 1
                        );
                        return Ok(None);
                    }
                }

                let batches = match read_log_batches_from_parquet_async(file_path.clone()).await {
                    Ok(b) => b,
                    Err(e @ FactoryCollectionError::JoinError(_)) => {
                        // spawn_blocking failure (panic/cancellation) — not file corruption.
                        // Propagate so the caller can abort catchup instead of deleting valid data.
                        return Err(e);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Corrupted log file {}: {} - deleting and requesting recollection for range {}-{}",
                            file_path.display(),
                            e,
                            start,
                            end - 1
                        );

                        // Delete the corrupted file
                        if let Err(del_err) = tokio::fs::remove_file(&file_path).await {
                            tracing::error!(
                                "Failed to delete corrupted log file {}: {}",
                                file_path.display(),
                                del_err
                            );
                        } else {
                            tracing::info!("Deleted corrupted log file: {}", file_path.display());
                        }

                        // Send recollect request
                        if let Some(tx) = recollect_tx {
                            let request = RecollectRequest {
                                range_start: start,
                                range_end: end,
                                _file_path: file_path.clone(),
                            };
                            if let Err(send_err) = tx.send(request).await {
                                tracing::error!(
                                    "Failed to send recollect request for range {}-{}: {}",
                                    start,
                                    end - 1,
                                    send_err
                                );
                            }
                        }

                        return Ok(None);
                    }
                };

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                tracing::info!(
                    "Catchup: processing factories for blocks {}-{} from existing logs file ({} rows)",
                    start,
                    end - 1,
                    total_rows
                );

                let data = process_range_batches(start, end, batches, &matchers, &output_dir, &existing_files, s3_manifest.as_ref().as_ref(), storage_manager.as_ref().as_ref(), &chain_name)
                    .await?;
                Ok(Some((start, end, data)))
            });
        }

        let mut catchup_results: Vec<FactoryAddressData> = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(Some((start, end, data)))) => {
                    processed_ranges.push((start, end));
                    catchup_results.push(data);
                }
                Ok(Ok(None)) => {} // Skipped (corrupt/unreadable file)
                Ok(Err(e)) => {
                    tracing::error!("Factory catchup task failed: {:?}", e);
                    return Err(e);
                }
                Err(e) => return Err(FactoryCollectionError::JoinError(e.to_string())),
            }
        }

        catchup_results.sort_by_key(|d| d.range_start);

        for factory_data in catchup_results {
            if let Some(ref tx) = logs_factory_tx {
                if tx.send(factory_data.clone()).await.is_err() {
                    tracing::error!(
                        "Failed to send catchup factory data for range {}-{} to logs_factory_tx - receiver dropped",
                        factory_data.range_start,
                        factory_data.range_end
                    );
                    return Err(FactoryCollectionError::ChannelSend(format!(
                        "logs_factory_tx (catchup {}-{}) - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    )));
                }
            }

            // eth_calls_factory_tx and call_decoder_tx sends skipped during catchup
            // (see comment in existing data forwarding loop above)

            let addresses: HashMap<String, Vec<Address>> = factory_data
                .addresses_by_block
                .values()
                .flatten()
                .fold(HashMap::new(), |mut acc, (_, addr, collection)| {
                    acc.entry(collection.clone()).or_default().push(*addr);
                    acc
                });

            if let Some(ref tx) = log_decoder_tx {
                if tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses,
                    })
                    .await
                    .is_err()
                {
                    tracing::error!(
                        "Failed to send factory addresses to log decoder for range {}-{} - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    );
                    return Err(FactoryCollectionError::ChannelSend(format!(
                        "log_decoder_tx (catchup factory data {}-{}) - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    )));
                }
            }

            catchup_count += 1;
        }
    }

    if catchup_count > 0 {
        tracing::info!(
            "Factory catchup complete: processed {} ranges from logs files for chain {}",
            catchup_count,
            chain.name
        );
    }

    // Write contract index for all processed ranges (accumulated in memory to
    // avoid concurrent writes from the JoinSet tasks).
    if !processed_ranges.is_empty() {
        for collection in &factory_collection_names {
            let index = contract_indexes.entry(collection.clone()).or_default();
            let mut wrote_any = false;
            for &(start, end) in &processed_ranges {
                let rk = range_key(start, end - 1);
                let expected_for_range =
                    build_expected_factory_contracts_for_range(&chain.contracts, end);
                if let Some(expected) = expected_for_range.get(collection) {
                    update_contract_index(index, &rk, expected);
                    wrote_any = true;
                }
            }
            if wrote_any {
                let dir = output_dir.join(collection);
                write_contract_index(&dir, index).map_err(|e| FactoryCollectionError::Io(e))?;

                // Upload contract index to S3
                if let Some(ref sm) = storage_manager.as_ref() {
                    let index_path = dir.join("contract_index.json");
                    upload_sidecar_to_s3(sm, &index_path).await.map_err(|e| {
                        FactoryCollectionError::Io(std::io::Error::other(e.to_string()))
                    })?;
                }
            }
        }
        tracing::info!(
            "Updated contract index for {} factory collections ({} ranges)",
            factory_collection_names.len(),
            processed_ranges.len()
        );
    }

    if let Some(tx) = factory_catchup_done_tx {
        let _ = tx.send(());
    }

    // Extract from Arc - at this point all tasks are done so we're the only owner
    let s3_manifest = Arc::try_unwrap(s3_manifest).unwrap_or_else(|arc| (*arc).clone());
    let storage_manager = Arc::try_unwrap(storage_manager).unwrap_or_else(|arc| (*arc).clone());
    let chain_name = Arc::try_unwrap(chain_name).unwrap_or_else(|arc| (*arc).clone());

    Ok(FactoryCatchupState {
        matchers,
        existing_files,
        output_dir,
        s3_manifest,
        storage_manager,
        chain_name,
    })
}
