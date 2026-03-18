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
    build_factory_matchers, get_existing_log_ranges, load_factory_addresses_from_parquet,
    process_range_batches, read_log_batches_from_parquet, scan_existing_parquet_files,
    FactoryAddressData, FactoryCatchupState, FactoryCollectionError,
    RecollectRequest,
};
use crate::storage::{DataLoader, S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

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
    let output_dir = PathBuf::from(format!("data/{}/historical/factories", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let existing_factory_data = load_factory_addresses_from_parquet(&output_dir)?;
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
                let _ = tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses,
                    })
                    .await;
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

    let existing_files = scan_existing_parquet_files(&output_dir);

    // Get the factory collection names from matchers
    let factory_collection_names: HashSet<String> =
        matchers.iter().map(|m| m.collection_name.clone()).collect();

    // =========================================================================
    // Catchup phase: Process existing logs files where factory files are missing
    // This avoids re-fetching receipts when logs already exist
    // =========================================================================
    let log_ranges = get_existing_log_ranges(&chain.name, s3_manifest.as_ref());
    let mut catchup_count = 0;

    let factory_concurrency = raw_data_config.factory_concurrency.unwrap_or(4);
    let matchers = Arc::new(matchers);
    let existing_files = Arc::new(existing_files);
    let output_dir = Arc::new(output_dir);
    let s3_manifest = Arc::new(s3_manifest);
    let storage_manager = Arc::new(storage_manager);
    let chain_name = Arc::new(chain.name.clone());

    {
        let semaphore = Arc::new(Semaphore::new(factory_concurrency));
        let mut join_set: JoinSet<Result<Option<FactoryAddressData>, FactoryCollectionError>> =
            JoinSet::new();

        for log_range in &log_ranges {
            let all_factory_files_exist = factory_collection_names.iter().all(|collection| {
                let rel_path = format!(
                    "{}/{}-{}.parquet",
                    collection,
                    log_range.start,
                    log_range.end - 1
                );
                existing_files.contains(&rel_path)
                    || s3_manifest.as_ref().as_ref().map_or(false, |m| {
                        m.has_factories(collection, log_range.start, log_range.end - 1)
                    })
            });

            if all_factory_files_exist {
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

                let file_path_for_read = file_path.clone();
                let file_path_display = file_path.display().to_string();
                let batches = match tokio::task::spawn_blocking(move || {
                    read_log_batches_from_parquet(&file_path_for_read)
                })
                .await
                {
                    Ok(Ok(b)) => b,
                    Ok(Err(e)) => {
                        tracing::warn!(
                            "Corrupted log file {}: {} - deleting and requesting recollection for range {}-{}",
                            file_path_display,
                            e,
                            start,
                            end - 1
                        );

                        // Delete the corrupted file
                        if let Err(del_err) = std::fs::remove_file(&file_path) {
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
                    Err(e) => {
                        return Err(FactoryCollectionError::JoinError(e.to_string()));
                    }
                };

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                tracing::info!(
                    "Catchup: processing factories for blocks {}-{} from existing logs file ({} rows)",
                    start,
                    end - 1,
                    total_rows
                );

                process_range_batches(start, end, batches, &matchers, &output_dir, &existing_files, s3_manifest.as_ref().as_ref(), storage_manager.as_ref().as_ref(), &chain_name)
                    .await
                    .map(Some)
            });
        }

        let mut catchup_results: Vec<FactoryAddressData> = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(Some(data))) => catchup_results.push(data),
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
                let _ = tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses,
                    })
                    .await;
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
