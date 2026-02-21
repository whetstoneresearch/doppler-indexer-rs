use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use alloy::primitives::B256;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::raw_data::historical::blocks::read_block_info_from_parquet;
use crate::raw_data::historical::factories::RecollectRequest;
use crate::raw_data::historical::receipts::{
    build_receipt_schema, extract_event_triggers, fetch_receipts_for_blocks, process_range,
    scan_existing_parquet_files, send_logs_to_channels, send_range_complete,
    write_full_receipts_to_parquet, write_minimal_receipts_to_parquet, BlockInfo, BlockRange,
    ChannelMetrics, EventTriggerMatcher, EventTriggerMessage, LogMessage, ReceiptBatchState,
    ReceiptCollectionError,
};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    mut block_rx: Receiver<(u64, u64, Vec<B256>)>,
    log_tx: Option<Sender<LogMessage>>,
    factory_log_tx: Option<Sender<LogMessage>>,
    event_trigger_tx: Option<Sender<EventTriggerMessage>>,
    event_matchers: Vec<EventTriggerMatcher>,
    mut recollect_rx: Option<Receiver<RecollectRequest>>,
) -> Result<(), ReceiptCollectionError> {
    let output_dir = PathBuf::from(format!("data/raw/{}/receipts", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;
    let block_receipt_concurrency = raw_data_config.block_receipt_concurrency.unwrap_or(10);
    let receipt_fields = &raw_data_config.fields.receipt_fields;
    let schema = build_receipt_schema(receipt_fields);

    let existing_files = scan_existing_parquet_files(&output_dir);

    // Batch states for early RPC fetching - track blocks received vs fetched
    let mut batch_states: HashMap<u64, ReceiptBatchState> = HashMap::new();

    // Get channel capacities for backpressure monitoring
    let log_tx_capacity = log_tx.as_ref().map(|s| s.max_capacity()).unwrap_or(0);
    let factory_log_tx_capacity = factory_log_tx.as_ref().map(|s| s.max_capacity()).unwrap_or(0);

    // Initialize channel metrics for backpressure tracking
    let mut log_tx_metrics = ChannelMetrics::default();
    let mut factory_log_tx_metrics = ChannelMetrics::default();

    tracing::info!(
        "Starting receipt collection (current mode) for chain {} (log_tx: {}, factory_log_tx: {}, log_tx_capacity: {}, factory_log_tx_capacity: {})",
        chain.name,
        log_tx.is_some(),
        factory_log_tx.is_some(),
        log_tx_capacity,
        factory_log_tx_capacity
    );

    // =========================================================================
    // Current phase: Process new blocks from the channel and handle recollect requests
    // Uses batch-based early fetching to start RPC work before full range is ready
    // =========================================================================
    let mut block_rx_closed = false;

    loop {
        tokio::select! {
            block_result = block_rx.recv(), if !block_rx_closed => {
                match block_result {
                    Some((block_number, timestamp, tx_hashes)) => {
                        let range_start = (block_number / range_size) * range_size;
                        let range_end = range_start + range_size;

                        // Initialize batch state if this is a new range
                        let state = batch_states.entry(range_start).or_insert_with(|| {
                            ReceiptBatchState {
                                range_start,
                                range_end,
                                blocks_received: HashMap::new(),
                                blocks_fetched: HashSet::new(),
                                minimal_records: Vec::new(),
                                full_records: Vec::new(),
                                logs: Vec::new(),
                            }
                        });

                        // Add block to received set
                        state.blocks_received.insert(block_number, BlockInfo {
                            block_number,
                            timestamp,
                            tx_hashes,
                        });

                        // Check if we should skip this range (file already exists)
                        let range = BlockRange { start: range_start, end: range_end };
                        if existing_files.contains(&range.file_name()) {
                            // Check if range is now complete
                            let expected: HashSet<u64> = (range_start..range_end).collect();
                            let received: HashSet<u64> = state.blocks_received.keys().copied().collect();
                            if expected.is_subset(&received) {
                                tracing::info!(
                                    "Skipping receipts for blocks {}-{} (already exists)",
                                    range.start,
                                    range.end - 1
                                );
                                batch_states.remove(&range_start);
                                send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
                            }
                            continue;
                        }

                        // Calculate unfetched blocks
                        let unfetched_blocks: Vec<u64> = state.blocks_received.keys()
                            .filter(|bn| !state.blocks_fetched.contains(bn))
                            .copied()
                            .collect();

                        // Early fetch: if we have enough unfetched blocks, fetch now
                        if unfetched_blocks.len() >= rpc_batch_size {
                            let mut blocks_to_fetch: Vec<u64> = unfetched_blocks
                                .into_iter()
                                .take(rpc_batch_size)
                                .collect();
                            blocks_to_fetch.sort();

                            let block_refs: Vec<&BlockInfo> = blocks_to_fetch
                                .iter()
                                .filter_map(|bn| state.blocks_received.get(bn))
                                .collect();

                            tracing::debug!(
                                "Early fetch: {} blocks for range {}-{} ({} of {} blocks received)",
                                block_refs.len(),
                                range_start,
                                range_end - 1,
                                state.blocks_received.len(),
                                range_size
                            );

                            let result = fetch_receipts_for_blocks(
                                &block_refs,
                                client,
                                receipt_fields,
                                chain.block_receipts_method.as_deref(),
                                block_receipt_concurrency,
                                rpc_batch_size,
                            )
                            .await?;

                            // Mark blocks as fetched
                            for bn in &blocks_to_fetch {
                                state.blocks_fetched.insert(*bn);
                            }

                            // Accumulate results
                            state.minimal_records.extend(result.minimal_records);
                            state.full_records.extend(result.full_records);

                            // Send logs immediately for early processing by downstream collectors
                            if !result.logs.is_empty() {
                                let triggers = if !event_matchers.is_empty() {
                                    extract_event_triggers(&result.logs, &event_matchers)
                                } else {
                                    Vec::new()
                                };

                                send_logs_to_channels(
                                    result.logs,
                                    &log_tx,
                                    &factory_log_tx,
                                    &mut log_tx_metrics,
                                    &mut factory_log_tx_metrics,
                                    log_tx_capacity,
                                    factory_log_tx_capacity,
                                    &mut std::time::Duration::default(),
                                )
                                .await?;

                                if !triggers.is_empty() {
                                    if let Some(tx) = &event_trigger_tx {
                                        tx.send(EventTriggerMessage::Triggers(triggers))
                                            .await
                                            .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
                                    }
                                }
                            }
                        }

                        // Check if range is complete
                        let expected: HashSet<u64> = (range_start..range_end).collect();
                        let received: HashSet<u64> = state.blocks_received.keys().copied().collect();

                        if expected.is_subset(&received) {
                            // Process any remaining unfetched blocks
                            let remaining_unfetched: Vec<u64> = state.blocks_received.keys()
                                .filter(|bn| !state.blocks_fetched.contains(bn))
                                .copied()
                                .collect();

                            if !remaining_unfetched.is_empty() {
                                let mut remaining_sorted = remaining_unfetched;
                                remaining_sorted.sort();

                                let block_refs: Vec<&BlockInfo> = remaining_sorted
                                    .iter()
                                    .filter_map(|bn| state.blocks_received.get(bn))
                                    .collect();

                                tracing::debug!(
                                    "Final fetch: {} remaining blocks for range {}-{}",
                                    block_refs.len(),
                                    range_start,
                                    range_end - 1
                                );

                                let result = fetch_receipts_for_blocks(
                                    &block_refs,
                                    client,
                                    receipt_fields,
                                    chain.block_receipts_method.as_deref(),
                                    block_receipt_concurrency,
                                    rpc_batch_size,
                                )
                                .await?;

                                state.minimal_records.extend(result.minimal_records);
                                state.full_records.extend(result.full_records);

                                // Send remaining logs
                                if !result.logs.is_empty() {
                                    let triggers = if !event_matchers.is_empty() {
                                        extract_event_triggers(&result.logs, &event_matchers)
                                    } else {
                                        Vec::new()
                                    };

                                    send_logs_to_channels(
                                        result.logs,
                                        &log_tx,
                                        &factory_log_tx,
                                        &mut log_tx_metrics,
                                        &mut factory_log_tx_metrics,
                                        log_tx_capacity,
                                        factory_log_tx_capacity,
                                        &mut std::time::Duration::default(),
                                    )
                                    .await?;

                                    if !triggers.is_empty() {
                                        if let Some(tx) = &event_trigger_tx {
                                            tx.send(EventTriggerMessage::Triggers(triggers))
                                                .await
                                                .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
                                        }
                                    }
                                }
                            }

                            // Remove state and write parquet
                            let final_state = batch_states.remove(&range_start).unwrap();

                            // Sort records by block number before writing
                            let mut minimal_records = final_state.minimal_records;
                            let mut full_records = final_state.full_records;
                            minimal_records.sort_by_key(|r| r.block_number);
                            full_records.sort_by_key(|r| r.block_number);

                            let total_receipts = match receipt_fields {
                                Some(fields) => {
                                    let count = minimal_records.len();
                                    let schema_clone = schema.clone();
                                    let fields_vec = fields.to_vec();
                                    let output_path = output_dir.join(range.file_name());
                                    tokio::task::spawn_blocking(move || {
                                        write_minimal_receipts_to_parquet(&minimal_records, &schema_clone, &fields_vec, &output_path)
                                    })
                                    .await
                                    .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))??;
                                    count
                                }
                                None => {
                                    let count = full_records.len();
                                    let schema_clone = schema.clone();
                                    let output_path = output_dir.join(range.file_name());
                                    tokio::task::spawn_blocking(move || {
                                        write_full_receipts_to_parquet(&full_records, &schema_clone, &output_path)
                                    })
                                    .await
                                    .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))??;
                                    count
                                }
                            };

                            tracing::info!(
                                "Receipts {}-{}: {} receipts written (early batch mode)",
                                range.start,
                                range.end - 1,
                                total_receipts
                            );

                            send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
                        }
                    }
                    None => {
                        block_rx_closed = true;
                    }
                }
            }

            recollect_result = async {
                match &mut recollect_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(request) = recollect_result {
                    tracing::info!(
                        "Recollecting range {}-{} (corrupted file was deleted)",
                        request.range_start,
                        request.range_end - 1
                    );

                    // Find the corresponding block file and read block info
                    let block_file_path = PathBuf::from(format!(
                        "data/raw/{}/blocks/blocks_{}-{}.parquet",
                        chain.name,
                        request.range_start,
                        request.range_end - 1
                    ));

                    let block_infos = match read_block_info_from_parquet(&block_file_path) {
                        Ok(infos) => infos,
                        Err(e) => {
                            tracing::error!(
                                "Failed to read block info for recollection {}-{}: {}",
                                request.range_start,
                                request.range_end - 1,
                                e
                            );
                            continue;
                        }
                    };

                    if block_infos.is_empty() {
                        tracing::warn!(
                            "No block info found for recollection range {}-{}",
                            request.range_start,
                            request.range_end - 1
                        );
                        continue;
                    }

                    let range = BlockRange {
                        start: request.range_start,
                        end: request.range_end,
                    };

                    let blocks: Vec<BlockInfo> = block_infos
                        .into_iter()
                        .map(|info| BlockInfo {
                            block_number: info.block_number,
                            timestamp: info.timestamp,
                            tx_hashes: info.tx_hashes,
                        })
                        .collect();

                    process_range(
                        &range,
                        blocks,
                        client,
                        receipt_fields,
                        &schema,
                        &output_dir,
                        &log_tx,
                        &factory_log_tx,
                        &event_trigger_tx,
                        &event_matchers,
                        rpc_batch_size,
                        &mut log_tx_metrics,
                        &mut factory_log_tx_metrics,
                        log_tx_capacity,
                        factory_log_tx_capacity,
                        chain.block_receipts_method.as_deref(),
                        block_receipt_concurrency,
                    )
                    .await?;

                    send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;

                    tracing::info!(
                        "Recollection complete for range {}-{}",
                        request.range_start,
                        request.range_end - 1
                    );
                } else {
                    // recollect_rx closed
                    recollect_rx = None;
                }
            }
        }

        // Exit loop when block_rx is closed and no more recollect requests are expected
        if block_rx_closed && recollect_rx.is_none() {
            break;
        }

        // If block_rx is closed but we might still get recollect requests, continue
        // (recollect_rx will be set to None when it closes)
        if block_rx_closed && recollect_rx.is_some() {
            // Keep waiting for recollect requests with a timeout
            // to avoid indefinite waiting if no more requests come
            continue;
        }
    }

    // Process any remaining incomplete ranges at shutdown
    for (range_start, mut state) in batch_states.drain() {
        if state.blocks_received.is_empty() {
            continue;
        }

        let max_block = state.blocks_received.keys().max().copied().unwrap_or(range_start);
        let range = BlockRange {
            start: range_start,
            end: max_block + 1,
        };

        if existing_files.contains(&range.file_name()) {
            tracing::info!(
                "Skipping receipts for blocks {}-{} (already exists)",
                range.start,
                range.end - 1
            );
            send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
            continue;
        }

        // Process any remaining unfetched blocks
        let remaining_unfetched: Vec<u64> = state.blocks_received.keys()
            .filter(|bn| !state.blocks_fetched.contains(bn))
            .copied()
            .collect();

        if !remaining_unfetched.is_empty() {
            let mut remaining_sorted = remaining_unfetched;
            remaining_sorted.sort();

            let block_refs: Vec<&BlockInfo> = remaining_sorted
                .iter()
                .filter_map(|bn| state.blocks_received.get(bn))
                .collect();

            let result = fetch_receipts_for_blocks(
                &block_refs,
                client,
                receipt_fields,
                chain.block_receipts_method.as_deref(),
                block_receipt_concurrency,
                rpc_batch_size,
            )
            .await?;

            state.minimal_records.extend(result.minimal_records);
            state.full_records.extend(result.full_records);

            // Send logs
            if !result.logs.is_empty() {
                let triggers = if !event_matchers.is_empty() {
                    extract_event_triggers(&result.logs, &event_matchers)
                } else {
                    Vec::new()
                };

                send_logs_to_channels(
                    result.logs,
                    &log_tx,
                    &factory_log_tx,
                    &mut log_tx_metrics,
                    &mut factory_log_tx_metrics,
                    log_tx_capacity,
                    factory_log_tx_capacity,
                    &mut std::time::Duration::default(),
                )
                .await?;

                if !triggers.is_empty() {
                    if let Some(tx) = &event_trigger_tx {
                        tx.send(EventTriggerMessage::Triggers(triggers))
                            .await
                            .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
                    }
                }
            }
        }

        // Sort and write parquet
        state.minimal_records.sort_by_key(|r| r.block_number);
        state.full_records.sort_by_key(|r| r.block_number);

        let total_receipts = match receipt_fields {
            Some(fields) => {
                let count = state.minimal_records.len();
                let schema_clone = schema.clone();
                let fields_vec = fields.to_vec();
                let output_path = output_dir.join(range.file_name());
                let minimal_records = state.minimal_records;
                tokio::task::spawn_blocking(move || {
                    write_minimal_receipts_to_parquet(&minimal_records, &schema_clone, &fields_vec, &output_path)
                })
                .await
                .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))??;
                count
            }
            None => {
                let count = state.full_records.len();
                let schema_clone = schema.clone();
                let output_path = output_dir.join(range.file_name());
                let full_records = state.full_records;
                tokio::task::spawn_blocking(move || {
                    write_full_receipts_to_parquet(&full_records, &schema_clone, &output_path)
                })
                .await
                .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))??;
                count
            }
        };

        tracing::info!(
            "Receipts {}-{}: {} receipts written (shutdown cleanup)",
            range.start,
            range.end - 1,
            total_receipts
        );

        send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
    }

    if let Some(sender) = &factory_log_tx {
        if sender.send(LogMessage::AllRangesComplete).await.is_err() {
            tracing::error!("Failed to send AllRangesComplete to factory_log_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "factory_log_tx (AllRangesComplete) - receiver dropped".to_string(),
            ));
        }
    }
    if let Some(sender) = &log_tx {
        if sender.send(LogMessage::AllRangesComplete).await.is_err() {
            tracing::error!("Failed to send AllRangesComplete to log_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "log_tx (AllRangesComplete) - receiver dropped".to_string(),
            ));
        }
    }
    if let Some(sender) = &event_trigger_tx {
        if sender.send(EventTriggerMessage::AllComplete).await.is_err() {
            tracing::error!("Failed to send AllComplete to event_trigger_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "event_trigger_tx (AllComplete) - receiver dropped".to_string(),
            ));
        }
    }

    // Log channel backpressure summaries
    if log_tx.is_some() {
        log_tx_metrics.log_summary("log_tx");
    }
    if factory_log_tx.is_some() {
        factory_log_tx_metrics.log_summary("factory_log_tx");
    }

    tracing::info!("Receipt collection complete for chain {}", chain.name);
    Ok(())
}
