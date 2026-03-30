use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::Schema;

use alloy::primitives::B256;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;

use crate::raw_data::historical::blocks::read_block_info_from_parquet_async;
use crate::raw_data::historical::catchup::receipts::ReceiptsCatchupState;
use crate::raw_data::historical::factories::RecollectRequest;
use crate::raw_data::historical::receipts::{
    build_receipt_schema, execute_receipt_write, extract_event_triggers,
    fetch_receipts_for_blocks, process_range, send_logs_to_channels, send_range_complete,
    write_full_receipts_to_parquet_async, write_minimal_receipts_to_parquet_async,
    BatchFetchResult, BlockInfo, ChannelMetrics, ChannelMetricsState, EventTriggerMatcher,
    EventTriggerMessage, LogMessage, PendingReceiptWrite, ReceiptBatchState,
    ReceiptCollectionError, ReceiptOutputChannels,
};
use crate::rpc::UnifiedRpcClient;
use crate::storage::paths::raw_receipts_dir;
use crate::storage::{upload_parquet_to_s3, BlockRange, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::{RawDataCollectionConfig, ReceiptField};

/// Result of a single-block receipt fetch dispatched into the JoinSet.
struct BlockReceiptResult {
    range_start: u64,
    block_number: u64,
    result: Result<BatchFetchResult, ReceiptCollectionError>,
}

/// Result of a completed parquet write + its S3 metadata.
struct WriteCompleteResult {
    result: Result<(), ReceiptCollectionError>,
    output_path: PathBuf,
    range_start: u64,
    range_end: u64, // exclusive
}

/// Check if all blocks in a range have been received and completed.
/// If so, remove the state, sort records, build a pending write, and spawn it.
///
/// Sends `EventTriggerMessage::RangeComplete` immediately so event-triggered
/// eth_call buffers stay aligned per-range. `LogMessage::RangeComplete` is
/// deferred to Branch 3 (after parquet write) so the transformation engine
/// can read the receipt file.
#[allow(clippy::too_many_arguments)]
async fn try_finalize_range(
    range_start: u64,
    batch_states: &mut HashMap<u64, ReceiptBatchState>,
    write_join_set: &mut JoinSet<WriteCompleteResult>,
    receipt_fields: &Option<Vec<ReceiptField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    event_trigger_tx: &Option<Sender<EventTriggerMessage>>,
) -> Result<bool, ReceiptCollectionError> {
    let state = match batch_states.get(&range_start) {
        Some(s) => s,
        None => return Ok(false),
    };

    let range_end = state.range_end;
    let expected: HashSet<u64> = (range_start..range_end).collect();
    let received: HashSet<u64> = state.blocks_received.keys().copied().collect();
    let all_received = expected.is_subset(&received);
    let all_completed = all_received && expected.is_subset(&state.blocks_completed);

    if !all_completed {
        return Ok(false);
    }

    let final_state = batch_states.remove(&range_start).unwrap();
    let range = BlockRange {
        start: range_start,
        end: range_end,
    };

    let mut minimal_records = final_state.minimal_records;
    let mut full_records = final_state.full_records;
    minimal_records.sort_by_key(|r| r.block_number);
    full_records.sort_by_key(|r| r.block_number);

    let output_path = output_dir.join(range.file_name("receipts"));
    let total_receipts;
    let pending_write = match receipt_fields {
        Some(fields) => {
            total_receipts = minimal_records.len();
            PendingReceiptWrite::Minimal {
                records: minimal_records,
                schema: schema.clone(),
                fields: fields.to_vec(),
                output_path: output_path.clone(),
            }
        }
        None => {
            total_receipts = full_records.len();
            PendingReceiptWrite::Full {
                records: full_records,
                schema: schema.clone(),
                output_path: output_path.clone(),
            }
        }
    };

    tracing::info!(
        "Receipts {}-{}: {} receipts, spawning write",
        range.start,
        range.end - 1,
        total_receipts
    );

    // Spawn parquet write; LogMessage::RangeComplete deferred to Branch 3.
    let wr_start = range.start;
    let wr_end = range.end;
    let wr_path = output_path;
    write_join_set.spawn(async move {
        let result = execute_receipt_write(pending_write).await;
        WriteCompleteResult {
            result,
            output_path: wr_path,
            range_start: wr_start,
            range_end: wr_end,
        }
    });

    // Send EventTriggerMessage::RangeComplete immediately so the event-triggered
    // eth_call buffer is flushed before triggers from the next range arrive.
    if let Some(tx) = event_trigger_tx {
        tx.send(EventTriggerMessage::RangeComplete {
            range_start: range.start,
            range_end: range.end,
        })
        .await
        .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
    }

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub async fn collect_receipts(
    chain: &ChainConfig,
    client: Arc<UnifiedRpcClient>,
    raw_data_config: &RawDataCollectionConfig,
    mut block_rx: Receiver<(u64, u64, Vec<B256>)>,
    log_tx: Option<Sender<LogMessage>>,
    factory_log_tx: Option<Sender<LogMessage>>,
    event_trigger_tx: Option<Sender<EventTriggerMessage>>,
    event_matchers: Vec<EventTriggerMatcher>,
    mut recollect_rx: Option<Receiver<RecollectRequest>>,
    catchup_state: ReceiptsCatchupState,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<(), ReceiptCollectionError> {
    let output_dir = raw_receipts_dir(&chain.name);
    tokio::fs::create_dir_all(&output_dir).await?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let receipt_fields = &raw_data_config.fields.receipt_fields;
    let schema = build_receipt_schema(receipt_fields);
    let block_receipt_concurrency = raw_data_config.block_receipt_concurrency.unwrap_or(10);

    // Use the existing files from catchup state (already scanned)
    let existing_files = catchup_state.existing_files;
    let s3_manifest = catchup_state.s3_manifest;

    // Batch states for per-block receipt fetching
    let mut batch_states: HashMap<u64, ReceiptBatchState> = HashMap::new();

    // Build output channels and metrics structs
    let channels = ReceiptOutputChannels {
        log_tx,
        factory_log_tx,
        event_trigger_tx,
    };

    let mut metrics = ChannelMetricsState {
        log_tx_metrics: ChannelMetrics::default(),
        factory_log_tx_metrics: ChannelMetrics::default(),
        log_tx_capacity: channels
            .log_tx
            .as_ref()
            .map(|s| s.max_capacity())
            .unwrap_or(0),
        factory_log_tx_capacity: channels
            .factory_log_tx
            .as_ref()
            .map(|s| s.max_capacity())
            .unwrap_or(0),
        total_channel_send_time: std::time::Duration::ZERO,
    };

    tracing::info!(
        "Starting receipt collection (current mode) for chain {} (log_tx: {}, factory_log_tx: {}, log_tx_capacity: {}, factory_log_tx_capacity: {})",
        chain.name,
        channels.log_tx.is_some(),
        channels.factory_log_tx.is_some(),
        metrics.log_tx_capacity,
        metrics.factory_log_tx_capacity
    );

    // =========================================================================
    // Current phase: Eager dispatch - spawn receipt fetches per-block as blocks
    // arrive, select on completions so the loop never blocks on RPC work.
    // =========================================================================
    let mut block_rx_closed = false;
    let mut recollect_closed = recollect_rx.is_none();

    // JoinSet for in-flight per-block receipt fetches
    let mut receipt_join_set: JoinSet<BlockReceiptResult> = JoinSet::new();
    // JoinSet for background parquet writes
    let mut write_join_set: JoinSet<WriteCompleteResult> = JoinSet::new();

    let block_receipts_method = chain.block_receipts_method.clone();

    loop {
        tokio::select! {
            // -----------------------------------------------------------------
            // Branch 1: A new block arrives from upstream
            // -----------------------------------------------------------------
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
                                blocks_dispatched: HashSet::new(),
                                blocks_completed: HashSet::new(),
                                minimal_records: Vec::new(),
                                full_records: Vec::new(),
                                logs: Vec::new(),
                            }
                        });

                        // Check if we should skip this range (file already exists locally or in S3)
                        let range = BlockRange { start: range_start, end: range_end };
                        let exists_in_s3 = s3_manifest
                            .as_ref()
                            .is_some_and(|m| m.raw_receipts.has(range.start, range.end - 1));
                        if existing_files.contains(&range.file_name("receipts")) || exists_in_s3 {
                            state.blocks_received.insert(block_number, BlockInfo {
                                block_number,
                                timestamp,
                                tx_hashes,
                            });
                            state.blocks_dispatched.insert(block_number);
                            state.blocks_completed.insert(block_number);

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
                                send_range_complete(&channels.factory_log_tx, &channels.log_tx, &channels.event_trigger_tx, range.start, range.end).await?;
                            }
                            continue;
                        }

                        let has_txs = !tx_hashes.is_empty();

                        // Clone tx_hashes for the spawned task before moving into state
                        let tx_hashes_for_fetch = if has_txs {
                            Some(tx_hashes.clone())
                        } else {
                            None
                        };

                        // Add block to received set
                        state.blocks_received.insert(block_number, BlockInfo {
                            block_number,
                            timestamp,
                            tx_hashes,
                        });

                        // Mark as dispatched
                        state.blocks_dispatched.insert(block_number);

                        let is_empty_block = if let Some(fetch_tx_hashes) = tx_hashes_for_fetch {
                            // Spawn receipt fetch for this block into the JoinSet
                            let client = client.clone();
                            let receipt_fields = receipt_fields.clone();
                            let method = block_receipts_method.clone();
                            receipt_join_set.spawn(async move {
                                let block_info = BlockInfo {
                                    block_number,
                                    timestamp,
                                    tx_hashes: fetch_tx_hashes,
                                };
                                let block_refs: Vec<&BlockInfo> = vec![&block_info];
                                let result = fetch_receipts_for_blocks(
                                    &block_refs,
                                    &*client,
                                    &receipt_fields,
                                    method.as_ref().map(|m| m.as_str()),
                                    1, // single-block fetch
                                )
                                .await;
                                BlockReceiptResult {
                                    range_start,
                                    block_number,
                                    result,
                                }
                            });
                            false
                        } else {
                            // No transactions: mark completed immediately
                            state.blocks_completed.insert(block_number);
                            true
                        };

                        // Drop `state` borrow before calling try_finalize_range
                        if is_empty_block {
                            try_finalize_range(
                                range_start,
                                &mut batch_states,
                                &mut write_join_set,
                                receipt_fields,
                                &schema,
                                &output_dir,
                                &channels.event_trigger_tx,
                            ).await?;
                        }
                    }
                    None => {
                        block_rx_closed = true;
                    }
                }
            }

            // -----------------------------------------------------------------
            // Branch 2: A receipt fetch completed
            // -----------------------------------------------------------------
            Some(join_result) = receipt_join_set.join_next() => {
                let block_result = join_result
                    .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))?;

                let range_start = block_result.range_start;
                let block_number = block_result.block_number;

                let fetch_result = block_result.result?;

                if let Some(state) = batch_states.get_mut(&range_start) {
                    // Store results
                    state.minimal_records.extend(fetch_result.minimal_records);
                    state.full_records.extend(fetch_result.full_records);

                    // Send logs immediately
                    if !fetch_result.logs.is_empty() {
                        let triggers = if !event_matchers.is_empty() {
                            extract_event_triggers(&fetch_result.logs, &event_matchers)
                        } else {
                            Vec::new()
                        };

                        send_logs_to_channels(fetch_result.logs, &channels, &mut metrics).await?;

                        if !triggers.is_empty() {
                            if let Some(tx) = &channels.event_trigger_tx {
                                tx.send(EventTriggerMessage::Triggers(triggers))
                                    .await
                                    .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
                            }
                        }
                    }

                    // Mark block as completed
                    state.blocks_completed.insert(block_number);
                }

                // Check if range is now complete and finalize
                try_finalize_range(
                    range_start,
                    &mut batch_states,
                    &mut write_join_set,
                    receipt_fields,
                    &schema,
                    &output_dir,
                    &channels.event_trigger_tx,
                ).await?;
            }

            // -----------------------------------------------------------------
            // Branch 3: A parquet write completed
            // -----------------------------------------------------------------
            Some(join_result) = write_join_set.join_next() => {
                let write_result = join_result
                    .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))?;

                write_result.result?;

                // Upload to S3 if configured
                if let Some(ref sm) = storage_manager {
                    upload_parquet_to_s3(
                        sm,
                        &write_result.output_path,
                        &chain.name,
                        "raw/receipts",
                        write_result.range_start,
                        write_result.range_end - 1,
                    )
                    .await
                    .map_err(|e| ReceiptCollectionError::Io(std::io::Error::other(e.to_string())))?;
                }

                // Signal log/factory RangeComplete now that the parquet file exists.
                // EventTriggerMessage::RangeComplete was already sent in try_finalize_range
                // to keep trigger batches aligned per-range.
                let log_message = LogMessage::RangeComplete {
                    range_start: write_result.range_start,
                    range_end: write_result.range_end,
                };
                if let Some(sender) = &channels.factory_log_tx {
                    sender.send(log_message.clone()).await
                        .map_err(|_| ReceiptCollectionError::ChannelSend(
                            format!("factory_log_tx (RangeComplete {}-{}) - receiver dropped",
                                write_result.range_start, write_result.range_end)))?;
                }
                if let Some(sender) = &channels.log_tx {
                    sender.send(log_message).await
                        .map_err(|_| ReceiptCollectionError::ChannelSend(
                            format!("log_tx (RangeComplete {}-{}) - receiver dropped",
                                write_result.range_start, write_result.range_end)))?;
                }
            }

            // -----------------------------------------------------------------
            // Branch 4: A recollect request
            // -----------------------------------------------------------------
            recollect_result = async {
                match &mut recollect_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            }, if !recollect_closed => {
                if let Some(request) = recollect_result {
                    tracing::info!(
                        "Recollecting range {}-{} (corrupted file was deleted)",
                        request.range_start,
                        request.range_end - 1
                    );

                    // Find the corresponding block file and read block info
                    let block_file_path = PathBuf::from(format!(
                        "data/{}/historical/raw/blocks/blocks_{}-{}.parquet",
                        chain.name,
                        request.range_start,
                        request.range_end - 1
                    ));

                    let block_infos = match read_block_info_from_parquet_async(block_file_path).await {
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

                    let pr = process_range(
                        &range,
                        blocks,
                        &*client,
                        receipt_fields,
                        &schema,
                        &output_dir,
                        &channels,
                        &event_matchers,
                        &mut metrics,
                        chain.block_receipts_method.as_ref().map(|m| m.as_str()),
                        block_receipt_concurrency,
                    )
                    .await?;

                    // Recollect is a one-off - write immediately.
                    execute_receipt_write(pr.pending_write).await?;

                    // Upload to S3 if configured
                    if let Some(ref sm) = storage_manager {
                        upload_parquet_to_s3(
                            sm,
                            &pr.output_path,
                            &chain.name,
                            "raw/receipts",
                            range.start,
                            range.end - 1,
                        )
                        .await
                        .map_err(|e| ReceiptCollectionError::Io(std::io::Error::other(e.to_string())))?;
                    }

                    send_range_complete(&channels.factory_log_tx, &channels.log_tx, &channels.event_trigger_tx, range.start, range.end).await?;

                    tracing::info!(
                        "Recollection complete for range {}-{}",
                        request.range_start,
                        request.range_end - 1
                    );
                } else {
                    // recollect_rx closed
                    recollect_rx = None;
                    recollect_closed = true;
                }
            }
        }

        // Exit when all work is done
        if block_rx_closed
            && receipt_join_set.is_empty()
            && write_join_set.is_empty()
            && (recollect_rx.is_none() || recollect_closed)
        {
            break;
        }
    }

    // Process any remaining incomplete ranges at shutdown
    for (range_start, mut state) in batch_states.drain() {
        if state.blocks_received.is_empty() {
            continue;
        }

        let max_block = state
            .blocks_received
            .keys()
            .max()
            .copied()
            .unwrap_or(range_start);
        let range = BlockRange {
            start: range_start,
            end: max_block + 1,
        };

        let exists_in_s3 = s3_manifest
            .as_ref()
            .is_some_and(|m| m.raw_receipts.has(range.start, range.end - 1));
        if existing_files.contains(&range.file_name("receipts")) || exists_in_s3 {
            tracing::info!(
                "Skipping receipts for blocks {}-{} (already exists)",
                range.start,
                range.end - 1
            );
            send_range_complete(
                &channels.factory_log_tx,
                &channels.log_tx,
                &channels.event_trigger_tx,
                range.start,
                range.end,
            )
            .await?;
            continue;
        }

        // Process any remaining unfetched blocks
        let remaining_unfetched: Vec<u64> = state
            .blocks_received
            .keys()
            .filter(|bn| !state.blocks_completed.contains(bn))
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
                &*client,
                receipt_fields,
                chain.block_receipts_method.as_ref().map(|m| m.as_str()),
                block_receipt_concurrency,
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

                send_logs_to_channels(result.logs, &channels, &mut metrics).await?;

                if !triggers.is_empty() {
                    if let Some(tx) = &channels.event_trigger_tx {
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

        let output_path = output_dir.join(range.file_name("receipts"));
        let total_receipts = match receipt_fields {
            Some(fields) => {
                let count = state.minimal_records.len();
                write_minimal_receipts_to_parquet_async(
                    state.minimal_records,
                    schema.clone(),
                    fields.to_vec(),
                    output_path.clone(),
                )
                .await?;
                count
            }
            None => {
                let count = state.full_records.len();
                write_full_receipts_to_parquet_async(
                    state.full_records,
                    schema.clone(),
                    output_path.clone(),
                )
                .await?;
                count
            }
        };

        tracing::info!(
            "Receipts {}-{}: {} receipts written (shutdown cleanup)",
            range.start,
            range.end - 1,
            total_receipts
        );

        // Upload to S3 if configured
        if let Some(ref sm) = storage_manager {
            upload_parquet_to_s3(
                sm,
                &output_path,
                &chain.name,
                "raw/receipts",
                range.start,
                range.end - 1,
            )
            .await
            .map_err(|e| ReceiptCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }

        // Signal range complete after parquet write + S3 upload.
        send_range_complete(
            &channels.factory_log_tx,
            &channels.log_tx,
            &channels.event_trigger_tx,
            range.start,
            range.end,
        )
        .await?;
    }

    if let Some(sender) = &channels.factory_log_tx {
        if sender.send(LogMessage::AllRangesComplete).await.is_err() {
            tracing::error!(
                "Failed to send AllRangesComplete to factory_log_tx - receiver dropped"
            );
            return Err(ReceiptCollectionError::ChannelSend(
                "factory_log_tx (AllRangesComplete) - receiver dropped".to_string(),
            ));
        }
    }
    if let Some(sender) = &channels.log_tx {
        if sender.send(LogMessage::AllRangesComplete).await.is_err() {
            tracing::error!("Failed to send AllRangesComplete to log_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "log_tx (AllRangesComplete) - receiver dropped".to_string(),
            ));
        }
    }
    if let Some(sender) = &channels.event_trigger_tx {
        if sender.send(EventTriggerMessage::AllComplete).await.is_err() {
            tracing::error!("Failed to send AllComplete to event_trigger_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "event_trigger_tx (AllComplete) - receiver dropped".to_string(),
            ));
        }
    }

    // Log channel backpressure summaries
    if channels.log_tx.is_some() {
        metrics.log_tx_metrics.log_summary("log_tx");
    }
    if channels.factory_log_tx.is_some() {
        metrics.factory_log_tx_metrics.log_summary("factory_log_tx");
    }

    tracing::info!("Receipt collection complete for chain {}", chain.name);
    Ok(())
}
