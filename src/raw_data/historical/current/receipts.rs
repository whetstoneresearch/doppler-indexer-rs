use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::Schema;

use alloy::primitives::B256;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;

use crate::metrics::set_receipt_pipeline_state;
use crate::raw_data::historical::blocks::read_block_info_from_parquet_async;
use crate::raw_data::historical::catchup::receipts::ReceiptsCatchupState;
use crate::raw_data::historical::factories::RecollectRequest;
use crate::raw_data::historical::receipts::{
    build_receipt_schema, execute_receipt_write, extract_event_triggers,
    fetch_block_receipts_bounded, fetch_receipts_for_blocks, fetch_tx_receipts_batched,
    process_range, send_logs_to_channels, send_range_complete,
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

/// Result of a receipt fetch dispatched into the JoinSet.
enum ReceiptFetchResult {
    /// Single-block fetch (block-receipt mode)
    SingleBlock {
        range_start: u64,
        block_number: u64,
        result: Result<BatchFetchResult, ReceiptCollectionError>,
    },
    /// Multi-block batch fetch (fallback micro-batch mode).
    /// May span multiple ranges when blocks from consecutive ranges
    /// accumulate in the pending buffer before a flush.
    FallbackBatch {
        /// (range_start, block_number) for each block in the batch
        blocks: Vec<(u64, u64)>,
        result: Result<BatchFetchResult, ReceiptCollectionError>,
    },
}

/// Remove a single fallback micro-batch from the front of the pending buffer.
///
/// Batches are sized by cumulative tx count, but always contain whole blocks.
/// When `allow_partial` is false, the function only drains when the pending
/// buffer has at least `target_tx_count` tx hashes available.
fn take_pending_fallback_batch(
    pending: &mut VecDeque<(u64, BlockInfo)>,
    pending_tx_count: &mut usize,
    target_tx_count: usize,
    allow_partial: bool,
) -> Vec<(u64, BlockInfo)> {
    if pending.is_empty() || (!allow_partial && *pending_tx_count < target_tx_count) {
        return Vec::new();
    }

    let mut blocks_to_fetch = Vec::new();
    let mut batch_tx_count = 0usize;

    while let Some((range_start, block_info)) = pending.pop_front() {
        batch_tx_count += block_info.tx_hashes.len();
        blocks_to_fetch.push((range_start, block_info));

        if batch_tx_count >= target_tx_count {
            break;
        }
    }

    *pending_tx_count = pending_tx_count.saturating_sub(batch_tx_count);
    blocks_to_fetch
}

/// Drain one pending fallback micro-batch and spawn its fetch task into the JoinSet.
fn spawn_fallback_flush(
    pending: &mut VecDeque<(u64, BlockInfo)>,
    pending_tx_count: &mut usize,
    flush_deadline: &mut Option<tokio::time::Instant>,
    join_set: &mut JoinSet<ReceiptFetchResult>,
    client: &Arc<UnifiedRpcClient>,
    receipt_fields: &Option<Vec<ReceiptField>>,
    target_tx_count: usize,
    allow_partial: bool,
) -> bool {
    let blocks_to_fetch =
        take_pending_fallback_batch(pending, pending_tx_count, target_tx_count, allow_partial);
    if blocks_to_fetch.is_empty() {
        return false;
    }

    // Build per-block (range_start, block_number) pairs for multi-range support
    let blocks_with_ranges: Vec<(u64, u64)> = blocks_to_fetch
        .iter()
        .map(|(rs, bi)| (*rs, bi.block_number))
        .collect();
    if pending.is_empty() {
        *flush_deadline = None;
    }

    let client_clone = client.clone();
    let rf = receipt_fields.clone();
    join_set.spawn(async move {
        let block_infos: Vec<BlockInfo> = blocks_to_fetch.into_iter().map(|(_, bi)| bi).collect();
        let refs: Vec<&BlockInfo> = block_infos.iter().collect();
        let result = fetch_tx_receipts_batched(&refs, &client_clone, &rf).await;
        ReceiptFetchResult::FallbackBatch {
            blocks: blocks_with_ranges,
            result,
        }
    });
    true
}

/// Spawn as many fallback micro-batches as possible without exceeding the
/// configured in-flight limit.
fn spawn_ready_fallback_flushes(
    pending: &mut VecDeque<(u64, BlockInfo)>,
    pending_tx_count: &mut usize,
    flush_deadline: &mut Option<tokio::time::Instant>,
    join_set: &mut JoinSet<ReceiptFetchResult>,
    client: &Arc<UnifiedRpcClient>,
    receipt_fields: &Option<Vec<ReceiptField>>,
    target_tx_count: usize,
    max_in_flight: usize,
    allow_partial: bool,
) -> usize {
    let mut spawned = 0usize;

    while join_set.len() < max_in_flight {
        let did_spawn = spawn_fallback_flush(
            pending,
            pending_tx_count,
            flush_deadline,
            join_set,
            client,
            receipt_fields,
            target_tx_count,
            allow_partial,
        );
        if !did_spawn {
            break;
        }
        spawned += 1;
    }

    spawned
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
/// Sends accumulated `EventTriggerMessage::Triggers` followed by
/// `EventTriggerMessage::RangeComplete` atomically so triggers cannot leak
/// across range boundaries. `LogMessage::RangeComplete` is deferred to
/// Branch 3 (after parquet write) so the transformation engine can read
/// the receipt file.
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

    // Send all accumulated event triggers atomically before RangeComplete.
    // Triggers are buffered per-range in ReceiptBatchState so that out-of-order
    // JoinSet completions cannot leak triggers from one range into another.
    if !final_state.event_triggers.is_empty() {
        if let Some(tx) = event_trigger_tx {
            tx.send(EventTriggerMessage::Triggers(final_state.event_triggers))
                .await
                .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
        }
    }

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
    let wr_path = output_path.to_path_buf();
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
    let receipt_fetch_concurrency = raw_data_config
        .block_receipt_concurrency
        .unwrap_or(10)
        .max(1);

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

    // JoinSet for in-flight receipt fetches (per-block or micro-batch)
    let mut receipt_join_set: JoinSet<ReceiptFetchResult> = JoinSet::new();
    // JoinSet for background parquet writes
    let mut write_join_set: JoinSet<WriteCompleteResult> = JoinSet::new();

    let block_receipts_method = chain.block_receipts_method.clone();
    let is_block_receipt_mode = block_receipts_method.is_some();
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;
    let metrics_chain = chain.name.clone();

    // Fallback micro-batching state (only used when block_receipts_method is None).
    // Blocks accumulate until the tx count reaches rpc_batch_size, a range
    // boundary is crossed, or a flush timeout fires.
    let mut pending_fallback_blocks: VecDeque<(u64, BlockInfo)> = VecDeque::new();
    let mut pending_tx_count: usize = 0;
    let mut flush_deadline: Option<tokio::time::Instant> = None;
    const FALLBACK_FLUSH_TIMEOUT: Duration = Duration::from_millis(150);
    set_receipt_pipeline_state(&metrics_chain, 0, 0);

    loop {
        tokio::select! {
            // -----------------------------------------------------------------
            // Branch 1: A new block arrives from upstream
            // -----------------------------------------------------------------
            // Block-receipt mode: backpressure via JoinSet concurrency cap.
            // Fallback mode: backpressure via pending tx/block buffer caps.
            // Both caps scale with fetch concurrency so sparse chains can queue
            // enough work to keep multiple micro-batches in flight.
            block_result = block_rx.recv(), if !block_rx_closed && if is_block_receipt_mode {
                receipt_join_set.len() < receipt_fetch_concurrency
            } else {
                pending_tx_count < rpc_batch_size * receipt_fetch_concurrency * 2
                    && pending_fallback_blocks.len() < rpc_batch_size * receipt_fetch_concurrency * 2
            } => {
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
                                event_triggers: Vec::new(),
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
                            set_receipt_pipeline_state(
                                &metrics_chain,
                                receipt_join_set.len(),
                                pending_tx_count,
                            );
                            continue;
                        }

                        let has_txs = !tx_hashes.is_empty();
                        let tx_count = tx_hashes.len();

                        // Clone tx_hashes for the dispatch target (task or pending buffer)
                        let tx_hashes_for_dispatch = if has_txs {
                            Some(tx_hashes.clone())
                        } else {
                            None
                        };

                        // Add block to received set (move original into state)
                        state.blocks_received.insert(block_number, BlockInfo {
                            block_number,
                            timestamp,
                            tx_hashes,
                        });
                        state.blocks_dispatched.insert(block_number);

                        if !has_txs {
                            // Empty block: mark completed immediately
                            state.blocks_completed.insert(block_number);
                        }
                        // NLL: `state` borrow ends here

                        if !has_txs {
                            try_finalize_range(
                                range_start,
                                &mut batch_states,
                                &mut write_join_set,
                                receipt_fields,
                                &schema,
                                &output_dir,
                                &channels.event_trigger_tx,
                            ).await?;
                        } else if is_block_receipt_mode {
                            // Block-receipt mode: spawn per-block fetch
                            let fetch_tx_hashes = tx_hashes_for_dispatch.unwrap();
                            let client = client.clone();
                            let receipt_fields = receipt_fields.clone();
                            let method_str = block_receipts_method.as_ref().unwrap().as_str().to_string();
                            receipt_join_set.spawn(async move {
                                let block_info = BlockInfo {
                                    block_number,
                                    timestamp,
                                    tx_hashes: fetch_tx_hashes,
                                };
                                let block_refs: Vec<&BlockInfo> = vec![&block_info];
                                let result = fetch_block_receipts_bounded(
                                    &block_refs,
                                    &client,
                                    &receipt_fields,
                                    &method_str,
                                    1, // single-block fetch
                                ).await;
                                ReceiptFetchResult::SingleBlock {
                                    range_start,
                                    block_number,
                                    result,
                                }
                            });
                            set_receipt_pipeline_state(
                                &metrics_chain,
                                receipt_join_set.len(),
                                pending_tx_count,
                            );
                        } else {
                            // Fallback mode: accumulate for micro-batching.
                            // The pending buffer may hold blocks from multiple
                            // consecutive ranges. No range-boundary flush —
                            // try_finalize_range checks blocks_completed so
                            // old-range blocks sitting in the buffer won't
                            // trigger premature finalization.

                            let fetch_tx_hashes = tx_hashes_for_dispatch.unwrap();
                            pending_fallback_blocks.push_back((range_start, BlockInfo {
                                block_number,
                                timestamp,
                                tx_hashes: fetch_tx_hashes,
                            }));
                            pending_tx_count += tx_count;

                            if flush_deadline.is_none() {
                                flush_deadline = Some(tokio::time::Instant::now() + FALLBACK_FLUSH_TIMEOUT);
                            }

                            // Flush full micro-batches eagerly, keeping up to
                            // `receipt_fetch_concurrency` fallback batches in flight.
                            if pending_tx_count >= rpc_batch_size {
                                spawn_ready_fallback_flushes(
                                    &mut pending_fallback_blocks,
                                    &mut pending_tx_count,
                                    &mut flush_deadline,
                                    &mut receipt_join_set,
                                    &client,
                                    receipt_fields,
                                    rpc_batch_size,
                                    receipt_fetch_concurrency,
                                    false,
                                );
                            }
                            set_receipt_pipeline_state(
                                &metrics_chain,
                                receipt_join_set.len(),
                                pending_tx_count,
                            );
                        }
                    }
                    None => {
                        block_rx_closed = true;
                        // Flush remaining pending fallback blocks if no
                        // in-flight slot is available. Branch 2 continues
                        // draining any remaining work as batches complete.
                        if !pending_fallback_blocks.is_empty() {
                            spawn_ready_fallback_flushes(
                                &mut pending_fallback_blocks,
                                &mut pending_tx_count,
                                &mut flush_deadline,
                                &mut receipt_join_set,
                                &client,
                                receipt_fields,
                                rpc_batch_size,
                                receipt_fetch_concurrency,
                                true,
                            );
                        }
                        set_receipt_pipeline_state(
                            &metrics_chain,
                            receipt_join_set.len(),
                            pending_tx_count,
                        );
                    }
                }
            }

            // -----------------------------------------------------------------
            // Branch 2: A receipt fetch completed (single-block or batch)
            // -----------------------------------------------------------------
            Some(join_result) = receipt_join_set.join_next() => {
                let fetch_result_enum = join_result
                    .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))?;

                match fetch_result_enum {
                    ReceiptFetchResult::SingleBlock { range_start, block_number, result } => {
                        let fetch_result = result?;

                        if let Some(state) = batch_states.get_mut(&range_start) {
                            state.minimal_records.extend(fetch_result.minimal_records);
                            state.full_records.extend(fetch_result.full_records);

                            if !fetch_result.logs.is_empty() {
                                if !event_matchers.is_empty() {
                                    let triggers = extract_event_triggers(&fetch_result.logs, &event_matchers);
                                    if !triggers.is_empty() {
                                        state.event_triggers.extend(triggers);
                                    }
                                }
                                send_logs_to_channels(fetch_result.logs, &channels, &mut metrics).await?;
                            }

                            state.blocks_completed.insert(block_number);
                        }

                        try_finalize_range(
                            range_start, &mut batch_states, &mut write_join_set,
                            receipt_fields, &schema, &output_dir, &channels.event_trigger_tx,
                        ).await?;
                    }

                    ReceiptFetchResult::FallbackBatch { blocks, result } => {
                        let fetch_result = result?;

                        // Group blocks by range
                        let mut blocks_by_range: HashMap<u64, Vec<u64>> = HashMap::new();
                        for &(range_start, block_number) in &blocks {
                            blocks_by_range.entry(range_start).or_default().push(block_number);
                        }

                        // Build block_number → range_start lookup for partitioning
                        let block_to_range: HashMap<u64, u64> = blocks.into_iter().collect();

                        // Partition records and logs by range
                        let mut minimal_by_range: HashMap<u64, Vec<_>> = HashMap::new();
                        for rec in fetch_result.minimal_records {
                            if let Some(&rs) = block_to_range.get(&rec.block_number) {
                                minimal_by_range.entry(rs).or_default().push(rec);
                            }
                        }
                        let mut full_by_range: HashMap<u64, Vec<_>> = HashMap::new();
                        for rec in fetch_result.full_records {
                            if let Some(&rs) = block_to_range.get(&rec.block_number) {
                                full_by_range.entry(rs).or_default().push(rec);
                            }
                        }
                        let mut logs_by_range: HashMap<u64, Vec<_>> = HashMap::new();
                        for log in fetch_result.logs {
                            if let Some(&rs) = block_to_range.get(&log.block_number) {
                                logs_by_range.entry(rs).or_default().push(log);
                            }
                        }

                        // Process each range independently
                        for (range_start, block_numbers) in blocks_by_range {
                            if let Some(state) = batch_states.get_mut(&range_start) {
                                if let Some(records) = minimal_by_range.remove(&range_start) {
                                    state.minimal_records.extend(records);
                                }
                                if let Some(records) = full_by_range.remove(&range_start) {
                                    state.full_records.extend(records);
                                }

                                let range_logs = logs_by_range.remove(&range_start).unwrap_or_default();
                                if !range_logs.is_empty() {
                                    if !event_matchers.is_empty() {
                                        let triggers = extract_event_triggers(&range_logs, &event_matchers);
                                        if !triggers.is_empty() {
                                            state.event_triggers.extend(triggers);
                                        }
                                    }
                                    send_logs_to_channels(range_logs, &channels, &mut metrics).await?;
                                }

                                for bn in block_numbers {
                                    state.blocks_completed.insert(bn);
                                }
                            }
                            // state borrow released

                            try_finalize_range(
                                range_start, &mut batch_states, &mut write_join_set,
                                receipt_fields, &schema, &output_dir, &channels.event_trigger_tx,
                            ).await?;
                        }
                    }
                }

                // Auto-flush: when fallback batches complete, immediately
                // dispatch additional ready work to keep the pipeline full.
                if !is_block_receipt_mode
                    && !pending_fallback_blocks.is_empty()
                {
                    let allow_partial = block_rx_closed;
                    spawn_ready_fallback_flushes(
                        &mut pending_fallback_blocks,
                        &mut pending_tx_count,
                        &mut flush_deadline,
                        &mut receipt_join_set,
                        &client,
                        receipt_fields,
                        rpc_batch_size,
                        receipt_fetch_concurrency,
                        allow_partial,
                    );
                }
                set_receipt_pipeline_state(
                    &metrics_chain,
                    receipt_join_set.len(),
                    pending_tx_count,
                );
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
                if channels.factory_log_tx.is_none() && channels.log_tx.is_none() {
                    // No log channels are consuming range completion notifications.
                    // Event trigger completion was already sent above.
                    continue;
                }

                let message = LogMessage::RangeComplete {
                    range_start: write_result.range_start,
                    range_end: write_result.range_end,
                };
                if channels.factory_log_tx.is_some() && channels.log_tx.is_some() {
                    if let Some(sender) = &channels.factory_log_tx {
                        sender
                            .send(message.clone())
                            .await
                            .map_err(|_| ReceiptCollectionError::ChannelSend(format!(
                                "factory_log_tx (RangeComplete {}-{}) - receiver dropped",
                                write_result.range_start, write_result.range_end
                            )))?;
                    }
                    if let Some(sender) = &channels.log_tx {
                        sender
                            .send(message)
                            .await
                            .map_err(|_| ReceiptCollectionError::ChannelSend(format!(
                                "log_tx (RangeComplete {}-{}) - receiver dropped",
                                write_result.range_start, write_result.range_end
                            )))?;
                    }
                } else if let Some(sender) = &channels.factory_log_tx {
                    sender
                        .send(message)
                        .await
                        .map_err(|_| ReceiptCollectionError::ChannelSend(format!(
                            "factory_log_tx (RangeComplete {}-{}) - receiver dropped",
                            write_result.range_start, write_result.range_end
                        )))?;
                } else if let Some(sender) = &channels.log_tx {
                    sender
                        .send(message)
                        .await
                        .map_err(|_| ReceiptCollectionError::ChannelSend(format!(
                            "log_tx (RangeComplete {}-{}) - receiver dropped",
                            write_result.range_start, write_result.range_end
                        )))?;
                }
            }

            // -----------------------------------------------------------------
            // Branch 5: Fallback flush timeout
            // -----------------------------------------------------------------
            _ = async {
                match flush_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            }, if !is_block_receipt_mode
                && !pending_fallback_blocks.is_empty()
                && receipt_join_set.len() < receipt_fetch_concurrency => {
                spawn_ready_fallback_flushes(
                    &mut pending_fallback_blocks,
                    &mut pending_tx_count,
                    &mut flush_deadline,
                    &mut receipt_join_set,
                    &client,
                    receipt_fields,
                    rpc_batch_size,
                    receipt_fetch_concurrency,
                    true,
                );
                set_receipt_pipeline_state(
                    &metrics_chain,
                    receipt_join_set.len(),
                    pending_tx_count,
                );
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
                        &client,
                        receipt_fields,
                        &schema,
                        &output_dir,
                        &channels,
                        &event_matchers,
                        &mut metrics,
                        chain.block_receipts_method.as_ref().map(|m| m.as_str()),
                        receipt_fetch_concurrency,
                    )
                    .await?;

                    // Recollect is a one-off - write immediately.
                    let recollect_output_path = pr.pending_write.output_path().to_path_buf();
                    execute_receipt_write(pr.pending_write).await?;

                    // Upload to S3 if configured
                    if let Some(ref sm) = storage_manager {
                        upload_parquet_to_s3(
                            sm,
                            &recollect_output_path,
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
            && pending_fallback_blocks.is_empty()
            && (recollect_rx.is_none() || recollect_closed)
        {
            break;
        }
    }

    // Flush any remaining pending fallback blocks before shutdown cleanup.
    // May span multiple ranges.
    if !pending_fallback_blocks.is_empty() {
        let blocks_to_fetch: Vec<(u64, BlockInfo)> = pending_fallback_blocks.drain(..).collect();
        pending_tx_count = 0;
        set_receipt_pipeline_state(&metrics_chain, receipt_join_set.len(), pending_tx_count);

        // Group by range for result attribution
        let mut blocks_by_range: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut block_to_range: HashMap<u64, u64> = HashMap::new();
        for (rs, bi) in &blocks_to_fetch {
            blocks_by_range
                .entry(*rs)
                .or_default()
                .push(bi.block_number);
            block_to_range.insert(bi.block_number, *rs);
        }

        let block_infos: Vec<BlockInfo> = blocks_to_fetch.into_iter().map(|(_, bi)| bi).collect();
        let refs: Vec<&BlockInfo> = block_infos.iter().collect();
        let result = fetch_tx_receipts_batched(&refs, &client, receipt_fields).await?;

        // Partition records by range
        let mut minimal_by_range: HashMap<u64, Vec<_>> = HashMap::new();
        for rec in result.minimal_records {
            if let Some(&rs) = block_to_range.get(&rec.block_number) {
                minimal_by_range.entry(rs).or_default().push(rec);
            }
        }
        let mut full_by_range: HashMap<u64, Vec<_>> = HashMap::new();
        for rec in result.full_records {
            if let Some(&rs) = block_to_range.get(&rec.block_number) {
                full_by_range.entry(rs).or_default().push(rec);
            }
        }
        let mut logs_by_range: HashMap<u64, Vec<_>> = HashMap::new();
        for log in result.logs {
            if let Some(&rs) = block_to_range.get(&log.block_number) {
                logs_by_range.entry(rs).or_default().push(log);
            }
        }

        for (range_start, block_numbers) in blocks_by_range {
            if let Some(state) = batch_states.get_mut(&range_start) {
                if let Some(records) = minimal_by_range.remove(&range_start) {
                    state.minimal_records.extend(records);
                }
                if let Some(records) = full_by_range.remove(&range_start) {
                    state.full_records.extend(records);
                }
                let range_logs = logs_by_range.remove(&range_start).unwrap_or_default();
                if !range_logs.is_empty() {
                    if !event_matchers.is_empty() {
                        let triggers = extract_event_triggers(&range_logs, &event_matchers);
                        if !triggers.is_empty() {
                            state.event_triggers.extend(triggers);
                        }
                    }
                    send_logs_to_channels(range_logs, &channels, &mut metrics).await?;
                }
                for bn in block_numbers {
                    state.blocks_completed.insert(bn);
                }
            }
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
                &client,
                receipt_fields,
                chain.block_receipts_method.as_ref().map(|m| m.as_str()),
                receipt_fetch_concurrency,
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
    set_receipt_pipeline_state(&metrics_chain, 0, 0);

    tracing::info!("Receipt collection complete for chain {}", chain.name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;

    fn make_block_info(block_number: u64, tx_count: usize) -> BlockInfo {
        BlockInfo {
            block_number,
            timestamp: 1000 + block_number,
            tx_hashes: (0..tx_count)
                .map(|i| {
                    let mut bytes = [0u8; 32];
                    bytes[0..8].copy_from_slice(&block_number.to_be_bytes());
                    bytes[8..16].copy_from_slice(&(i as u64).to_be_bytes());
                    B256::from(bytes)
                })
                .collect(),
        }
    }

    #[test]
    fn take_pending_fallback_batch_waits_for_full_batch_unless_forced() {
        let mut pending = VecDeque::from([(0u64, make_block_info(0, 3))]);
        let mut pending_tx_count = 3usize;

        let batch = take_pending_fallback_batch(&mut pending, &mut pending_tx_count, 5, false);
        assert!(batch.is_empty(), "should not drain a partial batch eagerly");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending_tx_count, 3);

        let batch = take_pending_fallback_batch(&mut pending, &mut pending_tx_count, 5, true);
        assert_eq!(
            batch.len(),
            1,
            "forced flush should drain the available block"
        );
        assert!(pending.is_empty());
        assert_eq!(pending_tx_count, 0);
    }

    #[test]
    fn take_pending_fallback_batch_respects_tx_target_and_order() {
        let mut pending = VecDeque::from([
            (0u64, make_block_info(10, 3)),
            (0u64, make_block_info(11, 4)),
            (1000u64, make_block_info(1000, 5)),
        ]);
        let mut pending_tx_count = 12usize;

        let batch = take_pending_fallback_batch(&mut pending, &mut pending_tx_count, 5, false);
        let block_numbers: Vec<u64> = batch.iter().map(|(_, block)| block.block_number).collect();
        let ranges: Vec<u64> = batch.iter().map(|(range_start, _)| *range_start).collect();

        assert_eq!(block_numbers, vec![10, 11]);
        assert_eq!(ranges, vec![0, 0]);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending_tx_count, 5);
        assert_eq!(pending.front().unwrap().1.block_number, 1000);
    }

    #[tokio::test]
    async fn spawn_ready_fallback_flushes_fills_multiple_in_flight_slots() {
        let client =
            Arc::new(UnifiedRpcClient::from_url("http://localhost:1").expect("URL parses"));
        let receipt_fields: Option<Vec<ReceiptField>> = None;

        let mut pending = VecDeque::from([
            (0u64, make_block_info(0, 3)),
            (0u64, make_block_info(1, 3)),
            (1000u64, make_block_info(1000, 3)),
            (1000u64, make_block_info(1001, 3)),
        ]);
        let mut pending_tx_count = 12usize;
        let mut deadline = Some(tokio::time::Instant::now());
        let mut join_set: JoinSet<ReceiptFetchResult> = JoinSet::new();

        let spawned = spawn_ready_fallback_flushes(
            &mut pending,
            &mut pending_tx_count,
            &mut deadline,
            &mut join_set,
            &client,
            &receipt_fields,
            5,
            2,
            false,
        );

        assert_eq!(spawned, 2, "should fill both available in-flight slots");
        assert_eq!(join_set.len(), 2);
        assert!(pending.is_empty());
        assert_eq!(pending_tx_count, 0);
        assert!(
            deadline.is_none(),
            "deadline should clear when buffer drains"
        );

        join_set.abort_all();
    }
}
