use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::raw_data::historical::blocks::{
    get_existing_block_ranges_async, read_block_info_from_parquet_async,
};
use crate::raw_data::historical::receipts::{
    build_receipt_schema, execute_receipt_write, process_range, scan_existing_logs_files_async,
    scan_existing_parquet_files_async, send_range_complete, BlockInfo, ChannelMetrics,
    ChannelMetricsState, EventTriggerMatcher, EventTriggerMessage, LogMessage,
    ReceiptCollectionError, ReceiptOutputChannels,
};
use crate::rpc::UnifiedRpcClient;
use crate::storage::paths::{raw_logs_dir, raw_receipts_dir};
use crate::storage::{upload_parquet_to_s3, BlockRange};
use crate::storage::{DataLoader, S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

/// Drain a pending background write, upload to S3, and send RangeComplete.
///
/// Both `pending_write_handle` and `pending_write_info` are `.take()`'d so the
/// caller cannot accidentally re-process the same write.
async fn drain_pending_write(
    pending_write_handle: &mut Option<tokio::task::JoinHandle<Result<(), ReceiptCollectionError>>>,
    pending_write_info: &mut Option<(PathBuf, u64, u64)>,
    storage_manager: &Option<Arc<StorageManager>>,
    chain_name: &str,
    channels: &ReceiptOutputChannels,
) -> Result<(), ReceiptCollectionError> {
    if let Some(handle) = pending_write_handle.take() {
        handle
            .await
            .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))??;

        if let Some((ref path, wr_start, wr_end)) = pending_write_info.take() {
            if let Some(ref sm) = storage_manager {
                upload_parquet_to_s3(sm, path, chain_name, "raw/receipts", wr_start, wr_end - 1)
                    .await
                    .map_err(|e| {
                        ReceiptCollectionError::Io(std::io::Error::other(e.to_string()))
                    })?;
            }

            send_range_complete(
                &channels.factory_log_tx,
                &channels.log_tx,
                &channels.event_trigger_tx,
                wr_start,
                wr_end,
            )
            .await?;
        }
    }
    Ok(())
}

/// State passed from catchup phase to current phase for receipt collection.
pub struct ReceiptsCatchupState {
    /// Set of existing receipt files (local filesystem)
    pub existing_files: HashSet<String>,
    /// Set of existing log files (local filesystem)
    #[allow(dead_code)]
    pub existing_logs_files: HashSet<String>,
    /// Optional S3 manifest for checking remote availability
    pub s3_manifest: Option<S3Manifest>,
}

#[allow(clippy::too_many_arguments)]
pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    log_tx: &Option<Sender<LogMessage>>,
    factory_log_tx: &Option<Sender<LogMessage>>,
    event_trigger_tx: &Option<Sender<EventTriggerMessage>>,
    event_matchers: &[EventTriggerMatcher],
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<ReceiptsCatchupState, ReceiptCollectionError> {
    let output_dir = raw_receipts_dir(&chain.name);
    tokio::fs::create_dir_all(&output_dir).await?;

    let receipt_fields = &raw_data_config.fields.receipt_fields;
    let schema = build_receipt_schema(receipt_fields);
    let block_receipt_concurrency = raw_data_config.block_receipt_concurrency.unwrap_or(10);

    let existing_files = scan_existing_parquet_files_async(output_dir.clone()).await?;

    let has_factories = factory_log_tx.is_some();

    let channels = ReceiptOutputChannels {
        log_tx: log_tx.clone(),
        factory_log_tx: factory_log_tx.clone(),
        event_trigger_tx: event_trigger_tx.clone(),
    };

    let mut metrics = ChannelMetricsState {
        log_tx_metrics: ChannelMetrics::default(),
        factory_log_tx_metrics: ChannelMetrics::default(),
        log_tx_capacity: log_tx.as_ref().map(|s| s.max_capacity()).unwrap_or(0),
        factory_log_tx_capacity: factory_log_tx
            .as_ref()
            .map(|s| s.max_capacity())
            .unwrap_or(0),
        total_channel_send_time: std::time::Duration::ZERO,
    };

    // =========================================================================
    // Catchup phase: Process any ranges where blocks exist but receipts don't
    // Also check for missing logs files - if receipts exist but logs don't, we
    // need to re-process to regenerate the logs data
    // =========================================================================
    let block_ranges =
        get_existing_block_ranges_async(chain.name.clone(), s3_manifest.as_ref().cloned()).await;
    let mut catchup_count = 0;

    // Check existing logs files
    let logs_dir = raw_logs_dir(&chain.name);
    let existing_logs_files = scan_existing_logs_files_async(logs_dir).await?;

    // Note: Factory catchup is handled by the factories module reading from logs files directly.
    // Receipts catchup only needs to check for receipts and logs.

    // Pipeline: overlap parquet writes with the next range's RPC fetch.
    let mut pending_write_handle: Option<
        tokio::task::JoinHandle<Result<(), ReceiptCollectionError>>,
    > = None;
    // (output_path, range_start, range_end_exclusive)
    let mut pending_write_info: Option<(PathBuf, u64, u64)> = None;

    for block_range in &block_ranges {
        let range = BlockRange {
            start: block_range.start,
            end: block_range.end,
        };

        let receipts_exist = existing_files.contains(&range.file_name("receipts"))
            || s3_manifest
                .as_ref()
                .is_some_and(|m| m.raw_receipts.has(range.start, range.end - 1));
        let logs_file_name = format!("logs_{}-{}.parquet", range.start, range.end - 1);
        let logs_exist = existing_logs_files.contains(&logs_file_name)
            || s3_manifest
                .as_ref()
                .is_some_and(|m| m.raw_logs.has(range.start, range.end - 1));

        // Skip if receipts and logs both exist (or logs aren't needed)
        // Factory catchup is handled separately by factories.rs reading from logs files
        if receipts_exist && (logs_exist || log_tx.is_none()) {
            // Drain any pending write from the previous range before sending
            // RangeComplete for this skipped range.  Otherwise the event-trigger
            // collector sees RangeComplete(B) before the write for A finishes,
            // flushing A's buffered triggers under B's range.
            drain_pending_write(
                &mut pending_write_handle,
                &mut pending_write_info,
                &storage_manager,
                &chain.name,
                &channels,
            )
            .await?;

            // Still need to signal range complete for downstream collectors
            // when catching up, so they know this range is done
            if has_factories || channels.event_trigger_tx.is_some() {
                send_range_complete(
                    &channels.factory_log_tx,
                    &channels.log_tx,
                    &channels.event_trigger_tx,
                    range.start,
                    range.end,
                )
                .await?;
            }
            continue;
        }

        // Drain the previous range's write before starting a new one.
        drain_pending_write(
            &mut pending_write_handle,
            &mut pending_write_info,
            &storage_manager,
            &chain.name,
            &channels,
        )
        .await?;

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

        // Read block info from the existing parquet file
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
            "Catchup: processing receipts for blocks {}-{} from existing block file",
            range.start,
            range.end - 1
        );

        let blocks: Vec<BlockInfo> = block_infos
            .into_iter()
            .map(|info| BlockInfo {
                block_number: info.block_number,
                timestamp: info.timestamp,
                tx_hashes: info.tx_hashes,
            })
            .collect();

        let result = process_range(
            &range,
            blocks,
            client,
            receipt_fields,
            &schema,
            &output_dir,
            &channels,
            event_matchers,
            &mut metrics,
            chain.block_receipts_method.as_ref().map(|m| m.as_str()),
            block_receipt_concurrency,
        )
        .await?;

        catchup_count += 1;

        // Spawn background write; RangeComplete deferred until drain confirms the write.
        let output_path = result.pending_write.output_path().to_path_buf();
        pending_write_info = Some((output_path, range.start, range.end));
        pending_write_handle =
            Some(tokio::spawn(execute_receipt_write(result.pending_write)));
    }

    // Drain the final pending write.
    drain_pending_write(
        &mut pending_write_handle,
        &mut pending_write_info,
        &storage_manager,
        &chain.name,
        &channels,
    )
    .await?;

    if catchup_count > 0 {
        tracing::info!(
            "Catchup complete: processed {} receipt ranges for chain {}",
            catchup_count,
            chain.name
        );
    }

    // Log channel backpressure summaries for catchup phase
    if channels.log_tx.is_some() {
        metrics.log_tx_metrics.log_summary("log_tx (catchup)");
    }
    if channels.factory_log_tx.is_some() {
        metrics
            .factory_log_tx_metrics
            .log_summary("factory_log_tx (catchup)");
    }

    Ok(ReceiptsCatchupState {
        existing_files,
        existing_logs_files,
        s3_manifest,
    })
}
