use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::raw_data::historical::blocks::{
    get_existing_block_ranges, read_block_info_from_parquet,
};
use crate::raw_data::historical::receipts::{
    build_receipt_schema, process_range, scan_existing_logs_files, scan_existing_parquet_files,
    send_range_complete, BlockInfo, ChannelMetrics, ChannelMetricsState, EventTriggerMatcher,
    EventTriggerMessage, LogMessage, ReceiptCollectionError, ReceiptOutputChannels,
};
use crate::rpc::UnifiedRpcClient;
use crate::storage::paths::{raw_logs_dir, raw_receipts_dir};
use crate::storage::BlockRange;
use crate::storage::{DataLoader, S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

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
    std::fs::create_dir_all(&output_dir)?;

    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;
    let block_receipt_concurrency = raw_data_config.block_receipt_concurrency.unwrap_or(10);
    let receipt_fields = &raw_data_config.fields.receipt_fields;
    let schema = build_receipt_schema(receipt_fields);

    let existing_files = scan_existing_parquet_files(&output_dir);

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
    let block_ranges = get_existing_block_ranges(&chain.name, s3_manifest.as_ref());
    let mut catchup_count = 0;

    // Check existing logs files
    let logs_dir = raw_logs_dir(&chain.name);
    let existing_logs_files = scan_existing_logs_files(&logs_dir);

    // Note: Factory catchup is handled by the factories module reading from logs files directly.
    // Receipts catchup only needs to check for receipts and logs.

    for block_range in &block_ranges {
        let range = BlockRange {
            start: block_range.start,
            end: block_range.end,
        };

        let receipts_exist = existing_files.contains(&range.file_name("receipts"))
            || s3_manifest
                .as_ref()
                .is_some_and(|m| m.has_raw_receipts(range.start, range.end - 1));
        let logs_file_name = format!("logs_{}-{}.parquet", range.start, range.end - 1);
        let logs_exist = existing_logs_files.contains(&logs_file_name)
            || s3_manifest
                .as_ref()
                .is_some_and(|m| m.has_raw_logs(range.start, range.end - 1));

        // Skip if receipts and logs both exist (or logs aren't needed)
        // Factory catchup is handled separately by factories.rs reading from logs files
        if receipts_exist && (logs_exist || log_tx.is_none()) {
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

        process_range(
            &range,
            blocks,
            client,
            receipt_fields,
            &schema,
            &output_dir,
            &channels,
            event_matchers,
            rpc_batch_size,
            &mut metrics,
            chain.block_receipts_method.as_deref(),
            block_receipt_concurrency,
            storage_manager.as_ref(),
            &chain.name,
        )
        .await?;

        send_range_complete(
            &channels.factory_log_tx,
            &channels.log_tx,
            &channels.event_trigger_tx,
            range.start,
            range.end,
        )
        .await?;
        catchup_count += 1;
    }

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
