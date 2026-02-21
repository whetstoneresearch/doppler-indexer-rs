use std::path::PathBuf;

use tokio::sync::mpsc::Sender;

use crate::raw_data::historical::blocks::{get_existing_block_ranges, read_block_info_from_parquet};
use crate::raw_data::historical::receipts::{
    build_receipt_schema, process_range, scan_existing_logs_files, scan_existing_parquet_files,
    send_range_complete, BlockInfo, BlockRange, ChannelMetrics, EventTriggerMatcher,
    EventTriggerMessage, LogMessage, ReceiptCollectionError,
};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    log_tx: &Option<Sender<LogMessage>>,
    factory_log_tx: &Option<Sender<LogMessage>>,
    event_trigger_tx: &Option<Sender<EventTriggerMessage>>,
    event_matchers: &[EventTriggerMatcher],
) -> Result<(), ReceiptCollectionError> {
    let output_dir = PathBuf::from(format!("data/raw/{}/receipts", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;
    let block_receipt_concurrency = raw_data_config.block_receipt_concurrency.unwrap_or(10);
    let receipt_fields = &raw_data_config.fields.receipt_fields;
    let schema = build_receipt_schema(receipt_fields);

    let existing_files = scan_existing_parquet_files(&output_dir);

    let has_factories = factory_log_tx.is_some();

    let log_tx_capacity = log_tx.as_ref().map(|s| s.max_capacity()).unwrap_or(0);
    let factory_log_tx_capacity = factory_log_tx.as_ref().map(|s| s.max_capacity()).unwrap_or(0);

    let mut log_tx_metrics = ChannelMetrics::default();
    let mut factory_log_tx_metrics = ChannelMetrics::default();

    // =========================================================================
    // Catchup phase: Process any ranges where blocks exist but receipts don't
    // Also check for missing logs files - if receipts exist but logs don't, we
    // need to re-process to regenerate the logs data
    // =========================================================================
    let block_ranges = get_existing_block_ranges(&chain.name);
    let mut catchup_count = 0;

    // Check existing logs files
    let logs_dir = PathBuf::from(format!("data/raw/{}/logs", chain.name));
    let existing_logs_files = scan_existing_logs_files(&logs_dir);

    // Note: Factory catchup is handled by the factories module reading from logs files directly.
    // Receipts catchup only needs to check for receipts and logs.

    for block_range in &block_ranges {
        let range = BlockRange {
            start: block_range.start,
            end: block_range.end,
        };

        let receipts_exist = existing_files.contains(&range.file_name());
        let logs_file_name = format!("logs_{}-{}.parquet", range.start, range.end - 1);
        let logs_exist = existing_logs_files.contains(&logs_file_name);

        // Skip if receipts and logs both exist (or logs aren't needed)
        // Factory catchup is handled separately by factories.rs reading from logs files
        if receipts_exist && (logs_exist || log_tx.is_none()) {
            // Still need to signal range complete for downstream collectors
            // when catching up, so they know this range is done
            if has_factories || event_trigger_tx.is_some() {
                send_range_complete(factory_log_tx, log_tx, event_trigger_tx, range.start, range.end).await?;
            }
            continue;
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
            log_tx,
            factory_log_tx,
            event_trigger_tx,
            event_matchers,
            rpc_batch_size,
            &mut log_tx_metrics,
            &mut factory_log_tx_metrics,
            log_tx_capacity,
            factory_log_tx_capacity,
            chain.block_receipts_method.as_deref(),
            block_receipt_concurrency,
        )
        .await?;

        send_range_complete(factory_log_tx, log_tx, event_trigger_tx, range.start, range.end).await?;
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
    if log_tx.is_some() {
        log_tx_metrics.log_summary("log_tx (catchup)");
    }
    if factory_log_tx.is_some() {
        factory_log_tx_metrics.log_summary("factory_log_tx (catchup)");
    }

    Ok(())
}
