//! Compaction service for merging live blocks into parquet ranges.
//!
//! Periodically checks for complete block ranges and compacts them:
//! 1. Finds ranges where all blocks are fully processed
//! 2. Converts bincode data to Arrow arrays
//! 3. Writes parquet files to raw data directory
//! 4. Updates progress tables
//! 5. Cleans up live storage

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use thiserror::Error;
use tokio::sync::Mutex;

use super::bincode_io::StorageError;
use super::progress::LiveProgressTracker;
use super::storage::LiveStorage;
use super::types::{LiveModeConfig, LivePipelineExpectations};
use crate::db::DbPool;
use crate::storage::StorageManager;

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Database error: {0}")]
    Database(String),
    #[error("S3 storage error: {0}")]
    S3Storage(#[from] crate::storage::StorageError),
}

/// A range of blocks that can be compacted.
#[derive(Debug, Clone)]
pub struct CompactableRange {
    pub start: u64,
    pub end: u64,
    pub block_count: usize,
}

/// Request to retry transformation for a block.
#[derive(Debug, Clone)]
pub struct TransformRetryRequest {
    pub block_number: u64,
    pub missing_handlers: Option<HashSet<String>>,
}

/// Service for compacting live blocks into parquet ranges.
pub struct CompactionService {
    chain_name: String,
    chain_id: u64,
    storage: LiveStorage,
    config: LiveModeConfig,
    db_pool: Option<Arc<DbPool>>,
    progress_tracker: Arc<Mutex<LiveProgressTracker>>,
    expectations: LivePipelineExpectations,
    /// Optional S3 storage manager for uploading compacted parquet files
    storage_manager: Option<Arc<StorageManager>>,
    /// Channel to request transformation retries
    retry_tx: Option<tokio::sync::mpsc::Sender<TransformRetryRequest>>,
    /// When each transform-ready block first became eligible for retry.
    stuck_blocks: Mutex<HashMap<u64, Instant>>,
    /// Minimum time a block must remain transform-ready and incomplete before retrying.
    retry_grace_period: Duration,
}

impl CompactionService {
    /// Create a new CompactionService.
    pub fn new(
        chain_name: String,
        chain_id: u64,
        config: LiveModeConfig,
        db_pool: Option<Arc<DbPool>>,
        progress_tracker: Arc<Mutex<LiveProgressTracker>>,
        expectations: LivePipelineExpectations,
        storage_manager: Option<Arc<StorageManager>>,
    ) -> Self {
        let storage = LiveStorage::new(&chain_name);

        Self {
            chain_name,
            chain_id,
            storage,
            config: config.clone(),
            db_pool,
            progress_tracker,
            expectations,
            storage_manager,
            retry_tx: None,
            stuck_blocks: Mutex::new(HashMap::new()),
            retry_grace_period: Duration::from_secs(config.transform_retry_grace_period_secs),
        }
    }

    /// Set the retry channel for transformation retries.
    pub fn with_retry_tx(
        mut self,
        retry_tx: tokio::sync::mpsc::Sender<TransformRetryRequest>,
    ) -> Self {
        self.retry_tx = Some(retry_tx);
        self
    }

    /// Run the compaction service loop.
    ///
    /// Runs until cancelled (when the containing task is dropped/aborted).
    pub async fn run(self) {
        let interval = Duration::from_secs(self.config.compaction_interval_secs);

        tracing::info!(
            "Compaction service started for chain {} with interval {:?}",
            self.chain_name,
            interval
        );

        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = self.run_compaction_cycle().await {
                tracing::error!("Compaction cycle error: {}", e);
            }
        }
    }

    /// Run a single compaction cycle.
    pub async fn run_compaction_cycle(&self) -> Result<(), CompactionError> {
        // Check for stuck blocks needing transformation retry
        self.check_for_stuck_blocks().await;

        let ranges = self.find_compactable_ranges().await?;

        if ranges.is_empty() {
            tracing::debug!("No ranges ready for compaction");
            return Ok(());
        }

        for range in ranges {
            tracing::info!(
                "Compacting range {}-{} ({} blocks)",
                range.start,
                range.end,
                range.block_count
            );

            if let Err(e) = self.compact_range(&range).await {
                tracing::error!(
                    "Failed to compact range {}-{}: {}",
                    range.start,
                    range.end,
                    e
                );
            }
        }

        Ok(())
    }

    /// Check for blocks that are stuck (decoded but not transformed) and request retries.
    async fn check_for_stuck_blocks(&self) {
        let Some(ref retry_tx) = self.retry_tx else {
            return;
        };

        if !self.expectations.expect_transformations {
            return;
        }

        // Phase 1 (no locks): gather block list and read status files from storage
        let blocks = match self.storage.list_blocks() {
            Ok(b) => b,
            Err(_) => return,
        };

        let now = Instant::now();
        let seen_blocks: HashSet<u64> = blocks.iter().copied().collect();

        // Collect blocks that are transform-ready but not yet transformed,
        // along with their failed handlers from the status file.
        let mut candidates: Vec<(u64, HashSet<String>)> = Vec::new();
        for block_number in blocks {
            let status = match self.storage.read_status(block_number) {
                Ok(s) => s,
                Err(_) => continue,
            };

            if status.transformed || !status.transform_inputs_ready_with(&self.expectations) {
                continue;
            }

            candidates.push((
                block_number,
                status.failed_handlers.iter().cloned().collect(),
            ));
        }

        // Phase 2a (progress_tracker lock only): collect complete blocks and pending handlers
        let mut blocks_with_missing: Vec<(u64, HashSet<String>)> = Vec::new();
        {
            let progress = self.progress_tracker.lock().await;
            for (block_number, mut failed_handlers) in candidates {
                if progress.is_block_complete(block_number) {
                    continue;
                }

                // Combine handlers pending in the progress tracker with
                // handlers that explicitly failed (stored in status file)
                let mut missing_handlers = progress.get_pending_handlers(block_number);
                missing_handlers.extend(failed_handlers.drain());

                if !missing_handlers.is_empty() {
                    blocks_with_missing.push((block_number, missing_handlers));
                }
            }
        } // progress_tracker lock dropped

        // Phase 2b (stuck_blocks lock only): update stuck state and build retry list
        let mut pending_retries = Vec::new();
        {
            let mut stuck_blocks = self.stuck_blocks.lock().await;

            for (block_number, missing_handlers) in blocks_with_missing {
                let first_seen = stuck_blocks.entry(block_number).or_insert(now);
                if now.duration_since(*first_seen) >= self.retry_grace_period {
                    pending_retries.push((block_number, missing_handlers));
                }
            }

            stuck_blocks.retain(|block_number, _| seen_blocks.contains(block_number));

            // Only remove from stuck_blocks on successful send. If try_send fails
            // (channel full), the block keeps its original grace timer so it doesn't
            // restart the grace period on the next cycle.
            for (block_number, missing_handlers) in pending_retries {
                match retry_tx.try_send(TransformRetryRequest {
                    block_number,
                    missing_handlers: Some(missing_handlers),
                }) {
                    Ok(()) => {
                        stuck_blocks.remove(&block_number);
                        tracing::info!(
                            "Requested transformation retry for stuck block {}",
                            block_number
                        );
                    }
                    Err(e) => {
                        tracing::debug!("Retry channel full for block {}: {}", block_number, e);
                    }
                }
            }
        } // stuck_blocks lock dropped
    }

    /// Find ranges that are ready for compaction.
    pub async fn find_compactable_ranges(&self) -> Result<Vec<CompactableRange>, CompactionError> {
        let blocks = self.storage.list_blocks()?;
        if blocks.is_empty() {
            return Ok(Vec::new());
        }

        // Group blocks into ranges
        let range_size = self.config.range_size;
        let mut ranges: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

        for block_number in blocks {
            let range_start = (block_number / range_size) * range_size;
            ranges.entry(range_start).or_default().push(block_number);
        }

        // Check which ranges are complete
        let mut compactable = Vec::new();
        let progress = self.progress_tracker.lock().await;

        for (range_start, mut block_numbers) in ranges {
            let range_end = range_start + range_size - 1;

            // Check if range is complete (all blocks present and sequential)
            if block_numbers.len() != range_size as usize {
                continue;
            }

            // Sort and verify blocks are sequential with no gaps
            block_numbers.sort_unstable();
            if block_numbers.first() != Some(&range_start)
                || block_numbers.last() != Some(&range_end)
            {
                tracing::warn!(
                    "Range {}-{} has correct count but wrong boundaries: first={:?}, last={:?}",
                    range_start,
                    range_end,
                    block_numbers.first(),
                    block_numbers.last()
                );
                continue;
            }

            // Verify no gaps in the sequence
            let has_gap = block_numbers.windows(2).any(|w| w[1] != w[0] + 1);
            if has_gap {
                tracing::warn!(
                    "Range {}-{} has gaps in block sequence",
                    range_start,
                    range_end
                );
                continue;
            }

            // Check if all blocks are fully processed
            let mut all_complete = true;
            for &block_number in &block_numbers {
                // Check status file
                if let Ok(status) = self.storage.read_status(block_number) {
                    if !status.is_complete_with(&self.expectations) {
                        all_complete = false;
                        break;
                    }
                } else {
                    all_complete = false;
                    break;
                }

                // Check progress tracker
                if !progress.is_block_complete(block_number) {
                    all_complete = false;
                    break;
                }
            }

            if all_complete {
                // Only compact ranges safely beyond reorg depth
                if let Ok(Some(latest_block)) = self.storage.max_block_number() {
                    let safe_boundary = latest_block.saturating_sub(self.config.reorg_depth);
                    if range_end >= safe_boundary {
                        tracing::debug!(
                            "Skipping range {}-{}: within reorg depth (latest={}, safe={})",
                            range_start,
                            range_end,
                            latest_block,
                            safe_boundary
                        );
                        continue;
                    }
                }

                compactable.push(CompactableRange {
                    start: range_start,
                    end: range_end,
                    block_count: block_numbers.len(),
                });
            }
        }

        Ok(compactable)
    }

    /// Compact a single range.
    async fn compact_range(&self, range: &CompactableRange) -> Result<(), CompactionError> {
        // Read all blocks in range
        let mut blocks = Vec::new();
        for block_number in range.start..=range.end {
            match self.storage.read_block(block_number) {
                Ok(block) => blocks.push(block),
                Err(StorageError::NotFound(_)) => {
                    // Block was deleted during compaction (likely due to reorg), skip this range
                    tracing::warn!(
                        "Block {} deleted during compaction (reorg?), skipping range {}-{}",
                        block_number,
                        range.start,
                        range.end
                    );
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Write blocks parquet
        let blocks_path = self.blocks_parquet_path(range.start, range.end);
        self.write_blocks_parquet(&blocks, &blocks_path)?;
        self.upload_to_s3(&blocks_path, "raw/blocks", range.start, range.end)
            .await?;

        // Read and write logs parquet
        let mut all_logs = Vec::new();
        for block_number in range.start..=range.end {
            if let Ok(logs) = self.storage.read_logs(block_number) {
                let block = &blocks[(block_number - range.start) as usize];
                for log in logs {
                    all_logs.push((block_number, block.timestamp, log));
                }
            }
        }

        if !all_logs.is_empty() {
            let logs_path = self.logs_parquet_path(range.start, range.end);
            self.write_logs_parquet(&all_logs, &logs_path)?;
            self.upload_to_s3(&logs_path, "raw/logs", range.start, range.end)
                .await?;
        }

        // Read and write factory addresses parquet
        // Factory records: (block_number, block_timestamp, address)
        let mut factory_records: HashMap<String, Vec<(u64, u64, [u8; 20])>> = HashMap::new();
        for block_number in range.start..=range.end {
            if let Ok(factories) = self.storage.read_factories(block_number) {
                for (collection_name, addresses) in factories.addresses_by_collection {
                    let records = factory_records.entry(collection_name).or_default();
                    for (timestamp, addr) in addresses {
                        records.push((block_number, timestamp, addr));
                    }
                }
            }
        }

        if !factory_records.is_empty() {
            for (collection_name, records) in &factory_records {
                let factory_path =
                    self.factories_parquet_path(range.start, range.end, collection_name);
                self.write_factories_parquet(records, &factory_path)?;
                self.upload_to_s3(
                    &factory_path,
                    &format!("factories/{}", collection_name),
                    range.start,
                    range.end,
                )
                .await?;
            }
            tracing::info!(
                "Wrote factory addresses for {} collections in range {}-{}",
                factory_records.len(),
                range.start,
                range.end
            );
        }

        // Read and write eth_calls parquet
        let mut all_eth_calls = Vec::new();
        for block_number in range.start..=range.end {
            if let Ok(calls) = self.storage.read_eth_calls(block_number) {
                all_eth_calls.extend(calls);
            }
        }

        if !all_eth_calls.is_empty() {
            let eth_calls_path = self.eth_calls_parquet_path(range.start, range.end);
            self.write_eth_calls_parquet(&all_eth_calls, &eth_calls_path)?;
            self.upload_to_s3(&eth_calls_path, "raw/eth_calls", range.start, range.end)
                .await?;
            tracing::info!(
                "Wrote {} eth_calls to {} in range {}-{}",
                all_eth_calls.len(),
                eth_calls_path.display(),
                range.start,
                range.end
            );
        }

        // Merge per-block contract indexes into historical format
        {
            use crate::storage::contract_index::{
                range_key, read_contract_index as read_historical_ci,
                update_contract_index as update_ci, write_contract_index as write_ci,
                ExpectedContracts,
            };

            let mut merged: HashMap<String, ExpectedContracts> = HashMap::new();
            for block_number in range.start..=range.end {
                if let Ok(block_ci) = self.storage.read_contract_index(block_number) {
                    for (collection_name, contracts) in block_ci {
                        let entry = merged.entry(collection_name).or_default();
                        for (contract_name, addresses) in contracts {
                            let addr_entry = entry.entry(contract_name).or_default();
                            for addr in addresses {
                                if !addr_entry.contains(&addr) {
                                    addr_entry.push(addr);
                                }
                            }
                        }
                    }
                }
            }

            for contracts in merged.values_mut() {
                for addresses in contracts.values_mut() {
                    addresses.sort();
                }
            }

            if !merged.is_empty() {
                let rk = range_key(range.start, range.end);
                let base_dir =
                    PathBuf::from(format!("data/{}/historical/raw/eth_calls", self.chain_name));

                for (collection_name, expected) in &merged {
                    let collection_dir = base_dir.join(collection_name);
                    if !collection_dir.exists() {
                        continue;
                    }

                    if let Ok(entries) = std::fs::read_dir(&collection_dir) {
                        for entry in entries.flatten() {
                            if entry.path().is_dir() {
                                let on_events_dir = entry.path().join("on_events");
                                if on_events_dir.exists() {
                                    let mut ci = read_historical_ci(&on_events_dir);
                                    update_ci(&mut ci, &rk, expected);
                                    if let Err(e) = write_ci(&on_events_dir, &ci) {
                                        tracing::warn!(
                                            "Failed to write compacted contract index for {}: {}",
                                            on_events_dir.display(),
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Compact decoded logs
        // Note: Full decoded data compaction requires schema information from the event
        // parser. For now, we just delete the decoded bincode files when compacting.
        // The historical decoder will re-decode from the raw parquet files as needed.
        // TODO: Implement full decoded data compaction in Phase 7.
        let decoded_log_types = self.collect_decoded_log_types(range.start, range.end)?;
        if !decoded_log_types.is_empty() {
            tracing::debug!(
                "Found {} decoded log types in range {}-{}, will be re-decoded from raw parquet",
                decoded_log_types.len(),
                range.start,
                range.end
            );
        }

        // Migrate progress from _live_progress to _handler_progress
        if let Some(ref db_pool) = self.db_pool {
            self.migrate_progress(range.start, range.end, db_pool)
                .await?;
        }

        // Clean up live storage (including decoded data)
        for block_number in range.start..=range.end {
            self.storage.delete_all(block_number)?;
        }

        // Clean up progress tracker
        {
            let mut progress = self.progress_tracker.lock().await;
            for block_number in range.start..=range.end {
                progress.clear_block(block_number);
            }
        }

        tracing::info!(
            "Successfully compacted range {}-{} ({} blocks, {} logs, {} eth_calls)",
            range.start,
            range.end,
            blocks.len(),
            all_logs.len(),
            all_eth_calls.len()
        );

        Ok(())
    }

    /// Get path for blocks parquet file.
    fn blocks_parquet_path(&self, start: u64, end: u64) -> PathBuf {
        PathBuf::from(format!(
            "data/raw/{}/blocks/{}_{}.parquet",
            self.chain_name, start, end
        ))
    }

    /// Get path for logs parquet file.
    fn logs_parquet_path(&self, start: u64, end: u64) -> PathBuf {
        PathBuf::from(format!(
            "data/raw/{}/logs/{}_{}.parquet",
            self.chain_name, start, end
        ))
    }

    /// Get path for factory parquet file.
    fn factories_parquet_path(&self, start: u64, end: u64, collection_name: &str) -> PathBuf {
        PathBuf::from(format!(
            "data/derived/{}/factories/{}/{}-{}.parquet",
            self.chain_name,
            collection_name,
            start,
            end - 1 // Use inclusive end for file naming
        ))
    }

    /// Write factory addresses to parquet file.
    fn write_factories_parquet(
        &self,
        records: &[(u64, u64, [u8; 20])],
        path: &Path,
    ) -> Result<(), CompactionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("factory_address", DataType::FixedSizeBinary(20), false),
        ]));

        let mut block_numbers = UInt64Builder::new();
        let mut timestamps = UInt64Builder::new();
        let mut addresses = FixedSizeBinaryBuilder::new(20);

        for (block_number, timestamp, addr) in records {
            block_numbers.append_value(*block_number);
            timestamps.append_value(*timestamp);
            addresses.append_value(addr)?;
        }

        write_compaction_parquet(
            path,
            schema,
            vec![
                Arc::new(block_numbers.finish()) as ArrayRef,
                Arc::new(timestamps.finish()) as ArrayRef,
                Arc::new(addresses.finish()) as ArrayRef,
            ],
        )
    }

    /// Get path for eth_calls parquet file.
    fn eth_calls_parquet_path(&self, start: u64, end: u64) -> PathBuf {
        PathBuf::from(format!(
            "data/raw/{}/eth_calls/{}_{}.parquet",
            self.chain_name, start, end
        ))
    }

    /// Write eth_calls to parquet file.
    fn write_eth_calls_parquet(
        &self,
        calls: &[super::types::LiveEthCall],
        path: &Path,
    ) -> Result<(), CompactionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("contract_name", DataType::Utf8, false),
            Field::new("contract_address", DataType::FixedSizeBinary(20), false),
            Field::new("function_name", DataType::Utf8, false),
            Field::new("result", DataType::Binary, false),
        ]));

        let mut block_numbers = UInt64Builder::new();
        let mut timestamps = UInt64Builder::new();
        let mut contract_names = StringBuilder::new();
        let mut addresses = FixedSizeBinaryBuilder::new(20);
        let mut function_names = StringBuilder::new();
        let mut results = BinaryBuilder::new();

        for call in calls {
            block_numbers.append_value(call.block_number);
            timestamps.append_value(call.block_timestamp);
            contract_names.append_value(&call.contract_name);
            addresses.append_value(call.contract_address)?;
            function_names.append_value(&call.function_name);
            results.append_value(&call.result);
        }

        write_compaction_parquet(
            path,
            schema,
            vec![
                Arc::new(block_numbers.finish()) as ArrayRef,
                Arc::new(timestamps.finish()) as ArrayRef,
                Arc::new(contract_names.finish()) as ArrayRef,
                Arc::new(addresses.finish()) as ArrayRef,
                Arc::new(function_names.finish()) as ArrayRef,
                Arc::new(results.finish()) as ArrayRef,
            ],
        )?;

        tracing::debug!("Wrote {} eth_calls to {}", calls.len(), path.display());

        Ok(())
    }

    /// Write blocks to parquet file.
    fn write_blocks_parquet(
        &self,
        blocks: &[super::types::LiveBlock],
        path: &Path,
    ) -> Result<(), CompactionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_hash", DataType::Binary, false),
            Field::new("parent_hash", DataType::Binary, false),
            Field::new("timestamp", DataType::UInt64, false),
        ]));

        let mut block_numbers = UInt64Builder::new();
        let mut block_hashes = BinaryBuilder::new();
        let mut parent_hashes = BinaryBuilder::new();
        let mut timestamps = UInt64Builder::new();

        for block in blocks {
            block_numbers.append_value(block.number);
            block_hashes.append_value(block.hash);
            parent_hashes.append_value(block.parent_hash);
            timestamps.append_value(block.timestamp);
        }

        write_compaction_parquet(
            path,
            schema,
            vec![
                Arc::new(block_numbers.finish()) as ArrayRef,
                Arc::new(block_hashes.finish()) as ArrayRef,
                Arc::new(parent_hashes.finish()) as ArrayRef,
                Arc::new(timestamps.finish()) as ArrayRef,
            ],
        )
    }

    /// Write logs to parquet file.
    fn write_logs_parquet(
        &self,
        logs: &[(u64, u64, super::types::LiveLog)],
        path: &Path,
    ) -> Result<(), CompactionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("transaction_hash", DataType::FixedSizeBinary(32), false),
            Field::new("address", DataType::Binary, false),
            Field::new("topic0", DataType::Binary, true),
            Field::new("topic1", DataType::Binary, true),
            Field::new("topic2", DataType::Binary, true),
            Field::new("topic3", DataType::Binary, true),
            Field::new("data", DataType::Binary, false),
        ]));

        let mut block_numbers = UInt64Builder::new();
        let mut timestamps = UInt64Builder::new();
        let mut log_indices = UInt32Builder::new();
        let mut tx_indices = UInt32Builder::new();
        let mut tx_hashes = FixedSizeBinaryBuilder::new(32);
        let mut addresses = BinaryBuilder::new();
        let mut topic0s = BinaryBuilder::new();
        let mut topic1s = BinaryBuilder::new();
        let mut topic2s = BinaryBuilder::new();
        let mut topic3s = BinaryBuilder::new();
        let mut datas = BinaryBuilder::new();

        for (block_number, timestamp, log) in logs {
            block_numbers.append_value(*block_number);
            timestamps.append_value(*timestamp);
            log_indices.append_value(log.log_index);
            tx_indices.append_value(log.transaction_index);
            tx_hashes.append_value(log.transaction_hash)?;
            addresses.append_value(log.address);

            // Handle topics (up to 4)
            if !log.topics.is_empty() {
                topic0s.append_value(log.topics[0]);
            } else {
                topic0s.append_null();
            }
            if log.topics.len() > 1 {
                topic1s.append_value(log.topics[1]);
            } else {
                topic1s.append_null();
            }
            if log.topics.len() > 2 {
                topic2s.append_value(log.topics[2]);
            } else {
                topic2s.append_null();
            }
            if log.topics.len() > 3 {
                topic3s.append_value(log.topics[3]);
            } else {
                topic3s.append_null();
            }

            datas.append_value(&log.data);
        }

        write_compaction_parquet(
            path,
            schema,
            vec![
                Arc::new(block_numbers.finish()) as ArrayRef,
                Arc::new(timestamps.finish()) as ArrayRef,
                Arc::new(log_indices.finish()) as ArrayRef,
                Arc::new(tx_indices.finish()) as ArrayRef,
                Arc::new(tx_hashes.finish()) as ArrayRef,
                Arc::new(addresses.finish()) as ArrayRef,
                Arc::new(topic0s.finish()) as ArrayRef,
                Arc::new(topic1s.finish()) as ArrayRef,
                Arc::new(topic2s.finish()) as ArrayRef,
                Arc::new(topic3s.finish()) as ArrayRef,
                Arc::new(datas.finish()) as ArrayRef,
            ],
        )
    }

    /// Migrate progress entries from _live_progress to _handler_progress.
    async fn migrate_progress(
        &self,
        range_start: u64,
        range_end: u64,
        db_pool: &DbPool,
    ) -> Result<(), CompactionError> {
        use tokio_postgres::types::ToSql;

        let chain_id = self.chain_id as i64;
        let start = range_start as i64;
        let end = range_end as i64;

        // Get all live progress entries for this range
        let rows = db_pool
            .query(
                "SELECT DISTINCT handler_key FROM _live_progress
                 WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3",
                &[
                    &chain_id as &(dyn ToSql + Sync),
                    &start as &(dyn ToSql + Sync),
                    &end as &(dyn ToSql + Sync),
                ],
            )
            .await
            .map_err(|e| CompactionError::Database(e.to_string()))?;

        // For each handler, update _handler_progress with the range
        for row in rows {
            let handler_key: String = row.get(0);

            // range_start in _handler_progress means "next range to process starts at"
            // After compacting range 0-999, the next range starts at 1000
            let nextrange_start = end + 1;

            // Insert or update _handler_progress using raw SQL via query
            db_pool
                .query(
                    "INSERT INTO _handler_progress (chain_id, handler_key, range_start)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (chain_id, handler_key) DO UPDATE
                     SET range_start = GREATEST(_handler_progress.range_start, EXCLUDED.range_start)",
                    &[
                        &chain_id as &(dyn ToSql + Sync),
                        &handler_key as &(dyn ToSql + Sync),
                        &nextrange_start as &(dyn ToSql + Sync),
                    ],
                )
                .await
                .map_err(|e| CompactionError::Database(e.to_string()))?;
        }

        // Delete live progress entries for this range
        db_pool
            .query(
                "DELETE FROM _live_progress
                 WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3",
                &[
                    &chain_id as &(dyn ToSql + Sync),
                    &start as &(dyn ToSql + Sync),
                    &end as &(dyn ToSql + Sync),
                ],
            )
            .await
            .map_err(|e| CompactionError::Database(e.to_string()))?;

        Ok(())
    }

    /// Collect all (contract_name, event_name) pairs with decoded logs in a range.
    fn collect_decoded_log_types(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<(String, String)>, CompactionError> {
        let mut all_types = std::collections::HashSet::new();

        for block_number in start..=end {
            if let Ok(types) = self.storage.list_decoded_log_types(block_number) {
                for (contract, event) in types {
                    all_types.insert((contract, event));
                }
            }
        }

        Ok(all_types.into_iter().collect())
    }

    /// Upload a local file to S3 and write a marker.
    ///
    /// If S3 is not configured, this is a no-op.
    async fn upload_to_s3(
        &self,
        local_path: &PathBuf,
        data_type: &str,
        start: u64,
        end: u64,
    ) -> Result<(), CompactionError> {
        let Some(ref storage_manager) = self.storage_manager else {
            return Ok(()); // S3 not configured
        };

        if !storage_manager.is_s3_enabled() {
            return Ok(());
        }

        // Read local file
        let data = tokio::fs::read(local_path).await?;

        // Compute S3 key from local path (strip "data/" prefix)
        let data_dir = PathBuf::from("data");
        let s3_key = match local_path.strip_prefix(&data_dir) {
            Ok(relative) => relative.to_string_lossy().to_string(),
            Err(_) => {
                // Fallback for paths not under data/ - use filename
                local_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| local_path.to_string_lossy().to_string())
            }
        };

        // Upload to S3
        storage_manager.backend().write(&s3_key, &data).await?;

        // Write marker
        if let Some(manifest_manager) = storage_manager.manifest_manager() {
            manifest_manager
                .write_marker(
                    &self.chain_name,
                    data_type,
                    start,
                    end,
                    &s3_key,
                    "compaction",
                )
                .await?;
        }

        tracing::debug!(
            "Uploaded {} to S3 and wrote marker for range {}-{}",
            s3_key,
            start,
            end
        );

        Ok(())
    }
}

/// Write a RecordBatch to a Snappy-compressed parquet file.
///
/// Creates parent directories if needed.
fn write_compaction_parquet(
    path: &std::path::Path,
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
) -> Result<(), CompactionError> {
    let batch = RecordBatch::try_new(schema, columns)?;
    crate::storage::atomic_write_parquet(&batch, path)?;
    Ok(())
}

impl std::fmt::Debug for CompactionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionService")
            .field("chain_name", &self.chain_name)
            .field("range_size", &self.config.range_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::*;
    use crate::live::types::{LiveBlock, LiveBlockStatus};
    use crate::live::LiveProgressTracker;

    /// Helper function to validate a range of blocks.
    /// Returns true if the blocks form a complete sequential range.
    fn validate_range(mut block_numbers: Vec<u64>, range_start: u64, range_size: u64) -> bool {
        let range_end = range_start + range_size - 1;

        // Check if range has correct count
        if block_numbers.len() != range_size as usize {
            return false;
        }

        // Sort and verify blocks are sequential with no gaps
        block_numbers.sort_unstable();
        if block_numbers.first() != Some(&range_start) || block_numbers.last() != Some(&range_end) {
            return false;
        }

        // Verify no gaps in the sequence
        let has_gap = block_numbers.windows(2).any(|w| w[1] != w[0] + 1);
        !has_gap
    }

    #[test]
    fn test_validate_range_complete_sequential() {
        // Complete sequential range 0-9
        let blocks: Vec<u64> = (0..10).collect();
        assert!(validate_range(blocks, 0, 10));
    }

    #[test]
    fn test_validate_range_complete_shuffled() {
        // Complete but shuffled range 0-9
        let blocks = vec![5, 2, 8, 0, 9, 3, 7, 1, 4, 6];
        assert!(validate_range(blocks, 0, 10));
    }

    #[test]
    fn test_validate_range_sparse_blocks() {
        // Sparse blocks [0,2,4,6,8,10,12,14,16,18] - correct count but gaps
        let blocks: Vec<u64> = (0..10).map(|x| x * 2).collect();
        assert!(!validate_range(blocks, 0, 10));
    }

    #[test]
    fn test_validate_range_wrong_start() {
        // Wrong start: 1-10 instead of 0-9
        let blocks: Vec<u64> = (1..11).collect();
        assert!(!validate_range(blocks, 0, 10));
    }

    #[test]
    fn test_validate_range_wrong_end() {
        // Wrong end: 0-8 + 10 (missing 9)
        let mut blocks: Vec<u64> = (0..9).collect();
        blocks.push(10);
        assert!(!validate_range(blocks, 0, 10));
    }

    #[test]
    fn test_validate_range_gap_in_middle() {
        // Gap in middle: 0-4, 6-10 (missing 5)
        let mut blocks: Vec<u64> = (0..5).collect();
        blocks.extend(6..11);
        // Note: this has 10 blocks but wrong range
        assert!(!validate_range(blocks, 0, 10));
    }

    #[test]
    fn test_validate_range_insufficient_count() {
        // Only 5 blocks in range that needs 10
        let blocks: Vec<u64> = (0..5).collect();
        assert!(!validate_range(blocks, 0, 10));
    }

    #[test]
    fn test_validate_range_higher_offset() {
        // Complete range 1000-1009
        let blocks: Vec<u64> = (1000..1010).collect();
        assert!(validate_range(blocks, 1000, 10));
    }

    #[test]
    fn test_progress_migration_value() {
        // After compacting range 0-999, the next range starts at 1000
        let _range_start: i64 = 0;
        let range_end: i64 = 999;
        let nextrange_start = range_end + 1;

        assert_eq!(nextrange_start, 1000);

        // After compacting range 1000-1999, the next range starts at 2000
        let range_end_2: i64 = 1999;
        let nextrange_start_2 = range_end_2 + 1;

        assert_eq!(nextrange_start_2, 2000);
    }

    #[tokio::test]
    async fn test_find_compactable_ranges_honors_expectations() {
        let tmp = TempDir::new().unwrap();
        let storage = LiveStorage::with_base_dir(tmp.path().join("live"));
        storage.ensure_dirs().unwrap();

        for block_number in [0_u64, 1, 10] {
            storage
                .write_block(&LiveBlock {
                    number: block_number,
                    hash: [block_number as u8; 32],
                    parent_hash: [block_number.saturating_sub(1) as u8; 32],
                    timestamp: block_number * 12,
                    tx_hashes: vec![],
                })
                .unwrap();
        }

        for block_number in [0_u64, 1] {
            let mut status = LiveBlockStatus::default();
            status.collected = true;
            status.block_fetched = true;
            status.receipts_collected = true;
            status.logs_collected = true;
            status.factories_extracted = true;
            storage.write_status(block_number, &status).unwrap();
        }

        let progress_tracker = Arc::new(Mutex::new(LiveProgressTracker::new(
            1,
            None,
            "test-chain".to_string(),
        )));

        let config = LiveModeConfig {
            reorg_depth: 2,
            compaction_interval_secs: 1,
            range_size: 2,
            transform_retry_grace_period_secs: 300,
        };

        let disabled_expectations = LivePipelineExpectations::default();
        let service_with_disabled_stages = CompactionService {
            chain_name: "test-chain".to_string(),
            chain_id: 1,
            storage: storage.clone(),
            config: config.clone(),
            db_pool: None,
            progress_tracker: progress_tracker.clone(),
            expectations: disabled_expectations,
            storage_manager: None,
            retry_tx: None,
            stuck_blocks: Mutex::new(HashMap::new()),
            retry_grace_period: Duration::from_secs(config.transform_retry_grace_period_secs),
        };

        let ranges = service_with_disabled_stages
            .find_compactable_ranges()
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1);

        let service_with_required_decode = CompactionService {
            expectations: LivePipelineExpectations {
                expect_log_decode: true,
                expect_transformations: true,
                ..disabled_expectations
            },
            ..service_with_disabled_stages
        };

        let ranges = service_with_required_decode
            .find_compactable_ranges()
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn test_check_for_stuck_blocks_skips_decode_incomplete_blocks() {
        let tmp = TempDir::new().unwrap();
        let storage = LiveStorage::with_base_dir(tmp.path().join("live"));
        storage.ensure_dirs().unwrap();

        storage
            .write_block(&LiveBlock {
                number: 100,
                hash: [1; 32],
                parent_hash: [0; 32],
                timestamp: 1200,
                tx_hashes: vec![],
            })
            .unwrap();

        let mut status = LiveBlockStatus::default();
        status.collected = true;
        status.block_fetched = true;
        status.receipts_collected = true;
        status.logs_collected = true;
        status.factories_extracted = true;
        status.logs_decoded = true;
        status.eth_calls_decoded = false;
        status.transformed = false;
        storage.write_status(100, &status).unwrap();

        let progress_tracker = Arc::new(Mutex::new(LiveProgressTracker::new(
            1,
            None,
            "test-chain".to_string(),
        )));
        progress_tracker.lock().await.register_handler("handler_a");

        let (retry_tx, mut retry_rx) = tokio::sync::mpsc::channel(4);
        let service = CompactionService {
            chain_name: "test-chain".to_string(),
            chain_id: 1,
            storage,
            config: LiveModeConfig::default(),
            db_pool: None,
            progress_tracker,
            expectations: LivePipelineExpectations {
                expect_log_decode: true,
                expect_eth_call_decode: true,
                expect_transformations: true,
                ..Default::default()
            },
            storage_manager: None,
            retry_tx: Some(retry_tx),
            stuck_blocks: Mutex::new(HashMap::new()),
            retry_grace_period: Duration::ZERO,
        };

        service.check_for_stuck_blocks().await;
        assert!(retry_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_check_for_stuck_blocks_retries_transform_ready_blocks_after_grace() {
        let tmp = TempDir::new().unwrap();
        let storage = LiveStorage::with_base_dir(tmp.path().join("live"));
        storage.ensure_dirs().unwrap();

        storage
            .write_block(&LiveBlock {
                number: 101,
                hash: [2; 32],
                parent_hash: [1; 32],
                timestamp: 1200,
                tx_hashes: vec![],
            })
            .unwrap();

        let mut status = LiveBlockStatus::default();
        status.collected = true;
        status.block_fetched = true;
        status.receipts_collected = true;
        status.logs_collected = true;
        status.factories_extracted = true;
        status.logs_decoded = true;
        status.eth_calls_decoded = true;
        status.transformed = false;
        storage.write_status(101, &status).unwrap();

        let progress_tracker = Arc::new(Mutex::new(LiveProgressTracker::new(
            1,
            None,
            "test-chain".to_string(),
        )));
        progress_tracker.lock().await.register_handler("handler_a");

        let (retry_tx, mut retry_rx) = tokio::sync::mpsc::channel(4);
        let service = CompactionService {
            chain_name: "test-chain".to_string(),
            chain_id: 1,
            storage,
            config: LiveModeConfig::default(),
            db_pool: None,
            progress_tracker,
            expectations: LivePipelineExpectations {
                expect_log_decode: true,
                expect_eth_call_decode: true,
                expect_transformations: true,
                ..Default::default()
            },
            storage_manager: None,
            retry_tx: Some(retry_tx),
            stuck_blocks: Mutex::new(HashMap::new()),
            retry_grace_period: Duration::ZERO,
        };

        service.check_for_stuck_blocks().await;

        let request = retry_rx.try_recv().unwrap();
        assert_eq!(request.block_number, 101);
        assert_eq!(
            request.missing_handlers,
            Some(HashSet::from(["handler_a".to_string()]))
        );
    }
}
