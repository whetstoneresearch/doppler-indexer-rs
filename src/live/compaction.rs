//! Compaction service for merging live blocks into parquet ranges.
//!
//! Periodically checks for complete block ranges and compacts them:
//! 1. Finds ranges where all blocks are fully processed
//! 2. Converts bincode data to Arrow arrays
//! 3. Writes parquet files to raw data directory
//! 4. Updates progress tables
//! 5. Cleans up live storage

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryBuilder, Int64Builder, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::Mutex;

use super::progress::LiveProgressTracker;
use super::storage::{LiveStorage, StorageError};
use super::types::LiveModeConfig;
use crate::db::DbPool;

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
}

/// A range of blocks that can be compacted.
#[derive(Debug, Clone)]
pub struct CompactableRange {
    pub start: u64,
    pub end: u64,
    pub block_count: usize,
}

/// Service for compacting live blocks into parquet ranges.
pub struct CompactionService {
    chain_name: String,
    chain_id: u64,
    storage: LiveStorage,
    config: LiveModeConfig,
    db_pool: Option<Arc<DbPool>>,
    progress_tracker: Arc<Mutex<LiveProgressTracker>>,
}

impl CompactionService {
    /// Create a new CompactionService.
    pub fn new(
        chain_name: String,
        chain_id: u64,
        config: LiveModeConfig,
        db_pool: Option<Arc<DbPool>>,
        progress_tracker: Arc<Mutex<LiveProgressTracker>>,
    ) -> Self {
        let storage = LiveStorage::new(&chain_name);

        Self {
            chain_name,
            chain_id,
            storage,
            config,
            db_pool,
            progress_tracker,
        }
    }

    /// Run the compaction service loop.
    pub async fn run(self, mut shutdown: tokio::sync::oneshot::Receiver<()>) {
        let interval = Duration::from_secs(self.config.compaction_interval_secs);

        tracing::info!(
            "Compaction service started for chain {} with interval {:?}",
            self.chain_name,
            interval
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.run_compaction_cycle().await {
                        tracing::error!("Compaction cycle error: {}", e);
                    }
                }
                _ = &mut shutdown => {
                    tracing::info!("Compaction service shutting down");
                    break;
                }
            }
        }
    }

    /// Run a single compaction cycle.
    pub async fn run_compaction_cycle(&self) -> Result<(), CompactionError> {
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
            let has_gap = block_numbers
                .windows(2)
                .any(|w| w[1] != w[0] + 1);
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
                    if !status.is_complete() {
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
            "Successfully compacted range {}-{} ({} blocks, {} logs)",
            range.start,
            range.end,
            blocks.len(),
            all_logs.len()
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

    /// Write blocks to parquet file.
    fn write_blocks_parquet(
        &self,
        blocks: &[super::types::LiveBlock],
        path: &PathBuf,
    ) -> Result<(), CompactionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

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
            block_hashes.append_value(&block.hash);
            parent_hashes.append_value(&block.parent_hash);
            timestamps.append_value(block.timestamp);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(block_numbers.finish()) as ArrayRef,
                Arc::new(block_hashes.finish()) as ArrayRef,
                Arc::new(parent_hashes.finish()) as ArrayRef,
                Arc::new(timestamps.finish()) as ArrayRef,
            ],
        )?;

        let file = std::fs::File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    /// Write logs to parquet file.
    fn write_logs_parquet(
        &self,
        logs: &[(u64, u64, super::types::LiveLog)],
        path: &PathBuf,
    ) -> Result<(), CompactionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("transaction_index", DataType::UInt32, false),
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
            addresses.append_value(&log.address);

            // Handle topics (up to 4)
            if log.topics.len() > 0 {
                topic0s.append_value(&log.topics[0]);
            } else {
                topic0s.append_null();
            }
            if log.topics.len() > 1 {
                topic1s.append_value(&log.topics[1]);
            } else {
                topic1s.append_null();
            }
            if log.topics.len() > 2 {
                topic2s.append_value(&log.topics[2]);
            } else {
                topic2s.append_null();
            }
            if log.topics.len() > 3 {
                topic3s.append_value(&log.topics[3]);
            } else {
                topic3s.append_null();
            }

            datas.append_value(&log.data);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(block_numbers.finish()) as ArrayRef,
                Arc::new(timestamps.finish()) as ArrayRef,
                Arc::new(log_indices.finish()) as ArrayRef,
                Arc::new(tx_indices.finish()) as ArrayRef,
                Arc::new(addresses.finish()) as ArrayRef,
                Arc::new(topic0s.finish()) as ArrayRef,
                Arc::new(topic1s.finish()) as ArrayRef,
                Arc::new(topic2s.finish()) as ArrayRef,
                Arc::new(topic3s.finish()) as ArrayRef,
                Arc::new(datas.finish()) as ArrayRef,
            ],
        )?;

        let file = std::fs::File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
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
            let next_range_start = end + 1;

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
                        &next_range_start as &(dyn ToSql + Sync),
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
        if block_numbers.first() != Some(&range_start)
            || block_numbers.last() != Some(&range_end)
        {
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
        let range_start: i64 = 0;
        let range_end: i64 = 999;
        let next_range_start = range_end + 1;

        assert_eq!(next_range_start, 1000);

        // After compacting range 1000-1999, the next range starts at 2000
        let range_end_2: i64 = 1999;
        let next_range_start_2 = range_end_2 + 1;

        assert_eq!(next_range_start_2, 2000);
    }
}
