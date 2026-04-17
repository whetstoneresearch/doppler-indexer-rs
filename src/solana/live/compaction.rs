//! Compaction service for merging live Solana slot data into historical parquet ranges.
//!
//! Periodically checks for complete slot ranges and compacts them:
//! 1. Finds ranges where all slots are fully processed
//! 2. Reads bincode data from live storage
//! 3. Writes parquet files to historical raw data directories
//! 4. Cleans up live storage
//!
//! Unlike EVM compaction, Solana ranges may have gaps from skipped slots.
//! This is normal -- we compact whatever slots exist in the range.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{mpsc, Mutex};

use super::storage::SolanaLiveStorage;
use super::types::SolanaLivePipelineExpectations;
use crate::live::{LiveModeConfig, LiveProgressTracker, TransformRetryRequest};
use crate::solana::raw_data::parquet::{
    build_event_schema, build_instruction_schema, build_slot_schema, write_events_to_parquet,
    write_instructions_to_parquet, write_slots_to_parquet,
};
use crate::solana::raw_data::types::{SolanaEventRecord, SolanaInstructionRecord, SolanaSlotRecord};
use crate::storage::paths::{raw_solana_events_dir, raw_solana_instructions_dir, raw_solana_slots_dir};
use crate::storage::skipped_slots;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum SolanaCompactionError {
    #[error("Storage error: {0}")]
    Storage(#[from] crate::live::bincode_io::StorageError),
    #[error("Parquet write error: {0}")]
    ParquetWrite(String),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Compactable range
// ---------------------------------------------------------------------------

/// A range of slots that can be compacted into historical parquet files.
#[derive(Debug)]
pub struct CompactableRange {
    /// First slot number in the aligned range.
    pub start: u64,
    /// Last slot number in the aligned range (inclusive).
    pub end: u64,
    /// Number of slots with data in this range (may be less than end - start + 1 due to skipped slots).
    pub slot_count: usize,
}

// ---------------------------------------------------------------------------
// Compaction service
// ---------------------------------------------------------------------------

/// Service for compacting live Solana slot data into historical parquet ranges.
///
/// Runs in a background loop, periodically scanning for complete slot ranges
/// that are safely beyond the reorg depth, then writing their data as parquet
/// files and cleaning up the live bincode storage.
pub struct SolanaCompactionService {
    chain_name: String,
    storage: SolanaLiveStorage,
    config: LiveModeConfig,
    progress_tracker: Arc<Mutex<LiveProgressTracker>>,
    expectations: SolanaLivePipelineExpectations,
    /// Channel to request transformation retries for stuck slots.
    retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
    /// When each transform-ready slot first became eligible for retry.
    stuck_slots: std::sync::Mutex<HashMap<u64, Instant>>,
    /// Minimum time a slot must remain transform-ready and incomplete before retrying.
    retry_grace_period: Duration,
}

impl SolanaCompactionService {
    /// Create a new SolanaCompactionService.
    pub fn new(
        chain_name: String,
        config: LiveModeConfig,
        progress_tracker: Arc<Mutex<LiveProgressTracker>>,
        expectations: SolanaLivePipelineExpectations,
    ) -> Self {
        let storage = SolanaLiveStorage::new(&chain_name);
        let retry_grace_period = Duration::from_secs(config.transform_retry_grace_period_secs);

        Self {
            chain_name,
            storage,
            config,
            progress_tracker,
            expectations,
            retry_tx: None,
            stuck_slots: std::sync::Mutex::new(HashMap::new()),
            retry_grace_period,
        }
    }

    /// Set the retry channel for transformation retries.
    pub fn with_retry_tx(mut self, tx: mpsc::Sender<TransformRetryRequest>) -> Self {
        self.retry_tx = Some(tx);
        self
    }

    /// Run the compaction service loop.
    ///
    /// Runs until cancelled (when the containing task is dropped/aborted).
    pub async fn run(self) {
        let interval = Duration::from_secs(self.config.compaction_interval_secs);

        tracing::info!(
            chain = %self.chain_name,
            ?interval,
            "Solana compaction service started"
        );

        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = self.run_compaction_cycle().await {
                tracing::error!(chain = %self.chain_name, "Compaction cycle failed: {}", e);
            }
        }
    }

    /// Run a single compaction cycle.
    pub async fn run_compaction_cycle(&self) -> Result<(), SolanaCompactionError> {
        // Check for stuck slots needing transformation retry
        self.check_for_stuck_slots().await;

        let ranges = self.find_compactable_ranges().await?;

        if ranges.is_empty() {
            tracing::debug!(chain = %self.chain_name, "No Solana ranges ready for compaction");
            return Ok(());
        }

        for range in ranges {
            tracing::info!(
                chain = %self.chain_name,
                start = range.start,
                end = range.end,
                slot_count = range.slot_count,
                "Compacting Solana slot range"
            );

            if let Err(e) = self.compact_range(&range).await {
                tracing::error!(
                    chain = %self.chain_name,
                    start = range.start,
                    end = range.end,
                    "Failed to compact Solana range: {}",
                    e
                );
            }
        }

        Ok(())
    }

    // =========================================================================
    // Stuck slot detection and retry
    // =========================================================================

    /// Check for slots that are stuck (decode inputs ready but not transformed)
    /// and request retries via the retry channel.
    async fn check_for_stuck_slots(&self) {
        let Some(ref retry_tx) = self.retry_tx else {
            return;
        };

        if !self.expectations.expect_transformations {
            return;
        }

        let slots = match self.storage.list_slots() {
            Ok(s) => s,
            Err(_) => return,
        };

        let now = Instant::now();
        let seen_slots: HashSet<u64> = slots.iter().copied().collect();

        // Phase 1: Collect candidate stuck slots (sync mutex only, no await)
        let mut candidates: Vec<(u64, HashSet<String>)> = Vec::new();
        {
            let mut stuck_slots = self.stuck_slots.lock().unwrap();

            for &slot_number in &slots {
                let status = match self.storage.read_status(slot_number) {
                    Ok(s) => s,
                    Err(_) => {
                        stuck_slots.remove(&slot_number);
                        continue;
                    }
                };

                // Already transformed or inputs not ready -- not stuck
                if status.transformed || !status.transform_inputs_ready_with(&self.expectations) {
                    stuck_slots.remove(&slot_number);
                    continue;
                }

                candidates.push((slot_number, status.failed_handlers));
            }

            // Remove entries for slots that no longer exist in storage
            stuck_slots.retain(|slot, _| seen_slots.contains(slot));
        }
        // sync MutexGuard dropped here

        // Phase 2: Check progress tracker (async, no sync mutex held)
        let mut pending_retries = Vec::new();
        for (slot_number, failed_handlers) in candidates {
            let progress = self.progress_tracker.lock().await;
            if progress.is_block_complete(slot_number) {
                let mut stuck_slots = self.stuck_slots.lock().unwrap();
                stuck_slots.remove(&slot_number);
                continue;
            }

            let mut missing_handlers = progress.get_pending_handlers(slot_number);
            drop(progress);

            missing_handlers.extend(failed_handlers);

            if missing_handlers.is_empty() {
                let mut stuck_slots = self.stuck_slots.lock().unwrap();
                stuck_slots.remove(&slot_number);
                continue;
            }

            // Track when this slot first became stuck
            let mut stuck_slots = self.stuck_slots.lock().unwrap();
            let first_seen = stuck_slots.entry(slot_number).or_insert(now);
            if now.duration_since(*first_seen) >= self.retry_grace_period {
                pending_retries.push((slot_number, missing_handlers));
            }
        }

        // Phase 3: Send retry requests
        for (slot_number, missing_handlers) in pending_retries {
            match retry_tx.try_send(TransformRetryRequest {
                block_number: slot_number,
                missing_handlers: Some(missing_handlers),
            }) {
                Ok(()) => {
                    let mut stuck_slots = self.stuck_slots.lock().unwrap();
                    stuck_slots.remove(&slot_number);
                    tracing::info!(
                        chain = %self.chain_name,
                        slot = slot_number,
                        "Requested transformation retry for stuck Solana slot"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        chain = %self.chain_name,
                        slot = slot_number,
                        "Retry channel full for Solana slot: {}",
                        e
                    );
                }
            }
        }
    }

    // =========================================================================
    // Range discovery
    // =========================================================================

    /// Find ranges of slots that are ready for compaction.
    ///
    /// A range is compactable when:
    /// - All stored slots in the range are fully processed (`is_complete_with`)
    /// - The entire range is safely beyond the reorg depth
    ///
    /// Solana ranges may have gaps from skipped slots. We compact whatever
    /// slots exist in the aligned range -- we do not require every slot number
    /// to have a corresponding block.
    pub async fn find_compactable_ranges(
        &self,
    ) -> Result<Vec<CompactableRange>, SolanaCompactionError> {
        let slots = self.storage.list_slots()?;
        let status_slots = self.storage.list_statuses()?;

        if slots.is_empty() && status_slots.is_empty() {
            return Ok(Vec::new());
        }

        let range_size = self.config.range_size;
        let slots_set: HashSet<u64> = slots.iter().copied().collect();

        // Use max of all known slots (data + status-only) for the reorg boundary.
        // Without this, ranges made entirely of skipped slots would have no
        // data-slot max to compare against and would bypass the reorg check.
        let latest_known = slots.iter().chain(status_slots.iter()).copied().max();

        // Group data slots by aligned range.
        let mut ranges: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for &slot_number in &slots {
            let range_start = (slot_number / range_size) * range_size;
            ranges.entry(range_start).or_default().push(slot_number);
        }

        // Build a map of status-only (skipped) slots per range.  Ranges
        // that have only skipped slots are inserted into `ranges` with an
        // empty data-slot list so they become candidates below.
        let mut status_only_by_range: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for &slot_number in &status_slots {
            if !slots_set.contains(&slot_number) {
                let range_start = (slot_number / range_size) * range_size;
                status_only_by_range
                    .entry(range_start)
                    .or_default()
                    .push(slot_number);
                ranges.entry(range_start).or_default();
            }
        }

        let mut compactable = Vec::new();
        let progress = self.progress_tracker.lock().await;

        for (range_start, slot_numbers) in ranges {
            let range_end = range_start + range_size - 1;

            // Check if all data slots in this range are fully processed.
            let mut all_complete = true;
            for &slot_number in &slot_numbers {
                if let Ok(status) = self.storage.read_status(slot_number) {
                    if !status.is_complete_with(&self.expectations) {
                        all_complete = false;
                        break;
                    }
                } else {
                    all_complete = false;
                    break;
                }

                if !progress.is_block_complete(slot_number) {
                    all_complete = false;
                    break;
                }
            }

            // Check status-only (skipped) slots in this range.  Skipped
            // slots are not registered in the progress tracker, so we only
            // check the persisted status file.
            if all_complete {
                if let Some(skipped) = status_only_by_range.get(&range_start) {
                    for &slot_number in skipped {
                        match self.storage.read_status(slot_number) {
                            Ok(status) if status.is_complete_with(&self.expectations) => {}
                            _ => {
                                all_complete = false;
                                break;
                            }
                        }
                    }
                }
            }

            if !all_complete {
                continue;
            }

            // Only compact ranges safely beyond reorg depth.
            if let Some(latest) = latest_known {
                let safe_boundary = latest.saturating_sub(self.config.reorg_depth);
                if range_end >= safe_boundary {
                    tracing::debug!(
                        chain = %self.chain_name,
                        range_start,
                        range_end,
                        latest_slot = latest,
                        safe_boundary,
                        "Skipping Solana range: within reorg depth"
                    );
                    continue;
                }
            }

            compactable.push(CompactableRange {
                start: range_start,
                end: range_end,
                slot_count: slot_numbers.len(),
            });
        }

        Ok(compactable)
    }

    // =========================================================================
    // Range compaction
    // =========================================================================

    /// Compact a single range: read live data, write parquet, delete live storage.
    async fn compact_range(&self, range: &CompactableRange) -> Result<(), SolanaCompactionError> {
        // Collect all events and instructions from live storage for slots in range.
        // We iterate over stored slots (not all numbers in the range) because
        // Solana has skipped slots with no blocks.
        let stored_slots = self.storage.list_slots()?;
        let status_slots = self.storage.list_statuses()?;
        let slots_in_range: Vec<u64> = stored_slots
            .into_iter()
            .filter(|&s| s >= range.start && s <= range.end)
            .collect();
        let slots_in_range_set: HashSet<u64> = slots_in_range.iter().copied().collect();
        let skipped_slots_in_range: Vec<u64> = status_slots
            .into_iter()
            .filter(|&s| s >= range.start && s <= range.end)
            .filter(|s| !slots_in_range_set.contains(s))
            .collect();
        let mut slots_to_cleanup = slots_in_range.clone();
        slots_to_cleanup.extend(skipped_slots_in_range.iter().copied());

        let mut all_events: Vec<SolanaEventRecord> = Vec::new();
        let mut all_instructions: Vec<SolanaInstructionRecord> = Vec::new();
        let mut slot_records: Vec<SolanaSlotRecord> = Vec::new();

        for &slot_number in &slots_in_range {
            match self.storage.read_events(slot_number) {
                Ok(events) => all_events.extend(events),
                Err(crate::live::bincode_io::StorageError::NotFound(_)) => {
                    // Slot was deleted during compaction (reorg?), skip this range
                    tracing::warn!(
                        chain = %self.chain_name,
                        slot = slot_number,
                        start = range.start,
                        end = range.end,
                        "Slot deleted during compaction (reorg?), skipping range"
                    );
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }

            match self.storage.read_instructions(slot_number) {
                Ok(instructions) => all_instructions.extend(instructions),
                Err(crate::live::bincode_io::StorageError::NotFound(_)) => {
                    // Same as above -- slot vanished
                    tracing::warn!(
                        chain = %self.chain_name,
                        slot = slot_number,
                        start = range.start,
                        end = range.end,
                        "Slot instructions deleted during compaction (reorg?), skipping range"
                    );
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }

            // Build SolanaSlotRecord for the slots parquet.  Transactions are
            // read for their signatures; if missing (e.g. events-only programs)
            // the signatures list is left empty but the record is still written.
            if let Ok(live_slot) = self.storage.read_slot(slot_number) {
                let transaction_signatures: Vec<[u8; 64]> = self
                    .storage
                    .read_transactions(slot_number)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|tx| tx.signature)
                    .collect();
                slot_records.push(SolanaSlotRecord {
                    slot: live_slot.slot,
                    block_time: live_slot.block_time,
                    block_height: live_slot.block_height,
                    parent_slot: live_slot.parent_slot,
                    blockhash: live_slot.blockhash,
                    previous_blockhash: live_slot.previous_blockhash,
                    transaction_count: live_slot.transaction_count,
                    transaction_signatures,
                });
            }
        }

        // Always write slots parquet so that find_resume_slot() can advance past
        // this range on restart and collect_slots_selective() repair can merge
        // unrepaired slot metadata.  The file is created even when empty (all
        // slots in the range were skipped) to satisfy the parquet-existence check.
        let slots_dir = raw_solana_slots_dir(&self.chain_name);
        let slots_path = slots_dir.join(format!("{}-{}.parquet", range.start, range.end));
        self.write_slots_parquet(&slot_records, &slots_path)?;
        if !slot_records.is_empty() {
            tracing::info!(
                chain = %self.chain_name,
                count = slot_records.len(),
                path = %slots_path.display(),
                "Wrote Solana slots parquet"
            );
        }

        // Write events parquet to historical directory
        if !all_events.is_empty() {
            let events_dir = raw_solana_events_dir(&self.chain_name);
            let events_path = events_dir.join(format!("{}-{}.parquet", range.start, range.end));
            self.write_events_parquet(&all_events, &events_path)?;
            tracing::info!(
                chain = %self.chain_name,
                count = all_events.len(),
                path = %events_path.display(),
                "Wrote Solana events parquet"
            );
        }

        // Write instructions parquet to historical directory
        if !all_instructions.is_empty() {
            let instructions_dir = raw_solana_instructions_dir(&self.chain_name);
            let instructions_path =
                instructions_dir.join(format!("{}-{}.parquet", range.start, range.end));
            self.write_instructions_parquet(&all_instructions, &instructions_path)?;
            tracing::info!(
                chain = %self.chain_name,
                count = all_instructions.len(),
                path = %instructions_path.display(),
                "Wrote Solana instructions parquet"
            );
        }

        // The skipped-slots sidecar is the authoritative completion marker for
        // historical Solana ranges, so write it only after the parquet files.
        let events_dir = raw_solana_events_dir(&self.chain_name);
        let range_key = skipped_slots::range_key(range.start, range.end);
        let mut skipped_index = skipped_slots::read_skipped_slots_index(&events_dir);
        skipped_index.insert(range_key, skipped_slots_in_range.clone());
        skipped_slots::write_skipped_slots_index(&events_dir, &skipped_index)?;

        // Clean up live storage for all compacted slots, including skipped
        // slots that only have status metadata.
        for &slot_number in &slots_to_cleanup {
            self.storage.delete_all(slot_number)?;
        }

        // Clean up progress tracker
        {
            let mut progress = self.progress_tracker.lock().await;
            for &slot_number in &slots_to_cleanup {
                progress.clear_block(slot_number);
            }
        }

        tracing::info!(
            chain = %self.chain_name,
            start = range.start,
            end = range.end,
            slot_count = slots_in_range.len(),
            skipped_slots = skipped_slots_in_range.len(),
            events = all_events.len(),
            instructions = all_instructions.len(),
            "Successfully compacted Solana slot range"
        );

        Ok(())
    }

    // =========================================================================
    // Parquet writing helpers
    // =========================================================================

    /// Write event records to a parquet file, reusing the existing schema and writer.
    fn write_events_parquet(
        &self,
        events: &[SolanaEventRecord],
        path: &PathBuf,
    ) -> Result<(), SolanaCompactionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = build_event_schema();
        write_events_to_parquet(events, &schema, path).map_err(|e| {
            SolanaCompactionError::ParquetWrite(format!("Failed to write events parquet: {}", e))
        })
    }

    /// Write instruction records to a parquet file, reusing the existing schema and writer.
    fn write_instructions_parquet(
        &self,
        instructions: &[SolanaInstructionRecord],
        path: &PathBuf,
    ) -> Result<(), SolanaCompactionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = build_instruction_schema();
        write_instructions_to_parquet(instructions, &schema, path).map_err(|e| {
            SolanaCompactionError::ParquetWrite(format!(
                "Failed to write instructions parquet: {}",
                e
            ))
        })
    }

    /// Write slot records to a parquet file.
    fn write_slots_parquet(
        &self,
        slots: &[SolanaSlotRecord],
        path: &PathBuf,
    ) -> Result<(), SolanaCompactionError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = build_slot_schema();
        write_slots_to_parquet(slots, &schema, path).map_err(|e| {
            SolanaCompactionError::ParquetWrite(format!("Failed to write slots parquet: {}", e))
        })
    }
}

impl std::fmt::Debug for SolanaCompactionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SolanaCompactionService")
            .field("chain_name", &self.chain_name)
            .field("range_size", &self.config.range_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::LiveProgressTracker;
    use crate::solana::live::types::{LiveSlot, LiveSlotStatus};
    use tempfile::TempDir;

    fn make_config(range_size: u64) -> LiveModeConfig {
        LiveModeConfig {
            reorg_depth: 150,
            compaction_interval_secs: 10,
            range_size,
            transform_retry_grace_period_secs: 30,
        }
    }

    fn make_expectations() -> SolanaLivePipelineExpectations {
        SolanaLivePipelineExpectations {
            expect_event_decode: false,
            expect_instruction_decode: false,
            expect_account_reads: false,
            expect_transformations: false,
        }
    }

    fn make_complete_status(expectations: &SolanaLivePipelineExpectations) -> LiveSlotStatus {
        let mut status = LiveSlotStatus {
            collected: true,
            block_fetched: true,
            events_extracted: true,
            instructions_extracted: true,
            ..Default::default()
        };
        status.apply_expectations(expectations);
        status
    }

    fn make_slot(slot: u64) -> LiveSlot {
        LiveSlot {
            slot,
            block_time: Some(1_700_000_000 + slot as i64),
            block_height: Some(slot),
            parent_slot: slot.saturating_sub(1),
            blockhash: [0u8; 32],
            previous_blockhash: [0u8; 32],
            transaction_count: 1,
        }
    }

    /// Helper to build a service with a temp directory for storage.
    fn make_service(
        tmp: &TempDir,
        range_size: u64,
    ) -> (SolanaCompactionService, SolanaLivePipelineExpectations) {
        make_service_with_chain(tmp, "test-solana", range_size)
    }

    fn make_service_with_chain(
        tmp: &TempDir,
        chain_name: &str,
        range_size: u64,
    ) -> (SolanaCompactionService, SolanaLivePipelineExpectations) {
        let expectations = make_expectations();
        let progress = Arc::new(Mutex::new(LiveProgressTracker::new(
            0,
            None,
            chain_name.to_string(),
        )));
        let config = make_config(range_size);

        let mut service =
            SolanaCompactionService::new(chain_name.to_string(), config, progress, expectations);
        // Override storage to use temp dir
        service.storage = SolanaLiveStorage::with_base_dir(tmp.path().to_path_buf());
        service.storage.ensure_dirs().unwrap();

        (service, expectations)
    }

    #[tokio::test]
    async fn test_find_compactable_ranges_empty() {
        let tmp = TempDir::new().unwrap();
        let (service, _) = make_service(&tmp, 1000);

        let ranges = service.find_compactable_ranges().await.unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn test_find_compactable_ranges_incomplete_slot() {
        let tmp = TempDir::new().unwrap();
        let (service, _) = make_service(&tmp, 1000);

        // Write a slot with incomplete status
        let slot = make_slot(500);
        service.storage.write_slot(500, &slot).unwrap();
        service
            .storage
            .write_status(500, &LiveSlotStatus::collected())
            .unwrap();

        let ranges = service.find_compactable_ranges().await.unwrap();
        assert!(
            ranges.is_empty(),
            "Incomplete slot should not be compactable"
        );
    }

    #[tokio::test]
    async fn test_find_compactable_ranges_within_reorg_depth() {
        let tmp = TempDir::new().unwrap();
        let (service, expectations) = make_service(&tmp, 10);

        // Create slots 0..10 (a complete range of 10)
        for i in 0..10 {
            let slot = make_slot(i);
            service.storage.write_slot(i, &slot).unwrap();
            service
                .storage
                .write_status(i, &make_complete_status(&expectations))
                .unwrap();
        }

        // Also create a "latest" slot that makes the range within reorg depth.
        // reorg_depth=150, latest=100, safe_boundary=100-150=0 (saturating)
        // range_end=9, safe_boundary=0 => 9 >= 0, so it's within reorg depth
        // We need latest to be far enough that range_end < safe_boundary.
        // With latest=100, safe=100-150=0 (saturating), range_end=9 >= 0 => skipped.
        let latest = make_slot(100);
        service.storage.write_slot(100, &latest).unwrap();
        service
            .storage
            .write_status(100, &make_complete_status(&expectations))
            .unwrap();

        let ranges = service.find_compactable_ranges().await.unwrap();
        // range 0-9: end=9, latest=100, safe_boundary=max(0,100-150)=0 => 9 >= 0 => skipped
        assert!(
            ranges.is_empty(),
            "Range within reorg depth should not be compactable"
        );
    }

    #[tokio::test]
    async fn test_find_compactable_ranges_beyond_reorg_depth() {
        let tmp = TempDir::new().unwrap();
        let (service, expectations) = make_service(&tmp, 10);

        // Create slots 0..10 (range 0-9)
        for i in 0..10 {
            let slot = make_slot(i);
            service.storage.write_slot(i, &slot).unwrap();
            service
                .storage
                .write_status(i, &make_complete_status(&expectations))
                .unwrap();
        }

        // Create a latest slot far enough away: 200
        // safe_boundary = 200 - 150 = 50, range_end = 9 < 50 => compactable
        let latest = make_slot(200);
        service.storage.write_slot(200, &latest).unwrap();
        service
            .storage
            .write_status(200, &make_complete_status(&expectations))
            .unwrap();

        let ranges = service.find_compactable_ranges().await.unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 9);
        assert_eq!(ranges[0].slot_count, 10);
    }

    #[tokio::test]
    async fn test_find_compactable_ranges_with_skipped_slots() {
        let tmp = TempDir::new().unwrap();
        let (service, expectations) = make_service(&tmp, 10);

        // Create slots 0, 2, 5, 7, 9 (skipped: 1, 3, 4, 6, 8)
        for i in [0, 2, 5, 7, 9] {
            let slot = make_slot(i);
            service.storage.write_slot(i, &slot).unwrap();
            service
                .storage
                .write_status(i, &make_complete_status(&expectations))
                .unwrap();
        }

        // Latest slot far enough away
        let latest = make_slot(500);
        service.storage.write_slot(500, &latest).unwrap();
        service
            .storage
            .write_status(500, &make_complete_status(&expectations))
            .unwrap();

        let ranges = service.find_compactable_ranges().await.unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 9);
        // Only 5 slots have data (skipped slots don't count)
        assert_eq!(ranges[0].slot_count, 5);
    }

    #[tokio::test]
    async fn test_compact_range_writes_parquet_and_cleans_up() {
        let tmp = TempDir::new().unwrap();
        let (service, expectations) = make_service(&tmp, 10);

        // Create slots with events and instructions
        for i in 0..5 {
            let slot = make_slot(i);
            service.storage.write_slot(i, &slot).unwrap();
            service
                .storage
                .write_status(i, &make_complete_status(&expectations))
                .unwrap();

            let events = vec![SolanaEventRecord {
                slot: i,
                block_time: Some(1_700_000_000 + i as i64),
                transaction_signature: [0xAA; 64],
                program_id: [0xBB; 32],
                event_discriminator: [0xCC; 8],
                event_data: vec![1, 2, 3],
                log_index: i as u32,
                instruction_index: 0,
                inner_instruction_index: None,
            }];
            service.storage.write_events(i, &events).unwrap();

            let instructions = vec![SolanaInstructionRecord {
                slot: i,
                block_time: Some(1_700_000_000 + i as i64),
                transaction_signature: [0xAA; 64],
                program_id: [0xBB; 32],
                data: vec![4, 5, 6],
                accounts: vec![[0xDD; 32]],
                instruction_index: 0,
                inner_instruction_index: None,
            }];
            service
                .storage
                .write_instructions(i, &instructions)
                .unwrap();
        }

        let range = CompactableRange {
            start: 0,
            end: 9,
            slot_count: 5,
        };

        service.compact_range(&range).await.unwrap();

        // Verify live storage is cleaned up
        let remaining_slots = service.storage.list_slots().unwrap();
        for i in 0..5 {
            assert!(
                !remaining_slots.contains(&i),
                "Slot {} should be deleted after compaction",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_compact_range_empty_events_and_instructions() {
        let tmp = TempDir::new().unwrap();
        let chain_name = "test-solana-empty-evt-instr";
        let historical_dir = PathBuf::from(format!("data/{chain_name}"));
        let _ = std::fs::remove_dir_all(&historical_dir);

        let (service, expectations) = make_service_with_chain(&tmp, chain_name, 10);

        for i in 0..3 {
            let slot = make_slot(i);
            service.storage.write_slot(i, &slot).unwrap();
            service
                .storage
                .write_status(i, &make_complete_status(&expectations))
                .unwrap();
            service.storage.write_events(i, &[]).unwrap();
            service.storage.write_instructions(i, &[]).unwrap();
        }

        let range = CompactableRange {
            start: 0,
            end: 9,
            slot_count: 3,
        };

        service.compact_range(&range).await.unwrap();

        // All live slots should be cleaned up
        let remaining = service.storage.list_slots().unwrap();
        assert!(remaining.is_empty());

        // Slots parquet must exist even though events/instructions are empty
        let slots_path = raw_solana_slots_dir(chain_name).join("0-9.parquet");
        assert!(
            slots_path.exists(),
            "Slots parquet must be written for find_resume_slot to advance"
        );

        let _ = std::fs::remove_dir_all(&historical_dir);
    }

    #[tokio::test]
    async fn test_compact_range_slots_parquet_has_correct_records() {
        use crate::solana::raw_data::parquet::read_slots_from_parquet;

        let tmp = TempDir::new().unwrap();
        let chain_name = "test-solana-slots-parquet-records";
        let historical_dir = PathBuf::from(format!("data/{chain_name}"));
        let _ = std::fs::remove_dir_all(&historical_dir);

        let (service, expectations) = make_service_with_chain(&tmp, chain_name, 10);

        for i in 0..3u64 {
            let slot = make_slot(i);
            service.storage.write_slot(i, &slot).unwrap();
            service
                .storage
                .write_status(i, &make_complete_status(&expectations))
                .unwrap();
            service.storage.write_events(i, &[]).unwrap();
            service.storage.write_instructions(i, &[]).unwrap();
        }

        let range = CompactableRange {
            start: 0,
            end: 9,
            slot_count: 3,
        };
        service.compact_range(&range).await.unwrap();

        let slots_path = raw_solana_slots_dir(chain_name).join("0-9.parquet");
        let records = read_slots_from_parquet(&slots_path).unwrap();
        assert_eq!(records.len(), 3);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.slot, i as u64);
        }

        let _ = std::fs::remove_dir_all(&historical_dir);
    }

    #[tokio::test]
    async fn test_find_compactable_ranges_skipped_only() {
        // Ranges composed entirely of skipped slots (status files only, no
        // data files) must be discovered so their status dirs can be cleaned
        // up and their skipped-slot markers reach skipped_slots.json.
        let tmp = TempDir::new().unwrap();
        let (service, expectations) = make_service(&tmp, 1); // range_size=1 triggers the bug

        // Slots 5, 6, 7 are all skipped
        for i in [5u64, 6, 7] {
            let mut status = LiveSlotStatus::collected();
            status.block_fetched = true;
            status.events_extracted = true;
            status.instructions_extracted = true;
            status.apply_expectations(&expectations);
            service.storage.write_status(i, &status).unwrap();
        }

        // A data slot far enough away that slots 5-7 are beyond reorg depth
        // (reorg_depth=150, latest=200, safe_boundary=50 > 7)
        let latest = make_slot(200);
        service.storage.write_slot(200, &latest).unwrap();
        service
            .storage
            .write_status(200, &make_complete_status(&expectations))
            .unwrap();

        let ranges = service.find_compactable_ranges().await.unwrap();
        let range_starts: Vec<u64> = ranges.iter().map(|r| r.start).collect();

        assert!(
            range_starts.contains(&5),
            "Skipped-only range 5 should be compactable"
        );
        assert!(
            range_starts.contains(&6),
            "Skipped-only range 6 should be compactable"
        );
        assert!(
            range_starts.contains(&7),
            "Skipped-only range 7 should be compactable"
        );

        for range in &ranges {
            if [5u64, 6, 7].contains(&range.start) {
                assert_eq!(
                    range.slot_count, 0,
                    "Status-only range should have slot_count=0"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_compact_skipped_only_range() {
        // Compacting a range made entirely of skipped slots should clean up
        // the status files, write skipped_slots.json, and emit a slots parquet
        // so find_resume_slot can advance past the range.
        let tmp = TempDir::new().unwrap();
        let chain_name = "test-solana-skipped-only-compact";
        let historical_dir = PathBuf::from(format!("data/{chain_name}"));
        let _ = std::fs::remove_dir_all(&historical_dir);

        let (service, expectations) = make_service_with_chain(&tmp, chain_name, 10);

        for i in 0..10u64 {
            let mut status = LiveSlotStatus::collected();
            status.block_fetched = true;
            status.events_extracted = true;
            status.instructions_extracted = true;
            status.apply_expectations(&expectations);
            service.storage.write_status(i, &status).unwrap();
        }

        let range = CompactableRange {
            start: 0,
            end: 9,
            slot_count: 0,
        };
        service.compact_range(&range).await.unwrap();

        // All status files cleaned up
        assert!(service.storage.list_statuses().unwrap().is_empty());

        // All 10 slots recorded as skipped
        let events_dir = raw_solana_events_dir(chain_name);
        let skipped_index = skipped_slots::read_skipped_slots_index(&events_dir);
        let rk = skipped_slots::range_key(0, 9);
        let skipped = skipped_index.get(&rk).cloned().unwrap_or_default();
        assert_eq!(skipped.len(), 10, "All 10 slots should be in skipped_slots.json");

        // Slots parquet must exist for find_resume_slot
        let slots_path = raw_solana_slots_dir(chain_name).join("0-9.parquet");
        assert!(
            slots_path.exists(),
            "Slots parquet must be written even for all-skipped ranges"
        );

        let _ = std::fs::remove_dir_all(&historical_dir);
    }

    #[tokio::test]
    async fn test_compact_range_records_skipped_slots_and_cleans_statuses() {
        let tmp = TempDir::new().unwrap();
        let chain_name = "test-solana-compaction-skipped-slots";
        let historical_dir = PathBuf::from(format!("data/{chain_name}"));
        let _ = std::fs::remove_dir_all(&historical_dir);

        let (service, expectations) = make_service_with_chain(&tmp, chain_name, 10);

        for i in [0, 2, 5, 7, 9] {
            let slot = make_slot(i);
            service.storage.write_slot(i, &slot).unwrap();
            service
                .storage
                .write_status(i, &make_complete_status(&expectations))
                .unwrap();
            service.storage.write_events(i, &[]).unwrap();
            service.storage.write_instructions(i, &[]).unwrap();
        }

        for skipped in [1, 3, 4, 6, 8] {
            service
                .storage
                .write_status(skipped, &make_complete_status(&expectations))
                .unwrap();
        }

        let range = CompactableRange {
            start: 0,
            end: 9,
            slot_count: 5,
        };

        service.compact_range(&range).await.unwrap();

        let skipped_index =
            skipped_slots::read_skipped_slots_index(&raw_solana_events_dir(chain_name));
        let range_key = skipped_slots::range_key(range.start, range.end);
        assert_eq!(skipped_index.get(&range_key), Some(&vec![1, 3, 4, 6, 8]));
        assert!(service.storage.list_slots().unwrap().is_empty());
        assert!(service.storage.list_statuses().unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&historical_dir);
    }

    #[tokio::test]
    async fn test_stuck_slots_grace_period() {
        let tmp = TempDir::new().unwrap();
        let expectations = SolanaLivePipelineExpectations {
            expect_event_decode: true,
            expect_instruction_decode: true,
            expect_account_reads: false,
            expect_transformations: true,
        };

        let mut progress_tracker = LiveProgressTracker::new(0, None, "test-solana".to_string());
        progress_tracker.register_handler("handler_a");
        let progress = Arc::new(Mutex::new(progress_tracker));

        let config = LiveModeConfig {
            reorg_depth: 150,
            compaction_interval_secs: 10,
            range_size: 1000,
            // Very long grace period so nothing triggers during test
            transform_retry_grace_period_secs: 3600,
        };

        let (retry_tx, mut retry_rx) = mpsc::channel(10);

        let mut service =
            SolanaCompactionService::new("test-solana".to_string(), config, progress, expectations);
        service.storage = SolanaLiveStorage::with_base_dir(tmp.path().to_path_buf());
        service.storage.ensure_dirs().unwrap();
        service = service.with_retry_tx(retry_tx);

        // Create a slot that has decode inputs ready but is not transformed
        let slot = make_slot(100);
        service.storage.write_slot(100, &slot).unwrap();
        let status = LiveSlotStatus {
            collected: true,
            block_fetched: true,
            events_extracted: true,
            instructions_extracted: true,
            events_decoded: true,
            instructions_decoded: true,
            accounts_read: true,
            accounts_decoded: true,
            transformed: false,
            ..Default::default()
        };
        service.storage.write_status(100, &status).unwrap();

        // Check for stuck slots -- grace period is very long, so no retry should be sent
        service.check_for_stuck_slots().await;

        // No retry should have been sent (still within grace period)
        assert!(retry_rx.try_recv().is_err());
    }
}
