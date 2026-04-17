//! Solana live catchup service — restart recovery for incomplete slots.
//!
//! On restart, live mode may have slots that were partially processed:
//! - Slots collected but block not fetched (collector crashed early)
//! - Slots fetched but events/instructions not extracted
//! - Slots extracted but not decoded
//! - Slots decoded but not transformed (some handlers ran, others didn't)
//!
//! This service scans storage for incomplete slots and categorises them
//! into resume buckets so the caller can replay each slot through the
//! appropriate pipeline stage.

use std::collections::HashSet;

use crate::live::bincode_io::StorageError;

use super::storage::SolanaLiveStorage;
use super::types::{LiveSlotStatus, SolanaLivePipelineExpectations};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Which collection stage a slot needs to resume from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotCollectionResumeStage {
    /// Block data was never fetched — restart from `getBlock`.
    FetchBlock,
    /// Block was fetched but events/instructions were not extracted.
    ExtractEventsAndInstructions,
}

/// A single slot that needs collection resumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotCollectionResumeRequest {
    pub slot: u64,
    pub stage: SlotCollectionResumeStage,
}

/// Aggregated result of scanning for incomplete slots.
#[derive(Debug, Default)]
pub struct SolanaCatchupScanResult {
    /// Slots that need collection resumed from a specific stage.
    pub slots_needing_collection_resume: Vec<SlotCollectionResumeRequest>,
    /// Slots that need event decoding (events extracted but not decoded).
    pub slots_needing_event_decode: Vec<u64>,
    /// Slots that need instruction decoding (instructions extracted but not decoded).
    pub slots_needing_instruction_decode: Vec<u64>,
    /// Slots that need account reads (block fetched but accounts not read).
    pub slots_needing_account_reads: Vec<u64>,
    /// Slots that need transformation, with the set of handlers still pending.
    /// `(slot, missing_handler_keys)`
    pub slots_needing_transform: Vec<(u64, HashSet<String>)>,
}

impl SolanaCatchupScanResult {
    /// Check if any catchup work is needed.
    pub fn is_empty(&self) -> bool {
        self.slots_needing_collection_resume.is_empty()
            && self.slots_needing_event_decode.is_empty()
            && self.slots_needing_instruction_decode.is_empty()
            && self.slots_needing_account_reads.is_empty()
            && self.slots_needing_transform.is_empty()
    }

    /// Total number of unique slots needing some form of catchup.
    pub fn total_slots(&self) -> usize {
        let mut slots: HashSet<u64> = HashSet::new();
        slots.extend(
            self.slots_needing_collection_resume
                .iter()
                .map(|req| req.slot),
        );
        slots.extend(&self.slots_needing_event_decode);
        slots.extend(&self.slots_needing_instruction_decode);
        slots.extend(&self.slots_needing_account_reads);
        slots.extend(self.slots_needing_transform.iter().map(|(s, _)| *s));
        slots.len()
    }
}

// ---------------------------------------------------------------------------
// Catchup service
// ---------------------------------------------------------------------------

/// Service for catching up incomplete slots on restart.
///
/// Parallels the EVM `LiveCatchupService` but operates on Solana slot
/// status fields (`events_extracted`, `instructions_extracted`, etc.)
/// instead of EVM-specific ones (`receipts_collected`, `logs_collected`).
pub struct SolanaLiveCatchupService {
    storage: SolanaLiveStorage,
    /// Handler keys that should be complete for a slot to be considered done.
    registered_handlers: HashSet<String>,
    /// Runtime expectations for optional pipeline stages.
    expectations: SolanaLivePipelineExpectations,
}

impl SolanaLiveCatchupService {
    /// Create a new catchup service.
    pub fn new(
        chain_name: &str,
        registered_handlers: HashSet<String>,
        expectations: SolanaLivePipelineExpectations,
    ) -> Self {
        Self {
            storage: SolanaLiveStorage::new(chain_name),
            registered_handlers,
            expectations,
        }
    }

    /// Scan all stored slots and categorise incomplete ones into resume
    /// buckets.
    ///
    /// For each slot that has a status file:
    /// 1. Check collection completeness (block fetched, events/instructions
    ///    extracted). If incomplete, add to `slots_needing_collection_resume`
    ///    and skip further checks (earlier stages must complete first).
    /// 2. Check decode completeness per expectation flags.
    /// 3. Check account-read completeness.
    /// 4. Check transformation completeness.
    pub fn scan_incomplete_slots(&self) -> Result<SolanaCatchupScanResult, StorageError> {
        let mut result = SolanaCatchupScanResult::default();

        let slots = self.storage.list_slots()?;
        if slots.is_empty() {
            tracing::debug!("Catchup scan: no slots in storage");
            return Ok(result);
        }

        tracing::debug!(
            "Catchup scan: checking {} slots ({} to {})",
            slots.len(),
            slots.first().unwrap_or(&0),
            slots.last().unwrap_or(&0),
        );

        for slot in slots {
            let status = match self.storage.read_status(slot) {
                Ok(s) => s,
                Err(StorageError::NotFound(_)) => {
                    tracing::warn!(slot, "Slot has data but no status file, skipping catchup",);
                    continue;
                }
                Err(e) => return Err(e),
            };

            // 1. Collection resume — if the slot hasn't finished collection,
            //    it needs to restart from the earliest incomplete stage.
            if let Some(stage) = self.collection_resume_stage(&status) {
                result
                    .slots_needing_collection_resume
                    .push(SlotCollectionResumeRequest { slot, stage });

                // If we need to re-fetch the block, skip further checks.
                // If we need to re-extract, we can still check decode for
                // events/instructions that were already extracted in a
                // previous partial run — but for simplicity we skip.
                continue;
            }

            // 2. Event decode
            if self.expectations.expect_event_decode && !status.events_decoded {
                result.slots_needing_event_decode.push(slot);
            }

            // 3. Instruction decode
            if self.expectations.expect_instruction_decode && !status.instructions_decoded {
                result.slots_needing_instruction_decode.push(slot);
            }

            // 4. Account reads
            if self.expectations.expect_account_reads
                && (!status.accounts_read || !status.accounts_decoded)
            {
                result.slots_needing_account_reads.push(slot);
            }

            // 5. Transformation
            if self.expectations.expect_transformations
                && status.transform_inputs_ready_with(&self.expectations)
                && !status.transformed
            {
                let missing = self.get_missing_handlers(&status);
                if !missing.is_empty() {
                    result.slots_needing_transform.push((slot, missing));
                }
            }
        }

        // Sort all buckets in ascending slot order.
        result
            .slots_needing_collection_resume
            .sort_unstable_by_key(|req| req.slot);
        result.slots_needing_event_decode.sort_unstable();
        result.slots_needing_instruction_decode.sort_unstable();
        result.slots_needing_account_reads.sort_unstable();
        result
            .slots_needing_transform
            .sort_unstable_by_key(|(s, _)| *s);

        Ok(result)
    }

    /// Determine which collection stage a slot needs to resume from, if any.
    ///
    /// Returns `None` when collection is complete (block fetched, events and
    /// instructions extracted).
    pub fn collection_resume_stage(
        &self,
        status: &LiveSlotStatus,
    ) -> Option<SlotCollectionResumeStage> {
        if !status.block_fetched {
            return Some(SlotCollectionResumeStage::FetchBlock);
        }

        if !status.events_extracted || !status.instructions_extracted {
            return Some(SlotCollectionResumeStage::ExtractEventsAndInstructions);
        }

        None
    }

    /// Return the set of registered handler keys that have *not* yet
    /// completed for the given slot status.
    pub fn get_missing_handlers(&self, status: &LiveSlotStatus) -> HashSet<String> {
        self.registered_handlers
            .difference(&status.completed_handlers)
            .cloned()
            .collect()
    }

    /// Reconstruct missing status files by inspecting which data files exist
    /// for each slot.
    ///
    /// Walks `raw/slots/` to enumerate slot numbers, then for each slot
    /// that is missing a status file, checks for the presence of other data
    /// files (events, instructions, accounts) and builds a best-effort status.
    ///
    /// Returns the number of status files reconstructed.
    pub fn reconstruct_missing_status_files(&self) -> Result<usize, StorageError> {
        let slots = self.storage.list_slots()?;
        let mut reconstructed = 0;

        for slot in slots {
            // Skip slots that already have a status file.
            match self.storage.read_status(slot) {
                Ok(_) => continue,
                Err(StorageError::NotFound(_)) => { /* needs reconstruction */ }
                Err(e) => return Err(e),
            }

            let status = self.reconstruct_status(slot)?;
            self.storage.write_status(slot, &status)?;

            tracing::info!(
                slot,
                collected = status.collected,
                block_fetched = status.block_fetched,
                events_extracted = status.events_extracted,
                instructions_extracted = status.instructions_extracted,
                events_decoded = status.events_decoded,
                instructions_decoded = status.instructions_decoded,
                accounts_read = status.accounts_read,
                transformed = status.transformed,
                "Reconstructed status file for slot",
            );

            reconstructed += 1;
        }

        if reconstructed > 0 {
            tracing::info!(
                count = reconstructed,
                "Reconstructed missing status files from local storage",
            );
        }

        Ok(reconstructed)
    }

    /// Build a `LiveSlotStatus` from data-file presence.
    fn reconstruct_status(&self, slot: u64) -> Result<LiveSlotStatus, StorageError> {
        let mut status = LiveSlotStatus::default();

        // If we have a slot file at all, collection happened.
        let slot_exists = self.storage.slot_exists(slot);
        if slot_exists {
            status.collected = true;
            // If the slot file exists, the block was fetched (the collector
            // writes the slot file after a successful getBlock).
            status.block_fetched = true;
        }

        // Check for extracted raw data.
        let events_exist = self.storage.read_events(slot).is_ok();
        let instructions_exist = self.storage.read_instructions(slot).is_ok();
        let accounts_exist = self.storage.read_accounts(slot).is_ok();

        if events_exist {
            status.events_extracted = true;
        }
        if instructions_exist {
            status.instructions_extracted = true;
        }

        // Check for decoded data presence.
        let decoded_events_exist = !self.storage.list_decoded_types("events", slot)?.is_empty();
        let decoded_instructions_exist = !self
            .storage
            .list_decoded_types("instructions", slot)?
            .is_empty();
        let decoded_accounts_exist = !self
            .storage
            .list_decoded_types("accounts", slot)?
            .is_empty();

        // If events are empty (or decoded data exists), mark decoded.
        let events_empty = events_exist
            && self
                .storage
                .read_events(slot)
                .map(|e| e.is_empty())
                .unwrap_or(false);
        if decoded_events_exist || !events_exist || events_empty {
            status.events_decoded = true;
        }

        let instructions_empty = instructions_exist
            && self
                .storage
                .read_instructions(slot)
                .map(|i| i.is_empty())
                .unwrap_or(false);
        if decoded_instructions_exist || !instructions_exist || instructions_empty {
            status.instructions_decoded = true;
        }

        // Account reads
        if accounts_exist || decoded_accounts_exist {
            status.accounts_read = true;
        }
        let accounts_empty = accounts_exist
            && self
                .storage
                .read_accounts(slot)
                .map(|a| a.is_empty())
                .unwrap_or(false);
        if decoded_accounts_exist || !accounts_exist || accounts_empty {
            status.accounts_decoded = true;
        }

        // Apply expectation flags so disabled stages don't block completeness.
        status.apply_expectations(&self.expectations);

        // Transformation: if all decode inputs are ready and no handlers are
        // registered, mark as transformed.
        if status.transform_inputs_ready_with(&self.expectations) {
            if !self.expectations.expect_transformations || self.registered_handlers.is_empty() {
                status.transformed = true;
            }
        }

        Ok(status)
    }

    /// Get the underlying storage reference (for external replay logic).
    #[allow(dead_code)]
    pub fn storage(&self) -> &SolanaLiveStorage {
        &self.storage
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::live::types::LiveSlot;
    use tempfile::TempDir;

    /// Create a test storage and catchup service with the given expectations.
    fn test_setup(
        handlers: HashSet<String>,
        expectations: SolanaLivePipelineExpectations,
    ) -> (SolanaLiveStorage, SolanaLiveCatchupService, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage = SolanaLiveStorage::with_base_dir(tmp.path().to_path_buf());
        storage.ensure_dirs().unwrap();

        let service = SolanaLiveCatchupService {
            storage: SolanaLiveStorage::with_base_dir(tmp.path().to_path_buf()),
            registered_handlers: handlers,
            expectations,
        };

        (storage, service, tmp)
    }

    fn make_slot(slot: u64) -> LiveSlot {
        LiveSlot {
            slot,
            block_time: Some(1_700_000_000),
            block_height: Some(slot),
            parent_slot: slot.saturating_sub(1),
            blockhash: [0u8; 32],
            previous_blockhash: [0u8; 32],
            transaction_count: 5,
        }
    }

    // -----------------------------------------------------------------------
    // scan_incomplete_slots
    // -----------------------------------------------------------------------

    #[test]
    fn test_scan_empty_storage() {
        let (_storage, service, _tmp) = test_setup(
            HashSet::from(["handler_a".to_string()]),
            SolanaLivePipelineExpectations {
                expect_event_decode: true,
                ..Default::default()
            },
        );

        let result = service.scan_incomplete_slots().unwrap();
        assert!(result.is_empty());
        assert_eq!(result.total_slots(), 0);
    }

    #[test]
    fn test_scan_complete_slot() {
        let (storage, service, _tmp) = test_setup(
            HashSet::from(["handler_a".to_string()]),
            SolanaLivePipelineExpectations {
                expect_event_decode: true,
                expect_instruction_decode: true,
                expect_transformations: true,
                ..Default::default()
            },
        );

        storage.write_slot(100, &make_slot(100)).unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.events_extracted = true;
        status.instructions_extracted = true;
        status.events_decoded = true;
        status.instructions_decoded = true;
        status.accounts_read = true;
        status.accounts_decoded = true;
        status.transformed = true;
        status.completed_handlers.insert("handler_a".to_string());
        storage.write_status(100, &status).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_needs_block_fetch() {
        let (storage, service, _tmp) =
            test_setup(HashSet::new(), SolanaLivePipelineExpectations::default());

        storage.write_slot(200, &make_slot(200)).unwrap();

        let mut status = LiveSlotStatus::collected();
        // block_fetched is false by default
        storage.write_status(200, &status).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        assert_eq!(result.slots_needing_collection_resume.len(), 1);
        assert_eq!(
            result.slots_needing_collection_resume[0],
            SlotCollectionResumeRequest {
                slot: 200,
                stage: SlotCollectionResumeStage::FetchBlock,
            }
        );
        // Should not appear in any other bucket.
        assert!(result.slots_needing_event_decode.is_empty());
        assert!(result.slots_needing_instruction_decode.is_empty());
        assert!(result.slots_needing_transform.is_empty());
    }

    #[test]
    fn test_scan_needs_extraction() {
        let (storage, service, _tmp) =
            test_setup(HashSet::new(), SolanaLivePipelineExpectations::default());

        storage.write_slot(300, &make_slot(300)).unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        // events_extracted and instructions_extracted are false
        storage.write_status(300, &status).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        assert_eq!(result.slots_needing_collection_resume.len(), 1);
        assert_eq!(
            result.slots_needing_collection_resume[0].stage,
            SlotCollectionResumeStage::ExtractEventsAndInstructions,
        );
    }

    #[test]
    fn test_scan_needs_event_decode() {
        let (storage, service, _tmp) = test_setup(
            HashSet::new(),
            SolanaLivePipelineExpectations {
                expect_event_decode: true,
                ..Default::default()
            },
        );

        storage.write_slot(400, &make_slot(400)).unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.events_extracted = true;
        status.instructions_extracted = true;
        status.events_decoded = false;
        status.apply_expectations(&SolanaLivePipelineExpectations {
            expect_event_decode: true,
            ..Default::default()
        });
        storage.write_status(400, &status).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        assert!(result.slots_needing_collection_resume.is_empty());
        assert_eq!(result.slots_needing_event_decode, vec![400]);
    }

    #[test]
    fn test_scan_needs_instruction_decode() {
        let (storage, service, _tmp) = test_setup(
            HashSet::new(),
            SolanaLivePipelineExpectations {
                expect_instruction_decode: true,
                ..Default::default()
            },
        );

        storage.write_slot(500, &make_slot(500)).unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.events_extracted = true;
        status.instructions_extracted = true;
        status.instructions_decoded = false;
        status.apply_expectations(&SolanaLivePipelineExpectations {
            expect_instruction_decode: true,
            ..Default::default()
        });
        storage.write_status(500, &status).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        assert!(result.slots_needing_collection_resume.is_empty());
        assert_eq!(result.slots_needing_instruction_decode, vec![500]);
    }

    #[test]
    fn test_scan_needs_account_reads() {
        let (storage, service, _tmp) = test_setup(
            HashSet::new(),
            SolanaLivePipelineExpectations {
                expect_account_reads: true,
                ..Default::default()
            },
        );

        storage.write_slot(600, &make_slot(600)).unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.events_extracted = true;
        status.instructions_extracted = true;
        status.events_decoded = true;
        status.instructions_decoded = true;
        status.accounts_read = false;
        status.accounts_decoded = false;
        storage.write_status(600, &status).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        assert_eq!(result.slots_needing_account_reads, vec![600]);
    }

    #[test]
    fn test_scan_needs_transform_missing_handler() {
        let (storage, service, _tmp) = test_setup(
            HashSet::from(["handler_a".to_string(), "handler_b".to_string()]),
            SolanaLivePipelineExpectations {
                expect_event_decode: true,
                expect_instruction_decode: true,
                expect_transformations: true,
                ..Default::default()
            },
        );

        storage.write_slot(700, &make_slot(700)).unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.events_extracted = true;
        status.instructions_extracted = true;
        status.events_decoded = true;
        status.instructions_decoded = true;
        status.accounts_read = true;
        status.accounts_decoded = true;
        status.transformed = false;
        status.completed_handlers.insert("handler_a".to_string());
        storage.write_status(700, &status).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        assert!(result.slots_needing_collection_resume.is_empty());
        assert!(result.slots_needing_event_decode.is_empty());
        assert_eq!(result.slots_needing_transform.len(), 1);

        let (slot_num, missing) = &result.slots_needing_transform[0];
        assert_eq!(*slot_num, 700);
        assert_eq!(missing.len(), 1);
        assert!(missing.contains("handler_b"));
    }

    #[test]
    fn test_scan_skips_missing_status() {
        let (storage, service, _tmp) =
            test_setup(HashSet::new(), SolanaLivePipelineExpectations::default());

        // Write slot data but no status file.
        storage.write_slot(800, &make_slot(800)).unwrap();

        let result = service.scan_incomplete_slots().unwrap();
        // Should be skipped entirely.
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_ascending_order() {
        let (storage, service, _tmp) =
            test_setup(HashSet::new(), SolanaLivePipelineExpectations::default());

        // Write slots out of order, all needing block fetch.
        for &slot in &[903, 901, 902] {
            storage.write_slot(slot, &make_slot(slot)).unwrap();
            let status = LiveSlotStatus::collected();
            storage.write_status(slot, &status).unwrap();
        }

        let result = service.scan_incomplete_slots().unwrap();
        let slots: Vec<u64> = result
            .slots_needing_collection_resume
            .iter()
            .map(|r| r.slot)
            .collect();
        assert_eq!(slots, vec![901, 902, 903]);
    }

    // -----------------------------------------------------------------------
    // collection_resume_stage
    // -----------------------------------------------------------------------

    #[test]
    fn test_collection_resume_stage_fetch_block() {
        let (_, service, _tmp) = test_setup(HashSet::new(), Default::default());

        let status = LiveSlotStatus::collected();
        assert_eq!(
            service.collection_resume_stage(&status),
            Some(SlotCollectionResumeStage::FetchBlock),
        );
    }

    #[test]
    fn test_collection_resume_stage_extract() {
        let (_, service, _tmp) = test_setup(HashSet::new(), Default::default());

        let status = LiveSlotStatus {
            collected: true,
            block_fetched: true,
            events_extracted: false,
            instructions_extracted: false,
            ..Default::default()
        };
        assert_eq!(
            service.collection_resume_stage(&status),
            Some(SlotCollectionResumeStage::ExtractEventsAndInstructions),
        );
    }

    #[test]
    fn test_collection_resume_stage_partial_extract() {
        let (_, service, _tmp) = test_setup(HashSet::new(), Default::default());

        // Events extracted but instructions not.
        let status = LiveSlotStatus {
            collected: true,
            block_fetched: true,
            events_extracted: true,
            instructions_extracted: false,
            ..Default::default()
        };
        assert_eq!(
            service.collection_resume_stage(&status),
            Some(SlotCollectionResumeStage::ExtractEventsAndInstructions),
        );
    }

    #[test]
    fn test_collection_resume_stage_complete() {
        let (_, service, _tmp) = test_setup(HashSet::new(), Default::default());

        let status = LiveSlotStatus {
            collected: true,
            block_fetched: true,
            events_extracted: true,
            instructions_extracted: true,
            ..Default::default()
        };
        assert_eq!(service.collection_resume_stage(&status), None);
    }

    // -----------------------------------------------------------------------
    // get_missing_handlers
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_missing_handlers_all_missing() {
        let (_, service, _tmp) = test_setup(
            HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()]),
            Default::default(),
        );

        let status = LiveSlotStatus::default();
        let missing = service.get_missing_handlers(&status);
        assert_eq!(missing.len(), 3);
    }

    #[test]
    fn test_get_missing_handlers_some_complete() {
        let (_, service, _tmp) = test_setup(
            HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()]),
            Default::default(),
        );

        let mut status = LiveSlotStatus::default();
        status.completed_handlers.insert("a".to_string());
        status.completed_handlers.insert("c".to_string());

        let missing = service.get_missing_handlers(&status);
        assert_eq!(missing.len(), 1);
        assert!(missing.contains("b"));
    }

    #[test]
    fn test_get_missing_handlers_all_complete() {
        let (_, service, _tmp) = test_setup(HashSet::from(["x".to_string()]), Default::default());

        let mut status = LiveSlotStatus::default();
        status.completed_handlers.insert("x".to_string());

        let missing = service.get_missing_handlers(&status);
        assert!(missing.is_empty());
    }

    // -----------------------------------------------------------------------
    // reconstruct_missing_status_files
    // -----------------------------------------------------------------------

    #[test]
    fn test_reconstruct_no_missing_status() {
        let (storage, service, _tmp) =
            test_setup(HashSet::new(), SolanaLivePipelineExpectations::default());

        storage.write_slot(100, &make_slot(100)).unwrap();
        storage
            .write_status(100, &LiveSlotStatus::collected())
            .unwrap();

        let count = service.reconstruct_missing_status_files().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_reconstruct_missing_status_slot_only() {
        let (storage, service, _tmp) =
            test_setup(HashSet::new(), SolanaLivePipelineExpectations::default());

        // Only the slot file exists — no status.
        storage.write_slot(200, &make_slot(200)).unwrap();

        let count = service.reconstruct_missing_status_files().unwrap();
        assert_eq!(count, 1);

        let status = storage.read_status(200).unwrap();
        assert!(status.collected);
        assert!(status.block_fetched);
        // No events/instructions files, so extracted remains false.
        assert!(!status.events_extracted);
        assert!(!status.instructions_extracted);
        // With default expectations (all disabled), decode + transform are true.
        assert!(status.transformed);
    }

    #[test]
    fn test_reconstruct_status_with_events_and_instructions() {
        let (storage, service, _tmp) = test_setup(
            HashSet::new(),
            SolanaLivePipelineExpectations {
                expect_event_decode: true,
                expect_instruction_decode: true,
                ..Default::default()
            },
        );

        storage.write_slot(300, &make_slot(300)).unwrap();
        // Write empty events and instructions (they exist but are empty).
        storage.write_events(300, &[]).unwrap();
        storage.write_instructions(300, &[]).unwrap();

        let count = service.reconstruct_missing_status_files().unwrap();
        assert_eq!(count, 1);

        let status = storage.read_status(300).unwrap();
        assert!(status.collected);
        assert!(status.block_fetched);
        assert!(status.events_extracted);
        assert!(status.instructions_extracted);
        // Events and instructions are empty, so decoded is true.
        assert!(status.events_decoded);
        assert!(status.instructions_decoded);
    }

    #[test]
    fn test_reconstruct_status_with_accounts() {
        let (storage, service, _tmp) = test_setup(
            HashSet::new(),
            SolanaLivePipelineExpectations {
                expect_account_reads: true,
                ..Default::default()
            },
        );

        storage.write_slot(400, &make_slot(400)).unwrap();
        storage.write_events(400, &[]).unwrap();
        storage.write_instructions(400, &[]).unwrap();
        // Write empty accounts.
        storage.write_accounts(400, &[]).unwrap();

        let count = service.reconstruct_missing_status_files().unwrap();
        assert_eq!(count, 1);

        let status = storage.read_status(400).unwrap();
        assert!(status.accounts_read);
        assert!(status.accounts_decoded);
    }

    #[test]
    fn test_reconstruct_multiple_slots() {
        let (storage, service, _tmp) =
            test_setup(HashSet::new(), SolanaLivePipelineExpectations::default());

        // Three slots, only one has a status file.
        storage.write_slot(500, &make_slot(500)).unwrap();
        storage.write_slot(501, &make_slot(501)).unwrap();
        storage.write_slot(502, &make_slot(502)).unwrap();
        storage
            .write_status(501, &LiveSlotStatus::collected())
            .unwrap();

        let count = service.reconstruct_missing_status_files().unwrap();
        assert_eq!(count, 2);

        // 500 and 502 should have status now; 501 unchanged.
        assert!(storage.read_status(500).is_ok());
        assert!(storage.read_status(501).is_ok());
        assert!(storage.read_status(502).is_ok());
    }

    // -----------------------------------------------------------------------
    // SolanaCatchupScanResult helpers
    // -----------------------------------------------------------------------

    #[test]
    fn test_scan_result_is_empty() {
        let result = SolanaCatchupScanResult::default();
        assert!(result.is_empty());
        assert_eq!(result.total_slots(), 0);
    }

    #[test]
    fn test_scan_result_total_slots_deduplicates() {
        let result = SolanaCatchupScanResult {
            slots_needing_collection_resume: vec![SlotCollectionResumeRequest {
                slot: 100,
                stage: SlotCollectionResumeStage::FetchBlock,
            }],
            slots_needing_event_decode: vec![100, 200],
            slots_needing_instruction_decode: vec![200],
            slots_needing_account_reads: vec![300],
            slots_needing_transform: vec![(300, HashSet::from(["h".to_string()]))],
        };
        // Unique slots: 100, 200, 300
        assert_eq!(result.total_slots(), 3);
        assert!(!result.is_empty());
    }
}
