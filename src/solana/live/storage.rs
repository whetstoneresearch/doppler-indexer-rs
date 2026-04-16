//! Storage for Solana live mode slot data.
//!
//! Uses bincode for fast serialization, with JSON for status files.
//! Directory structure:
//! ```text
//! data/{chain}/live/
//! ├── raw/
//! │   ├── slots/{slot}.bin
//! │   ├── transactions/{slot}.bin
//! │   ├── events/{slot}.bin
//! │   ├── instructions/{slot}.bin
//! │   └── accounts/{slot}.bin
//! ├── decoded/
//! │   ├── events/{slot}/{source}/{event_name}.bin
//! │   ├── instructions/{slot}/{source}/{instruction_name}.bin
//! │   └── accounts/{slot}/{source}/{account_type}.bin
//! ├── snapshots/{slot}.bin
//! └── status/{slot}.json
//! ```

use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::live::bincode_io::{
    atomic_write_with, is_temp_file, list_numbered_entries, map_io_not_found, map_not_found,
    read_bincode, safe_delete, safe_delete_dir_all, write_bincode, StorageError,
};
use crate::live::LiveUpsertSnapshot;
use crate::solana::raw_data::types::{SolanaEventRecord, SolanaInstructionRecord};
use crate::storage_entity;

use super::types::{LiveAccountRead, LiveSlot, LiveSlotStatus, LiveTransaction};

/// Storage manager for Solana live mode slot data.
#[derive(Debug, Clone)]
pub struct SolanaLiveStorage {
    base_dir: PathBuf,
    /// When true, status writes call sync_all() for crash durability.
    /// Data writes (slots, transactions, etc.) never sync -- they are
    /// rebuildable, and the status file is the crash-safety gatekeeper.
    /// Controlled by `DOPPLER_DURABLE_WRITES` env var (default: true).
    durable_writes: bool,
}

impl SolanaLiveStorage {
    /// Create a new SolanaLiveStorage for the given chain.
    pub fn new(chain_name: &str) -> Self {
        let base_dir = PathBuf::from(format!("data/{}/live", chain_name));
        let durable_writes = std::env::var("DOPPLER_DURABLE_WRITES")
            .map(|v| v != "false" && v != "0")
            .unwrap_or(true);
        Self {
            base_dir,
            durable_writes,
        }
    }

    /// Create a new SolanaLiveStorage with a custom base directory.
    #[allow(dead_code)]
    pub fn with_base_dir(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            durable_writes: true,
        }
    }

    /// Get the base directory for this storage.
    #[allow(dead_code)]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Ensure all subdirectories exist and clean up leftover .tmp files.
    pub fn ensure_dirs(&self) -> Result<(), StorageError> {
        let subdirs = [
            "raw/slots",
            "raw/transactions",
            "raw/events",
            "raw/instructions",
            "raw/accounts",
            "decoded/events",
            "decoded/instructions",
            "decoded/accounts",
            "snapshots",
            "status",
        ];

        for subdir in &subdirs {
            fs::create_dir_all(self.base_dir.join(subdir))?;
        }

        // Clean up leftover .tmp files from interrupted writes
        self.cleanup_temp_files(&subdirs)?;

        Ok(())
    }

    /// Clean up leftover temp files from interrupted writes.
    /// Matches files with `.tmp.{random}` suffix pattern.
    fn cleanup_temp_files(&self, subdirs: &[&str]) -> Result<(), StorageError> {
        for subdir in subdirs {
            let dir = self.base_dir.join(subdir);
            if !dir.exists() {
                continue;
            }

            if let Ok(entries) = fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if is_temp_file(&path) {
                        tracing::debug!("Cleaning up leftover temp file: {:?}", path);
                        let _ = fs::remove_file(&path);
                    }
                    // Recursively clean subdirectories (for decoded/{category}/{slot}/{source})
                    if path.is_dir() {
                        self.cleanup_temp_files_recursive(&path)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Recursively clean up temp files in nested directories.
    fn cleanup_temp_files_recursive(&self, dir: &Path) -> Result<(), StorageError> {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    self.cleanup_temp_files_recursive(&path)?;
                } else if is_temp_file(&path) {
                    tracing::debug!("Cleaning up leftover temp file: {:?}", path);
                    let _ = fs::remove_file(&path);
                }
            }
        }
        Ok(())
    }

    // =========================================================================
    // Slot operations (manual — uses `ref` pattern for single struct)
    // =========================================================================

    storage_entity!(ref slot, LiveSlot, "raw/slots");

    /// List all slot numbers in storage, sorted ascending.
    pub fn list_slots(&self) -> Result<Vec<u64>, StorageError> {
        list_numbered_entries(&self.base_dir.join("raw/slots"))
    }

    /// List all slot numbers that have status files, sorted ascending.
    ///
    /// This includes skipped slots (which write only a status file, no slot
    /// data) and is used by `find_gaps()` to avoid false positives.
    pub fn list_statuses(&self) -> Result<Vec<u64>, StorageError> {
        list_numbered_entries(&self.base_dir.join("status"))
    }

    /// Check if a slot exists in storage.
    #[allow(dead_code)]
    pub fn slot_exists(&self, slot: u64) -> bool {
        self.slot_path(slot).exists()
    }

    // =========================================================================
    // Transaction operations (macro-generated)
    // =========================================================================

    storage_entity!(slice transactions, LiveTransaction, "raw/transactions");

    // =========================================================================
    // Event operations (macro-generated)
    // =========================================================================

    storage_entity!(slice events, SolanaEventRecord, "raw/events");

    // =========================================================================
    // Instruction operations (macro-generated)
    // =========================================================================

    storage_entity!(slice instructions, SolanaInstructionRecord, "raw/instructions");

    // =========================================================================
    // Account read operations (macro-generated)
    // =========================================================================

    storage_entity!(slice accounts, LiveAccountRead, "raw/accounts");

    // =========================================================================
    // Status operations (manual, JSON format for debuggability)
    // =========================================================================

    fn status_path(&self, slot: u64) -> PathBuf {
        self.base_dir.join(format!("status/{}.json", slot))
    }

    /// Write status for a slot. Syncs to disk when durable_writes is enabled.
    pub fn write_status(&self, slot: u64, status: &LiveSlotStatus) -> Result<(), StorageError> {
        let path = self.status_path(slot);
        atomic_write_with(&path, self.durable_writes, |writer| {
            serde_json::to_writer_pretty(writer, status)?;
            Ok(())
        })
    }

    /// Read status for a slot.
    pub fn read_status(&self, slot: u64) -> Result<LiveSlotStatus, StorageError> {
        let path = self.status_path(slot);
        let file = fs::File::open(&path).map_err(|e| map_io_not_found(e, slot))?;
        let reader = BufReader::new(file);
        let status = serde_json::from_reader(reader)?;
        Ok(status)
    }

    /// Atomically update status for a slot using a closure.
    ///
    /// This provides atomic read-modify-write semantics by using file locking
    /// to prevent concurrent updates from overwriting each other's changes.
    /// The closure receives the current status and can modify it in place.
    ///
    /// If the status file doesn't exist, returns NotFound error.
    pub fn update_status_atomic<F>(&self, slot: u64, update_fn: F) -> Result<(), StorageError>
    where
        F: FnOnce(&mut LiveSlotStatus),
    {
        use fs2::FileExt;

        let path = self.status_path(slot);
        let lock_path = path.with_extension("json.lock");

        // Use a separate lock file so the status file has no open handles during rename.
        // This keeps the lock held through the entire read-modify-write-rename cycle
        // (no lost-update race) while keeping the rename target handle-free.
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&lock_path)
            .map_err(StorageError::Io)?;

        lock_file
            .lock_exclusive()
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;

        // Read current status while holding lock
        let data = fs::read(&path).map_err(|e| map_io_not_found(e, slot))?;
        let mut status: LiveSlotStatus = serde_json::from_slice(&data)?;

        // Apply the update
        update_fn(&mut status);

        // Write to temp file, then rename -- all while holding the lock
        atomic_write_with(&path, self.durable_writes, |writer| {
            serde_json::to_writer_pretty(writer, &status)?;
            Ok(())
        })
        // lock_file dropped here, releasing the exclusive lock
    }

    /// Delete status for a slot.
    pub fn delete_status(&self, slot: u64) -> Result<(), StorageError> {
        let path = self.status_path(slot);
        safe_delete(&path)?;
        safe_delete(&path.with_extension("json.lock"))
    }

    // =========================================================================
    // Decoded data operations (generic, nested paths)
    // =========================================================================

    /// Write decoded data for a specific category/source/name combination.
    ///
    /// Path: `decoded/{category}/{slot}/{source}/{name}.bin`
    pub fn write_decoded<T: Serialize>(
        &self,
        category: &str,
        slot: u64,
        source: &str,
        name: &str,
        data: &[T],
    ) -> Result<(), StorageError> {
        let path = self.base_dir.join(format!(
            "decoded/{}/{}/{}/{}.bin",
            category, slot, source, name
        ));
        write_bincode(&path, data)
    }

    /// Read decoded data for a specific category/source/name combination.
    ///
    /// Path: `decoded/{category}/{slot}/{source}/{name}.bin`
    pub fn read_decoded<T: DeserializeOwned>(
        &self,
        category: &str,
        slot: u64,
        source: &str,
        name: &str,
    ) -> Result<Vec<T>, StorageError> {
        let path = self.base_dir.join(format!(
            "decoded/{}/{}/{}/{}.bin",
            category, slot, source, name
        ));
        read_bincode(&path).map_err(|e| map_not_found(e, slot))
    }

    /// Delete all decoded data for a specific category and slot.
    ///
    /// Removes the entire `decoded/{category}/{slot}/` directory tree.
    pub fn delete_all_decoded(&self, category: &str, slot: u64) -> Result<(), StorageError> {
        safe_delete_dir_all(&self.base_dir.join(format!("decoded/{}/{}", category, slot)))
    }

    /// List all (source, name) pairs with decoded data for a category and slot.
    pub fn list_decoded_types(
        &self,
        category: &str,
        slot: u64,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let slot_dir = self.base_dir.join(format!("decoded/{}/{}", category, slot));
        if !slot_dir.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for source_entry in fs::read_dir(&slot_dir)? {
            let source_entry = source_entry?;
            let source_name = source_entry.file_name().to_string_lossy().into_owned();

            if source_entry.path().is_dir() {
                for file_entry in fs::read_dir(source_entry.path())? {
                    let file_entry = file_entry?;
                    if let Some(stem) = file_entry.path().file_stem() {
                        let name = stem.to_string_lossy().into_owned();
                        results.push((source_name.clone(), name));
                    }
                }
            }
        }

        Ok(results)
    }

    // =========================================================================
    // Snapshot operations (for reorg rollback)
    // =========================================================================

    fn snapshots_path(&self, slot: u64) -> PathBuf {
        self.base_dir.join(format!("snapshots/{}.bin", slot))
    }

    /// Write upsert snapshots for a slot. No-op when the slice is empty.
    pub fn write_snapshots(
        &self,
        slot: u64,
        snapshots: &[LiveUpsertSnapshot],
    ) -> Result<(), StorageError> {
        if snapshots.is_empty() {
            return Ok(());
        }
        write_bincode(&self.snapshots_path(slot), snapshots)
    }

    /// Read upsert snapshots for a slot.
    pub fn read_snapshots(&self, slot: u64) -> Result<Vec<LiveUpsertSnapshot>, StorageError> {
        read_bincode(&self.snapshots_path(slot)).map_err(|e| map_not_found(e, slot))
    }

    /// Delete upsert snapshots for a slot.
    pub fn delete_snapshots(&self, slot: u64) -> Result<(), StorageError> {
        safe_delete(&self.snapshots_path(slot))
    }

    // =========================================================================
    // Reorg detection helpers
    // =========================================================================

    /// Get recent slots for seeding the reorg detector on restart.
    ///
    /// Returns up to `count` most recent slots.
    pub fn get_recent_slots_for_reorg(&self, count: u64) -> Result<Vec<LiveSlot>, StorageError> {
        let slots = self.list_slots()?;
        let start = slots.len().saturating_sub(count as usize);

        let mut result = Vec::with_capacity(slots.len() - start);
        for &slot_number in &slots[start..] {
            if let Ok(slot) = self.read_slot(slot_number) {
                result.push(slot);
            }
        }

        Ok(result)
    }

    /// Get the maximum slot number in storage.
    pub fn max_slot_number(&self) -> Result<Option<u64>, StorageError> {
        Ok(self.list_slots()?.into_iter().max())
    }

    /// Find gaps between stored slots.
    ///
    /// Returns ranges of missing slot numbers as (start, end) tuples (inclusive).
    /// Unions slot data files with status files so that skipped Solana slots
    /// (which write only a status file, no slot data) are not reported as gaps.
    pub fn find_gaps(&self) -> Result<Vec<(u64, u64)>, StorageError> {
        let mut all_known = std::collections::BTreeSet::new();
        all_known.extend(self.list_slots()?);
        all_known.extend(self.list_statuses()?);

        let slots: Vec<u64> = all_known.into_iter().collect();
        if slots.len() < 2 {
            return Ok(Vec::new());
        }

        let mut gaps = Vec::new();
        for window in slots.windows(2) {
            if window[1] > window[0] + 1 {
                gaps.push((window[0] + 1, window[1] - 1));
            }
        }
        Ok(gaps)
    }

    // =========================================================================
    // Bulk operations
    // =========================================================================

    /// Delete all data for a slot (including decoded data and snapshots).
    pub fn delete_all(&self, slot: u64) -> Result<(), StorageError> {
        self.delete_slot(slot)?;
        self.delete_transactions(slot)?;
        self.delete_events(slot)?;
        self.delete_instructions(slot)?;
        self.delete_accounts(slot)?;
        self.delete_status(slot)?;
        self.delete_all_decoded("events", slot)?;
        self.delete_all_decoded("instructions", slot)?;
        self.delete_all_decoded("accounts", slot)?;
        self.delete_snapshots(slot)?;
        Ok(())
    }

    /// Delete all data for a range of slots (inclusive).
    #[allow(dead_code)]
    pub fn delete_range(&self, start: u64, end: u64) -> Result<(), StorageError> {
        for slot in start..=end {
            self.delete_all(slot)?;
        }
        Ok(())
    }
}

// =========================================================================
// ProgressStatusStorage impl (for LiveProgressTracker)
// =========================================================================

impl crate::live::ProgressStatusStorage for SolanaLiveStorage {
    fn update_handler_completion(
        &self,
        number: u64,
        handler_key: &str,
        all_complete: bool,
    ) -> Result<(), StorageError> {
        let hk = handler_key.to_string();
        self.update_status_atomic(number, move |status| {
            status.completed_handlers.insert(hk);
            if all_complete {
                status.transformed = true;
            }
        })
    }

    fn mark_transformed(&self, number: u64) -> Result<(), StorageError> {
        self.update_status_atomic(number, |status| {
            status.transformed = true;
        })
    }

    fn read_handler_sets(
        &self,
        number: u64,
    ) -> Result<
        (
            std::collections::HashSet<String>,
            std::collections::HashSet<String>,
        ),
        StorageError,
    > {
        let status = self.read_status(number)?;
        Ok((status.failed_handlers, status.completed_handlers))
    }

    fn update_handler_sets_atomic(
        &self,
        number: u64,
        update_fn: &mut dyn FnMut(
            &mut std::collections::HashSet<String>,
            &mut std::collections::HashSet<String>,
            &mut bool,
        ),
    ) -> Result<(), StorageError> {
        self.update_status_atomic(number, |status| {
            update_fn(
                &mut status.completed_handlers,
                &mut status.failed_handlers,
                &mut status.transformed,
            );
        })
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_storage() -> (SolanaLiveStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage = SolanaLiveStorage::with_base_dir(tmp.path().to_path_buf());
        storage.ensure_dirs().unwrap();
        (storage, tmp)
    }

    #[test]
    fn test_slot_roundtrip() {
        let (storage, _tmp) = test_storage();

        let slot = LiveSlot {
            slot: 250_000_000,
            block_time: Some(1_700_000_000),
            block_height: Some(200_000_000),
            parent_slot: 249_999_999,
            blockhash: [0xAA; 32],
            previous_blockhash: [0xBB; 32],
            transaction_count: 42,
        };

        storage.write_slot(slot.slot, &slot).unwrap();
        let read_slot = storage.read_slot(250_000_000).unwrap();

        assert_eq!(read_slot.slot, slot.slot);
        assert_eq!(read_slot.block_time, slot.block_time);
        assert_eq!(read_slot.blockhash, slot.blockhash);
        assert_eq!(read_slot.transaction_count, 42);
    }

    #[test]
    fn test_transactions_roundtrip() {
        let (storage, _tmp) = test_storage();

        let txs = vec![
            LiveTransaction {
                slot: 100,
                block_time: Some(1_700_000_000),
                signature: [0xCC; 64],
                is_err: false,
                err_msg: None,
                fee: 5000,
                compute_units_consumed: Some(200_000),
                log_messages: vec!["Program log: hello".to_string()],
                account_keys: vec![[0xDD; 32]],
            },
            LiveTransaction {
                slot: 100,
                block_time: Some(1_700_000_000),
                signature: [0xEE; 64],
                is_err: true,
                err_msg: Some("InstructionError".to_string()),
                fee: 5000,
                compute_units_consumed: None,
                log_messages: vec![],
                account_keys: vec![],
            },
        ];

        storage.write_transactions(100, &txs).unwrap();
        let read_txs = storage.read_transactions(100).unwrap();

        assert_eq!(read_txs.len(), 2);
        assert_eq!(read_txs[0].signature, [0xCC; 64]);
        assert!(!read_txs[0].is_err);
        assert!(read_txs[1].is_err);
        assert_eq!(read_txs[1].err_msg, Some("InstructionError".to_string()));
    }

    #[test]
    fn test_status_roundtrip() {
        let (storage, _tmp) = test_storage();

        let mut status = LiveSlotStatus::default();
        status.collected = true;
        status.block_fetched = true;
        status.events_extracted = false;
        status.completed_handlers.insert("handler_a".to_string());

        storage.write_status(100, &status).unwrap();
        let read_status = storage.read_status(100).unwrap();

        assert!(read_status.collected);
        assert!(read_status.block_fetched);
        assert!(!read_status.events_extracted);
        assert!(read_status.completed_handlers.contains("handler_a"));
    }

    #[test]
    fn test_status_atomic_update() {
        let (storage, _tmp) = test_storage();

        let status = LiveSlotStatus::collected();
        storage.write_status(200, &status).unwrap();

        storage
            .update_status_atomic(200, |s| {
                s.block_fetched = true;
                s.events_extracted = true;
                s.completed_handlers.insert("handler_x".to_string());
            })
            .unwrap();

        let updated = storage.read_status(200).unwrap();
        assert!(updated.collected);
        assert!(updated.block_fetched);
        assert!(updated.events_extracted);
        assert!(!updated.instructions_extracted);
        assert!(updated.completed_handlers.contains("handler_x"));
    }

    #[test]
    fn test_list_slots() {
        let (storage, _tmp) = test_storage();

        for num in [300, 302, 301] {
            let slot = LiveSlot {
                slot: num,
                block_time: None,
                block_height: None,
                parent_slot: num.saturating_sub(1),
                blockhash: [0u8; 32],
                previous_blockhash: [0u8; 32],
                transaction_count: 0,
            };
            storage.write_slot(num, &slot).unwrap();
        }

        let slots = storage.list_slots().unwrap();
        assert_eq!(slots, vec![300, 301, 302]);
    }

    #[test]
    fn test_slot_exists() {
        let (storage, _tmp) = test_storage();

        assert!(!storage.slot_exists(999));

        let slot = LiveSlot {
            slot: 999,
            block_time: None,
            block_height: None,
            parent_slot: 998,
            blockhash: [0u8; 32],
            previous_blockhash: [0u8; 32],
            transaction_count: 0,
        };
        storage.write_slot(999, &slot).unwrap();

        assert!(storage.slot_exists(999));
    }

    #[test]
    fn test_ensure_dirs() {
        let tmp = TempDir::new().unwrap();
        let storage = SolanaLiveStorage::with_base_dir(tmp.path().to_path_buf());
        storage.ensure_dirs().unwrap();

        assert!(tmp.path().join("raw/slots").is_dir());
        assert!(tmp.path().join("raw/transactions").is_dir());
        assert!(tmp.path().join("raw/events").is_dir());
        assert!(tmp.path().join("raw/instructions").is_dir());
        assert!(tmp.path().join("raw/accounts").is_dir());
        assert!(tmp.path().join("decoded/events").is_dir());
        assert!(tmp.path().join("decoded/instructions").is_dir());
        assert!(tmp.path().join("decoded/accounts").is_dir());
        assert!(tmp.path().join("snapshots").is_dir());
        assert!(tmp.path().join("status").is_dir());
    }

    #[test]
    fn test_delete_all() {
        let (storage, _tmp) = test_storage();

        let slot = LiveSlot {
            slot: 500,
            block_time: None,
            block_height: None,
            parent_slot: 499,
            blockhash: [0u8; 32],
            previous_blockhash: [0u8; 32],
            transaction_count: 0,
        };
        storage.write_slot(500, &slot).unwrap();
        storage
            .write_status(500, &LiveSlotStatus::default())
            .unwrap();

        assert!(storage.slot_exists(500));
        storage.delete_all(500).unwrap();
        assert!(!storage.slot_exists(500));
    }

    #[test]
    fn test_find_gaps() {
        let (storage, _tmp) = test_storage();

        // Create slots with gaps: 100, 101, 105, 106, 110
        for num in [100, 101, 105, 106, 110] {
            let slot = LiveSlot {
                slot: num,
                block_time: None,
                block_height: None,
                parent_slot: num.saturating_sub(1),
                blockhash: [0u8; 32],
                previous_blockhash: [0u8; 32],
                transaction_count: 0,
            };
            storage.write_slot(num, &slot).unwrap();
        }

        let gaps = storage.find_gaps().unwrap();
        assert_eq!(gaps, vec![(102, 104), (107, 109)]);
    }

    #[test]
    fn test_find_gaps_empty() {
        let (storage, _tmp) = test_storage();
        let gaps = storage.find_gaps().unwrap();
        assert!(gaps.is_empty());
    }

    #[test]
    fn test_find_gaps_with_skipped_slots() {
        let (storage, _tmp) = test_storage();

        // Create slots 100, 101, 103, 104 with slot files
        for num in [100, 101, 103, 104] {
            let slot = LiveSlot {
                slot: num,
                block_time: None,
                block_height: None,
                parent_slot: num.saturating_sub(1),
                blockhash: [0u8; 32],
                previous_blockhash: [0u8; 32],
                transaction_count: 0,
            };
            storage.write_slot(num, &slot).unwrap();
        }

        // Slot 102 was skipped — write only a status file (no slot data)
        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.events_extracted = true;
        status.instructions_extracted = true;
        storage.write_status(102, &status).unwrap();

        // No gaps: slot 102 has a status file so find_gaps() sees it
        let gaps = storage.find_gaps().unwrap();
        assert!(gaps.is_empty(), "Expected no gaps but got {:?}", gaps);
    }

    #[test]
    fn test_max_slot_number() {
        let (storage, _tmp) = test_storage();

        assert_eq!(storage.max_slot_number().unwrap(), None);

        for num in [50, 100, 75] {
            let slot = LiveSlot {
                slot: num,
                block_time: None,
                block_height: None,
                parent_slot: num.saturating_sub(1),
                blockhash: [0u8; 32],
                previous_blockhash: [0u8; 32],
                transaction_count: 0,
            };
            storage.write_slot(num, &slot).unwrap();
        }

        assert_eq!(storage.max_slot_number().unwrap(), Some(100));
    }

    #[test]
    fn test_decoded_roundtrip() {
        let (storage, _tmp) = test_storage();

        let data: Vec<u64> = vec![1, 2, 3, 42];
        storage
            .write_decoded("events", 100, "my_program", "Transfer", &data)
            .unwrap();

        let read_data: Vec<u64> = storage
            .read_decoded("events", 100, "my_program", "Transfer")
            .unwrap();
        assert_eq!(read_data, vec![1, 2, 3, 42]);
    }

    #[test]
    fn test_decoded_list_types() {
        let (storage, _tmp) = test_storage();

        let data: Vec<u64> = vec![1];
        storage
            .write_decoded("events", 100, "program_a", "Transfer", &data)
            .unwrap();
        storage
            .write_decoded("events", 100, "program_a", "Mint", &data)
            .unwrap();
        storage
            .write_decoded("events", 100, "program_b", "Swap", &data)
            .unwrap();

        let mut types = storage.list_decoded_types("events", 100).unwrap();
        types.sort();

        assert_eq!(types.len(), 3);
        assert!(types.contains(&("program_a".to_string(), "Mint".to_string())));
        assert!(types.contains(&("program_a".to_string(), "Transfer".to_string())));
        assert!(types.contains(&("program_b".to_string(), "Swap".to_string())));
    }

    #[test]
    fn test_decoded_delete_all() {
        let (storage, _tmp) = test_storage();

        let data: Vec<u64> = vec![1];
        storage
            .write_decoded("events", 100, "program_a", "Transfer", &data)
            .unwrap();

        storage.delete_all_decoded("events", 100).unwrap();

        let types = storage.list_decoded_types("events", 100).unwrap();
        assert!(types.is_empty());
    }

    #[test]
    fn test_snapshots_empty_noop() {
        let (storage, _tmp) = test_storage();

        // Writing empty snapshots should be a no-op
        storage.write_snapshots(100, &[]).unwrap();

        // Reading should fail with NotFound since nothing was written
        let result = storage.read_snapshots(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_recent_slots_for_reorg() {
        let (storage, _tmp) = test_storage();

        for num in 100..110 {
            let slot = LiveSlot {
                slot: num,
                block_time: None,
                block_height: None,
                parent_slot: num.saturating_sub(1),
                blockhash: [num as u8; 32],
                previous_blockhash: [(num - 1) as u8; 32],
                transaction_count: 0,
            };
            storage.write_slot(num, &slot).unwrap();
        }

        let recent = storage.get_recent_slots_for_reorg(3).unwrap();
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].slot, 107);
        assert_eq!(recent[1].slot, 108);
        assert_eq!(recent[2].slot, 109);
    }
}
