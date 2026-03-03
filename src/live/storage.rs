//! Storage for live mode block data.
//!
//! Uses bincode for fast serialization of block data, with JSON for status files
//! (debuggable).
//!
//! Directory structure:
//! ```text
//! data/live/{chain}/
//! ├── blocks/{block_number}.bin
//! ├── receipts/{block_number}.bin
//! ├── logs/{block_number}.bin
//! ├── eth_calls/{block_number}.bin
//! └── status/{block_number}.json
//! ```

use std::fs;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

use super::types::{
    LiveBlock, LiveBlockStatus, LiveDecodedCall, LiveDecodedEventCall,
    LiveDecodedLog, LiveDecodedOnceCall, LiveEthCall, LiveLog, LiveReceipt,
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Block not found: {0}")]
    NotFound(u64),
}

/// Storage manager for live mode block data.
#[derive(Debug, Clone)]
pub struct LiveStorage {
    base_dir: PathBuf,
}

impl LiveStorage {
    /// Create a new LiveStorage for the given chain.
    pub fn new(chain_name: &str) -> Self {
        let base_dir = PathBuf::from(format!("data/live/{}", chain_name));
        Self { base_dir }
    }

    /// Create a new LiveStorage with a custom base directory.
    pub fn with_base_dir(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Get the base directory for this storage.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Ensure all subdirectories exist.
    pub fn ensure_dirs(&self) -> Result<(), StorageError> {
        for subdir in &[
            "blocks",
            "receipts",
            "logs",
            "eth_calls",
            "status",
            "decoded/logs",
            "decoded/eth_calls",
        ] {
            fs::create_dir_all(self.base_dir.join(subdir))?;
        }
        Ok(())
    }

    // =========================================================================
    // Block operations
    // =========================================================================

    fn block_path(&self, block_number: u64) -> PathBuf {
        self.base_dir.join(format!("blocks/{}.bin", block_number))
    }

    /// Write a live block to storage.
    pub fn write_block(&self, block: &LiveBlock) -> Result<(), StorageError> {
        let path = self.block_path(block.number);
        write_bincode(&path, block)
    }

    /// Read a live block from storage.
    pub fn read_block(&self, block_number: u64) -> Result<LiveBlock, StorageError> {
        let path = self.block_path(block_number);
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// Delete a live block from storage.
    pub fn delete_block(&self, block_number: u64) -> Result<(), StorageError> {
        let path = self.block_path(block_number);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Check if a block exists in storage.
    pub fn block_exists(&self, block_number: u64) -> bool {
        self.block_path(block_number).exists()
    }

    /// List all block numbers in storage.
    pub fn list_blocks(&self) -> Result<Vec<u64>, StorageError> {
        list_block_numbers(&self.base_dir.join("blocks"))
    }

    // =========================================================================
    // Receipt operations
    // =========================================================================

    fn receipts_path(&self, block_number: u64) -> PathBuf {
        self.base_dir
            .join(format!("receipts/{}.bin", block_number))
    }

    /// Write receipts for a block.
    pub fn write_receipts(
        &self,
        block_number: u64,
        receipts: &[LiveReceipt],
    ) -> Result<(), StorageError> {
        let path = self.receipts_path(block_number);
        write_bincode_slice(&path, receipts)
    }

    /// Read receipts for a block.
    pub fn read_receipts(&self, block_number: u64) -> Result<Vec<LiveReceipt>, StorageError> {
        let path = self.receipts_path(block_number);
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// Delete receipts for a block.
    pub fn delete_receipts(&self, block_number: u64) -> Result<(), StorageError> {
        let path = self.receipts_path(block_number);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    // =========================================================================
    // Log operations
    // =========================================================================

    fn logs_path(&self, block_number: u64) -> PathBuf {
        self.base_dir.join(format!("logs/{}.bin", block_number))
    }

    /// Write logs for a block.
    pub fn write_logs(&self, block_number: u64, logs: &[LiveLog]) -> Result<(), StorageError> {
        let path = self.logs_path(block_number);
        write_bincode_slice(&path, logs)
    }

    /// Read logs for a block.
    pub fn read_logs(&self, block_number: u64) -> Result<Vec<LiveLog>, StorageError> {
        let path = self.logs_path(block_number);
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// Delete logs for a block.
    pub fn delete_logs(&self, block_number: u64) -> Result<(), StorageError> {
        let path = self.logs_path(block_number);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    // =========================================================================
    // Eth call operations
    // =========================================================================

    fn eth_calls_path(&self, block_number: u64) -> PathBuf {
        self.base_dir
            .join(format!("eth_calls/{}.bin", block_number))
    }

    /// Write eth call results for a block.
    pub fn write_eth_calls(
        &self,
        block_number: u64,
        calls: &[LiveEthCall],
    ) -> Result<(), StorageError> {
        let path = self.eth_calls_path(block_number);
        write_bincode_slice(&path, calls)
    }

    /// Read eth call results for a block.
    pub fn read_eth_calls(&self, block_number: u64) -> Result<Vec<LiveEthCall>, StorageError> {
        let path = self.eth_calls_path(block_number);
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// Delete eth call results for a block.
    pub fn delete_eth_calls(&self, block_number: u64) -> Result<(), StorageError> {
        let path = self.eth_calls_path(block_number);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    // =========================================================================
    // Status operations
    // =========================================================================

    fn status_path(&self, block_number: u64) -> PathBuf {
        self.base_dir.join(format!("status/{}.json", block_number))
    }

    /// Write status for a block.
    pub fn write_status(
        &self,
        block_number: u64,
        status: &LiveBlockStatus,
    ) -> Result<(), StorageError> {
        let path = self.status_path(block_number);
        let file = fs::File::create(&path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, status)?;
        Ok(())
    }

    /// Read status for a block.
    pub fn read_status(&self, block_number: u64) -> Result<LiveBlockStatus, StorageError> {
        let path = self.status_path(block_number);
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        let file = fs::File::open(&path)?;
        let reader = BufReader::new(file);
        let status = serde_json::from_reader(reader)?;
        Ok(status)
    }

    /// Delete status for a block.
    pub fn delete_status(&self, block_number: u64) -> Result<(), StorageError> {
        let path = self.status_path(block_number);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    // =========================================================================
    // Decoded log operations
    // =========================================================================

    fn decoded_logs_dir(&self, block_number: u64, contract_name: &str) -> PathBuf {
        self.base_dir
            .join(format!("decoded/logs/{}/{}", block_number, contract_name))
    }

    fn decoded_logs_path(
        &self,
        block_number: u64,
        contract_name: &str,
        event_name: &str,
    ) -> PathBuf {
        self.decoded_logs_dir(block_number, contract_name)
            .join(format!("{}.bin", event_name))
    }

    /// Write decoded logs for a specific event type.
    pub fn write_decoded_logs(
        &self,
        block_number: u64,
        contract_name: &str,
        event_name: &str,
        logs: &[LiveDecodedLog],
    ) -> Result<(), StorageError> {
        let path = self.decoded_logs_path(block_number, contract_name, event_name);
        write_bincode_slice(&path, logs)
    }

    /// Read decoded logs for a specific event type.
    pub fn read_decoded_logs(
        &self,
        block_number: u64,
        contract_name: &str,
        event_name: &str,
    ) -> Result<Vec<LiveDecodedLog>, StorageError> {
        let path = self.decoded_logs_path(block_number, contract_name, event_name);
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// Delete decoded logs for a specific event type.
    pub fn delete_decoded_logs(
        &self,
        block_number: u64,
        contract_name: &str,
        event_name: &str,
    ) -> Result<(), StorageError> {
        let path = self.decoded_logs_path(block_number, contract_name, event_name);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Delete all decoded logs for a block.
    pub fn delete_all_decoded_logs(&self, block_number: u64) -> Result<(), StorageError> {
        let dir = self.base_dir.join(format!("decoded/logs/{}", block_number));
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    /// List all (contract_name, event_name) pairs with decoded logs for a block.
    pub fn list_decoded_log_types(
        &self,
        block_number: u64,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let block_dir = self.base_dir.join(format!("decoded/logs/{}", block_number));
        if !block_dir.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for contract_entry in fs::read_dir(&block_dir)? {
            let contract_entry = contract_entry?;
            let contract_name = contract_entry
                .file_name()
                .to_string_lossy()
                .into_owned();

            if contract_entry.path().is_dir() {
                for event_entry in fs::read_dir(contract_entry.path())? {
                    let event_entry = event_entry?;
                    if let Some(stem) = event_entry.path().file_stem() {
                        let event_name = stem.to_string_lossy().into_owned();
                        results.push((contract_name.clone(), event_name));
                    }
                }
            }
        }

        Ok(results)
    }

    // =========================================================================
    // Decoded eth_call operations
    // =========================================================================

    fn decoded_calls_dir(&self, block_number: u64, contract_name: &str) -> PathBuf {
        self.base_dir
            .join(format!("decoded/eth_calls/{}/{}", block_number, contract_name))
    }

    fn decoded_calls_path(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
    ) -> PathBuf {
        self.decoded_calls_dir(block_number, contract_name)
            .join(format!("{}.bin", function_name))
    }

    /// Write decoded eth_calls for a specific function.
    pub fn write_decoded_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
        calls: &[LiveDecodedCall],
    ) -> Result<(), StorageError> {
        let path = self.decoded_calls_path(block_number, contract_name, function_name);
        write_bincode_slice(&path, calls)
    }

    /// Read decoded eth_calls for a specific function.
    pub fn read_decoded_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
    ) -> Result<Vec<LiveDecodedCall>, StorageError> {
        let path = self.decoded_calls_path(block_number, contract_name, function_name);
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// Delete decoded eth_calls for a specific function.
    pub fn delete_decoded_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
    ) -> Result<(), StorageError> {
        let path = self.decoded_calls_path(block_number, contract_name, function_name);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Delete all decoded eth_calls for a block.
    pub fn delete_all_decoded_calls(&self, block_number: u64) -> Result<(), StorageError> {
        let dir = self.base_dir.join(format!("decoded/eth_calls/{}", block_number));
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    /// Write decoded event-triggered eth_calls.
    pub fn write_decoded_event_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
        calls: &[LiveDecodedEventCall],
    ) -> Result<(), StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join(format!("{}_event.bin", function_name));
        write_bincode_slice(&path, calls)
    }

    /// Read decoded event-triggered eth_calls.
    pub fn read_decoded_event_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
    ) -> Result<Vec<LiveDecodedEventCall>, StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join(format!("{}_event.bin", function_name));
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// Write decoded "once" calls.
    pub fn write_decoded_once_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        calls: &[LiveDecodedOnceCall],
    ) -> Result<(), StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join("_once.bin");
        write_bincode_slice(&path, calls)
    }

    /// Read decoded "once" calls.
    pub fn read_decoded_once_calls(
        &self,
        block_number: u64,
        contract_name: &str,
    ) -> Result<Vec<LiveDecodedOnceCall>, StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join("_once.bin");
        if !path.exists() {
            return Err(StorageError::NotFound(block_number));
        }
        read_bincode(&path)
    }

    /// List all (contract_name, function_name) pairs with decoded calls for a block.
    pub fn list_decoded_call_types(
        &self,
        block_number: u64,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let block_dir = self.base_dir.join(format!("decoded/eth_calls/{}", block_number));
        if !block_dir.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for contract_entry in fs::read_dir(&block_dir)? {
            let contract_entry = contract_entry?;
            let contract_name = contract_entry
                .file_name()
                .to_string_lossy()
                .into_owned();

            if contract_entry.path().is_dir() {
                for func_entry in fs::read_dir(contract_entry.path())? {
                    let func_entry = func_entry?;
                    if let Some(stem) = func_entry.path().file_stem() {
                        let function_name = stem.to_string_lossy().into_owned();
                        // Skip special files
                        if !function_name.starts_with('_') {
                            results.push((contract_name.clone(), function_name));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    // =========================================================================
    // Bulk operations
    // =========================================================================

    /// Delete all data for a block (including decoded data).
    pub fn delete_all(&self, block_number: u64) -> Result<(), StorageError> {
        self.delete_block(block_number)?;
        self.delete_receipts(block_number)?;
        self.delete_logs(block_number)?;
        self.delete_eth_calls(block_number)?;
        self.delete_status(block_number)?;
        self.delete_all_decoded_logs(block_number)?;
        self.delete_all_decoded_calls(block_number)?;
        Ok(())
    }

    /// Delete all data for a range of blocks.
    pub fn delete_range(&self, start: u64, end: u64) -> Result<(), StorageError> {
        for block_number in start..=end {
            self.delete_all(block_number)?;
        }
        Ok(())
    }

    /// Get all blocks in a range that have complete status.
    pub fn get_complete_blocks_in_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<u64>, StorageError> {
        let mut complete = Vec::new();
        for block_number in start..=end {
            if let Ok(status) = self.read_status(block_number) {
                if status.is_complete() {
                    complete.push(block_number);
                }
            }
        }
        Ok(complete)
    }
}

// =========================================================================
// Helper functions
// =========================================================================

fn write_bincode<T: Serialize>(path: &Path, data: &T) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(path)?;
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, data)?;
    Ok(())
}

fn write_bincode_slice<T: Serialize>(path: &Path, data: &[T]) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(path)?;
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, data)?;
    Ok(())
}

fn read_bincode<T: DeserializeOwned>(path: &Path) -> Result<T, StorageError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let data = bincode::deserialize_from(reader)?;
    Ok(data)
}

fn list_block_numbers(dir: &Path) -> Result<Vec<u64>, StorageError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut blocks = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(stem) = path.file_stem() {
            if let Some(stem_str) = stem.to_str() {
                if let Ok(block_number) = stem_str.parse::<u64>() {
                    blocks.push(block_number);
                }
            }
        }
    }
    blocks.sort_unstable();
    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_storage() -> (LiveStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage = LiveStorage::with_base_dir(tmp.path().to_path_buf());
        storage.ensure_dirs().unwrap();
        (storage, tmp)
    }

    #[test]
    fn test_block_roundtrip() {
        let (storage, _tmp) = test_storage();

        let block = LiveBlock {
            number: 12345,
            hash: [1u8; 32],
            parent_hash: [2u8; 32],
            timestamp: 1234567890,
            tx_hashes: vec![[3u8; 32], [4u8; 32]],
        };

        storage.write_block(&block).unwrap();
        let read_block = storage.read_block(12345).unwrap();

        assert_eq!(read_block.number, block.number);
        assert_eq!(read_block.hash, block.hash);
        assert_eq!(read_block.tx_hashes.len(), 2);
    }

    #[test]
    fn test_status_roundtrip() {
        let (storage, _tmp) = test_storage();

        let status = LiveBlockStatus {
            collected: true,
            block_fetched: true,
            receipts_collected: true,
            logs_collected: false,
            decoded: false,
            transformed: false,
        };

        storage.write_status(100, &status).unwrap();
        let read_status = storage.read_status(100).unwrap();

        assert!(read_status.collected);
        assert!(read_status.block_fetched);
        assert!(!read_status.logs_collected);
    }

    #[test]
    fn test_list_blocks() {
        let (storage, _tmp) = test_storage();

        for num in [100, 102, 101] {
            let block = LiveBlock {
                number: num,
                hash: [0u8; 32],
                parent_hash: [0u8; 32],
                timestamp: 0,
                tx_hashes: vec![],
            };
            storage.write_block(&block).unwrap();
        }

        let blocks = storage.list_blocks().unwrap();
        assert_eq!(blocks, vec![100, 101, 102]);
    }

    #[test]
    fn test_delete_all() {
        let (storage, _tmp) = test_storage();

        let block = LiveBlock {
            number: 999,
            hash: [0u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 0,
            tx_hashes: vec![],
        };
        storage.write_block(&block).unwrap();
        storage.write_status(999, &LiveBlockStatus::default()).unwrap();

        assert!(storage.block_exists(999));
        storage.delete_all(999).unwrap();
        assert!(!storage.block_exists(999));
    }
}
