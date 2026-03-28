//! Storage for live mode block data.
//!
//! Uses bincode for fast serialization of block data, with JSON for status files
//! (debuggable).
//!
//! All public methods are async, running blocking file I/O inside
//! `tokio::task::spawn_blocking` to avoid starving the async runtime.
//!
//! Directory structure:
//! ```text
//! data/{chain}/live/
//! ├── raw/
//! │   ├── blocks/{block_number}.bin
//! │   ├── receipts/{block_number}.bin
//! │   ├── logs/{block_number}.bin
//! │   └── eth_calls/{block_number}.bin
//! ├── decoded/
//! │   ├── logs/{block_number}/{contract}/{event}.bin
//! │   └── eth_calls/{block_number}/{contract}/{function}.bin
//! ├── factories/{block_number}.bin
//! ├── snapshots/{block_number}.bin
//! └── status/{block_number}.json
//! ```

use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

use super::types::{
    LiveBlock, LiveBlockStatus, LiveDecodedCall, LiveDecodedEventCall, LiveDecodedLog,
    LiveDecodedOnceCall, LiveEthCall, LiveFactoryAddresses, LiveLog, LiveReceipt,
    LiveUpsertSnapshot,
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
    #[error("Spawn blocking task failed: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

/// Generate path, write, read, and delete methods for a bincode storage entity.
///
/// All generated public methods are `async fn`, wrapping blocking I/O via
/// `spawn_blocking` with `write_bincode_sync` (which uses `serialize_into`
/// to stream directly to disk without an intermediate `Vec<u8>` buffer).
///
/// Two forms:
/// - `slice`: write takes `&[T]`, read returns `Vec<T>`
/// - `ref`: write takes `&T`, read returns `T`
macro_rules! storage_entity {
    // Slice variant: write_foo(block_number, &[T]), read_foo(block_number) -> Vec<T>
    (slice $name:ident, $type:ty, $subdir:expr) => {
        paste::paste! {
            fn [<$name _path>](&self, block_number: u64) -> PathBuf {
                self.base_dir.join(format!(concat!($subdir, "/{}.bin"), block_number))
            }

            pub async fn [<write_ $name>](
                &self,
                block_number: u64,
                data: &[$type],
            ) -> Result<(), StorageError> {
                let path = self.[<$name _path>](block_number);
                let owned = data.to_vec();
                tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
            }

            pub async fn [<read_ $name>](
                &self,
                block_number: u64,
            ) -> Result<Vec<$type>, StorageError> {
                read_bincode_async(&self.[<$name _path>](block_number))
                    .await
                    .map_err(|e| map_not_found(e, block_number))
            }

            pub async fn [<delete_ $name>](&self, block_number: u64) -> Result<(), StorageError> {
                safe_delete_async(&self.[<$name _path>](block_number)).await
            }
        }
    };
    // Ref variant: write_foo(block_number, &T), read_foo(block_number) -> T
    (ref $name:ident, $type:ty, $subdir:expr) => {
        paste::paste! {
            fn [<$name _path>](&self, block_number: u64) -> PathBuf {
                self.base_dir.join(format!(concat!($subdir, "/{}.bin"), block_number))
            }

            pub async fn [<write_ $name>](
                &self,
                block_number: u64,
                data: &$type,
            ) -> Result<(), StorageError> {
                let path = self.[<$name _path>](block_number);
                let owned = data.clone();
                tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
            }

            pub async fn [<read_ $name>](
                &self,
                block_number: u64,
            ) -> Result<$type, StorageError> {
                read_bincode_async(&self.[<$name _path>](block_number))
                    .await
                    .map_err(|e| map_not_found(e, block_number))
            }

            pub async fn [<delete_ $name>](&self, block_number: u64) -> Result<(), StorageError> {
                safe_delete_async(&self.[<$name _path>](block_number)).await
            }
        }
    };
}

/// Storage manager for live mode block data.
#[derive(Debug, Clone)]
pub struct LiveStorage {
    base_dir: PathBuf,
}

impl LiveStorage {
    /// Create a new LiveStorage for the given chain.
    pub fn new(chain_name: &str) -> Self {
        let base_dir = PathBuf::from(format!("data/{}/live", chain_name));
        Self { base_dir }
    }

    /// Create a new LiveStorage with a custom base directory.
    #[allow(dead_code)]
    pub fn with_base_dir(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Get the base directory for this storage.
    #[allow(dead_code)]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Ensure all subdirectories exist and clean up leftover .tmp files.
    pub async fn ensure_dirs(&self) -> Result<(), StorageError> {
        let base_dir = self.base_dir.clone();
        tokio::task::spawn_blocking(move || {
            let subdirs = [
                "raw/blocks",
                "raw/receipts",
                "raw/logs",
                "raw/eth_calls",
                "factories",
                "status",
                "decoded/logs",
                "decoded/eth_calls",
                "snapshots",
            ];

            for subdir in &subdirs {
                fs::create_dir_all(base_dir.join(subdir))?;
            }

            // Clean up leftover .tmp files from interrupted writes
            cleanup_temp_files(&base_dir, &subdirs)?;

            Ok(())
        })
        .await?
    }

    // =========================================================================
    // Block operations
    // =========================================================================

    fn block_path(&self, block_number: u64) -> PathBuf {
        self.base_dir
            .join(format!("raw/blocks/{}.bin", block_number))
    }

    /// Write a live block to storage.
    pub async fn write_block(&self, block: &LiveBlock) -> Result<(), StorageError> {
        let path = self.block_path(block.number);
        let owned = block.clone();
        tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
    }

    /// Read a live block from storage.
    pub async fn read_block(&self, block_number: u64) -> Result<LiveBlock, StorageError> {
        let path = self.block_path(block_number);
        read_bincode_async(&path)
            .await
            .map_err(|e| map_not_found(e, block_number))
    }

    /// Delete a live block from storage.
    pub async fn delete_block(&self, block_number: u64) -> Result<(), StorageError> {
        safe_delete_async(&self.block_path(block_number)).await
    }

    /// Check if a block exists in storage.
    #[allow(dead_code)]
    pub fn block_exists(&self, block_number: u64) -> bool {
        self.block_path(block_number).exists()
    }

    /// List all block numbers in storage.
    pub async fn list_blocks(&self) -> Result<Vec<u64>, StorageError> {
        list_block_numbers_async(&self.base_dir.join("raw/blocks")).await
    }

    // =========================================================================
    // Receipt operations (macro-generated)
    // =========================================================================

    storage_entity!(slice receipts, LiveReceipt, "raw/receipts");

    // =========================================================================
    // Log operations (macro-generated)
    // =========================================================================

    storage_entity!(slice logs, LiveLog, "raw/logs");

    // =========================================================================
    // Eth call operations (macro-generated)
    // =========================================================================

    storage_entity!(slice eth_calls, LiveEthCall, "raw/eth_calls");

    // =========================================================================
    // Factory address operations (macro-generated)
    // =========================================================================

    storage_entity!(ref factories, LiveFactoryAddresses, "factories");

    /// List all block numbers with factory address files.
    pub async fn list_factory_blocks(&self) -> Result<Vec<u64>, StorageError> {
        list_block_numbers_async(&self.base_dir.join("factories")).await
    }

    // =========================================================================
    // Status operations
    // =========================================================================

    fn status_path(&self, block_number: u64) -> PathBuf {
        self.base_dir.join(format!("status/{}.json", block_number))
    }

    /// Write status for a block.
    pub async fn write_status(
        &self,
        block_number: u64,
        status: &LiveBlockStatus,
    ) -> Result<(), StorageError> {
        let path = self.status_path(block_number);
        let status = status.clone();
        tokio::task::spawn_blocking(move || write_status_sync(&path, &status)).await?
    }

    /// Read status for a block.
    pub async fn read_status(&self, block_number: u64) -> Result<LiveBlockStatus, StorageError> {
        let path = self.status_path(block_number);
        tokio::task::spawn_blocking(move || read_status_sync(&path, block_number)).await?
    }

    /// Atomically update status for a block using a closure.
    ///
    /// This provides atomic read-modify-write semantics by using file locking
    /// to prevent concurrent updates from overwriting each other's changes.
    /// The closure receives the current status and can modify it in place.
    ///
    /// If the status file doesn't exist, returns NotFound error.
    ///
    /// The entire read-modify-write sequence runs in a single `spawn_blocking`
    /// call to maintain atomicity.
    pub async fn update_status_atomic<F>(
        &self,
        block_number: u64,
        update_fn: F,
    ) -> Result<(), StorageError>
    where
        F: FnOnce(&mut LiveBlockStatus) + Send + 'static,
    {
        let path = self.status_path(block_number);
        tokio::task::spawn_blocking(move || {
            update_status_atomic_sync(&path, block_number, update_fn)
        })
        .await?
    }

    /// Delete status for a block.
    pub async fn delete_status(&self, block_number: u64) -> Result<(), StorageError> {
        let path = self.status_path(block_number);
        let lock_path = path.with_extension("json.lock");
        safe_delete_async(&path).await?;
        safe_delete_async(&lock_path).await
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
    pub async fn write_decoded_logs(
        &self,
        block_number: u64,
        contract_name: &str,
        event_name: &str,
        logs: &[LiveDecodedLog],
    ) -> Result<(), StorageError> {
        let path = self.decoded_logs_path(block_number, contract_name, event_name);
        let owned = logs.to_vec();
        tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
    }

    /// Read decoded logs for a specific event type.
    #[allow(dead_code)]
    pub async fn read_decoded_logs(
        &self,
        block_number: u64,
        contract_name: &str,
        event_name: &str,
    ) -> Result<Vec<LiveDecodedLog>, StorageError> {
        let path = self.decoded_logs_path(block_number, contract_name, event_name);
        read_bincode_async(&path)
            .await
            .map_err(|e| map_not_found(e, block_number))
    }

    /// Delete decoded logs for a specific event type.
    #[allow(dead_code)]
    pub async fn delete_decoded_logs(
        &self,
        block_number: u64,
        contract_name: &str,
        event_name: &str,
    ) -> Result<(), StorageError> {
        safe_delete_async(&self.decoded_logs_path(block_number, contract_name, event_name)).await
    }

    /// Delete all decoded logs for a block.
    pub async fn delete_all_decoded_logs(&self, block_number: u64) -> Result<(), StorageError> {
        safe_delete_dir_all_async(&self.base_dir.join(format!("decoded/logs/{}", block_number)))
            .await
    }

    /// List all (contract_name, event_name) pairs with decoded logs for a block.
    pub async fn list_decoded_log_types(
        &self,
        block_number: u64,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let block_dir = self.base_dir.join(format!("decoded/logs/{}", block_number));
        tokio::task::spawn_blocking(move || list_decoded_types_sync(&block_dir)).await?
    }

    // =========================================================================
    // Decoded eth_call operations
    // =========================================================================

    fn decoded_calls_dir(&self, block_number: u64, contract_name: &str) -> PathBuf {
        self.base_dir.join(format!(
            "decoded/eth_calls/{}/{}",
            block_number, contract_name
        ))
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
    pub async fn write_decoded_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
        calls: &[LiveDecodedCall],
    ) -> Result<(), StorageError> {
        let path = self.decoded_calls_path(block_number, contract_name, function_name);
        let owned = calls.to_vec();
        tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
    }

    /// Read decoded eth_calls for a specific function.
    #[allow(dead_code)]
    pub async fn read_decoded_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
    ) -> Result<Vec<LiveDecodedCall>, StorageError> {
        let path = self.decoded_calls_path(block_number, contract_name, function_name);
        read_bincode_async(&path)
            .await
            .map_err(|e| map_not_found(e, block_number))
    }

    /// Delete decoded eth_calls for a specific function.
    #[allow(dead_code)]
    pub async fn delete_decoded_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
    ) -> Result<(), StorageError> {
        safe_delete_async(&self.decoded_calls_path(block_number, contract_name, function_name))
            .await
    }

    /// Delete all decoded eth_calls for a block.
    pub async fn delete_all_decoded_calls(&self, block_number: u64) -> Result<(), StorageError> {
        safe_delete_dir_all_async(
            &self
                .base_dir
                .join(format!("decoded/eth_calls/{}", block_number)),
        )
        .await
    }

    /// Write decoded event-triggered eth_calls.
    pub async fn write_decoded_event_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
        calls: &[LiveDecodedEventCall],
    ) -> Result<(), StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join(format!("{}_event.bin", function_name));
        let owned = calls.to_vec();
        tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
    }

    /// Read decoded event-triggered eth_calls.
    #[allow(dead_code)]
    pub async fn read_decoded_event_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        function_name: &str,
    ) -> Result<Vec<LiveDecodedEventCall>, StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join(format!("{}_event.bin", function_name));
        read_bincode_async(&path)
            .await
            .map_err(|e| map_not_found(e, block_number))
    }

    /// Write decoded "once" calls.
    pub async fn write_decoded_once_calls(
        &self,
        block_number: u64,
        contract_name: &str,
        calls: &[LiveDecodedOnceCall],
    ) -> Result<(), StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join("_once.bin");
        let owned = calls.to_vec();
        tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
    }

    /// Read decoded "once" calls.
    #[allow(dead_code)]
    pub async fn read_decoded_once_calls(
        &self,
        block_number: u64,
        contract_name: &str,
    ) -> Result<Vec<LiveDecodedOnceCall>, StorageError> {
        let path = self
            .decoded_calls_dir(block_number, contract_name)
            .join("_once.bin");
        read_bincode_async(&path)
            .await
            .map_err(|e| map_not_found(e, block_number))
    }

    /// List all (contract_name, function_name) pairs with decoded calls for a block.
    pub async fn list_decoded_call_types(
        &self,
        block_number: u64,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let block_dir = self
            .base_dir
            .join(format!("decoded/eth_calls/{}", block_number));
        tokio::task::spawn_blocking(move || list_decoded_types_with_filter_sync(&block_dir)).await?
    }

    // =========================================================================
    // Snapshot operations (for reorg rollback)
    // Manual: write_snapshots has an early-return-on-empty guard.
    // =========================================================================

    fn snapshots_path(&self, block_number: u64) -> PathBuf {
        self.base_dir
            .join(format!("snapshots/{}.bin", block_number))
    }

    /// Write upsert snapshots for a block. No-op when the slice is empty.
    pub async fn write_snapshots(
        &self,
        block_number: u64,
        snapshots: &[LiveUpsertSnapshot],
    ) -> Result<(), StorageError> {
        if snapshots.is_empty() {
            return Ok(());
        }
        let path = self.snapshots_path(block_number);
        let owned = snapshots.to_vec();
        tokio::task::spawn_blocking(move || write_bincode_sync(&path, &owned)).await?
    }

    /// Read upsert snapshots for a block.
    pub async fn read_snapshots(
        &self,
        block_number: u64,
    ) -> Result<Vec<LiveUpsertSnapshot>, StorageError> {
        let path = self.snapshots_path(block_number);
        read_bincode_async(&path)
            .await
            .map_err(|e| map_not_found(e, block_number))
    }

    /// Delete upsert snapshots for a block.
    pub async fn delete_snapshots(&self, block_number: u64) -> Result<(), StorageError> {
        safe_delete_async(&self.snapshots_path(block_number)).await
    }

    // =========================================================================
    // Reorg detection helpers
    // =========================================================================

    /// Get recent blocks for seeding the reorg detector on restart.
    ///
    /// Returns up to `count` most recent blocks for seeding the reorg detector.
    pub async fn get_recent_blocks_for_reorg(
        &self,
        count: u64,
    ) -> Result<Vec<LiveBlock>, StorageError> {
        let blocks = self.list_blocks().await?;
        let start = blocks.len().saturating_sub(count as usize);

        let mut result = Vec::with_capacity(blocks.len() - start);
        for &block_number in &blocks[start..] {
            if let Ok(block) = self.read_block(block_number).await {
                result.push(block);
            }
        }

        Ok(result)
    }

    /// Get the maximum block number in storage.
    pub async fn max_block_number(&self) -> Result<Option<u64>, StorageError> {
        Ok(self.list_blocks().await?.into_iter().max())
    }

    /// Find gaps (missing block numbers) in storage.
    /// Returns ranges of missing blocks as (start, end) tuples (inclusive).
    pub async fn find_gaps(&self) -> Result<Vec<(u64, u64)>, StorageError> {
        let blocks = self.list_blocks().await?;
        if blocks.len() < 2 {
            return Ok(Vec::new());
        }

        let mut gaps = Vec::new();
        for window in blocks.windows(2) {
            let current = window[0];
            let next = window[1];
            if next > current + 1 {
                // Gap found: blocks from current+1 to next-1 are missing
                gaps.push((current + 1, next - 1));
            }
        }
        Ok(gaps)
    }

    // =========================================================================
    // Bulk operations
    // =========================================================================

    /// Delete all data for a block (including decoded data and snapshots).
    pub async fn delete_all(&self, block_number: u64) -> Result<(), StorageError> {
        self.delete_block(block_number).await?;
        self.delete_receipts(block_number).await?;
        self.delete_logs(block_number).await?;
        self.delete_eth_calls(block_number).await?;
        self.delete_factories(block_number).await?;
        self.delete_status(block_number).await?;
        self.delete_all_decoded_logs(block_number).await?;
        self.delete_all_decoded_calls(block_number).await?;
        self.delete_snapshots(block_number).await?;
        Ok(())
    }

    /// Delete all data for a range of blocks.
    #[allow(dead_code)]
    pub async fn delete_range(&self, start: u64, end: u64) -> Result<(), StorageError> {
        for block_number in start..=end {
            self.delete_all(block_number).await?;
        }
        Ok(())
    }

    /// Get all blocks in a range that have complete status.
    #[allow(dead_code)]
    pub async fn get_complete_blocks_in_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<u64>, StorageError> {
        let mut complete = Vec::new();
        for block_number in start..=end {
            if let Ok(status) = self.read_status(block_number).await {
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

/// Synchronous helper: serialize data directly to disk via BufWriter (no intermediate
/// `Vec<u8>` buffer). Uses `serialize_into` to stream the serialized form straight to
/// the file, keeping peak RSS low for large artifacts.
///
/// All callers clone/own the data and move it into `spawn_blocking`, so serialization
/// never runs on the async runtime.
fn write_bincode_sync<T: Serialize + ?Sized>(path: &Path, data: &T) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let random_suffix: u32 = rand::random();
    let temp_name = format!(
        "{}.tmp.{}",
        path.file_name().unwrap().to_string_lossy(),
        random_suffix
    );
    let temp_path = path.with_file_name(temp_name);

    let file = fs::File::create(&temp_path)?;
    let mut writer = BufWriter::new(file);

    let result = (|| -> Result<(), StorageError> {
        bincode::serialize_into(&mut writer, data)?;
        writer.flush()?;
        writer
            .into_inner()
            .map_err(std::io::Error::other)?
            .sync_all()?;
        fs::rename(&temp_path, path)?;
        Ok(())
    })();

    if let Err(e) = &result {
        tracing::debug!(
            "Cleaning up temp file after write error: {:?} ({})",
            temp_path,
            e
        );
        let _ = fs::remove_file(&temp_path);
    }

    result
}

/// Async wrapper: read and deserialize bincode data from disk in a blocking task.
async fn read_bincode_async<T: DeserializeOwned + Send + 'static>(
    path: &Path,
) -> Result<T, StorageError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_bincode_sync(&path)).await?
}

/// Synchronous helper: read and deserialize bincode data from a file.
fn read_bincode_sync<T: DeserializeOwned>(path: &Path) -> Result<T, StorageError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let data = bincode::deserialize_from(reader)?;
    Ok(data)
}

/// Synchronous helper: write status JSON to file atomically.
fn write_status_sync(path: &Path, status: &LiveBlockStatus) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let random_suffix: u32 = rand::random();
    let temp_name = format!(
        "{}.tmp.{}",
        path.file_name().unwrap().to_string_lossy(),
        random_suffix
    );
    let temp_path = path.with_file_name(temp_name);

    let file = fs::File::create(&temp_path)?;
    let mut writer = BufWriter::new(file);

    let result = (|| -> Result<(), StorageError> {
        serde_json::to_writer_pretty(&mut writer, status)?;
        writer.flush()?;
        writer
            .into_inner()
            .map_err(std::io::Error::other)?
            .sync_all()?;
        fs::rename(&temp_path, path)?;
        Ok(())
    })();

    if let Err(e) = &result {
        tracing::debug!(
            "Cleaning up temp file after status write error: {:?} ({})",
            temp_path,
            e
        );
        let _ = fs::remove_file(&temp_path);
    }

    result
}

/// Synchronous helper: read status JSON from file.
fn read_status_sync(path: &Path, block_number: u64) -> Result<LiveBlockStatus, StorageError> {
    let file = fs::File::open(path).map_err(|e| map_io_not_found(e, block_number))?;
    let reader = BufReader::new(file);
    let status = serde_json::from_reader(reader)?;
    Ok(status)
}

/// Synchronous helper: atomic read-modify-write of status file.
fn update_status_atomic_sync<F>(
    path: &Path,
    block_number: u64,
    update_fn: F,
) -> Result<(), StorageError>
where
    F: FnOnce(&mut LiveBlockStatus),
{
    use fs2::FileExt;

    let lock_path = path.with_extension("json.lock");

    let lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .map_err(StorageError::Io)?;

    lock_file
        .lock_exclusive()
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;

    let data = fs::read(path).map_err(|e| map_io_not_found(e, block_number))?;
    let mut status: LiveBlockStatus = serde_json::from_slice(&data)?;

    update_fn(&mut status);

    let random_suffix: u32 = rand::random();
    let temp_name = format!(
        "{}.tmp.{}",
        path.file_name().unwrap().to_string_lossy(),
        random_suffix
    );
    let temp_path = path.with_file_name(temp_name);

    let result = (|| -> Result<(), StorageError> {
        {
            let temp_file = fs::File::create(&temp_path)?;
            let mut writer = BufWriter::new(temp_file);
            serde_json::to_writer_pretty(&mut writer, &status)?;
            writer.flush()?;
            writer
                .into_inner()
                .map_err(std::io::Error::other)?
                .sync_all()?;
        }

        fs::rename(&temp_path, path)?;
        Ok(())
    })();

    if let Err(e) = &result {
        tracing::debug!(
            "Cleaning up temp file after atomic status write error: {:?} ({})",
            temp_path,
            e
        );
        let _ = fs::remove_file(&temp_path);
    }

    result
}

/// Map IO NotFound errors to StorageError::NotFound for bincode operations.
fn map_not_found(err: StorageError, block_number: u64) -> StorageError {
    match err {
        StorageError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            StorageError::NotFound(block_number)
        }
        other => other,
    }
}

/// Map raw IO NotFound errors to StorageError::NotFound.
fn map_io_not_found(err: std::io::Error, block_number: u64) -> StorageError {
    if err.kind() == std::io::ErrorKind::NotFound {
        StorageError::NotFound(block_number)
    } else {
        StorageError::Io(err)
    }
}

/// Check if a file is a temp file (has `.tmp.{random}` suffix).
fn is_temp_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|name| name.contains(".tmp."))
}

/// Synchronous helper: list block numbers from a directory.
fn list_block_numbers_sync(dir: &Path) -> Result<Vec<u64>, StorageError> {
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

/// Async wrapper: list block numbers in a blocking task.
async fn list_block_numbers_async(dir: &Path) -> Result<Vec<u64>, StorageError> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || list_block_numbers_sync(&dir)).await?
}

/// Synchronous helper: list (parent, child) decoded type pairs from a two-level directory.
fn list_decoded_types_sync(block_dir: &Path) -> Result<Vec<(String, String)>, StorageError> {
    if !block_dir.exists() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    for contract_entry in fs::read_dir(block_dir)? {
        let contract_entry = contract_entry?;
        let contract_name = contract_entry.file_name().to_string_lossy().into_owned();

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

/// Synchronous helper: list decoded call types, filtering out special `_` prefixed files.
fn list_decoded_types_with_filter_sync(
    block_dir: &Path,
) -> Result<Vec<(String, String)>, StorageError> {
    if !block_dir.exists() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    for contract_entry in fs::read_dir(block_dir)? {
        let contract_entry = contract_entry?;
        let contract_name = contract_entry.file_name().to_string_lossy().into_owned();

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

/// Async wrapper: TOCTOU-safe file deletion using tokio::fs.
async fn safe_delete_async(path: &Path) -> Result<(), StorageError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// Async wrapper: TOCTOU-safe directory deletion using tokio::fs.
async fn safe_delete_dir_all_async(path: &Path) -> Result<(), StorageError> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// Clean up leftover temp files from interrupted writes.
fn cleanup_temp_files(base_dir: &Path, subdirs: &[&str]) -> Result<(), StorageError> {
    for subdir in subdirs {
        let dir = base_dir.join(subdir);
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
                // Recursively clean subdirectories (for decoded/logs/{block}/{contract})
                if path.is_dir() {
                    cleanup_temp_files_recursive(&path)?;
                }
            }
        }
    }
    Ok(())
}

/// Recursively clean up temp files in nested directories.
fn cleanup_temp_files_recursive(dir: &Path) -> Result<(), StorageError> {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                cleanup_temp_files_recursive(&path)?;
            } else if is_temp_file(&path) {
                tracing::debug!("Cleaning up leftover temp file: {:?}", path);
                let _ = fs::remove_file(&path);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn test_storage() -> (LiveStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage = LiveStorage::with_base_dir(tmp.path().to_path_buf());
        storage.ensure_dirs().await.unwrap();
        (storage, tmp)
    }

    #[tokio::test]
    async fn test_block_roundtrip() {
        let (storage, _tmp) = test_storage().await;

        let block = LiveBlock {
            number: 12345,
            hash: [1u8; 32],
            parent_hash: [2u8; 32],
            timestamp: 1234567890,
            tx_hashes: vec![[3u8; 32], [4u8; 32]],
        };

        storage.write_block(&block).await.unwrap();
        let read_block = storage.read_block(12345).await.unwrap();

        assert_eq!(read_block.number, block.number);
        assert_eq!(read_block.hash, block.hash);
        assert_eq!(read_block.tx_hashes.len(), 2);
    }

    #[tokio::test]
    async fn test_status_roundtrip() {
        let (storage, _tmp) = test_storage().await;

        let mut status = LiveBlockStatus::default();
        status.collected = true;
        status.block_fetched = true;
        status.receipts_collected = true;
        status.logs_collected = false;
        status.completed_handlers.insert("handler_a".to_string());

        storage.write_status(100, &status).await.unwrap();
        let read_status = storage.read_status(100).await.unwrap();

        assert!(read_status.collected);
        assert!(read_status.block_fetched);
        assert!(!read_status.logs_collected);
        assert!(read_status.completed_handlers.contains("handler_a"));
    }

    #[tokio::test]
    async fn test_list_blocks() {
        let (storage, _tmp) = test_storage().await;

        for num in [100, 102, 101] {
            let block = LiveBlock {
                number: num,
                hash: [0u8; 32],
                parent_hash: [0u8; 32],
                timestamp: 0,
                tx_hashes: vec![],
            };
            storage.write_block(&block).await.unwrap();
        }

        let blocks = storage.list_blocks().await.unwrap();
        assert_eq!(blocks, vec![100, 101, 102]);
    }

    #[tokio::test]
    async fn test_delete_all() {
        let (storage, _tmp) = test_storage().await;

        let block = LiveBlock {
            number: 999,
            hash: [0u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 0,
            tx_hashes: vec![],
        };
        storage.write_block(&block).await.unwrap();
        storage
            .write_status(999, &LiveBlockStatus::default())
            .await
            .unwrap();

        assert!(storage.block_exists(999));
        storage.delete_all(999).await.unwrap();
        assert!(!storage.block_exists(999));
    }

    #[tokio::test]
    async fn test_find_gaps() {
        let (storage, _tmp) = test_storage().await;

        // Create blocks with gaps: 100, 101, 105, 106, 110
        for num in [100, 101, 105, 106, 110] {
            let block = LiveBlock {
                number: num,
                hash: [0u8; 32],
                parent_hash: [0u8; 32],
                timestamp: 0,
                tx_hashes: vec![],
            };
            storage.write_block(&block).await.unwrap();
        }

        let gaps = storage.find_gaps().await.unwrap();
        // Gap 1: 102-104, Gap 2: 107-109
        assert_eq!(gaps, vec![(102, 104), (107, 109)]);
    }

    #[tokio::test]
    async fn test_find_gaps_no_gaps() {
        let (storage, _tmp) = test_storage().await;

        // Create consecutive blocks
        for num in 100..=105 {
            let block = LiveBlock {
                number: num,
                hash: [0u8; 32],
                parent_hash: [0u8; 32],
                timestamp: 0,
                tx_hashes: vec![],
            };
            storage.write_block(&block).await.unwrap();
        }

        let gaps = storage.find_gaps().await.unwrap();
        assert!(gaps.is_empty());
    }

    #[tokio::test]
    async fn test_find_gaps_empty_storage() {
        let (storage, _tmp) = test_storage().await;
        let gaps = storage.find_gaps().await.unwrap();
        assert!(gaps.is_empty());
    }

    #[tokio::test]
    async fn test_find_gaps_single_block() {
        let (storage, _tmp) = test_storage().await;

        let block = LiveBlock {
            number: 100,
            hash: [0u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 0,
            tx_hashes: vec![],
        };
        storage.write_block(&block).await.unwrap();

        let gaps = storage.find_gaps().await.unwrap();
        assert!(gaps.is_empty());
    }

    #[tokio::test]
    async fn test_logs_with_transaction_hash() {
        let (storage, _tmp) = test_storage().await;

        // Create a block first (required for logs)
        let block = LiveBlock {
            number: 1000,
            hash: [1u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 1234567890,
            tx_hashes: vec![[5u8; 32]],
        };
        storage.write_block(&block).await.unwrap();

        // Create logs with transaction_hash field
        let logs = vec![
            LiveLog {
                address: [0xAAu8; 20],
                topics: vec![[0xBBu8; 32]],
                data: vec![1, 2, 3, 4],
                log_index: 0,
                transaction_index: 0,
                transaction_hash: [0xCCu8; 32],
            },
            LiveLog {
                address: [0xDDu8; 20],
                topics: vec![[0xEEu8; 32], [0xFFu8; 32]],
                data: vec![5, 6, 7],
                log_index: 1,
                transaction_index: 0,
                transaction_hash: [0xCCu8; 32],
            },
        ];

        storage.write_logs(1000, &logs).await.unwrap();
        let read_logs = storage.read_logs(1000).await.unwrap();

        assert_eq!(read_logs.len(), 2);
        // Verify transaction_hash is correctly serialized and deserialized
        assert_eq!(read_logs[0].transaction_hash, [0xCCu8; 32]);
        assert_eq!(read_logs[1].transaction_hash, [0xCCu8; 32]);
        assert_eq!(read_logs[0].address, [0xAAu8; 20]);
        assert_eq!(read_logs[1].topics.len(), 2);
    }
}
