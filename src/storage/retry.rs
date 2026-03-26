//! Retry queue for failed S3 uploads.
//!
//! When an S3 upload fails, the item is queued for retry with exponential backoff.
//! The queue is persisted to disk for crash recovery.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::manifest::ManifestManager;
use super::s3::S3Backend;
use super::{StorageBackend, StorageError};
use crate::types::config::storage::SyncConfig;

/// A pending upload that failed and needs retry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingUpload {
    /// S3 key to upload to
    pub key: String,
    /// Path to local file containing the data
    pub local_path: PathBuf,
    /// Number of retry attempts so far
    pub attempts: u32,
    /// Unix timestamp of when this was first queued
    pub queued_at: u64,
    /// Unix timestamp of when to next attempt
    pub next_attempt_at: u64,
    /// Chain identifier for marker writing after successful retry
    #[serde(default)]
    pub chain: Option<String>,
    /// Data type identifier for marker writing after successful retry
    #[serde(default)]
    pub data_type: Option<String>,
    /// Block range start for marker writing after successful retry
    #[serde(default)]
    pub start: Option<u64>,
    /// Block range end for marker writing after successful retry
    #[serde(default)]
    pub end: Option<u64>,
}

impl PendingUpload {
    /// Create a new pending upload.
    pub fn new(key: String, local_path: PathBuf) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            key,
            local_path,
            attempts: 0,
            queued_at: now,
            next_attempt_at: now,
            chain: None,
            data_type: None,
            start: None,
            end: None,
        }
    }

    /// Calculate the next retry delay with exponential backoff.
    fn backoff_delay(&self, base_secs: u64) -> Duration {
        // Exponential backoff: base * 2^attempts, capped at 1 hour
        let delay_secs = base_secs * (1 << self.attempts.min(7));
        Duration::from_secs(delay_secs.min(3600))
    }

    /// Update for next retry attempt.
    pub fn schedule_retry(&mut self, base_secs: u64) {
        self.attempts += 1;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.next_attempt_at = now + self.backoff_delay(base_secs).as_secs();
    }

    /// Check if it's time to retry.
    pub fn is_due(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now >= self.next_attempt_at
    }
}

/// Queue of failed uploads awaiting retry.
pub struct RetryQueue {
    /// Pending uploads
    queue: RwLock<VecDeque<PendingUpload>>,
    /// Path to persistence file
    persistence_path: PathBuf,
    /// Configuration
    config: SyncConfig,
    /// S3 backend for retries
    s3: S3Backend,
    /// Manifest manager for writing markers after successful retry uploads
    manifest_manager: std::sync::RwLock<Option<Arc<ManifestManager>>>,
}

#[allow(dead_code)]
impl RetryQueue {
    /// Create a new RetryQueue.
    pub fn new(local_base: PathBuf, config: SyncConfig, s3: S3Backend) -> Self {
        let persistence_path = local_base.join(".upload_queue.json");

        Self {
            queue: RwLock::new(VecDeque::new()),
            persistence_path,
            config,
            s3,
            manifest_manager: std::sync::RwLock::new(None),
        }
    }

    /// Load the queue from disk.
    pub async fn load(&self) -> Result<(), StorageError> {
        let data = match tokio::fs::read(&self.persistence_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(StorageError::Io(e)),
        };

        let items: Vec<PendingUpload> = serde_json::from_slice(&data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let mut queue = self.queue.write().await;
        queue.clear();
        queue.extend(items);

        tracing::info!("Loaded {} pending uploads from queue", queue.len());

        Ok(())
    }

    /// Save the queue to disk.
    pub async fn save(&self) -> Result<(), StorageError> {
        let queue = self.queue.read().await;
        let items: Vec<_> = queue.iter().cloned().collect();

        let data = serde_json::to_vec_pretty(&items)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Atomic write
        let tmp_path = self.persistence_path.with_extension("tmp");
        tokio::fs::write(&tmp_path, data).await?;
        tokio::fs::rename(&tmp_path, &self.persistence_path).await?;

        Ok(())
    }

    /// Set the manifest manager for writing markers after successful retries.
    ///
    /// This is called after construction because the ManifestManager is created
    /// after the RetryQueue in StorageManager::new().
    pub fn set_manifest_manager(&self, mm: Arc<ManifestManager>) {
        let mut guard = self.manifest_manager.write().unwrap();
        *guard = Some(mm);
    }

    /// Add a failed upload to the queue and persist immediately for crash safety.
    pub async fn enqueue(&self, key: String, local_path: PathBuf) -> Result<(), StorageError> {
        {
            let mut queue = self.queue.write().await;
            let pending = PendingUpload::new(key, local_path);
            queue.push_back(pending);
            tracing::warn!("Queued upload for retry: {} items in queue", queue.len());
        }
        self.save().await?;
        Ok(())
    }

    /// Add a failed upload with marker metadata so a marker is written after successful retry.
    pub async fn enqueue_with_marker(
        &self,
        key: String,
        local_path: PathBuf,
        chain: String,
        data_type: String,
        start: u64,
        end: u64,
    ) -> Result<(), StorageError> {
        {
            let mut queue = self.queue.write().await;
            let mut pending = PendingUpload::new(key, local_path);
            pending.chain = Some(chain);
            pending.data_type = Some(data_type);
            pending.start = Some(start);
            pending.end = Some(end);
            queue.push_back(pending);
            tracing::warn!(
                "Queued upload for retry (with marker): {} items in queue",
                queue.len()
            );
        }
        self.save().await?;
        Ok(())
    }

    /// Get the number of pending uploads.
    pub async fn len(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Check if the queue is empty.
    pub async fn is_empty(&self) -> bool {
        self.queue.read().await.is_empty()
    }

    /// Process due retries.
    ///
    /// Returns the number of successful retries.
    pub async fn process_due(&self) -> Result<usize, StorageError> {
        let mut successful = 0;

        loop {
            // Get next due item
            let item = {
                let mut queue = self.queue.write().await;
                // Find first due item
                queue
                    .iter()
                    .position(|p| p.is_due())
                    .map(|pos| queue.remove(pos).unwrap())
            };

            let Some(mut pending) = item else {
                break;
            };

            // Check max retries
            if pending.attempts >= self.config.max_retries {
                tracing::error!(
                    "Upload {} exceeded max retries ({}), dropping",
                    pending.key,
                    self.config.max_retries
                );
                continue;
            }

            // Try to upload
            match self.retry_upload(&pending).await {
                Ok(()) => {
                    tracing::info!(
                        "Retry upload succeeded for {} (attempt {})",
                        pending.key,
                        pending.attempts + 1
                    );

                    // Write marker if metadata is available
                    let mm = self.manifest_manager.read().unwrap().clone();
                    if let (Some(ref mm), Some(ref chain), Some(ref dt), Some(start), Some(end)) = (
                        &mm,
                        &pending.chain,
                        &pending.data_type,
                        pending.start,
                        pending.end,
                    ) {
                        if let Err(e) = mm
                            .write_marker(chain, dt, start, end, &pending.key, "retry")
                            .await
                        {
                            tracing::warn!(
                                "Failed to write marker after retry upload for {}: {}",
                                pending.key,
                                e
                            );
                        }
                    }

                    successful += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        "Retry upload failed for {} (attempt {}): {}",
                        pending.key,
                        pending.attempts + 1,
                        e
                    );

                    // Schedule next retry
                    pending.schedule_retry(self.config.retry_interval_secs);

                    let mut queue = self.queue.write().await;
                    queue.push_back(pending);
                }
            }
        }

        // Persist queue state
        self.save().await?;

        Ok(successful)
    }

    /// Attempt to upload a pending item.
    async fn retry_upload(&self, pending: &PendingUpload) -> Result<(), StorageError> {
        // Read data from local file
        let data = tokio::fs::read(&pending.local_path).await?;

        // Upload to S3
        self.s3.write(&pending.key, &data).await?;

        Ok(())
    }

    /// Run the retry loop as a background task.
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::oneshot::Receiver<()>) {
        let interval = Duration::from_secs(self.config.retry_interval_secs);

        tracing::info!("Retry queue started with interval {:?}", interval);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.process_due().await {
                        tracing::error!("Retry queue error: {}", e);
                    }
                }
                _ = &mut shutdown => {
                    tracing::info!("Retry queue shutting down");
                    // Final save
                    let _ = self.save().await;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_upload_backoff() {
        let mut pending = PendingUpload::new("test".to_string(), PathBuf::from("/tmp/test"));

        assert_eq!(pending.backoff_delay(30), Duration::from_secs(30));

        pending.attempts = 1;
        assert_eq!(pending.backoff_delay(30), Duration::from_secs(60));

        pending.attempts = 2;
        assert_eq!(pending.backoff_delay(30), Duration::from_secs(120));

        // Cap at 1 hour
        pending.attempts = 10;
        assert_eq!(pending.backoff_delay(30), Duration::from_secs(3600));
    }

    #[test]
    fn test_pending_upload_is_due() {
        let mut pending = PendingUpload::new("test".to_string(), PathBuf::from("/tmp/test"));

        // Newly created should be due immediately
        assert!(pending.is_due());

        // After scheduling retry with delay, should not be due
        pending.next_attempt_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;
        assert!(!pending.is_due());
    }

    #[test]
    fn test_pending_upload_with_marker_fields_serialization() {
        let mut pending = PendingUpload::new(
            "eth/historical/raw/blocks/blocks_0-999.parquet".to_string(),
            PathBuf::from("/tmp/blocks.parquet"),
        );
        pending.chain = Some("ethereum".to_string());
        pending.data_type = Some("raw/blocks".to_string());
        pending.start = Some(0);
        pending.end = Some(999);

        let json = serde_json::to_string(&pending).unwrap();
        let deserialized: PendingUpload = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.chain.as_deref(), Some("ethereum"));
        assert_eq!(deserialized.data_type.as_deref(), Some("raw/blocks"));
        assert_eq!(deserialized.start, Some(0));
        assert_eq!(deserialized.end, Some(999));
        assert_eq!(deserialized.key, pending.key);
    }

    #[test]
    fn test_pending_upload_without_marker_fields_backwards_compat() {
        // Simulate an old-format JSON without the marker fields
        let old_json = r#"{
            "key": "test/file.parquet",
            "local_path": "/tmp/test",
            "attempts": 2,
            "queued_at": 1000000,
            "next_attempt_at": 1000060
        }"#;

        let deserialized: PendingUpload = serde_json::from_str(old_json).unwrap();

        assert_eq!(deserialized.key, "test/file.parquet");
        assert_eq!(deserialized.attempts, 2);
        assert!(deserialized.chain.is_none());
        assert!(deserialized.data_type.is_none());
        assert!(deserialized.start.is_none());
        assert!(deserialized.end.is_none());
    }

    fn create_test_retry_queue(base_path: PathBuf) -> RetryQueue {
        let store = Arc::new(object_store::memory::InMemory::new());
        let s3 = S3Backend::from_store(store, "test-bucket".to_string());
        let config = SyncConfig::default();
        RetryQueue::new(base_path, config, s3)
    }

    #[tokio::test]
    async fn test_enqueue_persists_to_disk() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        // Create a dummy local file for the pending upload to reference
        let local_file = base_path.join("test_file.parquet");
        tokio::fs::write(&local_file, b"test data").await.unwrap();

        // Enqueue an item
        let queue = create_test_retry_queue(base_path.clone());
        queue
            .enqueue("test/key.parquet".to_string(), local_file.clone())
            .await
            .unwrap();

        // Verify the persistence file was written
        let persistence_path = base_path.join(".upload_queue.json");
        assert!(persistence_path.exists());

        // Create a new RetryQueue and load from disk
        let queue2 = create_test_retry_queue(base_path);
        queue2.load().await.unwrap();
        assert_eq!(queue2.len().await, 1);
    }

    #[tokio::test]
    async fn test_enqueue_with_marker_persists_to_disk() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let local_file = base_path.join("test_file.parquet");
        tokio::fs::write(&local_file, b"test data").await.unwrap();

        let queue = create_test_retry_queue(base_path.clone());
        queue
            .enqueue_with_marker(
                "eth/raw/blocks/blocks_0-999.parquet".to_string(),
                local_file,
                "ethereum".to_string(),
                "raw/blocks".to_string(),
                0,
                999,
            )
            .await
            .unwrap();

        // Load into a fresh queue and verify marker fields survived
        let queue2 = create_test_retry_queue(base_path);
        queue2.load().await.unwrap();
        assert_eq!(queue2.len().await, 1);

        let items = queue2.queue.read().await;
        let item = items.front().unwrap();
        assert_eq!(item.chain.as_deref(), Some("ethereum"));
        assert_eq!(item.data_type.as_deref(), Some("raw/blocks"));
        assert_eq!(item.start, Some(0));
        assert_eq!(item.end, Some(999));
    }
}
