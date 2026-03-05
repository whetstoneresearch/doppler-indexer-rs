//! Retry queue for failed S3 uploads.
//!
//! When an S3 upload fails, the item is queued for retry with exponential backoff.
//! The queue is persisted to disk for crash recovery.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

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
}

impl RetryQueue {
    /// Create a new RetryQueue.
    pub fn new(local_base: PathBuf, config: SyncConfig, s3: S3Backend) -> Self {
        let persistence_path = local_base.join(".upload_queue.json");

        Self {
            queue: RwLock::new(VecDeque::new()),
            persistence_path,
            config,
            s3,
        }
    }

    /// Load the queue from disk.
    pub async fn load(&self) -> Result<(), StorageError> {
        if !self.persistence_path.exists() {
            return Ok(());
        }

        let data = tokio::fs::read(&self.persistence_path).await?;
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

    /// Add a failed upload to the queue.
    pub async fn enqueue(&self, key: String, local_path: PathBuf) {
        let mut queue = self.queue.write().await;
        let pending = PendingUpload::new(key, local_path);
        queue.push_back(pending);

        tracing::warn!("Queued upload for retry: {} items in queue", queue.len());
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
                if let Some(pos) = queue.iter().position(|p| p.is_due()) {
                    Some(queue.remove(pos).unwrap())
                } else {
                    None
                }
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
}
