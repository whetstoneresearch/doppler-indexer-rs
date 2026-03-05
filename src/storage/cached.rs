//! Cached storage backend with write-through to S3 and LRU eviction.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::local::LocalBackend;
use super::s3::S3Backend;
use super::{StorageBackend, StorageError};
use crate::types::config::storage::CacheConfig;

/// Entry in the cache index, tracking access time and size.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Size in bytes
    size: u64,
    /// Last access time (for LRU)
    last_access: Instant,
    /// Whether this entry is pinned (never evicted)
    pinned: bool,
}

/// Cached storage backend combining local filesystem and S3.
///
/// Write-through: All writes go to both local and S3 (synchronously).
/// Read: Try local first, on miss fetch from S3 and cache locally.
/// Eviction: LRU eviction of non-pinned entries when cache exceeds threshold.
pub struct CachedBackend {
    local: LocalBackend,
    s3: S3Backend,
    config: CacheConfig,
    local_base: PathBuf,
    /// Cache index tracking entries
    entries: RwLock<HashMap<String, CacheEntry>>,
    /// Total cache size in bytes
    total_size: AtomicU64,
}

impl CachedBackend {
    /// Create a new CachedBackend.
    pub fn new(
        local: LocalBackend,
        s3: S3Backend,
        config: CacheConfig,
        local_base: PathBuf,
    ) -> Self {
        Self {
            local,
            s3,
            config,
            local_base,
            entries: RwLock::new(HashMap::new()),
            total_size: AtomicU64::new(0),
        }
    }

    /// Get access to the underlying S3 backend.
    pub fn s3_backend(&self) -> &S3Backend {
        &self.s3
    }

    /// Check if a key should be pinned (never evicted).
    fn is_pinned(&self, key: &str) -> bool {
        self.config
            .pinned_prefixes
            .iter()
            .any(|prefix| key.contains(prefix))
    }

    /// Record a cache entry (new or updated).
    async fn record_entry(&self, key: &str, size: u64) {
        let pinned = self.is_pinned(key);
        let entry = CacheEntry {
            size,
            last_access: Instant::now(),
            pinned,
        };

        let mut entries = self.entries.write().await;
        if let Some(old) = entries.insert(key.to_string(), entry) {
            // Update size delta
            self.total_size.fetch_sub(old.size, Ordering::Relaxed);
        }
        self.total_size.fetch_add(size, Ordering::Relaxed);
    }

    /// Update last access time for a key.
    async fn touch(&self, key: &str) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(key) {
            entry.last_access = Instant::now();
        }
    }

    /// Remove an entry from the cache index.
    async fn remove_entry(&self, key: &str) -> Option<CacheEntry> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
            Some(entry)
        } else {
            None
        }
    }

    /// Get the cache size threshold in bytes.
    fn eviction_threshold_bytes(&self) -> u64 {
        (self.config.max_size_gb * 1024 * 1024 * 1024) as u64
            * (self.config.eviction_threshold * 100.0) as u64
            / 100
    }

    /// Evict entries if cache exceeds threshold.
    ///
    /// Uses LRU eviction on non-pinned entries.
    pub async fn evict_if_needed(&self) -> Result<(), StorageError> {
        let threshold = self.eviction_threshold_bytes();
        let current_size = self.total_size.load(Ordering::Relaxed);

        if current_size <= threshold {
            return Ok(());
        }

        let target_size = threshold * 80 / 100; // Evict to 80% of threshold
        let mut to_evict = current_size - target_size;

        // Get candidates sorted by last access (oldest first)
        let candidates: Vec<(String, CacheEntry)> = {
            let entries = self.entries.read().await;
            let mut candidates: Vec<_> = entries
                .iter()
                .filter(|(_, e)| !e.pinned)
                .map(|(k, e)| (k.clone(), e.clone()))
                .collect();
            candidates.sort_by(|a, b| a.1.last_access.cmp(&b.1.last_access));
            candidates
        };

        for (key, entry) in candidates {
            if to_evict == 0 {
                break;
            }

            // Delete from local cache
            if let Err(e) = self.local.delete(&key).await {
                tracing::warn!("Failed to evict {}: {}", key, e);
                continue;
            }

            self.remove_entry(&key).await;

            if entry.size >= to_evict {
                to_evict = 0;
            } else {
                to_evict -= entry.size;
            }

            tracing::debug!("Evicted {} ({} bytes)", key, entry.size);
        }

        Ok(())
    }

    /// Scan local directory and rebuild cache index.
    ///
    /// Call this on startup to rebuild the index from existing files.
    pub async fn rebuild_index(&self, prefix: &str) -> Result<(), StorageError> {
        let files = self.local.list(prefix).await?;

        let mut entries = self.entries.write().await;
        let mut total_size = 0u64;

        for file in files {
            let key = if prefix.is_empty() {
                file
            } else {
                format!("{}/{}", prefix, file)
            };

            let full_path = self.local_base.join(&key);
            if let Ok(metadata) = tokio::fs::metadata(&full_path).await {
                let size = metadata.len();
                let pinned = self.is_pinned(&key);

                entries.insert(
                    key,
                    CacheEntry {
                        size,
                        last_access: Instant::now(),
                        pinned,
                    },
                );
                total_size += size;
            }
        }

        self.total_size.store(total_size, Ordering::Relaxed);

        tracing::info!(
            "Rebuilt cache index: {} entries, {} GB",
            entries.len(),
            total_size as f64 / (1024.0 * 1024.0 * 1024.0)
        );

        Ok(())
    }
}

#[async_trait]
impl StorageBackend for CachedBackend {
    async fn read(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        // Try local first
        match self.local.read(key).await {
            Ok(data) => {
                self.touch(key).await;
                return Ok(data);
            }
            Err(StorageError::NotFound(_)) => {
                // Cache miss, continue to S3
            }
            Err(e) => return Err(e),
        }

        // Fetch from S3
        let data = self.s3.read(key).await?;

        // Cache locally
        self.local.write(key, &data).await?;
        self.record_entry(key, data.len() as u64).await;

        // Trigger eviction check (but don't block on it)
        let _ = self.evict_if_needed().await;

        Ok(data)
    }

    async fn write(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        // Write to local first
        self.local.write(key, data).await?;
        self.record_entry(key, data.len() as u64).await;

        // Write to S3 (synchronous for consistency)
        self.s3.write(key, data).await?;

        // Trigger eviction check
        let _ = self.evict_if_needed().await;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        // Delete from local
        self.local.delete(key).await?;
        self.remove_entry(key).await;

        // Delete from S3
        self.s3.delete(key).await?;

        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        // Check local first
        if self.local.exists(key).await? {
            return Ok(true);
        }

        // Check S3
        self.s3.exists(key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        // List from S3 (authoritative source)
        // For distributed deployments, S3 has the complete view
        self.s3.list(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn create_test_backend(temp_dir: &std::path::Path) -> CachedBackend {
        let local = LocalBackend::new(temp_dir.to_path_buf());
        let s3_store = Arc::new(InMemory::new());
        let s3 = S3Backend::from_store(s3_store, "test-bucket".to_string());
        let config = CacheConfig {
            max_size_gb: 1,
            pinned_prefixes: vec!["decoded".to_string()],
            eviction_threshold: 0.8,
        };

        CachedBackend::new(local, s3, config, temp_dir.to_path_buf())
    }

    #[tokio::test]
    async fn test_cached_backend_write_through() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path());

        // Write through both
        backend.write("test/file.parquet", b"data").await.unwrap();

        // Should exist in both local and S3
        assert!(backend.local.exists("test/file.parquet").await.unwrap());
        assert!(backend.s3.exists("test/file.parquet").await.unwrap());
    }

    #[tokio::test]
    async fn test_cached_backend_read_local_first() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path());

        // Write to local only
        backend.local.write("local/file.txt", b"local data").await.unwrap();

        // Should read from local (even though not in S3)
        let data = backend.read("local/file.txt").await.unwrap();
        assert_eq!(data, b"local data");
    }

    #[tokio::test]
    async fn test_cached_backend_read_from_s3_on_miss() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path());

        // Write to S3 only
        backend.s3.write("s3/file.txt", b"s3 data").await.unwrap();

        // Not in local
        assert!(!backend.local.exists("s3/file.txt").await.unwrap());

        // Read should fetch from S3 and cache locally
        let data = backend.read("s3/file.txt").await.unwrap();
        assert_eq!(data, b"s3 data");

        // Should now be cached locally
        assert!(backend.local.exists("s3/file.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_cached_backend_pinned() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path());

        assert!(backend.is_pinned("chain/decoded/logs/event.parquet"));
        assert!(!backend.is_pinned("chain/raw/blocks/blocks.parquet"));
    }
}
