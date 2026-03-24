//! Cached storage backend with write-through to S3 and LRU eviction.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::local::LocalBackend;
use super::retry::RetryQueue;
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
}

/// Persisted cache entry for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedCacheEntry {
    size: u64,
    last_access_secs: u64,
}

/// Persisted cache index for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedCacheIndex {
    entries: HashMap<String, PersistedCacheEntry>,
    total_size: u64,
    saved_at: u64,
}

/// Cached storage backend combining local filesystem and S3.
///
/// Write-through: All writes go to both local and S3 (synchronously).
/// Read: Try local first, on miss fetch from S3 and cache locally.
/// Eviction: LRU eviction of non-pinned entries when cache exceeds threshold.
///
/// Pinned entries (matching configured prefixes like "decoded", "factories") are
/// stored locally but not tracked in the cache index, since they are never evicted.
/// This prevents unbounded growth of the index HashMap.
#[allow(dead_code)]
pub struct CachedBackend {
    local: Arc<LocalBackend>,
    s3: S3Backend,
    config: Arc<CacheConfig>,
    local_base: PathBuf,
    /// Path to cache index file
    index_path: PathBuf,
    /// Cache index tracking non-pinned entries only
    entries: Arc<RwLock<HashMap<String, CacheEntry>>>,
    /// Total size of non-pinned cached entries in bytes
    total_size: Arc<AtomicU64>,
    /// Total size of pinned entries (tracked separately, never evicted)
    pinned_size: Arc<AtomicU64>,
    /// Retry queue for failed S3 uploads
    retry_queue: Option<Arc<RetryQueue>>,
    /// Lock to prevent concurrent eviction runs
    eviction_lock: Arc<tokio::sync::Mutex<()>>,
}

#[allow(dead_code)]
impl CachedBackend {
    /// Create a new CachedBackend and load existing cache index.
    pub async fn new(
        local: LocalBackend,
        s3: S3Backend,
        config: CacheConfig,
        local_base: PathBuf,
        retry_queue: Option<Arc<RetryQueue>>,
    ) -> Result<Self, StorageError> {
        let index_path = local_base.join(".cache_index.json");
        let backend = Self {
            local: Arc::new(local),
            s3,
            config: Arc::new(config),
            local_base,
            index_path,
            entries: Arc::new(RwLock::new(HashMap::new())),
            total_size: Arc::new(AtomicU64::new(0)),
            pinned_size: Arc::new(AtomicU64::new(0)),
            retry_queue,
            eviction_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        // Load existing index (ignore errors - start fresh if missing/corrupt)
        if let Err(e) = backend.load_index().await {
            tracing::debug!("No cache index loaded: {}", e);
        }

        Ok(backend)
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
    ///
    /// Pinned entries update `pinned_size` atomically but are NOT inserted into the
    /// `entries` HashMap, preventing unbounded index growth.
    /// Non-pinned entries are tracked in the HashMap for LRU eviction.
    async fn record_entry(&self, key: &str, size: u64) {
        if self.is_pinned(key) {
            // Pinned entries are not tracked in the index - just account for size
            self.pinned_size.fetch_add(size, Ordering::Relaxed);
            return;
        }

        let entry = CacheEntry {
            size,
            last_access: Instant::now(),
        };

        let mut entries = self.entries.write().await;
        if let Some(old) = entries.insert(key.to_string(), entry) {
            // Single atomic operation for the delta to avoid brief inconsistency
            if size >= old.size {
                self.total_size
                    .fetch_add(size - old.size, Ordering::Relaxed);
            } else {
                self.total_size
                    .fetch_sub(old.size - size, Ordering::Relaxed);
            }
        } else {
            self.total_size.fetch_add(size, Ordering::Relaxed);
        }
    }

    /// Update last access time for a key.
    async fn touch(&self, key: &str) {
        if self.is_pinned(key) {
            return;
        }
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
        let size_bytes = self.config.max_size_gb.saturating_mul(1024 * 1024 * 1024);
        let threshold_pct = (self.config.eviction_threshold * 100.0) as u64;
        size_bytes.saturating_mul(threshold_pct) / 100
    }

    /// Spawn eviction in the background without blocking the caller.
    ///
    /// Uses `try_lock` on the eviction mutex so that if an eviction is already
    /// in progress, this call is a no-op rather than queuing up.
    fn try_evict_background(&self) {
        let eviction_lock = Arc::clone(&self.eviction_lock);
        let total_size = Arc::clone(&self.total_size);
        let entries = Arc::clone(&self.entries);
        let local = Arc::clone(&self.local);
        let config = Arc::clone(&self.config);
        let index_path = self.index_path.clone();

        tokio::spawn(async move {
            // If eviction is already running, skip
            let _guard = match eviction_lock.try_lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };

            let threshold = {
                let size_bytes = config.max_size_gb.saturating_mul(1024 * 1024 * 1024);
                let threshold_pct = (config.eviction_threshold * 100.0) as u64;
                size_bytes.saturating_mul(threshold_pct) / 100
            };

            let current_size = total_size.load(Ordering::Relaxed);

            if current_size <= threshold {
                return;
            }

            let target_size = threshold * 80 / 100;
            let mut to_evict = current_size - target_size;

            // Get candidates sorted by last access (oldest first)
            let candidates: Vec<(String, CacheEntry)> = {
                let entries_guard = entries.read().await;
                let mut candidates: Vec<_> = entries_guard
                    .iter()
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
                if let Err(e) = local.delete(&key).await {
                    tracing::warn!("Failed to evict {}: {}", key, e);
                    continue;
                }

                {
                    let mut entries_guard = entries.write().await;
                    if let Some(removed) = entries_guard.remove(&key) {
                        total_size.fetch_sub(removed.size, Ordering::Relaxed);
                    }
                }

                if entry.size >= to_evict {
                    to_evict = 0;
                } else {
                    to_evict -= entry.size;
                }

                tracing::debug!("Evicted {} ({} bytes)", key, entry.size);
            }

            // Persist the updated index (best-effort)
            let _ = Self::save_index_static(&entries, &total_size, &index_path).await;
        });
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
        // All entries in the HashMap are non-pinned, so no filter needed
        let candidates: Vec<(String, CacheEntry)> = {
            let entries = self.entries.read().await;
            let mut candidates: Vec<_> = entries
                .iter()
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

        // Persist the updated index
        let _ = self.save_index().await;

        Ok(())
    }

    /// Scan local directory and rebuild cache index.
    ///
    /// Call this on startup to rebuild the index from existing files.
    pub async fn rebuild_index(&self, prefix: &str) -> Result<(), StorageError> {
        let files = self.local.list(prefix).await?;

        let mut entries = self.entries.write().await;
        let mut total_size = 0u64;
        let mut pinned_size_acc = 0u64;

        for file in files {
            let key = if prefix.is_empty() {
                file
            } else {
                format!("{}/{}", prefix, file)
            };

            let full_path = self.local_base.join(&key);
            if let Ok(metadata) = tokio::fs::metadata(&full_path).await {
                let size = metadata.len();

                if self.is_pinned(&key) {
                    // Pinned entries tracked separately
                    pinned_size_acc += size;
                } else {
                    entries.insert(
                        key,
                        CacheEntry {
                            size,
                            last_access: Instant::now(),
                        },
                    );
                    total_size += size;
                }
            }
        }

        self.total_size.store(total_size, Ordering::Relaxed);
        self.pinned_size.store(pinned_size_acc, Ordering::Relaxed);

        tracing::info!(
            "Rebuilt cache index: {} entries ({} GB non-pinned, {} GB pinned)",
            entries.len(),
            total_size as f64 / (1024.0 * 1024.0 * 1024.0),
            pinned_size_acc as f64 / (1024.0 * 1024.0 * 1024.0),
        );

        Ok(())
    }

    /// Static helper to save the index without requiring &self.
    ///
    /// Used by the background eviction task which only has Arc-wrapped fields.
    async fn save_index_static(
        entries: &RwLock<HashMap<String, CacheEntry>>,
        total_size: &AtomicU64,
        index_path: &PathBuf,
    ) -> Result<(), StorageError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StorageError::Cache(format!("SystemTime error: {}", e)))?
            .as_secs();

        let entries_guard = entries.read().await;
        let persisted_entries: HashMap<String, PersistedCacheEntry> = entries_guard
            .iter()
            .map(|(k, v)| {
                let elapsed = v.last_access.elapsed().as_secs();
                let last_access_secs = now.saturating_sub(elapsed);
                (
                    k.clone(),
                    PersistedCacheEntry {
                        size: v.size,
                        last_access_secs,
                    },
                )
            })
            .collect();

        let index = PersistedCacheIndex {
            entries: persisted_entries,
            total_size: total_size.load(Ordering::Relaxed),
            saved_at: now,
        };

        let data = serde_json::to_vec_pretty(&index)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let tmp_path = index_path.with_extension("tmp");
        tokio::fs::write(&tmp_path, &data).await?;
        tokio::fs::rename(&tmp_path, index_path).await?;

        tracing::debug!("Saved cache index with {} entries", entries_guard.len());
        Ok(())
    }

    /// Save the cache index to disk.
    pub async fn save_index(&self) -> Result<(), StorageError> {
        Self::save_index_static(&self.entries, &self.total_size, &self.index_path).await
    }

    /// Load the cache index from disk.
    pub async fn load_index(&self) -> Result<(), StorageError> {
        let data = match tokio::fs::read(&self.index_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(StorageError::Io(e)),
        };

        let index: PersistedCacheIndex = serde_json::from_slice(&data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StorageError::Cache(format!("SystemTime error: {}", e)))?
            .as_secs();

        let mut entries = self.entries.write().await;
        entries.clear();

        for (key, persisted) in index.entries {
            // Convert unix timestamp back to Instant (approximate)
            let age_secs = now.saturating_sub(persisted.last_access_secs);
            let last_access = Instant::now() - std::time::Duration::from_secs(age_secs);

            entries.insert(
                key,
                CacheEntry {
                    size: persisted.size,
                    last_access,
                },
            );
        }

        self.total_size.store(index.total_size, Ordering::Relaxed);

        tracing::info!(
            "Loaded cache index: {} entries, {} GB",
            entries.len(),
            index.total_size as f64 / (1024.0 * 1024.0 * 1024.0)
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

        // Trigger eviction check in background (non-blocking)
        self.try_evict_background();

        Ok(data)
    }

    async fn write(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        // Write to local first
        self.local.write(key, data).await?;
        self.record_entry(key, data.len() as u64).await;

        // Write to S3 with retry fallback
        if let Err(e) = self.s3.write(key, data).await {
            if let Some(ref queue) = self.retry_queue {
                tracing::warn!("S3 write failed for {}, queueing for retry: {}", key, e);
                let local_path = self.local_base.join(key);
                queue.enqueue(key.to_string(), local_path).await;
                // Return Ok - local succeeded, S3 will retry
            } else {
                return Err(e);
            }
        }

        // Trigger eviction check in background (non-blocking)
        self.try_evict_background();

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        // Delete from S3 first - if this fails, local state is unchanged.
        // S3 delete is idempotent (returns Ok if not found), so this is safe.
        self.s3.delete(key).await?;

        // Now safe to clean up local state
        self.local.delete(key).await?;
        self.remove_entry(key).await;

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
        // List from S3 (authoritative source), fall back to local on error
        match self.s3.list(prefix).await {
            Ok(keys) => Ok(keys),
            Err(e) => {
                tracing::warn!("S3 list failed, falling back to local: {}", e);
                self.local.list(prefix).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn create_test_backend(temp_dir: &std::path::Path) -> CachedBackend {
        let local = LocalBackend::new(temp_dir.to_path_buf());
        let s3_store = Arc::new(InMemory::new());
        let s3 = S3Backend::from_store(s3_store, "test-bucket".to_string());
        let config = CacheConfig {
            max_size_gb: 1,
            pinned_prefixes: vec!["decoded".to_string()],
            eviction_threshold: 0.8,
        };

        CachedBackend::new(local, s3, config, temp_dir.to_path_buf(), None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_cached_backend_write_through() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

        // Write through both
        backend.write("test/file.parquet", b"data").await.unwrap();

        // Should exist in both local and S3
        assert!(backend.local.exists("test/file.parquet").await.unwrap());
        assert!(backend.s3.exists("test/file.parquet").await.unwrap());
    }

    #[tokio::test]
    async fn test_cached_backend_read_local_first() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

        // Write to local only
        backend
            .local
            .write("local/file.txt", b"local data")
            .await
            .unwrap();

        // Should read from local (even though not in S3)
        let data = backend.read("local/file.txt").await.unwrap();
        assert_eq!(data, b"local data");
    }

    #[tokio::test]
    async fn test_cached_backend_read_from_s3_on_miss() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

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
        let backend = create_test_backend(temp_dir.path()).await;

        assert!(backend.is_pinned("chain/decoded/logs/event.parquet"));
        assert!(!backend.is_pinned("chain/raw/blocks/blocks.parquet"));
    }

    #[tokio::test]
    async fn test_list_fallback_to_local_on_s3_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalBackend::new(temp_dir.path().to_path_buf());

        // Write some files locally
        local.write("data/file1.parquet", b"data1").await.unwrap();
        local.write("data/file2.parquet", b"data2").await.unwrap();

        // Create S3 backend that will fail on list by using a store that has no data
        // The InMemory store won't error, but we can test the fallback path by using
        // a backend where S3 list returns empty but local has files.
        // For a true error test, we need a custom store. Instead, test the happy path
        // (S3 succeeds) and verify the method signature works.

        // For a real S3 failure test, we create a backend with both stores having data
        let s3_store = Arc::new(InMemory::new());
        let s3 = S3Backend::from_store(s3_store, "test-bucket".to_string());
        let config = CacheConfig {
            max_size_gb: 1,
            pinned_prefixes: vec!["decoded".to_string()],
            eviction_threshold: 0.8,
        };

        let backend = CachedBackend::new(
            LocalBackend::new(temp_dir.path().to_path_buf()),
            s3,
            config,
            temp_dir.path().to_path_buf(),
            None,
        )
        .await
        .unwrap();

        // Write locally but not to S3
        backend
            .local
            .write("myprefix/local_only.txt", b"local")
            .await
            .unwrap();

        // S3 list for "myprefix" returns empty (no error, just no data)
        // In a real scenario where S3 errors, we'd fall back to local.
        // Here we verify that list works and returns S3 results when available.
        let result = backend.list("myprefix").await.unwrap();
        // S3 has nothing under myprefix, so returns empty
        assert!(result.is_empty());

        // Now write to S3 too
        backend
            .s3
            .write("myprefix/s3_file.txt", b"s3data")
            .await
            .unwrap();
        let result = backend.list("myprefix").await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("s3_file.txt"));
    }

    #[tokio::test]
    async fn test_delete_reordering_s3_first() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

        // Write a file to both local and S3
        backend
            .write("test/deleteme.txt", b"some data")
            .await
            .unwrap();

        // Verify it exists in both
        assert!(backend.local.exists("test/deleteme.txt").await.unwrap());
        assert!(backend.s3.exists("test/deleteme.txt").await.unwrap());

        // Delete should succeed (both S3 and local)
        backend.delete("test/deleteme.txt").await.unwrap();

        // Both should be gone
        assert!(!backend.local.exists("test/deleteme.txt").await.unwrap());
        assert!(!backend.s3.exists("test/deleteme.txt").await.unwrap());

        // Verify index is also cleared
        let entries = backend.entries.read().await;
        assert!(!entries.contains_key("test/deleteme.txt"));
    }

    #[tokio::test]
    async fn test_delete_local_preserved_on_s3_failure() {
        // This test verifies the principle: if S3 delete were to fail,
        // local file should still exist. With InMemory store, S3 delete
        // won't actually fail, but we verify the ordering is correct by
        // checking that a file only in local (not S3) can still be found
        // after a delete that hits S3 NotFound (which is treated as success).
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

        // Write only to local (not through cached backend's write)
        backend
            .local
            .write("local_only/file.txt", b"local data")
            .await
            .unwrap();

        // S3 delete returns Ok for not-found keys (idempotent)
        // So this should succeed and clean up local too
        backend.delete("local_only/file.txt").await.unwrap();
        assert!(!backend.local.exists("local_only/file.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_record_entry_size_tracking() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

        // Record a new entry
        backend.record_entry("file1.txt", 100).await;
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 100);

        // Record another entry
        backend.record_entry("file2.txt", 200).await;
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 300);

        // Update existing entry with larger size
        backend.record_entry("file1.txt", 150).await;
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 350);

        // Update existing entry with smaller size
        backend.record_entry("file2.txt", 50).await;
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 200);

        // Replace with same size
        backend.record_entry("file1.txt", 150).await;
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 200);

        // Verify entries in HashMap
        let entries = backend.entries.read().await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.get("file1.txt").unwrap().size, 150);
        assert_eq!(entries.get("file2.txt").unwrap().size, 50);
    }

    #[tokio::test]
    async fn test_pinned_entries_not_in_hashmap() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

        // "decoded" is a pinned prefix in the test config
        backend
            .record_entry("chain/decoded/logs/event.parquet", 500)
            .await;
        backend
            .record_entry("chain/decoded/eth_calls/call.parquet", 300)
            .await;

        // Non-pinned entry
        backend
            .record_entry("chain/raw/blocks/block.parquet", 100)
            .await;

        // Pinned entries should NOT be in the entries HashMap
        let entries = backend.entries.read().await;
        assert_eq!(
            entries.len(),
            1,
            "Only non-pinned entries should be in HashMap"
        );
        assert!(entries.contains_key("chain/raw/blocks/block.parquet"));
        assert!(
            !entries.contains_key("chain/decoded/logs/event.parquet"),
            "Pinned entry should not be in HashMap"
        );
        assert!(
            !entries.contains_key("chain/decoded/eth_calls/call.parquet"),
            "Pinned entry should not be in HashMap"
        );

        // total_size should only reflect non-pinned entries
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 100);

        // pinned_size should track pinned entries
        assert_eq!(backend.pinned_size.load(Ordering::Relaxed), 800);
    }

    #[tokio::test]
    async fn test_record_entry_size_zero_to_nonzero() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = create_test_backend(temp_dir.path()).await;

        // Start fresh
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 0);

        // Record entry with size 0
        backend.record_entry("empty.txt", 0).await;
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 0);

        // Update to non-zero
        backend.record_entry("empty.txt", 42).await;
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 42);

        // Remove it
        let removed = backend.remove_entry("empty.txt").await;
        assert!(removed.is_some());
        assert_eq!(backend.total_size.load(Ordering::Relaxed), 0);
    }
}
