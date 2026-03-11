//! Storage abstraction for local and S3-compatible storage.
//!
//! This module provides a unified interface for reading and writing data to either
//! local filesystem or S3-compatible object storage. The primary use case is to
//! enable distributed deployments where different services (collector, decoder,
//! transformer) can share data via S3.
//!
//! # Architecture
//!
//! - `StorageBackend` trait: Common interface for all storage backends
//! - `LocalBackend`: Direct filesystem operations
//! - `S3Backend`: S3-compatible object storage via `object_store` crate
//! - `CachedBackend`: Write-through cache combining local and S3 with LRU eviction
//! - `StorageManager`: High-level manager that initializes and provides access to backends
//!
//! # Data Layout
//!
//! ```text
//! s3://{bucket}/{chain}/
//!   historical/
//!     raw/{blocks,logs,receipts,eth_calls}/
//!     decoded/{logs,eth_calls}/
//!     factories/
//!   markers/
//!     raw/{blocks,logs,receipts,eth_calls}/{start}_{end}.marker
//!     decoded/logs/{source}/{event}/{start}_{end}.marker
//!     decoded/eth_calls/{source}/{function}/{start}_{end}.marker
//!     factories/{start}_{end}.marker
//!   manifest.json
//! ```

mod cached;
mod data_loader;
mod error;
mod initial_sync;
mod local;
mod manifest;
mod retry;
mod s3;
mod upload;

pub use cached::CachedBackend;
pub use data_loader::DataLoader;
pub use error::StorageError;
pub use initial_sync::InitialSyncService;
pub use local::LocalBackend;
pub use manifest::{ManifestManager, S3Manifest};
pub use retry::RetryQueue;
pub use s3::S3Backend;
pub use upload::upload_parquet_to_s3;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::types::config::storage::StorageConfig;

/// Trait for storage backends providing read/write access to data files.
///
/// All paths are relative keys (e.g., "ethereum/historical/raw/blocks/blocks_0-999.parquet").
/// Implementations handle translating these to absolute paths or S3 keys.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Read data from the given key.
    ///
    /// Returns `StorageError::NotFound` if the key does not exist.
    async fn read(&self, key: &str) -> Result<Vec<u8>, StorageError>;

    /// Write data to the given key.
    ///
    /// Creates parent directories/prefixes as needed. Overwrites existing data.
    async fn write(&self, key: &str, data: &[u8]) -> Result<(), StorageError>;

    /// Delete data at the given key.
    ///
    /// Returns `Ok(())` if the key did not exist (idempotent delete).
    async fn delete(&self, key: &str) -> Result<(), StorageError>;

    /// Check if a key exists.
    async fn exists(&self, key: &str) -> Result<bool, StorageError>;

    /// List all keys under a prefix.
    ///
    /// Returns keys relative to the prefix (not full paths).
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
}

/// Manager for storage backends and manifests.
///
/// Handles initialization, backend selection, and manifest management.
pub struct StorageManager {
    /// The active storage backend (local-only or cached with S3)
    backend: Arc<dyn StorageBackend>,
    /// Manifest manager for S3 coordination (None if local-only)
    manifest_manager: Option<Arc<ManifestManager>>,
    /// Base path for local storage
    local_base: PathBuf,
    /// Whether S3 is enabled
    s3_enabled: bool,
}

impl StorageManager {
    /// Create a new StorageManager from configuration.
    ///
    /// If `config` is None or S3 is not configured, uses local-only mode.
    /// The optional `retry_queue` is passed to the cached backend for S3 failure handling.
    pub async fn new(
        config: Option<&StorageConfig>,
        local_base: PathBuf,
        retry_queue: Option<Arc<RetryQueue>>,
    ) -> Result<Self, StorageError> {
        match config.and_then(|c| c.s3.as_ref()) {
            Some(s3_config) => {
                // S3 is configured - create cached backend
                let s3_backend = S3Backend::from_config(s3_config)?;
                let local_backend = LocalBackend::new(local_base.clone());

                let cache_config = config
                    .and_then(|c| c.cache.as_ref())
                    .cloned()
                    .unwrap_or_default();

                let sync_config = config
                    .and_then(|c| c.sync.as_ref())
                    .cloned()
                    .unwrap_or_default();

                let cached_backend = CachedBackend::new(
                    local_backend,
                    s3_backend,
                    cache_config,
                    local_base.clone(),
                    retry_queue,
                )
                .await?;

                let manifest_manager = Arc::new(ManifestManager::new(
                    cached_backend.s3_backend().clone(),
                    local_base.clone(),
                    sync_config,
                ));

                tracing::info!("Storage initialized with S3 backend");

                Ok(Self {
                    backend: Arc::new(cached_backend),
                    manifest_manager: Some(manifest_manager),
                    local_base,
                    s3_enabled: true,
                })
            }
            None => {
                // No S3 - local only
                let local_backend = LocalBackend::new(local_base.clone());

                tracing::info!("Storage initialized in local-only mode");

                Ok(Self {
                    backend: Arc::new(local_backend),
                    manifest_manager: None,
                    local_base,
                    s3_enabled: false,
                })
            }
        }
    }

    /// Get the storage backend.
    pub fn backend(&self) -> Arc<dyn StorageBackend> {
        self.backend.clone()
    }

    /// Get the manifest manager (if S3 is enabled).
    pub fn manifest_manager(&self) -> Option<Arc<ManifestManager>> {
        self.manifest_manager.clone()
    }

    /// Whether S3 storage is enabled.
    pub fn is_s3_enabled(&self) -> bool {
        self.s3_enabled
    }

    /// Get the local base path.
    pub fn local_base(&self) -> &PathBuf {
        &self.local_base
    }

    /// Register a chain with the manifest manager.
    ///
    /// This must be called before refresh_manifest() to enable per-chain manifest loading.
    pub fn register_chain(&self, chain: &str) {
        if let Some(manager) = &self.manifest_manager {
            manager.register_chain(chain);
        }
    }

    /// Refresh the manifest from S3.
    ///
    /// No-op if S3 is not enabled.
    pub async fn refresh_manifest(&self) -> Result<(), StorageError> {
        if let Some(manager) = &self.manifest_manager {
            manager.refresh().await?;
        }
        Ok(())
    }

    /// Get cached manifest (if available).
    ///
    /// DEPRECATED: Use `manifest_for(chain)` for multi-chain deployments.
    /// This returns the first cached manifest regardless of chain.
    #[allow(dead_code)]
    pub fn manifest(&self) -> Option<S3Manifest> {
        self.manifest_manager
            .as_ref()
            .and_then(|m| m.cached_manifest())
    }

    /// Get cached manifest for a specific chain.
    pub fn manifest_for(&self, chain: &str) -> Option<S3Manifest> {
        self.manifest_manager
            .as_ref()
            .and_then(|m| m.cached_manifest_for(chain))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_manager_local_only() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = StorageManager::new(None, temp_dir.path().to_path_buf(), None)
            .await
            .unwrap();

        assert!(!manager.is_s3_enabled());
        assert!(manager.manifest_manager().is_none());
    }
}
