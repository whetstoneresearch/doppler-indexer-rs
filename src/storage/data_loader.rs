//! Data loader that can fetch from local disk or S3.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::{S3Manifest, StorageError, StorageManager};

/// Loader that ensures data is available locally, downloading from S3 if needed.
#[allow(dead_code)]
pub struct DataLoader {
    storage_manager: Option<Arc<StorageManager>>,
    chain_name: String,
    local_base: PathBuf,
}

#[allow(dead_code)]
impl DataLoader {
    /// Create a new DataLoader.
    pub fn new(
        storage_manager: Option<Arc<StorageManager>>,
        chain_name: &str,
        local_base: PathBuf,
    ) -> Self {
        Self {
            storage_manager,
            chain_name: chain_name.to_string(),
            local_base,
        }
    }

    /// Get the chain name.
    pub fn chain_name(&self) -> &str {
        &self.chain_name
    }

    /// Get the manifest for this chain (if available).
    pub fn manifest(&self) -> Option<S3Manifest> {
        self.storage_manager
            .as_ref()
            .and_then(|sm| sm.manifest_for(&self.chain_name))
    }

    /// Ensure a file exists locally, downloading from S3 if needed.
    ///
    /// Returns Ok(true) if file exists (locally or downloaded).
    /// Returns Ok(false) if file doesn't exist anywhere.
    pub async fn ensure_local(&self, local_path: &Path) -> Result<bool, StorageError> {
        // Check local first
        if local_path.exists() {
            return Ok(true);
        }

        // Try S3 if available
        let Some(ref storage_manager) = self.storage_manager else {
            return Ok(false);
        };

        if !storage_manager.is_s3_enabled() {
            return Ok(false);
        }

        // Compute S3 key from local path
        let s3_key = self.local_path_to_s3_key(local_path)?;

        // Try to download
        match storage_manager.backend().read(&s3_key).await {
            Ok(data) => {
                // Create parent directories
                if let Some(parent) = local_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                // Write atomically via temp file
                let tmp_path = local_path.with_extension("tmp");
                tokio::fs::write(&tmp_path, &data).await?;
                tokio::fs::rename(&tmp_path, local_path).await?;

                tracing::debug!("Downloaded {} from S3", local_path.display());
                Ok(true)
            }
            Err(StorageError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Convert a local path to an S3 key.
    fn local_path_to_s3_key(&self, local_path: &Path) -> Result<String, StorageError> {
        // Strip the local base to get relative path
        let relative = local_path
            .strip_prefix(&self.local_base)
            .or_else(|_| local_path.strip_prefix("data"))
            .map_err(|_| {
                StorageError::Config(format!(
                    "Cannot compute S3 key for path: {}",
                    local_path.display()
                ))
            })?;

        Ok(relative.to_string_lossy().to_string())
    }
}
