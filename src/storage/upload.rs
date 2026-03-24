//! Upload helper for historical collectors.

use std::path::Path;
use std::sync::Arc;

use crate::storage::{StorageBackend, StorageError, StorageManager};

/// Upload a parquet file to S3 and write a marker.
///
/// This is a no-op if S3 is not configured or not enabled.
/// Uses the direct S3 backend to avoid redundant local writes through the
/// cache layer (the file already exists locally).
pub async fn upload_parquet_to_s3(
    storage_manager: &Arc<StorageManager>,
    local_path: &Path,
    chain: &str,
    data_type: &str, // e.g., "raw/blocks", "raw/logs", "factories/v3_pools"
    start: u64,
    end: u64,
) -> Result<(), StorageError> {
    if !storage_manager.is_s3_enabled() {
        return Ok(());
    }

    // Read local file
    let data = tokio::fs::read(local_path).await?;

    // Compute S3 key from local path (strip "data/" prefix)
    let s3_key = compute_s3_key(local_path)?;

    // Upload directly to S3, bypassing the cache layer to avoid
    // a redundant local write (the file already exists on disk).
    match storage_manager.s3_backend() {
        Some(s3) => s3.write(&s3_key, &data).await?,
        None => {
            // Fallback: S3 is enabled but direct backend not available (shouldn't happen)
            storage_manager.backend().write(&s3_key, &data).await?;
        }
    }

    // Write marker
    if let Some(manifest_manager) = storage_manager.manifest_manager() {
        manifest_manager
            .write_marker(chain, data_type, start, end, &s3_key, "historical")
            .await?;
    }

    tracing::debug!(
        "Uploaded {} to S3 and wrote marker for range {}-{}",
        s3_key,
        start,
        end
    );

    Ok(())
}

/// Compute the S3 key from a local path by stripping the "data/" prefix.
///
/// Returns an error if the path does not contain a "data/" segment,
/// since a bare filename fallback would produce incorrect S3 keys.
fn compute_s3_key(local_path: &Path) -> Result<String, StorageError> {
    let path_str = local_path.to_string_lossy();

    // Try to strip "data/" prefix (relative path)
    if let Some(rest) = path_str.strip_prefix("data/") {
        return Ok(rest.to_string());
    }

    // Check for absolute path containing /data/
    if let Some(idx) = path_str.find("/data/") {
        return Ok(path_str[idx + 6..].to_string());
    }

    Err(StorageError::Config(format!(
        "Cannot compute S3 key: path missing 'data/' prefix: {}",
        local_path.display()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_s3_key_relative_data_prefix() {
        let path = Path::new("data/ethereum/historical/raw/blocks/blocks_0-999.parquet");
        let key = compute_s3_key(path).unwrap();
        assert_eq!(key, "ethereum/historical/raw/blocks/blocks_0-999.parquet");
    }

    #[test]
    fn compute_s3_key_absolute_data_prefix() {
        let path = Path::new("/home/user/project/data/base/historical/raw/logs/logs_0-999.parquet");
        let key = compute_s3_key(path).unwrap();
        assert_eq!(key, "base/historical/raw/logs/logs_0-999.parquet");
    }

    #[test]
    fn compute_s3_key_missing_data_prefix_returns_error() {
        let path = Path::new("/tmp/blocks_0-999.parquet");
        let result = compute_s3_key(path);
        assert!(result.is_err());
        match result {
            Err(StorageError::Config(msg)) => {
                assert!(
                    msg.contains("missing 'data/' prefix"),
                    "unexpected message: {}",
                    msg
                );
            }
            other => panic!("expected StorageError::Config, got {:?}", other),
        }
    }
}
