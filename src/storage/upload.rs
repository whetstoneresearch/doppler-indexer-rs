//! Upload helper for historical collectors.

use std::path::Path;
use std::sync::Arc;

use crate::storage::{StorageError, StorageManager};

/// Upload a parquet file to S3 and write a marker.
///
/// This is a no-op if S3 is not configured or not enabled.
/// Pattern matches `src/live/compaction.rs:824-883`.
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
    let s3_key = compute_s3_key(local_path);

    // Upload to S3
    storage_manager.backend().write(&s3_key, &data).await?;

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
fn compute_s3_key(local_path: &Path) -> String {
    // Try to strip "data/" prefix
    let path_str = local_path.to_string_lossy();
    if let Some(rest) = path_str.strip_prefix("data/") {
        return rest.to_string();
    }
    // Check for absolute path containing /data/
    if let Some(idx) = path_str.find("/data/") {
        return path_str[idx + 6..].to_string();
    }
    // Fallback: use filename
    local_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| path_str.to_string())
}
