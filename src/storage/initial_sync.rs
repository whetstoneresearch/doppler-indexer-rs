//! Initial sync service for uploading existing local data to S3.
//!
//! When S3 storage is first configured on a node that already has local data,
//! this service uploads the existing data to S3 so it becomes available to
//! other distributed services.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use super::local::LocalBackend;
use super::manifest::ManifestManager;
use super::s3::S3Backend;
use super::{StorageBackend, StorageError};

/// Progress state for the initial sync.
#[derive(Debug, Default)]
pub struct SyncProgress {
    /// Number of files scanned
    pub scanned: AtomicU64,
    /// Number of files already in S3
    pub already_synced: AtomicU64,
    /// Number of files uploaded
    pub uploaded: AtomicU64,
    /// Number of upload failures
    pub failed: AtomicU64,
    /// Total bytes uploaded
    pub bytes_uploaded: AtomicU64,
}

impl SyncProgress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn log_summary(&self) {
        let scanned = self.scanned.load(Ordering::Relaxed);
        let already = self.already_synced.load(Ordering::Relaxed);
        let uploaded = self.uploaded.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let bytes = self.bytes_uploaded.load(Ordering::Relaxed);

        tracing::info!(
            "Initial sync complete: scanned={}, already_synced={}, uploaded={} ({:.2} GB), failed={}",
            scanned,
            already,
            uploaded,
            bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            failed
        );
    }
}

/// Service to sync existing local data to S3 on first S3 configuration.
pub struct InitialSyncService {
    local: LocalBackend,
    s3: S3Backend,
    /// Prefixes to scan (e.g., ["ethereum/historical/raw", "raw/ethereum"])
    prefixes: Vec<String>,
    /// Concurrency limit for uploads
    concurrency: usize,
    /// Optional manifest manager for writing markers after uploads
    manifest_manager: Option<Arc<ManifestManager>>,
}

impl InitialSyncService {
    /// Create a new InitialSyncService.
    #[allow(dead_code)]
    pub fn new(local: LocalBackend, s3: S3Backend, prefixes: Vec<String>) -> Self {
        Self {
            local,
            s3,
            prefixes,
            concurrency: 10, // Default concurrency
            manifest_manager: None,
        }
    }

    /// Create a new InitialSyncService with a manifest manager for marker writing.
    pub fn with_manifest_manager(
        local: LocalBackend,
        s3: S3Backend,
        prefixes: Vec<String>,
        manifest_manager: Arc<ManifestManager>,
    ) -> Self {
        Self {
            local,
            s3,
            prefixes,
            concurrency: 10,
            manifest_manager: Some(manifest_manager),
        }
    }

    /// Run the initial sync.
    ///
    /// Scans all configured prefixes for local files, checks if they exist
    /// in S3, and uploads any missing files.
    pub async fn run(
        self: Arc<Self>,
        mut shutdown: oneshot::Receiver<()>,
    ) -> Result<SyncProgress, StorageError> {
        let progress = Arc::new(SyncProgress::new());

        tracing::info!("Starting initial S3 sync for prefixes: {:?}", self.prefixes);

        // Collect all local files across all prefixes
        let mut all_files = Vec::new();
        for prefix in &self.prefixes {
            match self.local.list(prefix).await {
                Ok(files) => {
                    for file in files {
                        // Construct full key relative to local_base
                        let key = if prefix.is_empty() {
                            file
                        } else {
                            format!("{}/{}", prefix, file)
                        };
                        // Only sync parquet files (not bincode live data or tmp files)
                        if key.ends_with(".parquet") {
                            all_files.push(key);
                        }
                    }
                }
                Err(StorageError::NotFound(_)) => {
                    // Prefix doesn't exist locally, skip
                    continue;
                }
                Err(e) => {
                    tracing::warn!("Failed to list prefix {}: {}", prefix, e);
                    continue;
                }
            }
        }

        let total = all_files.len();
        tracing::info!("Found {} parquet files to check for sync", total);

        if total == 0 {
            progress.log_summary();
            return Ok(Arc::try_unwrap(progress).unwrap_or_else(|p| SyncProgress {
                scanned: AtomicU64::new(p.scanned.load(Ordering::Relaxed)),
                already_synced: AtomicU64::new(p.already_synced.load(Ordering::Relaxed)),
                uploaded: AtomicU64::new(p.uploaded.load(Ordering::Relaxed)),
                failed: AtomicU64::new(p.failed.load(Ordering::Relaxed)),
                bytes_uploaded: AtomicU64::new(p.bytes_uploaded.load(Ordering::Relaxed)),
            }));
        }

        // Process files with bounded concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.concurrency));
        let mut handles = Vec::new();

        for key in all_files {
            // Check for shutdown
            if shutdown.try_recv().is_ok() {
                tracing::info!("Initial sync shutting down early");
                break;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let service = self.clone();
            let progress = progress.clone();

            let handle = tokio::spawn(async move {
                let _permit = permit;
                service.sync_file(&key, &progress).await
            });
            handles.push(handle);
        }

        // Wait for all uploads to complete
        for handle in handles {
            let _ = handle.await;
        }

        progress.log_summary();

        Ok(Arc::try_unwrap(progress).unwrap_or_else(|p| SyncProgress {
            scanned: AtomicU64::new(p.scanned.load(Ordering::Relaxed)),
            already_synced: AtomicU64::new(p.already_synced.load(Ordering::Relaxed)),
            uploaded: AtomicU64::new(p.uploaded.load(Ordering::Relaxed)),
            failed: AtomicU64::new(p.failed.load(Ordering::Relaxed)),
            bytes_uploaded: AtomicU64::new(p.bytes_uploaded.load(Ordering::Relaxed)),
        }))
    }

    /// Sync a single file from local to S3 if not already present.
    async fn sync_file(&self, key: &str, progress: &SyncProgress) {
        progress.scanned.fetch_add(1, Ordering::Relaxed);

        // Check if already in S3
        match self.s3.exists(key).await {
            Ok(true) => {
                progress.already_synced.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Ok(false) => {
                // Need to upload
            }
            Err(e) => {
                tracing::warn!("Failed to check S3 existence for {}: {}", key, e);
                progress.failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // Read from local
        let data = match self.local.read(key).await {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!("Failed to read local file {}: {}", key, e);
                progress.failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        let size = data.len() as u64;

        // Upload to S3
        match self.s3.write(key, &data).await {
            Ok(()) => {
                progress.uploaded.fetch_add(1, Ordering::Relaxed);
                progress.bytes_uploaded.fetch_add(size, Ordering::Relaxed);
                tracing::debug!("Uploaded {} ({} bytes)", key, size);

                // Write marker if manifest manager is available
                if let Some(ref mm) = self.manifest_manager {
                    if let Some((chain, data_type, start, end)) = parse_parquet_key(key) {
                        if let Err(e) = mm
                            .write_marker(&chain, &data_type, start, end, key, "initial_sync")
                            .await
                        {
                            tracing::warn!("Failed to write marker for {}: {}", key, e);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to upload {}: {}", key, e);
                progress.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Parse a parquet key into (chain, data_type, start, end).
///
/// Supports various key formats:
/// - "ethereum/historical/raw/blocks/blocks_0_999.parquet" -> ("ethereum", "raw/blocks", 0, 999)
/// - "raw/ethereum/blocks/0_999.parquet" -> ("ethereum", "raw/blocks", 0, 999)
/// - "ethereum/historical/decoded/logs/v3/Swap/0_999.parquet" -> ("ethereum", "decoded/logs/v3/Swap", 0, 999)
fn parse_parquet_key(key: &str) -> Option<(String, String, u64, u64)> {
    let key = key.trim_end_matches(".parquet");
    let parts: Vec<&str> = key.split('/').collect();

    if parts.len() < 3 {
        return None;
    }

    // Try to parse the filename for range: "blocks_0_999" or "0_999" or "logs_0-999"
    let filename = parts.last()?;
    let (start, end) = parse_range_from_filename(filename)?;

    // Determine chain and data_type based on path structure
    if parts.len() >= 4 && parts[1] == "historical" {
        // Format: {chain}/historical/{category}/{type}/...
        // e.g., "ethereum/historical/raw/blocks/blocks_0_999"
        let chain = parts[0].to_string();
        let category = parts[2]; // "raw", "decoded", "factories"

        if category == "decoded" && parts.len() >= 6 {
            // decoded/logs/{source}/{event}/...
            let data_type = parts[2..parts.len() - 1].join("/");
            return Some((chain, data_type, start, end));
        } else if parts.len() >= 5 {
            // raw/{type}/...
            let data_type = format!("{}/{}", category, parts[3]);
            return Some((chain, data_type, start, end));
        }
    } else if parts[0] == "raw" || parts[0] == "derived" {
        // Format: raw/{chain}/{type}/... or derived/{chain}/...
        // e.g., "raw/ethereum/blocks/0_999"
        if parts.len() >= 4 {
            let chain = parts[1].to_string();
            let data_type = format!("{}/{}", parts[0], parts[2]);
            return Some((chain, data_type, start, end));
        }
    }

    None
}

/// Parse range from a filename like "blocks_0_999", "0_999", or "logs_0-999".
fn parse_range_from_filename(filename: &str) -> Option<(u64, u64)> {
    // Try underscore-separated range at end: "blocks_0_999" or "0_999"
    let parts: Vec<&str> = filename.split('_').collect();
    if parts.len() >= 2 {
        if let (Ok(start), Ok(end)) = (
            parts[parts.len() - 2].parse::<u64>(),
            parts[parts.len() - 1].parse::<u64>(),
        ) {
            return Some((start, end));
        }
    }

    // Try hyphen-separated range: "logs_0-999"
    if let Some(range_part) = filename.split('_').last() {
        let range_parts: Vec<&str> = range_part.split('-').collect();
        if range_parts.len() == 2 {
            if let (Ok(start), Ok(end)) =
                (range_parts[0].parse::<u64>(), range_parts[1].parse::<u64>())
            {
                return Some((start, end));
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn create_test_backends(temp_dir: &std::path::Path) -> (LocalBackend, S3Backend) {
        let local = LocalBackend::new(temp_dir.to_path_buf());
        let s3_store = Arc::new(InMemory::new());
        let s3 = S3Backend::from_store(s3_store, "test-bucket".to_string());
        (local, s3)
    }

    #[test]
    fn test_parse_parquet_key_historical_raw() {
        let result = parse_parquet_key("ethereum/historical/raw/blocks/blocks_0_999.parquet");
        assert_eq!(
            result,
            Some(("ethereum".to_string(), "raw/blocks".to_string(), 0, 999))
        );
    }

    #[test]
    fn test_parse_parquet_key_historical_decoded() {
        let result = parse_parquet_key("ethereum/historical/decoded/logs/v3/Swap/0_999.parquet");
        assert_eq!(
            result,
            Some((
                "ethereum".to_string(),
                "decoded/logs/v3/Swap".to_string(),
                0,
                999
            ))
        );
    }

    #[test]
    fn test_parse_parquet_key_raw_prefix() {
        let result = parse_parquet_key("raw/ethereum/blocks/0_999.parquet");
        assert_eq!(
            result,
            Some(("ethereum".to_string(), "raw/blocks".to_string(), 0, 999))
        );
    }

    #[test]
    fn test_parse_range_from_filename() {
        assert_eq!(parse_range_from_filename("blocks_0_999"), Some((0, 999)));
        assert_eq!(parse_range_from_filename("0_999"), Some((0, 999)));
        assert_eq!(
            parse_range_from_filename("logs_1000_1999"),
            Some((1000, 1999))
        );
        assert_eq!(parse_range_from_filename("logs_0-999"), Some((0, 999)));
    }

    #[tokio::test]
    async fn test_initial_sync_uploads_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (local, s3) = create_test_backends(temp_dir.path());

        // Create local files
        local
            .write(
                "chain/historical/raw/blocks/blocks_0_999.parquet",
                b"blocks data",
            )
            .await
            .unwrap();
        local
            .write("chain/historical/raw/logs/logs_0_999.parquet", b"logs data")
            .await
            .unwrap();

        // Verify not in S3
        assert!(!s3
            .exists("chain/historical/raw/blocks/blocks_0_999.parquet")
            .await
            .unwrap());

        // Run sync
        let service = Arc::new(InitialSyncService::new(
            local.clone(),
            s3.clone(),
            vec!["chain/historical".to_string()],
        ));

        let (_tx, rx) = oneshot::channel();
        let progress = service.run(rx).await.unwrap();

        assert_eq!(progress.scanned.load(Ordering::Relaxed), 2);
        assert_eq!(progress.uploaded.load(Ordering::Relaxed), 2);
        assert_eq!(progress.already_synced.load(Ordering::Relaxed), 0);

        // Verify now in S3
        assert!(s3
            .exists("chain/historical/raw/blocks/blocks_0_999.parquet")
            .await
            .unwrap());
        assert!(s3
            .exists("chain/historical/raw/logs/logs_0_999.parquet")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_initial_sync_skips_existing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (local, s3) = create_test_backends(temp_dir.path());

        // Create local file
        local
            .write(
                "chain/historical/raw/blocks/blocks_0_999.parquet",
                b"blocks data",
            )
            .await
            .unwrap();

        // Also put in S3 (already synced)
        s3.write(
            "chain/historical/raw/blocks/blocks_0_999.parquet",
            b"blocks data",
        )
        .await
        .unwrap();

        // Run sync
        let service = Arc::new(InitialSyncService::new(
            local,
            s3,
            vec!["chain/historical".to_string()],
        ));

        let (_tx, rx) = oneshot::channel();
        let progress = service.run(rx).await.unwrap();

        assert_eq!(progress.scanned.load(Ordering::Relaxed), 1);
        assert_eq!(progress.uploaded.load(Ordering::Relaxed), 0);
        assert_eq!(progress.already_synced.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_initial_sync_skips_non_parquet() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (local, s3) = create_test_backends(temp_dir.path());

        // Create local files - mix of parquet and bincode
        local
            .write("chain/live/raw/blocks/12345.bin", b"bincode data")
            .await
            .unwrap();
        local
            .write(
                "chain/historical/raw/blocks/blocks_0_999.parquet",
                b"parquet data",
            )
            .await
            .unwrap();

        // Run sync
        let service = Arc::new(InitialSyncService::new(
            local,
            s3.clone(),
            vec!["chain".to_string()],
        ));

        let (_tx, rx) = oneshot::channel();
        let progress = service.run(rx).await.unwrap();

        // Should only scan the parquet file
        assert_eq!(progress.scanned.load(Ordering::Relaxed), 1);
        assert_eq!(progress.uploaded.load(Ordering::Relaxed), 1);

        // Bincode should NOT be in S3
        assert!(!s3.exists("chain/live/raw/blocks/12345.bin").await.unwrap());
    }
}
