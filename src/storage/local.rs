//! Local filesystem storage backend.

use std::path::PathBuf;

use async_trait::async_trait;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{StorageBackend, StorageError};

/// Storage backend for local filesystem operations.
///
/// Keys are treated as relative paths under the base directory.
#[derive(Clone)]
pub struct LocalBackend {
    base: PathBuf,
}

impl LocalBackend {
    /// Create a new LocalBackend with the given base directory.
    pub fn new(base: PathBuf) -> Self {
        Self { base }
    }

    /// Get the full path for a key.
    fn full_path(&self, key: &str) -> PathBuf {
        self.base.join(key)
    }
}

#[async_trait]
impl StorageBackend for LocalBackend {
    async fn read(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.full_path(key);
        match fs::read(&path).await {
            Ok(data) => Ok(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(key.to_string()))
            }
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn write(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let path = self.full_path(key);

        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Atomic write: write to temp file, then rename
        let tmp_path = path.with_extension("tmp");
        let mut file = fs::File::create(&tmp_path).await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        fs::rename(&tmp_path, &path).await?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let path = self.full_path(key);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // Idempotent
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let path = self.full_path(key);
        match fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let path = self.full_path(prefix);

        if !path.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut stack = vec![path.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(StorageError::Io(e)),
            };

            while let Some(entry) = entries.next_entry().await? {
                let entry_path = entry.path();
                let file_type = entry.file_type().await?;

                if file_type.is_dir() {
                    stack.push(entry_path);
                } else if file_type.is_file() {
                    // Convert to relative path from prefix
                    if let Ok(relative) = entry_path.strip_prefix(&path) {
                        results.push(relative.to_string_lossy().to_string());
                    }
                }
            }
        }

        results.sort();
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_backend_read_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path().to_path_buf());

        // Write
        backend.write("test/file.txt", b"hello world").await.unwrap();

        // Read
        let data = backend.read("test/file.txt").await.unwrap();
        assert_eq!(data, b"hello world");

        // Exists
        assert!(backend.exists("test/file.txt").await.unwrap());
        assert!(!backend.exists("test/nonexistent.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_backend_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path().to_path_buf());

        backend.write("test/file.txt", b"hello").await.unwrap();
        assert!(backend.exists("test/file.txt").await.unwrap());

        backend.delete("test/file.txt").await.unwrap();
        assert!(!backend.exists("test/file.txt").await.unwrap());

        // Idempotent delete
        backend.delete("test/file.txt").await.unwrap();
    }

    #[tokio::test]
    async fn test_local_backend_list() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path().to_path_buf());

        backend.write("data/blocks/0_999.parquet", b"blocks1").await.unwrap();
        backend.write("data/blocks/1000_1999.parquet", b"blocks2").await.unwrap();
        backend.write("data/logs/0_999.parquet", b"logs1").await.unwrap();

        let all = backend.list("data").await.unwrap();
        assert_eq!(all.len(), 3);

        let blocks = backend.list("data/blocks").await.unwrap();
        assert_eq!(blocks.len(), 2);

        let empty = backend.list("nonexistent").await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_local_backend_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path().to_path_buf());

        let result = backend.read("nonexistent.txt").await;
        assert!(matches!(result, Err(StorageError::NotFound(_))));
    }
}
