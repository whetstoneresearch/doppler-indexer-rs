//! S3-compatible storage backend using the object_store crate.

use std::env;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};

use super::{StorageBackend, StorageError};
use crate::types::config::storage::S3Config;

/// Storage backend for S3-compatible object storage.
///
/// Uses the `object_store` crate which supports AWS S3, MinIO, Cloudflare R2,
/// and other S3-compatible services.
#[allow(dead_code)]
#[derive(Clone)]
pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

impl S3Backend {
    /// Create a new S3Backend from configuration.
    ///
    /// Reads credentials from environment variables specified in the config.
    pub fn from_config(config: &S3Config) -> Result<Self, StorageError> {
        let endpoint = env::var(&config.endpoint_env_var).map_err(|_| {
            StorageError::Config(format!(
                "Environment variable {} not set",
                config.endpoint_env_var
            ))
        })?;

        let access_key = env::var(&config.access_key_env_var).map_err(|_| {
            StorageError::Config(format!(
                "Environment variable {} not set",
                config.access_key_env_var
            ))
        })?;

        let secret_key = env::var(&config.secret_key_env_var).map_err(|_| {
            StorageError::Config(format!(
                "Environment variable {} not set",
                config.secret_key_env_var
            ))
        })?;

        let bucket = env::var(&config.bucket_env_var).map_err(|_| {
            StorageError::Config(format!(
                "Environment variable {} not set",
                config.bucket_env_var
            ))
        })?;

        let store = AmazonS3Builder::new()
            .with_endpoint(&endpoint)
            .with_region(&config.region)
            .with_bucket_name(&bucket)
            .with_access_key_id(&access_key)
            .with_secret_access_key(&secret_key)
            // Allow HTTP for local MinIO/testing
            .with_allow_http(endpoint.starts_with("http://"))
            // Force path-style for MinIO compatibility
            .with_virtual_hosted_style_request(false)
            .build()
            .map_err(|e| StorageError::Config(format!("Failed to build S3 client: {}", e)))?;

        Ok(Self {
            store: Arc::new(store),
            bucket,
        })
    }

    /// Create an S3Backend from an existing ObjectStore (for testing).
    #[cfg(test)]
    pub fn from_store(store: Arc<dyn ObjectStore>, bucket: String) -> Self {
        Self { store, bucket }
    }

    /// Get the bucket name.
    #[allow(dead_code)]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn read(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = ObjectPath::from(key);

        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(bytes.to_vec())
            }
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageError::NotFound(key.to_string()))
            }
            Err(e) => Err(StorageError::ObjectStore(e)),
        }
    }

    async fn write(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(Bytes::copy_from_slice(data));

        self.store.put(&path, payload).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let path = ObjectPath::from(key);

        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()), // Idempotent
            Err(e) => Err(StorageError::ObjectStore(e)),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let path = ObjectPath::from(key);

        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StorageError::ObjectStore(e)),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let path = if prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(prefix))
        };

        let stream = self.store.list(path.as_ref());

        let entries: Vec<_> = stream.try_collect().await?;

        let keys: Vec<String> = entries
            .into_iter()
            .map(|meta| {
                let full_path = meta.location.to_string();
                // Strip the prefix to return relative paths
                if !prefix.is_empty() && full_path.starts_with(prefix) {
                    full_path[prefix.len()..]
                        .trim_start_matches('/')
                        .to_string()
                } else {
                    full_path
                }
            })
            .collect();

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn create_test_backend() -> S3Backend {
        let store = Arc::new(InMemory::new());
        S3Backend::from_store(store, "test-bucket".to_string())
    }

    #[tokio::test]
    async fn test_s3_backend_read_write() {
        let backend = create_test_backend();

        // Write
        backend
            .write("test/file.parquet", b"parquet data")
            .await
            .unwrap();

        // Read
        let data = backend.read("test/file.parquet").await.unwrap();
        assert_eq!(data, b"parquet data");

        // Exists
        assert!(backend.exists("test/file.parquet").await.unwrap());
        assert!(!backend.exists("test/nonexistent.parquet").await.unwrap());
    }

    #[tokio::test]
    async fn test_s3_backend_delete() {
        let backend = create_test_backend();

        backend.write("test/file.parquet", b"data").await.unwrap();
        assert!(backend.exists("test/file.parquet").await.unwrap());

        backend.delete("test/file.parquet").await.unwrap();
        assert!(!backend.exists("test/file.parquet").await.unwrap());

        // Idempotent delete
        backend.delete("test/file.parquet").await.unwrap();
    }

    #[tokio::test]
    async fn test_s3_backend_list() {
        let backend = create_test_backend();

        backend
            .write("data/blocks/0_999.parquet", b"blocks1")
            .await
            .unwrap();
        backend
            .write("data/blocks/1000_1999.parquet", b"blocks2")
            .await
            .unwrap();
        backend
            .write("data/logs/0_999.parquet", b"logs1")
            .await
            .unwrap();

        let all = backend.list("data").await.unwrap();
        assert_eq!(all.len(), 3);

        let blocks = backend.list("data/blocks").await.unwrap();
        assert_eq!(blocks.len(), 2);
    }

    #[tokio::test]
    async fn test_s3_backend_not_found() {
        let backend = create_test_backend();

        let result = backend.read("nonexistent.parquet").await;
        assert!(matches!(result, Err(StorageError::NotFound(_))));
    }
}
