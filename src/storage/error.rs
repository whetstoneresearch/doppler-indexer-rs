//! Storage error types.

use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Key not found: {0}")]
    NotFound(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Manifest error: {0}")]
    Manifest(String),
}
