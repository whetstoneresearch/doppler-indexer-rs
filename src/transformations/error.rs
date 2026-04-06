//! Transformation error types.

use thiserror::Error;

use crate::db::DbError;
use crate::live::StorageError;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum TransformationError {
    #[error("Handler '{handler_name}' failed: {message}")]
    HandlerError {
        handler_name: String,
        message: String,
    },

    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Missing required data: {0}")]
    MissingData(String),

    #[error("Missing column in parquet file: {0}")]
    MissingColumn(String),

    #[error("RPC error: {0}")]
    RpcError(String),

    #[error("Cannot access future block {requested}, current max is {current_max}")]
    FutureBlockAccess { requested: u64, current_max: u64 },

    #[error("Decode error: {0}")]
    DecodeError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Invalid configuration: {0}")]
    ConfigError(String),

    #[error("Channel send error: {0}")]
    ChannelError(String),

    #[error("Includes precompile address: {0}")]
    IncludesPrecompileError(String),

    #[error("Live storage error: {0}")]
    LiveStorage(#[from] StorageError),

    #[error("Handler transiently blocked: {0}")]
    TransientBlocked(String),
}

#[allow(dead_code)]
impl TransformationError {
    /// Create a handler error with context.
    pub fn handler(name: &str, message: impl Into<String>) -> Self {
        Self::HandlerError {
            handler_name: name.to_string(),
            message: message.into(),
        }
    }
}

// Direct conversions for pool operations to avoid verbose map_err chains.
// These allow using `?` directly on pool.get() and client.query() calls.

impl From<deadpool_postgres::PoolError> for TransformationError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Self::DatabaseError(DbError::PoolError(e))
    }
}

impl From<tokio_postgres::Error> for TransformationError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::DatabaseError(DbError::PostgresError(e))
    }
}
