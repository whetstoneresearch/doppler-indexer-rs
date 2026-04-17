//! Unified error types for live mode operations.
//!
//! Consolidates error types from various live mode modules into a single
//! hierarchy for easier error handling and propagation.

use thiserror::Error;

use super::bincode_io::StorageError;
use crate::db::DbError;
use crate::rpc::RpcError;

/// Unified error type for live mode operations.
///
/// This consolidates errors from progress tracking, eth_call collection,
/// and other live mode subsystems.
#[derive(Debug, Error)]
pub enum LiveError {
    /// Storage error (IO, serialization, etc.)
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// RPC communication error
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    /// Database operation error
    #[error("Database error: {0}")]
    Database(#[from] DbError),

    /// Channel closed (sender or receiver dropped)
    #[allow(dead_code)]
    #[error("Channel closed")]
    ChannelClosed,

    /// Block not found
    #[allow(dead_code)]
    #[error("Block not found: {0}")]
    BlockNotFound(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = LiveError::ChannelClosed;
        assert_eq!(err.to_string(), "Channel closed");

        let err = LiveError::BlockNotFound(12345);
        assert_eq!(err.to_string(), "Block not found: 12345");
    }

    #[test]
    fn test_database_from_db_error() {
        let db_err = DbError::ConfigError("test config error".to_string());
        let live_err = LiveError::from(db_err);
        assert!(matches!(live_err, LiveError::Database(_)));
        assert!(live_err.to_string().contains("test config error"));
    }
}
