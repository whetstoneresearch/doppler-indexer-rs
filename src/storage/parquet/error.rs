//! Shared error type for parquet reading operations.

/// Error type for parquet reading operations.
///
/// This is the canonical error for all functions that read data from parquet files.
/// Upstream callers wrap it via `From<ParquetReadError>` impls on their own error types,
/// so the `?` operator works seamlessly at call sites.
#[derive(Debug, thiserror::Error)]
pub enum ParquetReadError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}
