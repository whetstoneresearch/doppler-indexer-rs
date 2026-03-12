//! Centralized configuration default values.
//!
//! This module contains all default values for configuration options
//! in one place, making it easier to maintain and understand the
//! default behavior of the indexer.

/// Database configuration defaults
pub mod database {
    /// Default environment variable name for the database URL
    pub const URL_ENV_VAR: &str = "DATABASE_URL";
}

/// Transformation system defaults
pub mod transformations {
    /// Default number of concurrent handler executions
    pub const HANDLER_CONCURRENCY: usize = 4;

    /// Default maximum operations per transaction batch
    pub const BATCH_SIZE: usize = 1000;

    /// Default: use batch mode for historical catchup
    pub const BATCH_FOR_CATCHUP: bool = true;

    /// Default batch size in blocks for batch mode
    pub const CATCHUP_BATCH_SIZE: usize = 10000;
}

#[allow(dead_code)]
/// Raw data collection defaults
pub mod raw_data {
    /// Default capacity for main channels (blocks, logs, eth_calls)
    pub const CHANNEL_CAPACITY: usize = 1000;

    /// Default capacity for factory-related channels
    pub const FACTORY_CHANNEL_CAPACITY: usize = 1000;

    /// Default number of blocks to fetch receipts for concurrently
    pub const BLOCK_RECEIPT_CONCURRENCY: usize = 10;

    /// Default number of concurrent decoding tasks
    pub const DECODING_CONCURRENCY: usize = 4;

    /// Default number of concurrent tasks for factory collection catchup
    pub const FACTORY_CONCURRENCY: usize = 4;

    /// Default number of blocks to track for reorg detection
    pub const REORG_DEPTH: u64 = 128;

    /// Default interval in seconds between compaction checks
    pub const COMPACTION_INTERVAL_SECS: u64 = 10;

    /// Default grace period in seconds before retrying stuck transformations
    pub const TRANSFORM_RETRY_GRACE_PERIOD_SECS: u64 = 300;
}

/// RPC client defaults
pub mod rpc {
    /// Default maximum batch size for RPC requests
    pub const MAX_BATCH_SIZE: u32 = 100;

    /// Default concurrency limit for concurrent RPC requests
    pub const CONCURRENCY: usize = 100;

    /// Default compute units per second for Alchemy
    pub const ALCHEMY_CU_PER_SECOND: u32 = 7500;
}

/// Database pool defaults
#[allow(dead_code)]
pub mod db_pool {
    /// Default maximum pool size
    pub const MAX_SIZE: usize = 16;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_defaults() {
        assert_eq!(database::URL_ENV_VAR, "DATABASE_URL");
    }

    #[test]
    fn test_transformation_defaults() {
        assert_eq!(transformations::HANDLER_CONCURRENCY, 4);
        assert_eq!(transformations::BATCH_SIZE, 1000);
        assert!(transformations::BATCH_FOR_CATCHUP);
        assert_eq!(transformations::CATCHUP_BATCH_SIZE, 10000);
    }

    #[test]
    fn test_raw_data_defaults() {
        assert_eq!(raw_data::CHANNEL_CAPACITY, 1000);
        assert_eq!(raw_data::FACTORY_CHANNEL_CAPACITY, 1000);
        assert_eq!(raw_data::BLOCK_RECEIPT_CONCURRENCY, 10);
        assert_eq!(raw_data::DECODING_CONCURRENCY, 4);
        assert_eq!(raw_data::FACTORY_CONCURRENCY, 4);
        assert_eq!(raw_data::REORG_DEPTH, 128);
        assert_eq!(raw_data::COMPACTION_INTERVAL_SECS, 10);
        assert_eq!(raw_data::TRANSFORM_RETRY_GRACE_PERIOD_SECS, 300);
    }

    #[test]
    fn test_rpc_defaults() {
        assert_eq!(rpc::MAX_BATCH_SIZE, 100);
        assert_eq!(rpc::CONCURRENCY, 100);
        assert_eq!(rpc::ALCHEMY_CU_PER_SECOND, 7500);
    }

    #[test]
    fn test_db_pool_defaults() {
        assert_eq!(db_pool::MAX_SIZE, 16);
    }
}
