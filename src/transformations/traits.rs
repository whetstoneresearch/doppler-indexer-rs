//! Core traits for transformation handlers.
//!
//! Handlers implement these traits to receive decoded events and eth_calls,
//! transform the data, and produce database operations.

use async_trait::async_trait;

use crate::db::{DbOperation, DbPool};

use super::context::TransformationContext;
use super::error::TransformationError;

/// Core trait that all transformation handlers must implement.
///
/// Handlers are registered at compile-time and invoked when their
/// registered triggers (events or calls) are decoded.
///
/// Each handler declares a `version()` and a `handler_key()` used for
/// per-handler progress tracking. Bumping the version causes the handler
/// to reprocess all data into a new versioned output table.
#[async_trait]
pub trait TransformationHandler: Send + Sync + 'static {
    /// Unique name for this handler (used in logging and progress tracking).
    fn name(&self) -> &'static str;

    /// Version of this handler. Bump when the handler logic changes and you
    /// want to reprocess all data into a new versioned output table.
    fn version(&self) -> u32 {
        1
    }

    /// Computed identity key: `"{name}_v{version}"`.
    /// Used for progress tracking in the `_handler_progress` table.
    fn handler_key(&self) -> String {
        format!("{}_v{}", self.name(), self.version())
    }

    /// Migration paths for this handler's SQL files, relative to the project root.
    /// Each path can be either:
    /// - A directory: all `.sql` files in it are run in alphabetical order
    /// - A single `.sql` file: run directly
    ///
    /// Multiple paths are supported for handlers that write to multiple tables.
    /// Directories are scanned flat (no subdirectories).
    ///
    /// Examples:
    /// - `vec!["migrations/handlers/pools"]` — run all SQL in the `pools/` dir
    /// - `vec!["migrations/handlers/pools/create_table.sql"]` — run one file
    /// - `vec!["migrations/handlers/pools", "migrations/handlers/swaps"]` — multiple dirs
    fn migration_paths(&self) -> Vec<&'static str> {
        vec![]
    }

    /// Process decoded data for a block range.
    ///
    /// Called once per block range with all decoded events/calls matching
    /// this handler's triggers. Returns a list of database operations to
    /// execute transactionally.
    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError>;

    /// Optional: Called once at startup for initialization.
    ///
    /// Can be used to create indexes, warm caches, etc.
    #[allow(unused_variables)]
    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        Ok(())
    }

    /// Tables this handler writes to that can be rolled back on reorg.
    ///
    /// Each table returned must have a `block_number` column for deletion targeting.
    /// Tables are automatically scoped by `source` and `source_version` during reorg cleanup.
    ///
    /// Handlers opt-in by implementing this method. By default, no tables are tracked
    /// for reorg rollback (returns empty vec).
    ///
    /// # Example
    /// ```ignore
    /// fn reorg_tables(&self) -> Vec<&'static str> {
    ///     vec!["transfers", "swaps"]
    /// }
    /// ```
    fn reorg_tables(&self) -> Vec<&'static str> {
        vec![]
    }

    /// Whether this handler must process block ranges sequentially (one at a time, in order).
    ///
    /// When true, the catchup engine limits concurrency to 1 for this handler, relying on
    /// Tokio's FIFO semaphore to guarantee ranges execute in ascending block order.
    ///
    /// Required for handlers that maintain cumulative in-memory state that depends on
    /// block ordering (e.g., tracking running totals across block ranges).
    fn requires_sequential(&self) -> bool {
        false
    }

    /// Called after the finalizer has rolled back DB rows for orphaned blocks.
    /// Handlers that cache stateful DB-derived values in memory should invalidate
    /// or reload the affected entries here. Default is a no-op.
    #[allow(unused_variables)]
    async fn on_reorg(&self, orphaned: &[u64]) -> Result<(), TransformationError> {
        Ok(())
    }

    /// Called after the ops returned by `handle()` committed successfully.
    /// Lets handlers promote any in-flight optimistic state.
    #[allow(unused_variables)]
    async fn on_commit_success(
        &self,
        range: (u64, u64),
    ) -> Result<(), TransformationError> {
        Ok(())
    }

    /// Called after the ops returned by `handle()` failed to commit.
    /// Lets handlers revert optimistic state and mark the range for retry.
    #[allow(unused_variables)]
    async fn on_commit_failure(
        &self,
        range: (u64, u64),
    ) -> Result<(), TransformationError> {
        Ok(())
    }
}

/// Trigger for event-based handlers.
#[derive(Debug, Clone)]
pub struct EventTrigger {
    /// Contract name or factory collection name from config.
    pub source: String,
    /// Event signature (e.g., "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)").
    pub event_signature: String,
}

impl EventTrigger {
    pub fn new(source: impl Into<String>, event_signature: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            event_signature: event_signature.into(),
        }
    }
}

/// Trigger for eth_call-based handlers.
#[derive(Debug, Clone)]
pub struct EthCallTrigger {
    /// Contract name or factory collection name from config.
    pub source: String,
    /// Function name (e.g., "slot0").
    pub function_name: String,
}

impl EthCallTrigger {
    #[allow(dead_code)]
    pub fn new(source: impl Into<String>, function_name: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            function_name: function_name.into(),
        }
    }
}

/// Marker trait for handlers that respond to events.
pub trait EventHandler: TransformationHandler {
    /// Event triggers this handler responds to.
    fn triggers(&self) -> Vec<EventTrigger>;

    /// Declare eth_call types this handler needs access to.
    /// Returns (source_name, function_name) pairs that must be available
    /// in decoded parquet files before this handler can process a range.
    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![]
    }

    /// Handler names (via `TransformationHandler::name()`) that must complete
    /// before this handler can execute for a given block range.
    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec![]
    }
}

/// Marker trait for handlers that respond to eth_call results.
pub trait EthCallHandler: TransformationHandler {
    /// eth_call triggers this handler responds to.
    fn triggers(&self) -> Vec<EthCallTrigger>;
}
