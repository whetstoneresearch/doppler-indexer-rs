//! Per-block progress tracking for live mode.
//!
//! Tracks which handlers have completed processing for each block,
//! enabling the compaction service to know when ranges are ready.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio_postgres::types::ToSql;

use super::error::LiveError;
use super::storage::LiveStorage;
use crate::db::DbPool;

/// Tracks per-block progress for all handlers.
pub struct LiveProgressTracker {
    chain_id: i64,
    /// Chain name for LiveStorage access.
    chain_name: String,
    /// Set of (block_number, handler_key) pairs that are complete.
    completed: HashMap<u64, HashSet<String>>,
    /// All known handler keys.
    handler_keys: HashSet<String>,
    /// Database pool for persistence (optional).
    db_pool: Option<Arc<DbPool>>,
}

impl LiveProgressTracker {
    /// Create a new progress tracker.
    pub fn new(chain_id: i64, db_pool: Option<Arc<DbPool>>, chain_name: String) -> Self {
        Self {
            chain_id,
            chain_name,
            completed: HashMap::new(),
            handler_keys: HashSet::new(),
            db_pool,
        }
    }

    /// Register a handler key. Must be called before marking progress.
    pub fn register_handler(&mut self, handler_key: &str) {
        self.handler_keys.insert(handler_key.to_string());
    }

    /// Get all registered handler keys.
    pub fn handler_keys(&self) -> &HashSet<String> {
        &self.handler_keys
    }

    /// Mark a handler as complete for a block.
    ///
    /// Updates both in-memory state and persists to the status file for catchup.
    /// Uses atomic file updates to prevent race conditions when multiple handlers
    /// complete simultaneously.
    pub async fn mark_complete(
        &mut self,
        block_number: u64,
        handler_key: &str,
    ) -> Result<(), LiveError> {
        // Update in-memory state
        self.completed
            .entry(block_number)
            .or_default()
            .insert(handler_key.to_string());

        // Persist to database if available
        if let Some(ref db_pool) = self.db_pool {
            let handler_key_str = handler_key.to_string();
            let block_num = block_number as i64;

            db_pool
                .query(
                    "INSERT INTO _live_progress (chain_id, handler_key, block_number)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (chain_id, handler_key, block_number) DO NOTHING",
                    &[
                        &self.chain_id as &(dyn ToSql + Sync),
                        &handler_key_str as &(dyn ToSql + Sync),
                        &block_num as &(dyn ToSql + Sync),
                    ],
                )
                .await?;
        }

        // Persist handler completion to status file using atomic update
        // to prevent race conditions when multiple handlers complete simultaneously
        let storage = LiveStorage::new(&self.chain_name);
        let handler_key_owned = handler_key.to_string();
        let all_complete = self.is_block_complete(block_number);
        let handler_count = self.handler_keys.len();
        let pending = self.get_pending_handlers(block_number);

        let update_result = storage.update_status_atomic(block_number, |status| {
            status.completed_handlers.insert(handler_key_owned.clone());

            if all_complete {
                status.transformed = true;
            }
        });

        match update_result {
            Ok(()) => {
                if all_complete {
                    tracing::info!(
                        "Block {} fully transformed ({} handlers complete)",
                        block_number,
                        handler_count
                    );
                } else {
                    tracing::debug!(
                        "Block {} handler '{}' complete, {} remaining: {:?}",
                        block_number,
                        handler_key,
                        pending.len(),
                        pending
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Failed to update block status after handler completion: {}", e);
            }
        }

        Ok(())
    }

    /// Check if a block is complete (all handlers have processed it).
    pub fn is_block_complete(&self, block_number: u64) -> bool {
        if self.handler_keys.is_empty() {
            return true;
        }

        if let Some(completed) = self.completed.get(&block_number) {
            completed.len() == self.handler_keys.len()
                && self.handler_keys.iter().all(|k| completed.contains(k))
        } else {
            false
        }
    }

    /// Mark a block as transformed when no handlers are registered.
    /// This should be called after all collection/decoding is done.
    pub fn mark_transformed_if_no_handlers(&self, block_number: u64) {
        if !self.handler_keys.is_empty() {
            return;
        }

        let storage = LiveStorage::new(&self.chain_name);
        if let Ok(mut status) = storage.read_status(block_number) {
            status.transformed = true;
            if let Err(e) = storage.write_status(block_number, &status) {
                tracing::warn!("Failed to update block status (no handlers): {}", e);
            }
        }
    }

    /// Get the set of handlers that have completed for a block.
    pub fn get_completed_handlers(&self, block_number: u64) -> HashSet<String> {
        self.completed
            .get(&block_number)
            .cloned()
            .unwrap_or_default()
    }

    /// Get the set of handlers that have NOT completed for a block.
    pub fn get_pending_handlers(&self, block_number: u64) -> HashSet<String> {
        let completed = self.get_completed_handlers(block_number);
        self.handler_keys.difference(&completed).cloned().collect()
    }

    /// Clear progress for a block (used after compaction).
    pub fn clear_block(&mut self, block_number: u64) {
        self.completed.remove(&block_number);
    }

    /// Load progress from database for a range of blocks.
    #[allow(dead_code)]
    pub async fn load_from_db(&mut self, from: u64, to: u64) -> Result<(), LiveError> {
        let Some(ref db_pool) = self.db_pool else {
            return Ok(());
        };

        let from_i64 = from as i64;
        let to_i64 = to as i64;

        let rows = db_pool
            .query(
                "SELECT block_number, handler_key FROM _live_progress
                 WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3",
                &[
                    &self.chain_id as &(dyn ToSql + Sync),
                    &from_i64 as &(dyn ToSql + Sync),
                    &to_i64 as &(dyn ToSql + Sync),
                ],
            )
            .await?;

        for row in rows {
            let block_number: i64 = row.get(0);
            let handler_key: String = row.get(1);

            self.completed
                .entry(block_number as u64)
                .or_default()
                .insert(handler_key);
        }

        Ok(())
    }

    /// Load progress from storage status files.
    ///
    /// Reads completed_handlers from each block's status file to seed the
    /// in-memory state on restart. This enables catchup without database queries.
    pub fn load_from_storage(&mut self, storage: &LiveStorage) -> Result<(), LiveError> {
        let blocks = storage.list_blocks()?;

        for block_number in blocks {
            match storage.read_status(block_number) {
                Ok(status) => {
                    if !status.completed_handlers.is_empty() {
                        self.completed
                            .entry(block_number)
                            .or_default()
                            .extend(status.completed_handlers);
                    }
                }
                Err(super::storage::StorageError::NotFound(_)) => {
                    // Status file doesn't exist, skip
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to read status for block {} during load_from_storage: {}",
                        block_number,
                        e
                    );
                }
            }
        }

        tracing::info!(
            "Loaded progress from storage: {} blocks with handler completions",
            self.completed.len()
        );

        Ok(())
    }

    /// Get blocks that are complete within a range.
    #[allow(dead_code)]
    pub fn get_complete_blocks_in_range(&self, from: u64, to: u64) -> Vec<u64> {
        (from..=to).filter(|&n| self.is_block_complete(n)).collect()
    }

    /// Get the lowest block number that is not complete.
    #[allow(dead_code)]
    pub fn lowest_incomplete_block(&self, from: u64) -> Option<u64> {
        let mut block = from;
        loop {
            if !self.is_block_complete(block) {
                return Some(block);
            }
            // Check if we have any data for this block
            if !self.completed.contains_key(&block) {
                return Some(block);
            }
            block += 1;
            // Safeguard against infinite loop
            if block > from + 10000 {
                return None;
            }
        }
    }

    /// Get statistics about tracked progress.
    #[allow(dead_code)]
    pub fn stats(&self) -> ProgressStats {
        let total_blocks = self.completed.len();
        let complete_blocks = self
            .completed
            .keys()
            .filter(|&&n| self.is_block_complete(n))
            .count();

        ProgressStats {
            _total_blocks: total_blocks,
            _complete_blocks: complete_blocks,
            _handler_count: self.handler_keys.len(),
        }
    }
}

impl std::fmt::Debug for LiveProgressTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveProgressTracker")
            .field("chain_id", &self.chain_id)
            .field("completed_count", &self.completed.len())
            .field("handler_keys", &self.handler_keys)
            .finish()
    }
}

/// Statistics about progress tracking.
#[derive(Debug, Clone)]
pub struct ProgressStats {
    pub _total_blocks: usize,
    pub _complete_blocks: usize,
    pub _handler_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_tracker_basic() {
        let mut tracker = LiveProgressTracker::new(1, None, "test_chain".to_string());
        tracker.register_handler("handler_a");
        tracker.register_handler("handler_b");

        // Block not complete initially
        assert!(!tracker.is_block_complete(100));

        // Mark handler_a complete
        tokio_test::block_on(tracker.mark_complete(100, "handler_a")).unwrap();
        assert!(!tracker.is_block_complete(100));

        // Mark handler_b complete
        tokio_test::block_on(tracker.mark_complete(100, "handler_b")).unwrap();
        assert!(tracker.is_block_complete(100));
    }

    #[test]
    fn test_pending_handlers() {
        let mut tracker = LiveProgressTracker::new(1, None, "test_chain".to_string());
        tracker.register_handler("a");
        tracker.register_handler("b");
        tracker.register_handler("c");

        tokio_test::block_on(tracker.mark_complete(50, "a")).unwrap();
        tokio_test::block_on(tracker.mark_complete(50, "c")).unwrap();

        let pending = tracker.get_pending_handlers(50);
        assert_eq!(pending.len(), 1);
        assert!(pending.contains("b"));
    }

    #[test]
    fn test_clear_block() {
        let mut tracker = LiveProgressTracker::new(1, None, "test_chain".to_string());
        tracker.register_handler("handler");

        tokio_test::block_on(tracker.mark_complete(200, "handler")).unwrap();
        assert!(tracker.is_block_complete(200));

        tracker.clear_block(200);
        assert!(!tracker.is_block_complete(200));
    }

    #[test]
    fn test_empty_handlers() {
        let tracker = LiveProgressTracker::new(1, None, "test_chain".to_string());
        // With no handlers, all blocks are "complete"
        assert!(tracker.is_block_complete(100));
    }
}
