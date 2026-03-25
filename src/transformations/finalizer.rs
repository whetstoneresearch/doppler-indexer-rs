//! Range finalization, reorg cleanup, and progress tracking.
//!
//! Manages the lifecycle of processed ranges: recording handler progress,
//! detecting finalization readiness, executing finalization, and cleaning up
//! after reorgs.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use super::error::TransformationError;
use super::live_state::LiveProcessingState;
use super::registry::TransformationRegistry;
use crate::db::{DbOperation, DbPool, DbValue, WhereClause};
use crate::live::{LiveProgressTracker, LiveStorage, StorageError};

/// Handles range finalization, reorg cleanup, and progress tracking.
pub(crate) struct RangeFinalizer {
    pub registry: Arc<TransformationRegistry>,
    pub db_pool: Arc<DbPool>,
    pub chain_name: String,
    pub chain_id: u64,
    pub progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
    pub expect_log_completion: bool,
    pub expect_eth_call_completion: bool,
}

impl RangeFinalizer {
    // ─── Per-Handler Progress Tracking ───────────────────────────────

    /// Get completed ranges for a specific handler from the database.
    pub async fn get_completed_ranges_for_handler(
        &self,
        handler_key: &str,
    ) -> Result<HashSet<u64>, TransformationError> {
        let rows = self
            .db_pool
            .query(
                "SELECT range_start FROM _handler_progress WHERE chain_id = $1 AND handler_key = $2",
                &[&(self.chain_id as i64), &handler_key.to_string()],
            )
            .await?;

        let mut completed = HashSet::new();
        for row in rows {
            let range_start: i64 = row.get(0);
            completed.insert(range_start as u64);
        }

        Ok(completed)
    }

    /// Record a completed range for a specific handler.
    pub async fn record_completed_range_for_handler(
        &self,
        handler_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<(), TransformationError> {
        self.db_pool
            .execute_transaction(vec![DbOperation::Upsert {
                table: "_handler_progress".to_string(),
                columns: vec![
                    "chain_id".to_string(),
                    "handler_key".to_string(),
                    "range_start".to_string(),
                    "range_end".to_string(),
                ],
                values: vec![
                    DbValue::Int64(self.chain_id as i64),
                    DbValue::Text(handler_key.to_string()),
                    DbValue::Int64(range_start as i64),
                    DbValue::Int64(range_end as i64),
                ],
                conflict_columns: vec![
                    "chain_id".to_string(),
                    "handler_key".to_string(),
                    "range_start".to_string(),
                ],
                update_columns: vec!["range_end".to_string()],
            }])
            .await?;

        Ok(())
    }

    // ─── Range Completion ─────────────────────────────────────────────

    /// Process range completion signal.
    pub async fn process_range_complete(
        &self,
        range_key: (u64, u64),
        kind: super::engine::RangeCompleteKind,
        live_state: &Mutex<LiveProcessingState>,
    ) -> Result<(), TransformationError> {
        {
            let mut state = live_state.lock().await;
            state.completion.entry(range_key).or_default().mark(kind);
        }

        self.maybe_finalize_range(range_key, live_state).await?;
        Ok(())
    }

    /// Check if a range is ready for finalization and finalize if so.
    pub async fn maybe_finalize_range(
        &self,
        range_key: (u64, u64),
        live_state: &Mutex<LiveProcessingState>,
    ) -> Result<(), TransformationError> {
        let (should_finalize, timed_out_handlers) = {
            let mut state = live_state.lock().await;
            state.check_finalization_readiness(
                range_key,
                self.expect_log_completion,
                self.range_requires_eth_call_completion(range_key),
            )
        };

        if !timed_out_handlers.is_empty() {
            tracing::warn!(
                "Removed {} timed-out pending event handlers for range {:?}: {:?}",
                timed_out_handlers.len(),
                range_key,
                timed_out_handlers
            );
        }

        if !should_finalize {
            return Ok(());
        }

        self.finalize_range(range_key.0, range_key.1, live_state)
            .await
    }

    /// Finalize a range: record progress for non-failed handlers and clean up state.
    ///
    /// For single-block ranges, reads failed/completed handlers from the status file
    /// to avoid marking failed handlers as complete and to skip already-completed ones.
    /// Only sets `transformed=true` when no failed handlers remain.
    pub async fn finalize_range(
        &self,
        range_start: u64,
        range_end: u64,
        live_state: &Mutex<LiveProcessingState>,
    ) -> Result<(), TransformationError> {
        let range_key = (range_start, range_end);

        // Prevent double finalization
        {
            let mut state = live_state.lock().await;
            if !state.mark_finalized(range_key) {
                return Ok(());
            }
        }

        let is_single_block = range_end - range_start == 1;

        // Read failed/completed handlers from status file (single-block only)
        let (failed_handlers, completed_handlers) = if is_single_block {
            let storage = LiveStorage::new(&self.chain_name);
            match storage.read_status(range_start) {
                Ok(status) => (status.failed_handlers, status.completed_handlers),
                Err(StorageError::NotFound(_)) => (HashSet::new(), HashSet::new()),
                Err(e) => {
                    tracing::warn!(
                        "Failed to read status for block {}, finalizing all handlers: {}",
                        range_start,
                        e
                    );
                    (HashSet::new(), HashSet::new())
                }
            }
        } else {
            (HashSet::new(), HashSet::new())
        };

        let mut skipped_failed = 0usize;
        let mut skipped_completed = 0usize;

        // Mark handlers as complete, skipping failed and already-completed ones
        for handler in self.registry.all_handlers() {
            let handler_key = handler.handler_key();

            if failed_handlers.contains(&handler_key) {
                skipped_failed += 1;
                continue;
            }

            if completed_handlers.contains(&handler_key) {
                skipped_completed += 1;
                continue;
            }

            self.record_completed_range_for_handler(&handler_key, range_start, range_end)
                .await?;

            if is_single_block {
                if let Some(ref tracker) = self.progress_tracker {
                    let mut t = tracker.lock().await;
                    if let Err(e) = t.mark_complete(range_start, &handler_key).await {
                        tracing::warn!(
                            "Failed to mark live progress for block {} handler {}: {}",
                            range_start,
                            handler_key,
                            e
                        );
                    }
                }
            }
        }

        {
            let mut state = live_state.lock().await;
            state.cleanup_after_finalize(range_key);
        }

        if skipped_failed > 0 || skipped_completed > 0 {
            tracing::debug!(
                "Finalized range {}-{}: skipped {} failed, {} already-completed handlers",
                range_start,
                range_end,
                skipped_failed,
                skipped_completed
            );
        }

        tracing::debug!(
            "Recorded progress for {} handlers on range {}-{}",
            self.registry.all_handlers().len() - skipped_failed - skipped_completed,
            range_start,
            range_end
        );

        // Only set transformed=true if no failed handlers remain
        if is_single_block {
            let storage = LiveStorage::new(&self.chain_name);
            let registered_keys: HashSet<String> = self
                .registry
                .all_handlers()
                .iter()
                .map(|h| h.handler_key())
                .collect();
            if let Err(e) = update_finalization_status(&storage, range_start, &registered_keys) {
                if !matches!(e, StorageError::NotFound(_)) {
                    tracing::warn!(
                        "Failed to update status for block {} during finalization: {}",
                        range_start,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Check if this range requires eth_call completion signal before finalization.
    fn range_requires_eth_call_completion(&self, range_key: (u64, u64)) -> bool {
        self.expect_eth_call_completion && range_key.1.saturating_sub(range_key.0) == 1
    }

    // ─── Reorg Cleanup ─────────────────────────────────────────────────

    /// Process a reorg notification.
    pub async fn process_reorg(
        &self,
        common_ancestor: u64,
        orphaned: &[u64],
        live_state: &Mutex<LiveProcessingState>,
    ) -> Result<(), TransformationError> {
        tracing::info!(
            "Processing reorg: common_ancestor={}, orphaned={:?}",
            common_ancestor,
            orphaned
        );

        if orphaned.is_empty() {
            return Ok(());
        }

        // Phase 1: Clean up in-memory state
        let total_removed = {
            let mut state = live_state.lock().await;
            state.cleanup_for_orphaned_blocks(orphaned)
        };
        if total_removed > 0 {
            tracing::info!(
                "Reorg cleanup: removed {} pending events for {} orphaned blocks",
                total_removed,
                orphaned.len()
            );
        }

        // Phase 2: Delete committed rows from database tables
        self.cleanup_reorg_tables(orphaned).await?;

        // Phase 3: Clean up _live_progress entries
        self.cleanup_live_progress(orphaned).await?;

        Ok(())
    }

    /// Rollback committed rows for orphaned blocks using snapshots.
    async fn cleanup_reorg_tables(&self, orphaned: &[u64]) -> Result<(), TransformationError> {
        let mut tables_to_clean: HashSet<&str> = HashSet::new();
        for handler in self.registry.all_handlers() {
            tables_to_clean.extend(handler.reorg_tables());
        }

        if tables_to_clean.is_empty() {
            tracing::debug!("No reorg tables declared by handlers, skipping database cleanup");
            return Ok(());
        }

        let storage = LiveStorage::new(&self.chain_name);
        let mut restore_ops = Vec::new();
        let mut tables_with_snapshots: HashSet<String> = HashSet::new();

        // Phase 2a: Read snapshots and generate restore operations
        for &block_number in orphaned {
            match storage.read_snapshots(block_number) {
                Ok(snapshots) => {
                    for snapshot in snapshots {
                        tables_with_snapshots.insert(snapshot.table.clone());

                        match snapshot.previous_row {
                            Some(previous) => {
                                let columns: Vec<String> =
                                    previous.iter().map(|(k, _)| k.clone()).collect();
                                let values: Vec<DbValue> =
                                    previous.iter().map(|(_, v)| v.to_db_value()).collect();
                                let conflict_cols: Vec<String> = snapshot
                                    .key_columns
                                    .iter()
                                    .map(|(k, _)| k.clone())
                                    .collect();

                                let update_cols: Vec<String> = columns
                                    .iter()
                                    .filter(|c| !conflict_cols.contains(c))
                                    .cloned()
                                    .collect();

                                restore_ops.push(DbOperation::Upsert {
                                    table: snapshot.table,
                                    columns,
                                    values,
                                    conflict_columns: conflict_cols,
                                    update_columns: update_cols,
                                });
                            }
                            None => {
                                let key_conditions: Vec<(String, DbValue)> = snapshot
                                    .key_columns
                                    .into_iter()
                                    .map(|(k, v)| (k, v.to_db_value()))
                                    .collect();

                                restore_ops.push(DbOperation::Delete {
                                    table: snapshot.table,
                                    where_clause: WhereClause::And(key_conditions),
                                });
                            }
                        }
                    }
                }
                Err(StorageError::NotFound(_)) => {
                    tracing::debug!(
                        "No snapshots found for block {}, will use fallback delete",
                        block_number
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to read snapshots for block {}: {}, using fallback delete",
                        block_number,
                        e
                    );
                }
            }
        }

        if !restore_ops.is_empty() {
            tracing::info!(
                "Reorg rollback: executing {} restore operations from snapshots",
                restore_ops.len()
            );
            self.db_pool.execute_transaction(restore_ops).await?;
        }

        // Phase 2b: Delete remaining rows without snapshots
        let block_list = orphaned
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let mut fallback_ops = Vec::new();
        for table in &tables_to_clean {
            if tables_with_snapshots.contains(*table) {
                continue;
            }

            fallback_ops.push(DbOperation::Delete {
                table: table.to_string(),
                where_clause: WhereClause::Raw {
                    condition: format!(
                        "chain_id = {} AND block_number IN ({})",
                        self.chain_id, block_list
                    ),
                    params: vec![],
                },
            });
        }

        if !fallback_ops.is_empty() {
            tracing::info!(
                "Reorg cleanup: fallback deleting from {} tables for {} orphaned blocks",
                fallback_ops.len(),
                orphaned.len()
            );
            self.db_pool.execute_transaction(fallback_ops).await?;
        }

        // Delete snapshot files for orphaned blocks
        for &block_number in orphaned {
            if let Err(e) = storage.delete_snapshots(block_number) {
                tracing::warn!(
                    "Failed to delete snapshots for block {}: {}",
                    block_number,
                    e
                );
            }
        }

        Ok(())
    }

    /// Clean up _live_progress entries for orphaned blocks.
    async fn cleanup_live_progress(&self, orphaned: &[u64]) -> Result<(), TransformationError> {
        if orphaned.is_empty() {
            return Ok(());
        }

        let block_list = orphaned
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let ops = vec![DbOperation::Delete {
            table: "_live_progress".to_string(),
            where_clause: WhereClause::Raw {
                condition: format!(
                    "chain_id = {} AND block_number IN ({})",
                    self.chain_id, block_list
                ),
                params: vec![],
            },
        }];

        tracing::debug!(
            "Reorg cleanup: deleting _live_progress for {} orphaned blocks",
            orphaned.len()
        );
        self.db_pool.execute_transaction(ops).await?;

        Ok(())
    }
}

/// Update the status file during finalization: prune stale failed handler keys,
/// then set `transformed=true` only when no failures remain. This is the second
/// half of the two-phase protocol — retry records handler outcomes, finalization
/// gates the `transformed` flag.
pub(crate) fn update_finalization_status(
    storage: &LiveStorage,
    block_number: u64,
    registered_keys: &HashSet<String>,
) -> Result<(), StorageError> {
    storage.update_status_atomic(block_number, |status| {
        // Filter stale keys: only keep failed handlers that are still registered
        status
            .failed_handlers
            .retain(|k| registered_keys.contains(k));

        if status.failed_handlers.is_empty() {
            status.transformed = true;
        }
    })
}
