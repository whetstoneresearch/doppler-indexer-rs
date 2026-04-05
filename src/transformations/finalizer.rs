//! Range finalization, reorg cleanup, and progress tracking.
//!
//! Manages the lifecycle of processed ranges: recording handler progress,
//! detecting finalization readiness, executing finalization, and cleaning up
//! after reorgs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::Mutex;

use super::error::TransformationError;
use super::live_state::LiveProcessingState;
use super::registry::TransformationRegistry;
use crate::db::{DbOperation, DbPool, DbValue, WhereClause};
use crate::live::{LiveProgressTracker, LiveStorage, LiveUpsertSnapshot, StorageError};

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
                update_condition: None,
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

            // Persist timed-out handlers as failures so they remain retryable
            let is_single_block = range_key.1 - range_key.0 == 1;
            if is_single_block {
                let storage = LiveStorage::new(&self.chain_name);
                if let Err(e) = storage.update_status_atomic(range_key.0, |status| {
                    for key in &timed_out_handlers {
                        status.failed_handlers.insert(key.clone());
                        status.completed_handlers.remove(key);
                    }
                    status.transformed = false;
                }) {
                    if !matches!(e, StorageError::NotFound(_)) {
                        tracing::warn!(
                            "Failed to persist timed-out handlers for block {}: {}",
                            range_key.0,
                            e
                        );
                    }
                }
            }

            // Update in-memory state: mark failed, remove from completed
            {
                let mut state = live_state.lock().await;
                for key in &timed_out_handlers {
                    if let Some(name) = self.registry.handler_name_for_key(key) {
                        state
                            .failed_handlers
                            .entry(range_key)
                            .or_default()
                            .insert(name.to_string());
                        if let Some(completed) = state.completed_handlers.get_mut(&range_key) {
                            completed.remove(name);
                        }
                    }
                }
            }
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

        // Phase 2.5: Notify handlers so they can invalidate in-memory caches
        // that mirror rolled-back DB state.
        for handler in self.registry.all_handlers() {
            handler.on_reorg(orphaned).await?;
        }

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
        // Maps table_name -> set of block numbers for which we have snapshots.
        let mut tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();

        // Phase 2a: Read snapshots and dedupe per (table, source, source_version,
        // key_columns). Keep the OLDEST snapshot seen for each key so the
        // restored row reflects the pre-rollback state, not the most recent
        // orphaned write.
        let mut per_block_snapshots: Vec<(u64, Vec<LiveUpsertSnapshot>)> = Vec::new();
        for &block_number in orphaned {
            match storage.read_snapshots(block_number) {
                Ok(snapshots) => {
                    per_block_snapshots.push((block_number, snapshots));
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

        let deduped = dedupe_restore_snapshots(per_block_snapshots, &mut tables_covered);

        let mut restore_ops = Vec::new();
        for snapshot in deduped {
            match snapshot.previous_row {
                Some(previous) => {
                    let columns: Vec<String> = previous.iter().map(|(k, _)| k.clone()).collect();
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
                        update_condition: None,
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

        if !restore_ops.is_empty() {
            tracing::info!(
                "Reorg rollback: executing {} restore operations from snapshots",
                restore_ops.len()
            );
            self.db_pool.execute_transaction(restore_ops).await?;
        }

        // Phase 2b: Delete remaining rows without snapshots.
        // Only delete the specific blocks not covered by a snapshot for each table.
        let fallback_entries =
            compute_fallback_deletes(&tables_to_clean, &tables_covered, orphaned);
        let fallback_ops: Vec<DbOperation> = fallback_entries
            .into_iter()
            .map(|(table, uncovered)| {
                let block_list = uncovered
                    .iter()
                    .map(|b| b.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                DbOperation::Delete {
                    table,
                    where_clause: WhereClause::Raw {
                        condition: format!(
                            "chain_id = {} AND block_number IN ({})",
                            self.chain_id, block_list
                        ),
                        params: vec![],
                    },
                }
            })
            .collect();

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

/// Dedupe rollback snapshots so only the OLDEST snapshot per
/// `(table, source, source_version, key_columns)` is kept. Updates
/// `tables_covered` with the set of blocks that had a snapshot per table.
///
/// `per_block_snapshots` must be ordered ascending by block number; the
/// function relies on this so the first snapshot seen for a given key is
/// the one whose `previous_row` holds the truly pre-rollback state.
pub(crate) fn dedupe_restore_snapshots(
    per_block_snapshots: Vec<(u64, Vec<LiveUpsertSnapshot>)>,
    tables_covered: &mut HashMap<String, HashSet<u64>>,
) -> Vec<LiveUpsertSnapshot> {
    type DedupKey = (String, String, u32, Vec<(String, Vec<u8>)>);
    fn dedup_key(s: &LiveUpsertSnapshot) -> DedupKey {
        let serialized_keys: Vec<(String, Vec<u8>)> = s
            .key_columns
            .iter()
            .map(|(name, v)| (name.clone(), bincode::serialize(v).unwrap_or_default()))
            .collect();
        (
            s.table.clone(),
            s.source.clone(),
            s.source_version,
            serialized_keys,
        )
    }

    let mut first_snapshot_by_key: HashMap<DedupKey, LiveUpsertSnapshot> = HashMap::new();
    for (block_number, snapshots) in per_block_snapshots {
        for snapshot in snapshots {
            tables_covered
                .entry(snapshot.table.clone())
                .or_default()
                .insert(block_number);
            first_snapshot_by_key
                .entry(dedup_key(&snapshot))
                .or_insert(snapshot);
        }
    }
    first_snapshot_by_key.into_values().collect()
}

/// Compute per-table fallback delete block lists.
///
/// For each table in `tables_to_clean`, returns `(table_name, uncovered_blocks)`
/// where `uncovered_blocks` is the subset of `orphaned` that have no snapshot
/// coverage for that table.  Tables where all orphaned blocks are covered are
/// omitted entirely.
pub(crate) fn compute_fallback_deletes<'a>(
    tables_to_clean: &HashSet<&'a str>,
    tables_covered: &HashMap<String, HashSet<u64>>,
    orphaned: &[u64],
) -> Vec<(String, Vec<u64>)> {
    let mut result = Vec::new();
    for &table in tables_to_clean {
        let covered = tables_covered.get(table);
        let uncovered: Vec<u64> = orphaned
            .iter()
            .copied()
            .filter(|b| !covered.map(|s| s.contains(b)).unwrap_or(false))
            .collect();
        if !uncovered.is_empty() {
            result.push((table.to_string(), uncovered));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::LiveDbValue;

    fn make_snapshot(
        table: &str,
        pool_id_byte: u8,
        previous_value: Option<i64>,
    ) -> LiveUpsertSnapshot {
        LiveUpsertSnapshot {
            table: table.to_string(),
            source: "TestHandler".to_string(),
            source_version: 1,
            key_columns: vec![(
                "pool_id".to_string(),
                LiveDbValue::Bytes32([pool_id_byte; 32]),
            )],
            previous_row: previous_value.map(|v| {
                vec![
                    (
                        "pool_id".to_string(),
                        LiveDbValue::Bytes32([pool_id_byte; 32]),
                    ),
                    ("value".to_string(), LiveDbValue::Int64(v)),
                ]
            }),
        }
    }

    /// When two orphaned blocks each snapshot the same (table, key), the
    /// dedupe must keep the OLDEST block's snapshot — that snapshot's
    /// `previous_row` holds the true pre-rollback state.
    #[test]
    fn test_dedupe_restore_snapshots_keeps_oldest_per_key() {
        // Simulate: blocks 100 and 101 both touched the same pool row.
        // Block 100's snapshot says "before me, value was 50".
        // Block 101's snapshot says "before me, value was 60" (post-block-100).
        // Reorg of [100, 101] should restore value=50, so dedupe must
        // keep the snapshot from block 100.
        let per_block = vec![
            (100u64, vec![make_snapshot("pool_state", 0xAA, Some(50))]),
            (101u64, vec![make_snapshot("pool_state", 0xAA, Some(60))]),
        ];

        let mut tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();
        let deduped = dedupe_restore_snapshots(per_block, &mut tables_covered);

        assert_eq!(deduped.len(), 1, "expected one snapshot after dedup");
        let kept = &deduped[0];
        assert_eq!(kept.table, "pool_state");

        // Verify the kept snapshot is the oldest one (value=50).
        let row = kept
            .previous_row
            .as_ref()
            .expect("snapshot should have previous_row");
        let value_cell = row
            .iter()
            .find(|(k, _)| k == "value")
            .expect("value column should exist");
        match &value_cell.1 {
            LiveDbValue::Int64(n) => assert_eq!(*n, 50, "expected oldest snapshot (value=50)"),
            other => panic!("unexpected value type: {:?}", other),
        }

        // tables_covered should record both block numbers for pool_state.
        let covered = tables_covered
            .get("pool_state")
            .expect("pool_state should be covered");
        assert!(covered.contains(&100));
        assert!(covered.contains(&101));
    }

    /// Snapshots for different pools in the same block remain separate.
    #[test]
    fn test_dedupe_restore_snapshots_different_keys_preserved() {
        let per_block = vec![(
            100u64,
            vec![
                make_snapshot("pool_state", 0xAA, Some(50)),
                make_snapshot("pool_state", 0xBB, Some(60)),
            ],
        )];

        let mut tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();
        let deduped = dedupe_restore_snapshots(per_block, &mut tables_covered);

        assert_eq!(deduped.len(), 2, "two distinct pool rows should both be kept");
    }

    /// Snapshots for different tables (same key) remain separate.
    #[test]
    fn test_dedupe_restore_snapshots_different_tables_preserved() {
        let per_block = vec![(
            100u64,
            vec![
                make_snapshot("pool_state", 0xAA, Some(50)),
                make_snapshot("pool_snapshots", 0xAA, Some(50)),
            ],
        )];

        let mut tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();
        let deduped = dedupe_restore_snapshots(per_block, &mut tables_covered);

        assert_eq!(deduped.len(), 2, "two distinct tables should both be kept");
    }

    /// Insert-case snapshot (previous_row = None) is still deduped.
    #[test]
    fn test_dedupe_restore_snapshots_delete_case_keeps_oldest() {
        // Block 100: row was just inserted (previous_row = None → DELETE on reorg).
        // Block 101: same row was updated (previous_row = Some).
        // Dedupe should keep block 100's snapshot (the DELETE) since that's
        // the correct rollback: the row didn't exist before block 100.
        let per_block = vec![
            (100u64, vec![make_snapshot("pool_state", 0xAA, None)]),
            (101u64, vec![make_snapshot("pool_state", 0xAA, Some(60))]),
        ];

        let mut tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();
        let deduped = dedupe_restore_snapshots(per_block, &mut tables_covered);

        assert_eq!(deduped.len(), 1);
        assert!(
            deduped[0].previous_row.is_none(),
            "expected the DELETE snapshot (previous_row = None) from block 100"
        );
    }


    /// Fix C test: when block 100 has a snapshot but block 101 does not,
    /// the fallback DELETE should target only block 101.
    #[test]
    fn test_per_block_fallback_delete_coverage() {
        let mut tables_to_clean: HashSet<&str> = HashSet::new();
        tables_to_clean.insert("v3_pool_metrics_v1");

        let mut tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();
        // Block 100 has a snapshot for "v3_pool_metrics_v1"
        tables_covered
            .entry("v3_pool_metrics_v1".to_string())
            .or_default()
            .insert(100);
        // Block 101 has NO snapshot

        let orphaned = vec![100u64, 101u64];

        let fallbacks = compute_fallback_deletes(&tables_to_clean, &tables_covered, &orphaned);

        assert_eq!(fallbacks.len(), 1);
        let (table, blocks) = &fallbacks[0];
        assert_eq!(table, "v3_pool_metrics_v1");
        // Only block 101 should be in the fallback delete list
        assert_eq!(*blocks, vec![101u64]);
    }

    /// When all orphaned blocks have snapshots, no fallback deletes are needed.
    #[test]
    fn test_no_fallback_delete_when_all_blocks_covered() {
        let mut tables_to_clean: HashSet<&str> = HashSet::new();
        tables_to_clean.insert("v3_pool_metrics_v1");

        let mut tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();
        let mut covered_set = HashSet::new();
        covered_set.insert(100u64);
        covered_set.insert(101u64);
        tables_covered.insert("v3_pool_metrics_v1".to_string(), covered_set);

        let orphaned = vec![100u64, 101u64];

        let fallbacks = compute_fallback_deletes(&tables_to_clean, &tables_covered, &orphaned);

        assert!(fallbacks.is_empty());
    }

    /// When a table has no coverage at all, all orphaned blocks need deletes.
    #[test]
    fn test_all_blocks_need_fallback_when_no_coverage() {
        let mut tables_to_clean: HashSet<&str> = HashSet::new();
        tables_to_clean.insert("v3_pool_metrics_v1");

        let tables_covered: HashMap<String, HashSet<u64>> = HashMap::new();

        let orphaned = vec![100u64, 101u64, 102u64];

        let fallbacks = compute_fallback_deletes(&tables_to_clean, &tables_covered, &orphaned);

        assert_eq!(fallbacks.len(), 1);
        let (_, blocks) = &fallbacks[0];
        assert_eq!(*blocks, vec![100u64, 101u64, 102u64]);
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
