//! Handler execution engine.
//!
//! Provides the unified handler spawn-loop pattern used by live processing,
//! catchup, and retry paths. Handles context construction, source/version
//! injection, database execution, and optional snapshot capture.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::context::{DecodedCall, DecodedEvent, TransactionAddresses, TransformationContext};
use super::error::TransformationError;
use super::historical::HistoricalDataReader;
use super::traits::TransformationHandler;
use crate::db::{DbOperation, DbPool, DbValue, WhereClause};
use crate::live::{LiveDbValue, LiveStorage, LiveUpsertSnapshot};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::contract::Contracts;

/// Outcome of a successful handler execution.
pub(crate) struct HandlerOutcome {
    pub handler_key: String,
    pub handler_name: String,
    pub range_start: u64,
    pub range_end: u64,
}

/// How to execute database operations.
pub(crate) enum DbExecMode {
    /// Execute directly via transaction (historical catchup, live events without snapshots).
    Direct,
    /// Capture snapshots before executing (live mode for reorg rollback).
    WithSnapshotCapture { chain_name: String },
}

/// A single handler task to execute.
pub(crate) struct HandlerTask {
    pub handler: Arc<dyn TransformationHandler>,
    pub events: Arc<Vec<DecodedEvent>>,
    pub calls: Arc<Vec<DecodedCall>>,
    pub tx_addresses: HashMap<[u8; 32], TransactionAddresses>,
}

/// Executes transformation handlers concurrently with bounded parallelism.
pub(crate) struct HandlerExecutor {
    pub db_pool: Arc<DbPool>,
    pub historical_reader: Arc<HistoricalDataReader>,
    pub rpc_client: Arc<UnifiedRpcClient>,
    pub contracts: Arc<Contracts>,
    pub chain_name: String,
    pub chain_id: u64,
    pub handler_concurrency: usize,
}

impl HandlerExecutor {
    /// Execute a set of handler tasks concurrently, returning outcomes for successful handlers.
    /// Failed handlers are logged and excluded from the result.
    pub async fn execute_handlers(
        &self,
        tasks: Vec<HandlerTask>,
        range_start: u64,
        range_end: u64,
        db_exec_mode: &DbExecMode,
    ) -> Vec<HandlerOutcome> {
        if tasks.is_empty() {
            return Vec::new();
        }

        let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
        let mut join_set: JoinSet<Result<Option<HandlerOutcome>, TransformationError>> =
            JoinSet::new();

        for task in tasks {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let db_pool = self.db_pool.clone();
            let chain_name = self.chain_name.clone();
            let chain_id = self.chain_id;
            let historical = self.historical_reader.clone();
            let rpc = self.rpc_client.clone();
            let contracts = self.contracts.clone();
            let handler = task.handler;
            let events = task.events;
            let calls = task.calls;
            let tx_addresses = task.tx_addresses;
            let handler_name = handler.name();
            let handler_version = handler.version();
            let handler_key = handler.handler_key();
            let snapshot_chain = match db_exec_mode {
                DbExecMode::Direct => None,
                DbExecMode::WithSnapshotCapture { chain_name } => Some(chain_name.clone()),
            };

            join_set.spawn(async move {
                let _permit = permit;
                let ctx = TransformationContext::new(
                    chain_name,
                    chain_id,
                    range_start,
                    range_end,
                    events,
                    calls,
                    tx_addresses,
                    historical,
                    rpc,
                    contracts,
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops = inject_source_version(ops, handler_name, handler_version);

                            let result = if let Some(ref cn) = snapshot_chain {
                                let storage = LiveStorage::new(cn);
                                execute_with_snapshot_capture(
                                    ops,
                                    &db_pool,
                                    Some(&storage),
                                    range_start,
                                    handler_name,
                                    handler_version,
                                )
                                .await
                            } else {
                                db_pool
                                    .execute_transaction(ops)
                                    .await
                                    .map_err(TransformationError::DatabaseError)
                            };

                            if let Err(e) = result {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key,
                                    range_start,
                                    range_end,
                                    e
                                );
                                return Ok(None);
                            }
                        }
                        Ok(Some(HandlerOutcome {
                            handler_key,
                            handler_name: handler_name.to_string(),
                            range_start,
                            range_end,
                        }))
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for range {}-{}: {}",
                            handler_key,
                            range_start,
                            range_end,
                            e
                        );
                        Ok(None)
                    }
                }
            });
        }

        let mut outcomes = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(Some(outcome))) => outcomes.push(outcome),
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    tracing::error!("Handler task returned error: {}", e);
                }
                Err(e) => {
                    tracing::error!("Handler task panicked: {}", e);
                }
            }
        }

        outcomes
    }
}

// ─── Source/Version Injection (free functions) ───────────────────────

/// Inject `source` and `source_version` into each DbOperation.
/// Called after handler.handle() returns ops, before execute_transaction().
pub(crate) fn inject_source_version(
    ops: Vec<DbOperation>,
    source: &str,
    version: u32,
) -> Vec<DbOperation> {
    ops.into_iter()
        .map(|op| match op {
            DbOperation::Upsert {
                table,
                mut columns,
                mut values,
                mut conflict_columns,
                mut update_columns,
                update_condition,
            } => {
                columns.push("source".to_string());
                columns.push("source_version".to_string());
                values.push(DbValue::Text(source.to_string()));
                values.push(DbValue::Int32(version as i32));
                conflict_columns.push("source".to_string());
                conflict_columns.push("source_version".to_string());
                // Remove source/source_version from update_columns since they're part of conflict key
                update_columns.retain(|c| c != "source" && c != "source_version");
                DbOperation::Upsert {
                    table,
                    columns,
                    values,
                    conflict_columns,
                    update_columns,
                    update_condition,
                }
            }
            DbOperation::Insert {
                table,
                mut columns,
                mut values,
            } => {
                columns.push("source".to_string());
                columns.push("source_version".to_string());
                values.push(DbValue::Text(source.to_string()));
                values.push(DbValue::Int32(version as i32));
                DbOperation::Insert {
                    table,
                    columns,
                    values,
                }
            }
            DbOperation::Update {
                table,
                set_columns,
                where_clause,
            } => {
                let where_clause = inject_where_clause(where_clause, source, version);
                DbOperation::Update {
                    table,
                    set_columns,
                    where_clause,
                }
            }
            DbOperation::Delete {
                table,
                where_clause,
            } => {
                let where_clause = inject_where_clause(where_clause, source, version);
                DbOperation::Delete {
                    table,
                    where_clause,
                }
            }
            DbOperation::RawSql { query, params } => {
                tracing::warn!(
                    "RawSql operation skipped for source/version injection — handler must manage source/source_version manually"
                );
                DbOperation::RawSql { query, params }
            }
        })
        .collect()
}

/// Inject source/source_version conditions into a WhereClause.
fn inject_where_clause(clause: WhereClause, source: &str, version: u32) -> WhereClause {
    let source_conditions = vec![
        ("source".to_string(), DbValue::Text(source.to_string())),
        ("source_version".to_string(), DbValue::Int32(version as i32)),
    ];

    match clause {
        WhereClause::Eq(col, val) => {
            let mut conditions = vec![(col, val)];
            conditions.extend(source_conditions);
            WhereClause::And(conditions)
        }
        WhereClause::And(mut conditions) => {
            conditions.extend(source_conditions);
            WhereClause::And(conditions)
        }
        WhereClause::Raw { condition, params } => {
            tracing::warn!(
                "WhereClause::Raw skipped for source/version injection — handler must manage manually"
            );
            WhereClause::Raw { condition, params }
        }
    }
}

// ─── Snapshot-Capturing Execution ─────────────────────────────────────

/// Execute a transaction with optional snapshot capture for reorg rollback.
///
/// For live mode (single-block ranges), this function:
/// 1. Collects snapshot specs (table + key columns) for upserts with update_columns
/// 2. Executes snapshot reads and writes inside the same database transaction
/// 3. Writes snapshots to storage after the transaction commits
///
/// Snapshot reads happen inside the transaction so that concurrent handlers cannot
/// modify a row between the snapshot read and the handler's write.
/// Snapshots are written to storage after the transaction commits; orphan snapshots
/// from failed transactions are harmless and cleaned up during compaction.
pub(crate) async fn execute_with_snapshot_capture(
    ops: Vec<DbOperation>,
    db_pool: &DbPool,
    storage: Option<&LiveStorage>,
    block_number: u64,
    handler_source: &str,
    handler_version: u32,
) -> Result<(), TransformationError> {
    // If no storage provided, just execute directly (historical mode)
    let storage = match storage {
        Some(s) => s,
        None => {
            return db_pool
                .execute_transaction(ops)
                .await
                .map_err(TransformationError::DatabaseError)
        }
    };

    // Collect snapshot specs from upserts with update_columns
    let mut snapshot_specs: Vec<(String, Vec<(String, DbValue)>)> = Vec::new();
    // Track metadata for building LiveUpsertSnapshot after the transaction
    struct SnapshotMeta {
        table: String,
        conflict_columns: Vec<String>,
        values: Vec<DbValue>,
        columns: Vec<String>,
        /// Index into `ops` for this snapshot, used to check affected_rows.
        op_index: usize,
    }
    let mut snapshot_metas: Vec<SnapshotMeta> = Vec::new();

    for (op_index, op) in ops.iter().enumerate() {
        if let DbOperation::Upsert {
            table,
            columns,
            values,
            conflict_columns,
            update_columns,
            ..
        } = op
        {
            // Only capture snapshots for upserts that update existing rows
            if update_columns.is_empty() {
                continue;
            }

            // Build key columns from conflict_columns
            let mut key_columns: Vec<(String, DbValue)> = Vec::new();
            for conflict_col in conflict_columns {
                if let Some(idx) = columns.iter().position(|c| c == conflict_col) {
                    key_columns.push((conflict_col.clone(), values[idx].clone()));
                }
            }

            snapshot_specs.push((table.clone(), key_columns));
            snapshot_metas.push(SnapshotMeta {
                table: table.clone(),
                conflict_columns: conflict_columns.clone(),
                values: values.clone(),
                columns: columns.clone(),
                op_index,
            });
        }
    }

    if snapshot_specs.is_empty() {
        // No snapshots needed, just execute normally
        return db_pool
            .execute_transaction(ops)
            .await
            .map_err(TransformationError::DatabaseError);
    }

    // Execute transaction with snapshot reads inside the same transaction
    let (snapshot_results, affected_rows) = db_pool
        .execute_transaction_with_snapshot_reads(&snapshot_specs, ops)
        .await
        .map_err(TransformationError::DatabaseError)?;

    // Build snapshots from results
    let mut snapshots = Vec::new();
    for (i, meta) in snapshot_metas.iter().enumerate() {
        // Skip snapshot if the operation did not actually modify any row
        if affected_rows.get(meta.op_index).copied().unwrap_or(0) == 0 {
            continue;
        }

        let previous_row = snapshot_results.get(i).cloned().flatten();

        let key_columns: Vec<(String, DbValue)> = meta
            .conflict_columns
            .iter()
            .filter_map(|col| {
                meta.columns
                    .iter()
                    .position(|c| c == col)
                    .map(|idx| (col.clone(), meta.values[idx].clone()))
            })
            .collect();

        let live_key_columns: Vec<(String, LiveDbValue)> = key_columns
            .into_iter()
            .map(|(k, v)| (k, LiveDbValue::from_db_value(&v)))
            .collect();

        let live_previous_row = previous_row.map(|row| {
            row.into_iter()
                .map(|(k, v)| (k, LiveDbValue::from_db_value(&v)))
                .collect()
        });

        snapshots.push(LiveUpsertSnapshot {
            table: meta.table.clone(),
            source: handler_source.to_string(),
            source_version: handler_version,
            key_columns: live_key_columns,
            previous_row: live_previous_row,
        });
    }

    // Write snapshots to storage after transaction commits
    if !snapshots.is_empty() {
        let mut all_snapshots = storage.read_snapshots(block_number).unwrap_or_default();
        all_snapshots.extend(snapshots);

        if let Err(e) = storage.write_snapshots(block_number, &all_snapshots) {
            tracing::warn!(
                "Failed to write upsert snapshots for block {}: {}",
                block_number,
                e
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::{DbOperation, DbValue};

    /// Helper to build a upsert op with update_columns (triggers snapshot).
    fn make_upsert_op(table: &str) -> DbOperation {
        DbOperation::Upsert {
            table: table.to_string(),
            columns: vec![
                "chain_id".to_string(),
                "pool_address".to_string(),
                "value".to_string(),
            ],
            values: vec![
                DbValue::Int64(1),
                DbValue::Text("0xABC".to_string()),
                DbValue::Int64(42),
            ],
            conflict_columns: vec!["chain_id".to_string(), "pool_address".to_string()],
            update_columns: vec!["value".to_string()],
            update_condition: None,
        }
    }

    /// Helper to build an upsert op with no update_columns (no snapshot needed).
    fn make_insert_only_op(table: &str) -> DbOperation {
        DbOperation::Upsert {
            table: table.to_string(),
            columns: vec!["chain_id".to_string(), "pool_address".to_string()],
            values: vec![DbValue::Int64(1), DbValue::Text("0xDEF".to_string())],
            conflict_columns: vec!["chain_id".to_string(), "pool_address".to_string()],
            update_columns: vec![],
            update_condition: Some("FALSE".to_string()),
        }
    }

    /// Simulate the snapshot-filtering logic from `execute_with_snapshot_capture`
    /// in isolation: given `ops` and `affected_rows`, return the snapshot metas
    /// that would survive the filter.
    ///
    /// This mirrors the filtering block added in Fix A without requiring a real DB.
    fn collect_surviving_snapshot_tables(
        ops: &[DbOperation],
        affected_rows: &[u64],
    ) -> Vec<String> {
        struct SnapshotMeta {
            table: String,
            op_index: usize,
        }

        let mut metas: Vec<SnapshotMeta> = Vec::new();
        for (op_index, op) in ops.iter().enumerate() {
            if let DbOperation::Upsert {
                table,
                update_columns,
                ..
            } = op
            {
                if update_columns.is_empty() {
                    continue;
                }
                metas.push(SnapshotMeta {
                    table: table.clone(),
                    op_index,
                });
            }
        }

        metas
            .iter()
            .filter(|m| affected_rows.get(m.op_index).copied().unwrap_or(0) > 0)
            .map(|m| m.table.clone())
            .collect()
    }

    /// Fix A test: a upsert that actually modifies a row produces a snapshot;
    /// a conditional upsert that is a no-op (0 affected rows) produces no snapshot.
    #[test]
    fn test_no_snapshot_for_conditional_upsert_noop() {
        // op[0]: real upsert — affected_rows[0] = 1  → snapshot expected
        // op[1]: noop conditional upsert — affected_rows[1] = 0  → no snapshot
        let ops = vec![
            make_upsert_op("real_table"),
            make_upsert_op("noop_table"),
        ];
        let affected_rows = vec![1u64, 0u64];

        let survivors = collect_surviving_snapshot_tables(&ops, &affected_rows);
        assert_eq!(survivors, vec!["real_table".to_string()]);
    }

    /// When all ops modify rows, all produce snapshots.
    #[test]
    fn test_snapshot_for_all_affected_upserts() {
        let ops = vec![
            make_upsert_op("table_a"),
            make_upsert_op("table_b"),
        ];
        let affected_rows = vec![1u64, 2u64];

        let mut survivors = collect_surviving_snapshot_tables(&ops, &affected_rows);
        survivors.sort();
        assert_eq!(
            survivors,
            vec!["table_a".to_string(), "table_b".to_string()]
        );
    }

    /// Insert-only ops (empty update_columns) never produce snapshots regardless
    /// of affected_rows — they were not candidates to begin with.
    #[test]
    fn test_no_snapshot_for_insert_only_ops() {
        let ops = vec![make_insert_only_op("insert_table")];
        let affected_rows = vec![1u64]; // row was inserted, but still no snapshot wanted

        let survivors = collect_surviving_snapshot_tables(&ops, &affected_rows);
        assert!(survivors.is_empty());
    }
}
