//! Periodic writer for `pool_leaderboard_snapshot`.
//!
//! Materializes a point-in-time ranking of pools per sort key so the API
//! layer can paginate a leaderboard with stable, O(log N + limit) keyset
//! seeks. Rows are grouped under a monotonic `snapshot_id` pulled from the
//! `pool_leaderboard_snapshot_id_seq` sequence. Old snapshots are garbage
//! collected after a retention window; snapshots invalidated by a reorg
//! are deleted by `RangeFinalizer::process_reorg`.

use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::db::{DbError, DbOperation, DbPool, DbValue};
use crate::live::types::LeaderboardSnapshotConfig;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("Database error: {0}")]
    Database(#[from] DbError),
    #[error("Storage error: {0}")]
    Storage(#[from] std::io::Error),
    #[error("Status parse error: {0}")]
    StatusParse(#[from] serde_json::Error),
    #[error("Unknown sort key: {0}")]
    UnknownSortKey(String),
}

#[derive(Debug, Deserialize)]
struct StatusSummary {
    transformed: bool,
}

/// Per-chain periodic writer for `pool_leaderboard_snapshot`.
///
/// Runs a `sleep(interval_secs)` loop. Each cycle:
/// 1. Reads `MAX(block_height)` from `pool_state` joined against `active_versions`.
/// 2. If the head advanced by at least `interval_blocks` since the last write,
///    inserts one row per (sort_key, pool) under a new `snapshot_id`.
/// 3. Deletes snapshots older than `retention_secs`.
pub struct LeaderboardSnapshotService {
    chain_name: String,
    chain_id: u64,
    db_pool: Arc<DbPool>,
    config: LeaderboardSnapshotConfig,
    /// Last block for which a snapshot was written.
    last_snapshot_block: Mutex<Option<u64>>,
}

impl LeaderboardSnapshotService {
    pub fn new(
        chain_name: String,
        chain_id: u64,
        db_pool: Arc<DbPool>,
        config: LeaderboardSnapshotConfig,
    ) -> Self {
        Self {
            chain_name,
            chain_id,
            db_pool,
            config,
            last_snapshot_block: Mutex::new(None),
        }
    }

    /// Run the snapshot service loop until the containing task is aborted.
    pub async fn run(self) {
        let interval = Duration::from_secs(self.config.interval_secs);

        tracing::info!(
            "Leaderboard snapshot service started for chain {} \
             (interval={:?}, interval_blocks={}, retention_secs={}, sort_keys={:?})",
            self.chain_name,
            interval,
            self.config.interval_blocks,
            self.config.retention_secs,
            self.config.sort_keys,
        );

        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = self.run_cycle().await {
                tracing::error!(
                    "Leaderboard snapshot cycle error for chain {}: {}",
                    self.chain_name,
                    e
                );
            }
        }
    }

    /// Run a single snapshot cycle: take snapshot (if due) and GC expired rows.
    pub async fn run_cycle(&self) -> Result<(), SnapshotError> {
        let pool_state_head = self.current_head_block().await?;
        let transformed_head = latest_transformed_from_status_dir(&status_dir(&self.chain_name))?;

        match ready_snapshot_head(pool_state_head, transformed_head) {
            Some(head) => {
                let should_write = {
                    let last = self.last_snapshot_block.lock().await;
                    snapshot_due(*last, head, self.config.interval_blocks)
                };

                if should_write {
                    if let Some(prev) = *self.last_snapshot_block.lock().await {
                        if head < prev {
                            tracing::debug!(
                                chain = self.chain_name,
                                previous_head = prev,
                                head,
                                "snapshot head regressed; writing replacement snapshot"
                            );
                        }
                    }
                    let snapshot_id = self.take_snapshot(head).await?;
                    *self.last_snapshot_block.lock().await = Some(head);
                    tracing::debug!(
                        chain = self.chain_name,
                        snapshot_id,
                        head,
                        "wrote leaderboard snapshot"
                    );
                } else {
                    tracing::trace!(
                        chain = self.chain_name,
                        head,
                        "skipping snapshot: insufficient block advance"
                    );
                }
            }
            None if pool_state_head.is_none() => {
                tracing::trace!(
                    chain = self.chain_name,
                    "no pool_state rows yet; skipping snapshot"
                );
            }
            None if transformed_head.is_none() => {
                tracing::trace!(
                    chain = self.chain_name,
                    "no transformed live status yet; skipping snapshot"
                );
            }
            None => {
                tracing::trace!(
                    chain = self.chain_name,
                    pool_state_head = pool_state_head,
                    transformed_head = transformed_head,
                    "skipping snapshot: pool_state has not reached a fully transformed height"
                );
            }
        }

        let deleted = self.gc_expired().await?;
        if deleted > 0 {
            tracing::debug!(
                chain = self.chain_name,
                deleted,
                "gc'd expired leaderboard snapshots"
            );
        }

        Ok(())
    }

    /// `MAX(block_height)` from `pool_state` joined against `active_versions`
    /// for this chain. Returns `None` when no rows exist yet.
    async fn current_head_block(&self) -> Result<Option<u64>, SnapshotError> {
        let rows = self
            .db_pool
            .query(
                "SELECT MAX(ps.block_height)
                 FROM pool_state ps
                 INNER JOIN active_versions av
                     ON av.source = ps.source
                    AND av.active_version = ps.source_version
                 WHERE ps.chain_id = $1",
                &[&(self.chain_id as i64)],
            )
            .await?;

        let value: Option<i64> = rows.first().and_then(|r| r.get(0));
        Ok(value.map(|v| v as u64))
    }

    /// Insert rows for all configured sort keys under one new `snapshot_id`,
    /// atomically. Returns the assigned `snapshot_id`.
    async fn take_snapshot(&self, head_block: u64) -> Result<i64, SnapshotError> {
        let id_rows = self
            .db_pool
            .query("SELECT nextval('pool_leaderboard_snapshot_id_seq')", &[])
            .await?;
        let snapshot_id: i64 = id_rows
            .first()
            .map(|r| r.get::<_, i64>(0))
            .expect("nextval always returns a row");

        let mut ops: Vec<DbOperation> = Vec::with_capacity(self.config.sort_keys.len());
        for sort_key in &self.config.sort_keys {
            validate_sort_key(sort_key)?;
            let sql = build_insert_sql(sort_key);
            ops.push(DbOperation::RawSql {
                query: sql,
                params: vec![
                    DbValue::Int64(snapshot_id),
                    DbValue::Int64(self.chain_id as i64),
                    DbValue::Int64(head_block as i64),
                ],
                snapshot: None,
            });
        }

        self.db_pool.execute_transaction(ops).await?;
        Ok(snapshot_id)
    }

    /// Delete rows whose `taken_at` is older than `retention_secs`. Returns
    /// the number of rows deleted.
    async fn gc_expired(&self) -> Result<u64, SnapshotError> {
        let retention = self.config.retention_secs as f64;
        let rows = self
            .db_pool
            .query(
                &build_gc_sql(),
                &[&(self.chain_id as i64), &retention],
            )
            .await?;

        let count: i64 = rows.first().map(|r| r.get(0)).unwrap_or(0);
        Ok(count as u64)
    }
}

/// Allowlist for sort keys. Every entry must also be a column on `pool_state`.
fn validate_sort_key(sort_key: &str) -> Result<(), SnapshotError> {
    match sort_key {
        "active_liquidity_usd" | "tvl_usd" | "volume_24h_usd" | "market_cap_usd" => Ok(()),
        other => Err(SnapshotError::UnknownSortKey(other.to_string())),
    }
}

/// Build the INSERT...SELECT for one sort key. The `sort_key` string is
/// first validated against the allowlist in [`validate_sort_key`] before
/// being interpolated, so this is safe from injection.
fn build_insert_sql(sort_key: &str) -> String {
    format!(
        "INSERT INTO pool_leaderboard_snapshot
             (snapshot_id, chain_id, sort_key, pool_id, id, sort_val, block_height)
         SELECT $1, ps.chain_id, '{sort_key}', ps.pool_id, p.id,
                ps.{sort_key}, $3
         FROM pool_state ps
         INNER JOIN active_versions av
             ON av.source = ps.source
            AND av.active_version = ps.source_version
         INNER JOIN pools p
             ON p.chain_id = ps.chain_id
            AND p.address = ps.pool_id
            AND p.source = ps.source
            AND p.source_version = ps.source_version
         WHERE ps.chain_id = $2",
    )
}

fn build_gc_sql() -> String {
    "WITH deleted AS (
         DELETE FROM pool_leaderboard_snapshot
         WHERE chain_id = $1
           AND taken_at < NOW() - make_interval(secs => $2)
         RETURNING 1
     )
     SELECT COUNT(*)::BIGINT FROM deleted"
        .to_string()
}

fn status_dir(chain_name: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/live/status", chain_name))
}

fn latest_transformed_from_status_dir(status_dir: &Path) -> Result<Option<u64>, SnapshotError> {
    let entries = match fs::read_dir(status_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut latest = None;
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let Some(number) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
        else {
            continue;
        };

        let file = fs::File::open(&path)?;
        let status: StatusSummary = serde_json::from_reader(BufReader::new(file))?;
        if status.transformed {
            latest = Some(latest.map_or(number, |prev: u64| prev.max(number)));
        }
    }

    Ok(latest)
}

fn ready_snapshot_head(pool_state_head: Option<u64>, transformed_head: Option<u64>) -> Option<u64> {
    match (pool_state_head, transformed_head) {
        (Some(pool_head), Some(transformed_head)) if transformed_head >= pool_head => Some(pool_head),
        _ => None,
    }
}

fn snapshot_due(last_snapshot_block: Option<u64>, head: u64, interval_blocks: u64) -> bool {
    match last_snapshot_block {
        None => true,
        Some(prev) if head < prev => true,
        Some(prev) => head.saturating_sub(prev) >= interval_blocks,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use tempfile::TempDir;

    #[test]
    fn validate_sort_key_accepts_known_keys() {
        assert!(validate_sort_key("active_liquidity_usd").is_ok());
        assert!(validate_sort_key("tvl_usd").is_ok());
        assert!(validate_sort_key("volume_24h_usd").is_ok());
        assert!(validate_sort_key("market_cap_usd").is_ok());
    }

    #[test]
    fn validate_sort_key_rejects_unknown_and_injection() {
        assert!(matches!(
            validate_sort_key("foo"),
            Err(SnapshotError::UnknownSortKey(_))
        ));
        assert!(matches!(
            validate_sort_key("active_liquidity_usd; DROP TABLE pools;"),
            Err(SnapshotError::UnknownSortKey(_))
        ));
    }

    #[test]
    fn build_insert_sql_interpolates_column() {
        let sql = build_insert_sql("tvl_usd");
        assert!(sql.contains("'tvl_usd'"));
        assert!(sql.contains("ps.tvl_usd"));
        assert!(sql.contains("INSERT INTO pool_leaderboard_snapshot"));
    }

    #[test]
    fn build_gc_sql_is_chain_scoped() {
        let sql = build_gc_sql();
        assert!(sql.contains("DELETE FROM pool_leaderboard_snapshot"));
        assert!(sql.contains("WHERE chain_id = $1"));
        assert!(sql.contains("taken_at < NOW() - make_interval(secs => $2)"));
    }

    #[test]
    fn ready_snapshot_head_waits_for_transformed_pool_state() {
        assert_eq!(ready_snapshot_head(Some(100), Some(99)), None);
        assert_eq!(ready_snapshot_head(Some(100), Some(100)), Some(100));
        assert_eq!(ready_snapshot_head(Some(100), Some(101)), Some(100));
        assert_eq!(ready_snapshot_head(None, Some(101)), None);
        assert_eq!(ready_snapshot_head(Some(100), None), None);
    }

    #[test]
    fn snapshot_due_rewrites_after_head_regression() {
        assert!(snapshot_due(None, 100, 100));
        assert!(!snapshot_due(Some(100), 150, 100));
        assert!(snapshot_due(Some(100), 200, 100));
        assert!(snapshot_due(Some(100), 95, 100));
    }

    #[test]
    fn latest_transformed_from_status_dir_picks_highest_transformed_file() {
        let tmp = TempDir::new().unwrap();
        let status_dir = tmp.path().join("status");
        fs::create_dir_all(&status_dir).unwrap();

        fs::write(
            status_dir.join("100.json"),
            r#"{"transformed":true,"completed_handlers":["a"]}"#,
        )
        .unwrap();
        fs::write(
            status_dir.join("101.json"),
            r#"{"transformed":false,"completed_handlers":["a"]}"#,
        )
        .unwrap();
        fs::write(
            status_dir.join("102.json"),
            r#"{"transformed":true,"completed_handlers":["a"]}"#,
        )
        .unwrap();
        fs::write(status_dir.join("ignore.tmp"), b"junk").unwrap();

        let latest = latest_transformed_from_status_dir(&status_dir).unwrap();
        assert_eq!(latest, Some(102));
    }
}
