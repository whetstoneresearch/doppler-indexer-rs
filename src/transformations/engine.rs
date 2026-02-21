//! Transformation engine that orchestrates handler execution.
//!
//! The engine receives decoded events and calls, invokes registered handlers,
//! and writes results to PostgreSQL. It tracks progress per handler and performs
//! per-handler catchup from decoded parquet files on startup.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

use super::context::{DecodedCall, DecodedEvent, TransactionAddresses, TransformationContext};
use super::error::TransformationError;
use super::historical::HistoricalDataReader;
use super::registry::{extract_event_name, TransformationRegistry};
use super::traits::EventHandler;
use crate::db::{DbPool, DbValue, DbOperation, WhereClause};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::contract::Contracts;

/// Message containing decoded events for a block range.
#[derive(Debug)]
pub struct DecodedEventsMessage {
    pub range_start: u64,
    pub range_end: u64,
    pub source_name: String,
    pub event_name: String,
    pub events: Vec<DecodedEvent>,
}

/// Message containing decoded call results for a block range.
#[derive(Debug)]
pub struct DecodedCallsMessage {
    pub range_start: u64,
    pub range_end: u64,
    pub source_name: String,
    pub function_name: String,
    pub calls: Vec<DecodedCall>,
}

/// Signal that all decoding for a range is complete.
#[derive(Debug)]
pub struct RangeCompleteMessage {
    pub range_start: u64,
    pub range_end: u64,
}

/// Execution mode for the transformation engine.
#[derive(Debug, Clone, Copy)]
pub enum ExecutionMode {
    /// Process data as it arrives (for live/real-time data).
    Streaming,
    /// Process in larger batches (for historical catchup).
    Batch { batch_size: usize },
}

impl Default for ExecutionMode {
    fn default() -> Self {
        Self::Streaming
    }
}

/// Buffered event data waiting for call dependencies.
#[derive(Debug)]
struct PendingEventData {
    range_start: u64,
    range_end: u64,
    source_name: String,
    event_name: String,
    events: Vec<DecodedEvent>,
    /// Call dependencies needed: (source, function_name)
    required_calls: Vec<(String, String)>,
}

/// Live processing state for buffering events with call dependencies.
#[derive(Default)]
struct LiveProcessingState {
    /// Track which (source, function) calls have arrived for which ranges.
    /// Key: (source_name, function_name), Value: set of (range_start, range_end)
    received_calls: HashMap<(String, String), HashSet<(u64, u64)>>,
    /// Accumulated calls per range for event handlers with dependencies.
    /// Key: (range_start, range_end), Value: accumulated calls
    calls_buffer: HashMap<(u64, u64), Vec<DecodedCall>>,
    /// Buffer events waiting for calls, keyed by handler_key.
    pending_events: HashMap<String, Vec<PendingEventData>>,
}

/// The transformation engine processes decoded data and invokes handlers.
///
/// Progress is tracked per handler (keyed by `handler_key()`) in the
/// `_handler_progress` table, enabling independent catchup and versioning.
pub struct TransformationEngine {
    registry: Arc<TransformationRegistry>,
    db_pool: Arc<DbPool>,
    rpc_client: Arc<UnifiedRpcClient>,
    historical_reader: Arc<HistoricalDataReader>,
    chain_name: String,
    chain_id: u64,
    mode: ExecutionMode,
    decoded_logs_dir: PathBuf,
    decoded_calls_dir: PathBuf,
    raw_receipts_dir: PathBuf,
    contracts: Arc<Contracts>,
    /// Live processing state for buffering events with call dependencies.
    live_state: Mutex<LiveProcessingState>,
}

impl TransformationEngine {
    /// Create a new transformation engine.
    pub async fn new(
        registry: Arc<TransformationRegistry>,
        db_pool: Arc<DbPool>,
        rpc_client: Arc<UnifiedRpcClient>,
        chain_name: String,
        chain_id: u64,
        mode: ExecutionMode,
        contracts: Contracts,
    ) -> Result<Self, TransformationError> {
        let historical_reader = Arc::new(HistoricalDataReader::new(&chain_name)?);
        let decoded_logs_dir = PathBuf::from(format!("data/derived/{}/decoded/logs", chain_name));
        let decoded_calls_dir =
            PathBuf::from(format!("data/derived/{}/decoded/eth_calls", chain_name));
        let raw_receipts_dir = PathBuf::from(format!("data/raw/{}/receipts", chain_name));

        Ok(Self {
            registry,
            db_pool,
            rpc_client,
            historical_reader,
            chain_name,
            chain_id,
            mode,
            decoded_logs_dir,
            decoded_calls_dir,
            raw_receipts_dir,
            contracts: Arc::new(contracts),
            live_state: Mutex::new(LiveProcessingState::default()),
        })
    }

    /// Initialize the engine: run handler migrations, register sources, then initialize handlers.
    pub async fn initialize(&self) -> Result<(), TransformationError> {
        // Run handler-specified migrations first
        self.run_handler_migrations().await?;

        // Register handler sources in active_versions table
        self.register_handler_sources().await?;

        // Then run handler initialization
        for handler in self.registry.all_handlers() {
            tracing::debug!(
                "Initializing handler: {} ({})",
                handler.name(),
                handler.handler_key()
            );
            handler.initialize(&self.db_pool).await?;
        }
        Ok(())
    }

    // ─── Handler Migrations ──────────────────────────────────────────

    /// Run handler migration files from each handler's `migration_paths()`.
    /// Each path can be a directory (all `.sql` files run in alphabetical order)
    /// or a single `.sql` file. Tracked in the `_migrations` table with a `handlers/` prefix.
    async fn run_handler_migrations(&self) -> Result<(), TransformationError> {
        // Collect migration paths from all handlers (deduplicate)
        let mut migration_paths: Vec<PathBuf> = Vec::new();
        let mut seen = HashSet::new();

        for handler in self.registry.all_handlers() {
            for path_str in handler.migration_paths() {
                let path = PathBuf::from(path_str);
                if seen.insert(path.clone()) {
                    migration_paths.push(path);
                }
            }
        }

        if migration_paths.is_empty() {
            return Ok(());
        }

        // Check which migrations have already been applied
        let pool = self.db_pool.inner();
        let applied: HashSet<String> = {
            let client = pool.get().await.map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::PoolError(e))
            })?;
            let rows = client
                .query("SELECT name FROM _migrations", &[])
                .await
                .map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
                })?;
            rows.iter().map(|r| r.get(0)).collect()
        };

        // Resolve each path into (file_path, migration_name) pairs
        let mut sql_entries: Vec<(PathBuf, String)> = Vec::new();

        for path in &migration_paths {
            if !path.exists() {
                tracing::warn!(
                    "Handler migration path does not exist: {}",
                    path.display()
                );
                continue;
            }

            if path.is_file() {
                // Single file: track as "handlers/{path}"
                let migration_name = format!("{}", path.display());
                sql_entries.push((path.clone(), migration_name));
            } else if path.is_dir() {
                // Directory: collect and sort .sql files (flat scan)
                let mut dir_files: Vec<_> = std::fs::read_dir(path)?
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.path()
                            .extension()
                            .map(|x| x == "sql")
                            .unwrap_or(false)
                    })
                    .collect();
                dir_files.sort_by_key(|e| e.file_name());

                for entry in dir_files {
                    let file_name = entry.file_name().to_string_lossy().to_string();
                    let migration_name =
                        format!("{}", path.join(&file_name).display());
                    sql_entries.push((entry.path(), migration_name));
                }
            }
        }

        // Apply each migration
        for (file_path, migration_name) in &sql_entries {
            if applied.contains(migration_name) {
                continue;
            }

            let sql = std::fs::read_to_string(file_path)?;

            let mut client = pool.get().await.map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::PoolError(e))
            })?;
            let tx = client.transaction().await.map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
            })?;

            tx.batch_execute(&sql).await.map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::MigrationError(
                    format!(
                        "Handler migration {} failed: {}",
                        migration_name, e
                    ),
                ))
            })?;

            tx.execute(
                "INSERT INTO _migrations (name) VALUES ($1)",
                &[&migration_name],
            )
            .await
            .map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
            })?;

            tx.commit().await.map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
            })?;

            tracing::info!("Applied handler migration: {}", migration_name);
        }

        Ok(())
    }

    // ─── Source Registration ────────────────────────────────────────

    /// Register handler sources in the `active_versions` table.
    /// For new sources, inserts with the handler's current version.
    /// For existing sources with a different version, logs a warning.
    async fn register_handler_sources(&self) -> Result<(), TransformationError> {
        let pool = self.db_pool.inner();
        let client = pool.get().await.map_err(|e| {
            TransformationError::DatabaseError(crate::db::DbError::PoolError(e))
        })?;

        for handler in self.registry.all_handlers() {
            let source = handler.name().to_string();
            let version = handler.version() as i32;

            let rows = client
                .query(
                    "SELECT active_version FROM active_versions WHERE source = $1",
                    &[&source],
                )
                .await
                .map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
                })?;

            if rows.is_empty() {
                // New source — insert with current version
                client
                    .execute(
                        "INSERT INTO active_versions (source, active_version) VALUES ($1, $2)",
                        &[&source, &version],
                    )
                    .await
                    .map_err(|e| {
                        TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
                    })?;
                tracing::info!(
                    "Registered new source: {} with active_version={}",
                    source,
                    version
                );
            } else {
                let existing_version: i32 = rows[0].get(0);
                if existing_version != version {
                    tracing::warn!(
                        "Source '{}' has active_version={} but handler is at version={}. \
                         To activate the new version, run: \
                         UPDATE active_versions SET active_version = {}, updated_at = NOW() WHERE source = '{}';",
                        source,
                        existing_version,
                        version,
                        version,
                        source
                    );
                }
            }
        }

        Ok(())
    }

    // ─── Source/Version Injection ────────────────────────────────────

    /// Inject `source` and `source_version` into each DbOperation.
    /// Called after handler.handle() returns ops, before execute_transaction().
    fn inject_source_version(ops: Vec<DbOperation>, source: &str, version: u32) -> Vec<DbOperation> {
        ops.into_iter()
            .map(|op| match op {
                DbOperation::Upsert {
                    table,
                    mut columns,
                    mut values,
                    mut conflict_columns,
                    update_columns,
                } => {
                    columns.push("source".to_string());
                    columns.push("source_version".to_string());
                    values.push(DbValue::Text(source.to_string()));
                    values.push(DbValue::Int32(version as i32));
                    conflict_columns.push("source".to_string());
                    conflict_columns.push("source_version".to_string());
                    DbOperation::Upsert {
                        table,
                        columns,
                        values,
                        conflict_columns,
                        update_columns,
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
                    let where_clause = Self::inject_where_clause(where_clause, source, version);
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
                    let where_clause = Self::inject_where_clause(where_clause, source, version);
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

    // ─── Per-Handler Progress Tracking ───────────────────────────────

    /// Get completed ranges for a specific handler from the database.
    async fn get_completed_ranges_for_handler(
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
    async fn record_completed_range_for_handler(
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

    // ─── Range Scanning ──────────────────────────────────────────────

    /// Scan decoded parquet files to find available ranges.
    fn scan_available_ranges(&self, base_dir: &PathBuf) -> Result<Vec<(u64, u64)>, TransformationError> {
        let mut ranges = HashSet::new();

        if !base_dir.exists() {
            return Ok(Vec::new());
        }

        fn scan_recursive(dir: &std::path::Path, ranges: &mut HashSet<(u64, u64)>) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        scan_recursive(&path, ranges);
                    } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        // Parse range from filename: 28990000-28999999.parquet
                        if let Some(file_name) = path.file_stem().and_then(|s| s.to_str()) {
                            let parts: Vec<&str> = file_name.split('-').collect();
                            if parts.len() == 2 {
                                if let (Ok(start), Ok(end)) =
                                    (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                                {
                                    ranges.insert((start, end + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        scan_recursive(base_dir, &mut ranges);

        let mut sorted: Vec<_> = ranges.into_iter().collect();
        sorted.sort_by_key(|(start, _)| *start);

        Ok(sorted)
    }

    // ─── Per-Handler Catchup ─────────────────────────────────────────

    /// Run catchup phase: process decoded parquet files per handler.
    /// Each handler catches up independently based on its own progress.
    pub async fn run_catchup(&self) -> Result<(), TransformationError> {
        // Catch up event handlers
        self.run_event_handler_catchup().await?;

        // Catch up call handlers
        self.run_call_handler_catchup().await?;

        Ok(())
    }

    /// Run catchup for all event handlers.
    async fn run_event_handler_catchup(&self) -> Result<(), TransformationError> {
        let available = self.scan_available_ranges(&self.decoded_logs_dir)?;

        if available.is_empty() {
            tracing::info!(
                "Event handler catchup: no parquet ranges found for chain {}",
                self.chain_name
            );
            return Ok(());
        }

        for handler_info in self.registry.unique_event_handlers() {
            let handler = &handler_info.handler;
            let handler_key = handler.handler_key();
            let event_triggers: Vec<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|t| (t.source.clone(), extract_event_name(&t.event_signature)))
                .collect();

            // Get call dependencies for this handler
            let call_deps = handler_info.handler.call_dependencies();

            // Pre-compute available call ranges for each dependency
            let available_call_ranges: Vec<HashSet<(u64, u64)>> = call_deps
                .iter()
                .map(|(source, func)| {
                    let dir = self.decoded_calls_dir.join(source).join(func);
                    self.scan_available_ranges(&dir)
                        .unwrap_or_default()
                        .into_iter()
                        .collect()
                })
                .collect();

            let completed = self.get_completed_ranges_for_handler(&handler_key).await?;

            let to_process: Vec<_> = available
                .iter()
                .filter(|(start, _)| !completed.contains(start))
                .cloned()
                .collect();

            if to_process.is_empty() {
                tracing::info!("Handler {} catchup: already up to date", handler_key);
                continue;
            }

            tracing::info!(
                "Handler {} catchup: processing {} ranges (triggers: {:?}, call_deps: {:?})",
                handler_key,
                to_process.len(),
                event_triggers,
                call_deps
            );

            let total = to_process.len();
            let mut processed = 0;
            let mut skipped = 0;
            let mut errored = false;

            for (range_start, range_end) in &to_process {
                let events = self.read_decoded_events_for_triggers(
                    *range_start,
                    *range_end,
                    &event_triggers,
                )?;

                // Only check call dependencies when we have events that need them
                if !events.is_empty() && !call_deps.is_empty() {
                    let calls_ready = available_call_ranges
                        .iter()
                        .all(|ranges| ranges.contains(&(*range_start, *range_end)));

                    if !calls_ready {
                        tracing::debug!(
                            "Handler {} skipping range {}-{}: call dependencies not yet decoded",
                            handler_key,
                            range_start,
                            range_end
                        );
                        skipped += 1;
                        continue;
                    }
                }

                // Read call dependencies for this range
                let calls = if !events.is_empty() && !call_deps.is_empty() {
                    self.read_decoded_calls_for_triggers(*range_start, *range_end, &call_deps)?
                } else {
                    Vec::new()
                };

                processed += 1;

                if !events.is_empty() {
                    let tx_addresses = self.read_receipt_addresses(*range_start, *range_end);
                    let ctx = TransformationContext::new(
                        &self.chain_name,
                        self.chain_id,
                        *range_start,
                        *range_end,
                        &events,
                        &calls,
                        tx_addresses,
                        self.historical_reader.clone(),
                        self.rpc_client.clone(),
                        self.contracts.clone(),
                    );

                    match handler.handle(&ctx).await {
                        Ok(ops) => {
                            if !ops.is_empty() {
                                let ops = Self::inject_source_version(ops, handler.name(), handler.version());
                                self.db_pool.execute_transaction(ops).await?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Handler {} failed on range {}-{}: {}. Stopping catchup for this handler.",
                                handler_key, range_start, range_end, e
                            );
                            errored = true;
                            break;
                        }
                    }
                }

                // Record progress for this handler
                self.record_completed_range_for_handler(&handler_key, *range_start, *range_end)
                    .await?;

                if processed % 50 == 0 {
                    tracing::info!(
                        "Handler {} catchup progress: {}/{} (skipped {} waiting for calls)",
                        handler_key,
                        processed,
                        total,
                        skipped
                    );
                }
            }

            if skipped > 0 {
                tracing::info!(
                    "Handler {} catchup: skipped {} ranges waiting for call dependencies to be decoded",
                    handler_key,
                    skipped
                );
            }

            if !errored {
                tracing::info!(
                    "Handler {} catchup complete: processed {} ranges",
                    handler_key,
                    processed
                );
            }
        }

        Ok(())
    }

    /// Run catchup for all call handlers.
    async fn run_call_handler_catchup(&self) -> Result<(), TransformationError> {
        let available = self.scan_available_ranges(&self.decoded_calls_dir)?;

        if available.is_empty() {
            tracing::info!(
                "Call handler catchup: no parquet ranges found for chain {}",
                self.chain_name
            );
            return Ok(());
        }

        for handler_info in self.registry.unique_call_handlers() {
            let handler = &handler_info.handler;
            let handler_key = handler.handler_key();
            let call_triggers: Vec<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|t| (t.source.clone(), t.function_name.clone()))
                .collect();

            let completed = self.get_completed_ranges_for_handler(&handler_key).await?;

            let to_process: Vec<_> = available
                .iter()
                .filter(|(start, _)| !completed.contains(start))
                .cloned()
                .collect();

            if to_process.is_empty() {
                tracing::info!("Handler {} catchup: already up to date", handler_key);
                continue;
            }

            tracing::info!(
                "Handler {} catchup: processing {} ranges (triggers: {:?})",
                handler_key,
                to_process.len(),
                call_triggers
            );

            let total = to_process.len();
            let mut processed = 0;
            let mut errored = false;

            for (range_start, range_end) in &to_process {
                let calls = self.read_decoded_calls_for_triggers(
                    *range_start,
                    *range_end,
                    &call_triggers,
                )?;

                processed += 1;

                if !calls.is_empty() {
                    let ctx = TransformationContext::new(
                        &self.chain_name,
                        self.chain_id,
                        *range_start,
                        *range_end,
                        &[],
                        &calls,
                        HashMap::new(),
                        self.historical_reader.clone(),
                        self.rpc_client.clone(),
                        self.contracts.clone(),
                    );

                    match handler.handle(&ctx).await {
                        Ok(ops) => {
                            if !ops.is_empty() {
                                let ops = Self::inject_source_version(ops, handler.name(), handler.version());
                                self.db_pool.execute_transaction(ops).await?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Handler {} failed on range {}-{}: {}. Stopping catchup for this handler.",
                                handler_key, range_start, range_end, e
                            );
                            errored = true;
                            break;
                        }
                    }
                }

                self.record_completed_range_for_handler(&handler_key, *range_start, *range_end)
                    .await?;

                if processed % 50 == 0 {
                    tracing::info!(
                        "Handler {} catchup progress: {}/{}",
                        handler_key,
                        processed,
                        total
                    );
                }
            }

            if !errored {
                tracing::info!(
                    "Handler {} catchup complete: processed {} ranges",
                    handler_key,
                    processed
                );
            }
        }

        Ok(())
    }

    // ─── Receipt Address Reading ────────────────────────────────────

    /// Read transaction from/to addresses from receipt parquet files for a block range.
    fn read_receipt_addresses(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> HashMap<[u8; 32], TransactionAddresses> {
        use arrow::array::{Array, FixedSizeBinaryArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file_name = format!("receipts_{}-{}.parquet", range_start, range_end - 1);
        let file_path = self.raw_receipts_dir.join(&file_name);

        if !file_path.exists() {
            tracing::debug!(
                "No receipt file found at {}, tx addresses unavailable for range {}-{}",
                file_path.display(),
                range_start,
                range_end - 1
            );
            return HashMap::new();
        }

        let file = match std::fs::File::open(&file_path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("Failed to open receipt file {}: {}", file_path.display(), e);
                return HashMap::new();
            }
        };

        let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Failed to read receipt parquet {}: {}", file_path.display(), e);
                return HashMap::new();
            }
        };

        let reader = match builder.build() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to build receipt reader {}: {}", file_path.display(), e);
                return HashMap::new();
            }
        };

        let mut addresses = HashMap::new();

        for batch_result in reader {
            let batch = match batch_result {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("Failed to read receipt batch from {}: {}", file_path.display(), e);
                    continue;
                }
            };

            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            // Extract transaction_hash column
            let tx_hash_col = match batch.column_by_name("transaction_hash") {
                Some(col) => match col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    Some(arr) => arr.clone(),
                    None => continue,
                },
                None => continue,
            };

            // Extract from_address column
            let from_col = match batch.column_by_name("from_address") {
                Some(col) => match col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    Some(arr) => arr.clone(),
                    None => continue,
                },
                None => continue,
            };

            // Extract to_address column (nullable)
            let to_col = batch.column_by_name("to_address")
                .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>().cloned());

            for row in 0..num_rows {
                let tx_hash: [u8; 32] = match tx_hash_col.value(row).try_into() {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                let from_address: [u8; 20] = match from_col.value(row).try_into() {
                    Ok(a) => a,
                    Err(_) => continue,
                };

                let to_address = to_col.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        col.value(row).try_into().ok()
                    }
                });

                addresses.insert(tx_hash, TransactionAddresses {
                    from_address,
                    to_address,
                });
            }
        }

        tracing::debug!(
            "Read {} transaction addresses from {}",
            addresses.len(),
            file_path.display()
        );

        addresses
    }

    // ─── Parquet Reading ─────────────────────────────────────────────

    /// Read decoded events from parquet files for specific triggers only.
    fn read_decoded_events_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        event_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let mut all_events = Vec::new();
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

        for (source_name, event_name) in event_triggers {
            let file_path = self
                .decoded_logs_dir
                .join(source_name)
                .join(event_name)
                .join(&file_name);

            if !file_path.exists() {
                continue;
            }

            tracing::debug!("Reading decoded events from {}", file_path.display());

            match self
                .historical_reader
                .read_events_from_file(&file_path, source_name, event_name)
            {
                Ok(events) => {
                    tracing::debug!(
                        "Read {} events from {}",
                        events.len(),
                        file_path.display()
                    );
                    all_events.extend(events);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to read decoded events from {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }

        Ok(all_events)
    }

    /// Read decoded calls from parquet files for specific triggers only.
    fn read_decoded_calls_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        call_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let mut all_calls = Vec::new();
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

        for (source_name, function_name) in call_triggers {
            let file_path = self
                .decoded_calls_dir
                .join(source_name)
                .join(function_name)
                .join(&file_name);

            if !file_path.exists() {
                continue;
            }

            tracing::debug!("Reading decoded calls from {}", file_path.display());

            match self
                .historical_reader
                .read_calls_from_file(&file_path, source_name, function_name)
            {
                Ok(calls) => {
                    tracing::debug!(
                        "Read {} calls from {}",
                        calls.len(),
                        file_path.display()
                    );
                    all_calls.extend(calls);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to read decoded calls from {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }

        Ok(all_calls)
    }

    // ─── Live Processing ─────────────────────────────────────────────

    /// Run the transformation engine, processing messages from channels.
    ///
    /// This is spawned as a task similar to decode_logs and decode_eth_calls.
    /// Events and calls are processed immediately as they arrive.
    /// For event handlers with call dependencies, events are buffered until
    /// their required calls arrive.
    pub async fn run(
        &self,
        mut events_rx: Receiver<DecodedEventsMessage>,
        mut calls_rx: Receiver<DecodedCallsMessage>,
        mut _complete_rx: Receiver<RangeCompleteMessage>,
        decode_catchup_done_rx: Option<oneshot::Receiver<()>>,
    ) -> Result<(), TransformationError> {
        // Wait for decode catchup to finish so all decoded parquet files exist
        if let Some(rx) = decode_catchup_done_rx {
            tracing::info!("Waiting for eth_call decode catchup to complete before transformation catchup...");
            let _ = rx.await;
            tracing::info!("Eth_call decode catchup complete, proceeding with transformation catchup");
        }

        // Run catchup first
        self.run_catchup().await?;

        tracing::info!(
            "Transformation engine started for chain {} in {:?} mode",
            self.chain_name,
            self.mode
        );

        loop {
            tokio::select! {
                biased;

                Some(msg) = events_rx.recv() => {
                    if msg.events.is_empty() {
                        continue;
                    }

                    tracing::debug!(
                        "Processing {} events for {}/{} in range {}-{}",
                        msg.events.len(),
                        msg.source_name,
                        msg.event_name,
                        msg.range_start,
                        msg.range_end
                    );

                    // Process events, handling call dependencies
                    self.process_events_message(msg).await?;
                }

                Some(msg) = calls_rx.recv() => {
                    if msg.calls.is_empty() {
                        continue;
                    }

                    tracing::debug!(
                        "Processing {} calls for {}/{} in range {}-{}",
                        msg.calls.len(),
                        msg.source_name,
                        msg.function_name,
                        msg.range_start,
                        msg.range_end
                    );

                    // Process calls and check if any pending events can now be processed
                    self.process_calls_message(msg).await?;
                }

                else => {
                    // All channels closed
                    tracing::info!("All channels closed, transformation engine shutting down");
                    break;
                }
            }
        }

        tracing::info!(
            "Transformation engine completed for chain {}",
            self.chain_name
        );
        Ok(())
    }

    /// Process an events message, buffering events with unmet call dependencies.
    async fn process_events_message(
        &self,
        msg: DecodedEventsMessage,
    ) -> Result<(), TransformationError> {
        // Find handlers for this event
        let handlers = self.registry.handlers_for_event(&msg.source_name, &msg.event_name);

        for handler in &handlers {
            let call_deps = handler.call_dependencies();
            let handler_key = handler.handler_key();

            if call_deps.is_empty() {
                // No dependencies - process immediately
                let tx_addresses = self.read_receipt_addresses(msg.range_start, msg.range_end);
                let ctx = TransformationContext::new(
                    &self.chain_name,
                    self.chain_id,
                    msg.range_start,
                    msg.range_end,
                    &msg.events,
                    &[],
                    tx_addresses,
                    self.historical_reader.clone(),
                    self.rpc_client.clone(),
                    self.contracts.clone(),
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops = Self::inject_source_version(ops, handler.name(), handler.version());
                            if let Err(e) = self.db_pool.execute_transaction(ops).await {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key, msg.range_start, msg.range_end, e
                                );
                                continue;
                            }
                        }
                        self.record_completed_range_for_handler(
                            &handler_key,
                            msg.range_start,
                            msg.range_end,
                        ).await?;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for event {}/{}: {}",
                            handler_key, msg.source_name, msg.event_name, e
                        );
                    }
                }
            } else {
                // Has dependencies - check if they're satisfied
                let range_key = (msg.range_start, msg.range_end);
                let mut state = self.live_state.lock().await;

                let deps_ready = call_deps.iter().all(|dep| {
                    state.received_calls
                        .get(dep)
                        .map(|ranges| ranges.contains(&range_key))
                        .unwrap_or(false)
                });

                if deps_ready {
                    // Dependencies ready - process with buffered calls
                    let calls = state.calls_buffer.get(&range_key).cloned().unwrap_or_default();
                    drop(state); // Release lock before async work

                    let tx_addresses = self.read_receipt_addresses(msg.range_start, msg.range_end);
                    let ctx = TransformationContext::new(
                        &self.chain_name,
                        self.chain_id,
                        msg.range_start,
                        msg.range_end,
                        &msg.events,
                        &calls,
                        tx_addresses,
                        self.historical_reader.clone(),
                        self.rpc_client.clone(),
                        self.contracts.clone(),
                    );

                    match handler.handle(&ctx).await {
                        Ok(ops) => {
                            if !ops.is_empty() {
                                let ops = Self::inject_source_version(ops, handler.name(), handler.version());
                                if let Err(e) = self.db_pool.execute_transaction(ops).await {
                                    tracing::error!(
                                        "Handler {} transaction failed for range {}-{}: {:?}",
                                        handler_key, msg.range_start, msg.range_end, e
                                    );
                                    continue;
                                }
                            }
                            self.record_completed_range_for_handler(
                                &handler_key,
                                msg.range_start,
                                msg.range_end,
                            ).await?;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Handler {} failed for event {}/{}: {}",
                                handler_key, msg.source_name, msg.event_name, e
                            );
                        }
                    }
                } else {
                    // Buffer events for later processing
                    tracing::debug!(
                        "Handler {} buffering events for range {}-{}: waiting for call dependencies {:?}",
                        handler_key, msg.range_start, msg.range_end, call_deps
                    );

                    let pending = PendingEventData {
                        range_start: msg.range_start,
                        range_end: msg.range_end,
                        source_name: msg.source_name.clone(),
                        event_name: msg.event_name.clone(),
                        events: msg.events.clone(),
                        required_calls: call_deps.clone(),
                    };

                    state.pending_events
                        .entry(handler_key.clone())
                        .or_default()
                        .push(pending);
                }
            }
        }

        Ok(())
    }

    /// Process a calls message and check if any pending events can now be processed.
    async fn process_calls_message(
        &self,
        msg: DecodedCallsMessage,
    ) -> Result<(), TransformationError> {
        let range_key = (msg.range_start, msg.range_end);
        let call_key = (msg.source_name.clone(), msg.function_name.clone());

        // Mark this call type as received for this range and buffer the calls
        {
            let mut state = self.live_state.lock().await;
            state.received_calls
                .entry(call_key.clone())
                .or_default()
                .insert(range_key);
            state.calls_buffer
                .entry(range_key)
                .or_default()
                .extend(msg.calls.clone());
        }

        // Process call handlers (existing behavior)
        self.process_range(
            msg.range_start,
            msg.range_end,
            Vec::new(),
            msg.calls,
        ).await?;

        // Check if any pending events can now be processed
        self.try_process_pending_events(range_key).await?;

        Ok(())
    }

    /// Try to process pending events for a range now that new calls arrived.
    async fn try_process_pending_events(
        &self,
        range_key: (u64, u64),
    ) -> Result<(), TransformationError> {
        // Collect ready events under lock, then process outside lock
        let ready_events: Vec<(String, PendingEventData, Arc<dyn EventHandler>)> = {
            let mut state = self.live_state.lock().await;
            let mut ready = Vec::new();

            // Check each handler's pending events
            let handler_keys: Vec<_> = state.pending_events.keys().cloned().collect();
            for handler_key in handler_keys {
                // First pass: identify ready indices without holding mutable borrow
                let ready_indices: Vec<usize> = {
                    let pending = state.pending_events.get(&handler_key).unwrap();
                    pending
                        .iter()
                        .enumerate()
                        .filter(|(_, event_data)| {
                            (event_data.range_start, event_data.range_end) == range_key
                        })
                        .filter(|(_, event_data)| {
                            event_data.required_calls.iter().all(|dep| {
                                state.received_calls
                                    .get(dep)
                                    .map(|ranges| ranges.contains(&range_key))
                                    .unwrap_or(false)
                            })
                        })
                        .map(|(i, _)| i)
                        .collect()
                };

                // Second pass: extract ready events
                if !ready_indices.is_empty() {
                    let pending = state.pending_events.get_mut(&handler_key).unwrap();
                    // Extract in reverse order to preserve indices
                    for i in ready_indices.into_iter().rev() {
                        let event_data = pending.remove(i);
                        // Find the handler
                        let handlers = self.registry.handlers_for_event(
                            &event_data.source_name,
                            &event_data.event_name,
                        );
                        if let Some(handler) = handlers.into_iter().find(|h| h.handler_key() == handler_key) {
                            ready.push((handler_key.clone(), event_data, handler));
                        }
                    }
                }

                // Clean up empty entries
                if state.pending_events.get(&handler_key).map(|p| p.is_empty()).unwrap_or(true) {
                    state.pending_events.remove(&handler_key);
                }
            }

            ready
        };

        // Process ready events outside the lock
        for (handler_key, event_data, handler) in ready_events {
            let calls = {
                let state = self.live_state.lock().await;
                state.calls_buffer.get(&range_key).cloned().unwrap_or_default()
            };

            tracing::debug!(
                "Handler {} processing previously buffered events for range {}-{}",
                handler_key, event_data.range_start, event_data.range_end
            );

            let tx_addresses = self.read_receipt_addresses(event_data.range_start, event_data.range_end);
            let ctx = TransformationContext::new(
                &self.chain_name,
                self.chain_id,
                event_data.range_start,
                event_data.range_end,
                &event_data.events,
                &calls,
                tx_addresses,
                self.historical_reader.clone(),
                self.rpc_client.clone(),
                self.contracts.clone(),
            );

            match handler.handle(&ctx).await {
                Ok(ops) => {
                    if !ops.is_empty() {
                        let ops = Self::inject_source_version(ops, handler.name(), handler.version());
                        if let Err(e) = self.db_pool.execute_transaction(ops).await {
                            tracing::error!(
                                "Handler {} transaction failed for range {}-{}: {:?}",
                                handler_key, event_data.range_start, event_data.range_end, e
                            );
                            continue;
                        }
                    }
                    self.record_completed_range_for_handler(
                        &handler_key,
                        event_data.range_start,
                        event_data.range_end,
                    ).await?;
                }
                Err(e) => {
                    tracing::error!(
                        "Handler {} failed for buffered event {}/{}: {}",
                        handler_key, event_data.source_name, event_data.event_name, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Process a block range with per-handler transactions.
    /// Each handler's operations execute in their own transaction and
    /// progress is recorded independently.
    async fn process_range(
        &self,
        range_start: u64,
        range_end: u64,
        events: Vec<DecodedEvent>,
        calls: Vec<DecodedCall>,
    ) -> Result<(), TransformationError> {
        tracing::debug!(
            "Processing range {}-{} with {} events and {} calls",
            range_start,
            range_end,
            events.len(),
            calls.len()
        );

        // Create context for handlers
        let tx_addresses = self.read_receipt_addresses(range_start, range_end);
        let ctx = TransformationContext::new(
            &self.chain_name,
            self.chain_id,
            range_start,
            range_end,
            &events,
            &calls,
            tx_addresses,
            self.historical_reader.clone(),
            self.rpc_client.clone(),
            self.contracts.clone(),
        );

        // Get unique triggers from events
        let event_triggers: HashSet<_> = events
            .iter()
            .map(|e| (e.source_name.clone(), e.event_name.clone()))
            .collect();

        // Invoke each event handler independently with its own transaction
        for (source, event_name) in &event_triggers {
            for handler in self.registry.handlers_for_event(source, event_name) {
                let handler_key = handler.handler_key();

                tracing::debug!(
                    "Invoking handler {} for event {}/{}",
                    handler_key,
                    source,
                    event_name
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            tracing::debug!(
                                "Handler {} produced {} operations",
                                handler_key,
                                ops.len()
                            );

                            let ops = Self::inject_source_version(ops, handler.name(), handler.version());

                            // Per-handler transaction
                            if let Err(e) = self.db_pool.execute_transaction(ops).await {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key,
                                    range_start,
                                    range_end,
                                    e
                                );
                                // Continue with other handlers
                                continue;
                            }
                        }

                        // Record per-handler progress
                        self.record_completed_range_for_handler(
                            &handler_key,
                            range_start,
                            range_end,
                        )
                        .await?;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for event {}/{}: {}",
                            handler_key,
                            source,
                            event_name,
                            e
                        );
                        // Continue with other handlers
                        continue;
                    }
                }
            }
        }

        // Get unique triggers from calls
        let call_triggers: HashSet<_> = calls
            .iter()
            .map(|c| (c.source_name.clone(), c.function_name.clone()))
            .collect();

        // Invoke each call handler independently with its own transaction
        for (source, function_name) in &call_triggers {
            for handler in self.registry.handlers_for_call(source, function_name) {
                let handler_key = handler.handler_key();

                tracing::trace!(
                    "Invoking handler {} for call {}/{}",
                    handler_key,
                    source,
                    function_name
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            tracing::trace!(
                                "Handler {} produced {} operations",
                                handler_key,
                                ops.len()
                            );

                            let ops = Self::inject_source_version(ops, handler.name(), handler.version());

                            if let Err(e) = self.db_pool.execute_transaction(ops).await {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key,
                                    range_start,
                                    range_end,
                                    e
                                );
                                continue;
                            }
                        }

                        self.record_completed_range_for_handler(
                            &handler_key,
                            range_start,
                            range_end,
                        )
                        .await?;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for call {}/{}: {}",
                            handler_key,
                            source,
                            function_name,
                            e
                        );
                        continue;
                    }
                }
            }
        }

        Ok(())
    }
}
