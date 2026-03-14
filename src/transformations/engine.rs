//! Transformation engine that orchestrates handler execution.
//!
//! The engine receives decoded events and calls, invokes registered handlers,
//! and writes results to PostgreSQL. It tracks progress per handler and performs
//! per-handler catchup from decoded parquet files on startup.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use alloy::primitives::{I256, U256};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use super::context::{DecodedCall, DecodedEvent, TransactionAddresses, TransformationContext};
use super::error::TransformationError;
use super::historical::HistoricalDataReader;
use super::registry::{extract_event_name, TransformationRegistry};
use super::traits::EventHandler;
use crate::db::{DbOperation, DbPool, DbValue, WhereClause};
use crate::decoding::eth_calls::{
    build_decode_configs, build_result_map, CallDecodeConfig, DecodedValue as EthDecodedValue,
    EventCallDecodeConfig,
};
use crate::decoding::event_parsing::ParsedEvent;
use crate::decoding::logs::build_event_matchers;
use crate::live::{
    LiveDbValue, LiveDecodedValue, LiveProgressTracker, LiveStorage, LiveUpsertSnapshot,
    StorageError, TransformRetryRequest,
};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::contract::{Contracts, FactoryCollections};
use crate::types::config::eth_call::EvmType;

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
    pub kind: RangeCompleteKind,
}

/// Which decode stream has completed for a range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeCompleteKind {
    Logs,
    EthCalls,
}

/// Signal that a reorg occurred and orphaned blocks need cleanup.
#[derive(Debug)]
pub struct ReorgMessage {
    /// The block number of the common ancestor (last valid block).
    pub common_ancestor: u64,
    /// Block numbers that were orphaned and need cleanup.
    pub orphaned: Vec<u64>,
}

/// Execution mode for the transformation engine.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
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

/// Outcome of a single handler execution, used to track success/failure per handler.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum HandlerExecutionOutcome {
    /// Handler executed successfully and produced output.
    Succeeded {
        handler_key: String,
        range_start: u64,
        range_end: u64,
    },
    /// Handler executed successfully but produced no output (empty ops).
    SucceededEmpty {
        handler_key: String,
        range_start: u64,
        range_end: u64,
    },
    /// Handler execution failed (handler logic error).
    Failed {
        handler_key: String,
        range_start: u64,
        range_end: u64,
        error: String,
    },
    /// Database transaction failed after handler execution.
    DbTransactionFailed {
        handler_key: String,
        range_start: u64,
        range_end: u64,
        error: String,
    },
    /// Handler blocked waiting for missing dependencies.
    Blocked {
        handler_key: String,
        range_start: u64,
        range_end: u64,
        missing_dependencies: Vec<(String, String)>,
    },
}

#[allow(dead_code)]
impl HandlerExecutionOutcome {
    /// Get the handler key from any outcome variant.
    pub fn handler_key(&self) -> &str {
        match self {
            Self::Succeeded { handler_key, .. }
            | Self::SucceededEmpty { handler_key, .. }
            | Self::Failed { handler_key, .. }
            | Self::DbTransactionFailed { handler_key, .. }
            | Self::Blocked { handler_key, .. } => handler_key,
        }
    }

    /// Returns true if this outcome represents a successful execution.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded { .. } | Self::SucceededEmpty { .. })
    }

    /// Returns true if this outcome represents a failure.
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed { .. } | Self::DbTransactionFailed { .. })
    }

    /// Returns true if this outcome represents a blocked handler.
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked { .. })
    }

    /// Get the range (start, end) from any outcome variant.
    pub fn range(&self) -> (u64, u64) {
        match self {
            Self::Succeeded {
                range_start,
                range_end,
                ..
            }
            | Self::SucceededEmpty {
                range_start,
                range_end,
                ..
            }
            | Self::Failed {
                range_start,
                range_end,
                ..
            }
            | Self::DbTransactionFailed {
                range_start,
                range_end,
                ..
            }
            | Self::Blocked {
                range_start,
                range_end,
                ..
            } => (*range_start, *range_end),
        }
    }
}

/// Aggregate handler outcomes into success/failure/blocked sets.
fn aggregate_outcomes(
    outcomes: &[HandlerExecutionOutcome],
) -> (HashSet<String>, HashSet<String>, HashSet<String>) {
    let mut successful = HashSet::new();
    let mut failed = HashSet::new();
    let mut blocked = HashSet::new();

    for outcome in outcomes {
        let key = outcome.handler_key().to_string();
        match outcome {
            HandlerExecutionOutcome::Succeeded { .. }
            | HandlerExecutionOutcome::SucceededEmpty { .. } => {
                successful.insert(key);
            }
            HandlerExecutionOutcome::Failed { .. }
            | HandlerExecutionOutcome::DbTransactionFailed { .. } => {
                failed.insert(key);
            }
            HandlerExecutionOutcome::Blocked { .. } => {
                blocked.insert(key);
            }
        }
    }

    (successful, failed, blocked)
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

/// Default timeout for stuck pending events (5 minutes).
const PENDING_EVENT_TIMEOUT_SECS: u64 = 300;

/// Live processing state for buffering events with call dependencies.
struct LiveProcessingState {
    /// Track which (source, function) calls have arrived for which ranges.
    /// Key: (source_name, function_name), Value: set of (range_start, range_end)
    received_calls: HashMap<(String, String), HashSet<(u64, u64)>>,
    /// Accumulated calls per range for event handlers with dependencies.
    /// Key: (range_start, range_end), Value: accumulated calls
    calls_buffer: HashMap<(u64, u64), Vec<DecodedCall>>,
    /// Buffer events waiting for calls, keyed by handler_key.
    pending_events: HashMap<String, Vec<PendingEventData>>,
    /// Tracks which decode streams have completed for a range.
    completion: HashMap<(u64, u64), RangeCompletionState>,
    /// Track when events first become pending for timeout detection.
    /// Key: (range_start, range_end, handler_key), Value: when first added
    pending_event_timestamps: HashMap<(u64, u64, String), Instant>,
    /// Ranges that have been finalized (to prevent double finalization).
    finalized_ranges: HashSet<(u64, u64)>,
}

impl Default for LiveProcessingState {
    fn default() -> Self {
        Self {
            received_calls: HashMap::new(),
            calls_buffer: HashMap::new(),
            pending_events: HashMap::new(),
            completion: HashMap::new(),
            pending_event_timestamps: HashMap::new(),
            finalized_ranges: HashSet::new(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RangeCompletionState {
    logs_complete: bool,
    eth_calls_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LiveRetryCallArtifactKind {
    Regular,
    EventTriggered { base_name: String },
}

impl RangeCompletionState {
    fn mark(&mut self, kind: RangeCompleteKind) {
        match kind {
            RangeCompleteKind::Logs => self.logs_complete = true,
            RangeCompleteKind::EthCalls => self.eth_calls_complete = true,
        }
    }

    fn is_ready(self, expect_logs: bool, expect_eth_calls: bool) -> bool {
        (!expect_logs || self.logs_complete) && (!expect_eth_calls || self.eth_calls_complete)
    }
}

fn resolve_retry_missing_handlers(
    request_missing: Option<HashSet<String>>,
    tracker_missing: Option<HashSet<String>>,
    all_handlers: HashSet<String>,
) -> HashSet<String> {
    tracker_missing.unwrap_or_else(|| request_missing.unwrap_or(all_handlers))
}

fn missing_retry_call_dependencies(
    required_calls: &HashSet<(String, String)>,
    calls: &[DecodedCall],
) -> HashSet<(String, String)> {
    let available_calls: HashSet<(String, String)> = calls
        .iter()
        .map(|call| (call.source_name.clone(), call.function_name.clone()))
        .collect();

    required_calls
        .difference(&available_calls)
        .cloned()
        .collect()
}

fn classify_live_retry_call_artifact(
    source_name: &str,
    function_name: &str,
    regular_keys: &HashSet<(String, String)>,
    event_keys: &HashSet<(String, String)>,
) -> Result<LiveRetryCallArtifactKind, TransformationError> {
    let exact_key = (source_name.to_string(), function_name.to_string());
    if regular_keys.contains(&exact_key) {
        return Ok(LiveRetryCallArtifactKind::Regular);
    }

    if let Some(base_name) = function_name.strip_suffix("_event") {
        let event_key = (source_name.to_string(), base_name.to_string());
        if event_keys.contains(&event_key) {
            return Ok(LiveRetryCallArtifactKind::EventTriggered {
                base_name: base_name.to_string(),
            });
        }
    }

    Err(TransformationError::MissingData(format!(
        "missing call schema for live retry {}/{}",
        source_name, function_name
    )))
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
    factory_collections: Arc<FactoryCollections>,
    /// Maximum number of handlers to execute concurrently.
    handler_concurrency: usize,
    /// Live processing state for buffering events with call dependencies.
    live_state: Mutex<LiveProcessingState>,
    /// Live mode progress tracker for marking block completion.
    progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
    /// Whether live ranges require an eth_call completion signal before finalization.
    expect_eth_call_completion: bool,
    /// Whether ranges require a log completion signal before finalization.
    expect_log_completion: bool,
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
        factory_collections: FactoryCollections,
        handler_concurrency: usize,
        progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
        expect_log_completion: bool,
        expect_eth_call_completion: bool,
    ) -> Result<Self, TransformationError> {
        let historical_reader = Arc::new(HistoricalDataReader::new(&chain_name)?);
        let decoded_logs_dir =
            PathBuf::from(format!("data/{}/historical/decoded/logs", chain_name));
        let decoded_calls_dir =
            PathBuf::from(format!("data/{}/historical/decoded/eth_calls", chain_name));
        let raw_receipts_dir =
            PathBuf::from(format!("data/{}/historical/raw/receipts", chain_name));

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
            factory_collections: Arc::new(factory_collections),
            handler_concurrency,
            live_state: Mutex::new(LiveProcessingState::default()),
            progress_tracker,
            expect_eth_call_completion,
            expect_log_completion,
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
            let client = pool.get().await?;
            let rows = client.query("SELECT name FROM _migrations", &[]).await?;
            rows.iter().map(|r| r.get(0)).collect()
        };

        // Resolve each path into (file_path, migration_name) pairs
        let mut sql_entries: Vec<(PathBuf, String)> = Vec::new();

        for path in &migration_paths {
            if !path.exists() {
                tracing::warn!("Handler migration path does not exist: {}", path.display());
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
                    .filter(|e| e.path().extension().map(|x| x == "sql").unwrap_or(false))
                    .collect();
                dir_files.sort_by_key(|e| e.file_name());

                for entry in dir_files {
                    let file_name = entry.file_name().to_string_lossy().to_string();
                    let migration_name = format!("{}", path.join(&file_name).display());
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

            let mut client = pool.get().await?;
            let tx = client.transaction().await?;

            tx.batch_execute(&sql).await.map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::MigrationError(format!(
                    "Handler migration {} failed: {}",
                    migration_name, e
                )))
            })?;

            tx.execute(
                "INSERT INTO _migrations (name) VALUES ($1)",
                &[&migration_name],
            )
            .await?;

            tx.commit().await?;

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
        let client = pool.get().await?;

        for handler in self.registry.all_handlers() {
            let source = handler.name().to_string();
            let version = handler.version() as i32;

            let rows = client
                .query(
                    "SELECT active_version FROM active_versions WHERE source = $1",
                    &[&source],
                )
                .await?;

            if rows.is_empty() {
                // New source — insert with current version
                client
                    .execute(
                        "INSERT INTO active_versions (source, active_version) VALUES ($1, $2)",
                        &[&source, &version],
                    )
                    .await?;
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
    fn inject_source_version(
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
    fn scan_available_ranges(
        &self,
        base_dir: &PathBuf,
    ) -> Result<Vec<(u64, u64)>, TransformationError> {
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
    /// Processes `handler_concurrency` ranges concurrently per handler.
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

            let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
            let mut join_set: JoinSet<Result<Option<(String, u64, u64)>, TransformationError>> =
                JoinSet::new();

            for (range_start, range_end) in &to_process {
                let events = self
                    .read_decoded_events_for_triggers(*range_start, *range_end, &event_triggers)
                    .await?;

                // Filter events by contract start_block
                let events = self.filter_events_by_start_block(events);

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
                    let calls = self
                        .read_decoded_calls_for_triggers(*range_start, *range_end, &call_deps)
                        .await?;
                    // Filter calls by contract start_block
                    self.filter_calls_by_start_block(calls)
                } else {
                    Vec::new()
                };

                processed += 1;

                if !events.is_empty() {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let handler = handler.clone();
                    let db_pool = self.db_pool.clone();
                    let handler_key = handler_key.clone();
                    let handler_name = handler.name();
                    let handler_version = handler.version();
                    let tx_addresses = self.read_receipt_addresses(*range_start, *range_end);
                    let chain_name = self.chain_name.clone();
                    let chain_id = self.chain_id;
                    let historical = self.historical_reader.clone();
                    let rpc = self.rpc_client.clone();
                    let contracts = self.contracts.clone();
                    let rs = *range_start;
                    let re = *range_end;

                    join_set.spawn(async move {
                        let _permit = permit;
                        let ctx = TransformationContext::new(
                            chain_name,
                            chain_id,
                            rs,
                            re,
                            Arc::new(events),
                            Arc::new(calls),
                            tx_addresses,
                            historical,
                            rpc,
                            contracts,
                        );

                        match handler.handle(&ctx).await {
                            Ok(ops) => {
                                if !ops.is_empty() {
                                    let ops = Self::inject_source_version(
                                        ops,
                                        handler_name,
                                        handler_version,
                                    );
                                    db_pool.execute_transaction(ops).await?;
                                }
                                Ok(Some((handler_key, rs, re)))
                            }
                            Err(e) => Err(e),
                        }
                    });
                } else {
                    // No events — record progress directly
                    self.record_completed_range_for_handler(&handler_key, *range_start, *range_end)
                        .await?;
                }

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

            // Drain remaining tasks
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(Some((hk, rs, re)))) => {
                        self.record_completed_range_for_handler(&hk, rs, re).await?;
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => {
                        tracing::error!(
                            "Handler {} failed during catchup: {}. Stopping catchup for this handler.",
                            handler_key, e
                        );
                        errored = true;
                        break;
                    }
                    Err(e) => {
                        tracing::error!("Handler {} catchup task panicked: {}", handler_key, e);
                        errored = true;
                        break;
                    }
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
    /// Processes `handler_concurrency` ranges concurrently per handler.
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

            let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
            let mut join_set: JoinSet<Result<Option<(String, u64, u64)>, TransformationError>> =
                JoinSet::new();

            for (range_start, range_end) in &to_process {
                let calls = self
                    .read_decoded_calls_for_triggers(*range_start, *range_end, &call_triggers)
                    .await?;

                // Filter calls by contract start_block
                let calls = self.filter_calls_by_start_block(calls);

                processed += 1;

                if !calls.is_empty() {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let handler = handler.clone();
                    let db_pool = self.db_pool.clone();
                    let handler_key = handler_key.clone();
                    let handler_name = handler.name();
                    let handler_version = handler.version();
                    let chain_name = self.chain_name.clone();
                    let chain_id = self.chain_id;
                    let historical = self.historical_reader.clone();
                    let rpc = self.rpc_client.clone();
                    let contracts = self.contracts.clone();
                    let rs = *range_start;
                    let re = *range_end;

                    join_set.spawn(async move {
                        let _permit = permit;
                        let ctx = TransformationContext::new(
                            chain_name,
                            chain_id,
                            rs,
                            re,
                            Arc::new(Vec::new()),
                            Arc::new(calls),
                            HashMap::new(),
                            historical,
                            rpc,
                            contracts,
                        );

                        match handler.handle(&ctx).await {
                            Ok(ops) => {
                                if !ops.is_empty() {
                                    let ops = Self::inject_source_version(
                                        ops,
                                        handler_name,
                                        handler_version,
                                    );
                                    db_pool.execute_transaction(ops).await?;
                                }
                                Ok(Some((handler_key, rs, re)))
                            }
                            Err(e) => Err(e),
                        }
                    });
                } else {
                    // No calls — record progress directly
                    self.record_completed_range_for_handler(&handler_key, *range_start, *range_end)
                        .await?;
                }

                if processed % 50 == 0 {
                    tracing::info!(
                        "Handler {} catchup progress: {}/{}",
                        handler_key,
                        processed,
                        total
                    );
                }
            }

            // Drain remaining tasks
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(Some((hk, rs, re)))) => {
                        self.record_completed_range_for_handler(&hk, rs, re).await?;
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => {
                        tracing::error!(
                            "Handler {} failed during catchup: {}. Stopping catchup for this handler.",
                            handler_key, e
                        );
                        errored = true;
                        break;
                    }
                    Err(e) => {
                        tracing::error!("Handler {} catchup task panicked: {}", handler_key, e);
                        errored = true;
                        break;
                    }
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
                tracing::warn!(
                    "Failed to read receipt parquet {}: {}",
                    file_path.display(),
                    e
                );
                return HashMap::new();
            }
        };

        let reader = match builder.build() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "Failed to build receipt reader {}: {}",
                    file_path.display(),
                    e
                );
                return HashMap::new();
            }
        };

        let mut addresses = HashMap::new();

        for batch_result in reader {
            let batch = match batch_result {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        "Failed to read receipt batch from {}: {}",
                        file_path.display(),
                        e
                    );
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
            let to_col = batch
                .column_by_name("to_address")
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

                addresses.insert(
                    tx_hash,
                    TransactionAddresses {
                        from_address,
                        to_address,
                    },
                );
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

    /// Read decoded events from parquet files for specific triggers,
    /// using `spawn_blocking` to avoid blocking the async runtime.
    /// Multiple trigger files are read concurrently.
    async fn read_decoded_events_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        event_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let mut read_tasks = JoinSet::new();

        for (source_name, event_name) in event_triggers {
            let file_path = self
                .decoded_logs_dir
                .join(source_name)
                .join(event_name)
                .join(&file_name);

            if !file_path.exists() {
                continue;
            }

            let reader = self.historical_reader.clone();
            let src = source_name.clone();
            let evt = event_name.clone();

            read_tasks.spawn_blocking(move || {
                tracing::debug!("Reading decoded events from {}", file_path.display());
                match reader.read_events_from_file(&file_path, &src, &evt) {
                    Ok(events) => {
                        tracing::debug!(
                            "Read {} events from {}",
                            events.len(),
                            file_path.display()
                        );
                        Ok(events)
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read decoded events from {}: {}",
                            file_path.display(),
                            e
                        );
                        Ok(Vec::new())
                    }
                }
            });
        }

        let mut all_events = Vec::new();
        while let Some(result) = read_tasks.join_next().await {
            match result {
                Ok(Ok(events)) => all_events.extend(events),
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(TransformationError::IoError(std::io::Error::other(
                        e.to_string(),
                    )))
                }
            }
        }

        Ok(all_events)
    }

    /// Read decoded calls from parquet files for specific triggers,
    /// using `spawn_blocking` to avoid blocking the async runtime.
    /// Multiple trigger files are read concurrently.
    async fn read_decoded_calls_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        call_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let mut read_tasks = JoinSet::new();

        for (source_name, function_name) in call_triggers {
            let file_path = self
                .decoded_calls_dir
                .join(source_name)
                .join(function_name)
                .join(&file_name);

            if !file_path.exists() {
                continue;
            }

            let reader = self.historical_reader.clone();
            let src = source_name.clone();
            let func = function_name.clone();

            read_tasks.spawn_blocking(move || {
                tracing::debug!("Reading decoded calls from {}", file_path.display());
                match reader.read_calls_from_file(&file_path, &src, &func) {
                    Ok(calls) => {
                        tracing::debug!("Read {} calls from {}", calls.len(), file_path.display());
                        Ok(calls)
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read decoded calls from {}: {}",
                            file_path.display(),
                            e
                        );
                        Ok(Vec::new())
                    }
                }
            });
        }

        let mut all_calls = Vec::new();
        while let Some(result) = read_tasks.join_next().await {
            match result {
                Ok(Ok(calls)) => all_calls.extend(calls),
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(TransformationError::IoError(std::io::Error::other(
                        e.to_string(),
                    )))
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
    ///
    /// The optional `reorg_rx` channel receives reorg notifications from live mode
    /// to clean up pending events for orphaned blocks.
    pub async fn run(
        &self,
        mut events_rx: Receiver<DecodedEventsMessage>,
        mut calls_rx: Receiver<DecodedCallsMessage>,
        mut complete_rx: Receiver<RangeCompleteMessage>,
        mut reorg_rx: Option<Receiver<ReorgMessage>>,
        mut retry_rx: Option<Receiver<TransformRetryRequest>>,
        decode_catchup_done_rx: Option<oneshot::Receiver<()>>,
    ) -> Result<(), TransformationError> {
        // Wait for decode catchup to finish so all decoded parquet files exist
        if let Some(rx) = decode_catchup_done_rx {
            tracing::info!(
                "Waiting for eth_call decode catchup to complete before transformation catchup..."
            );
            let _ = rx.await;
            tracing::info!(
                "Eth_call decode catchup complete, proceeding with transformation catchup"
            );
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

                    // Process events, handling call dependencies
                    self.process_events_message(msg).await?;
                }

                Some(msg) = calls_rx.recv() => {
                    if msg.calls.is_empty() {
                        continue;
                    }

                    // Process calls and check if any pending events can now be processed
                    self.process_calls_message(msg).await?;
                }

                Some(msg) = complete_rx.recv() => {
                    self.process_range_complete(msg).await?;
                }

                Some(msg) = async {
                    match reorg_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.process_reorg(msg).await?;
                }

                Some(msg) = async {
                    match retry_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.process_transform_retry(msg).await?;
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
    /// Ready handlers run concurrently bounded by `handler_concurrency`.
    async fn process_events_message(
        &self,
        msg: DecodedEventsMessage,
    ) -> Result<(), TransformationError> {
        let handlers = self
            .registry
            .handlers_for_event(&msg.source_name, &msg.event_name);
        if handlers.is_empty() {
            tracing::debug!(
                "No handlers registered for {}/{}",
                msg.source_name,
                msg.event_name
            );
        } else {
            tracing::debug!(
                "Processing {} events for {}/{} block {} with {} handlers",
                msg.events.len(),
                msg.source_name,
                msg.event_name,
                msg.range_start,
                handlers.len()
            );
        }

        // Filter events by contract start_block
        let filtered_events = self.filter_events_by_start_block(msg.events.clone());
        let events = Arc::new(filtered_events.clone());

        // Categorize handlers: ready to run vs needs buffering
        let range_key = (msg.range_start, msg.range_end);
        let mut ready_handlers: Vec<(Arc<dyn EventHandler>, Arc<Vec<DecodedCall>>)> = Vec::new();
        {
            let mut state = self.live_state.lock().await;
            for handler in &handlers {
                let call_deps = handler.call_dependencies();
                let handler_key = handler.handler_key();

                if call_deps.is_empty() {
                    ready_handlers.push((handler.clone(), Arc::new(Vec::new())));
                } else {
                    let deps_ready = call_deps.iter().all(|dep| {
                        state
                            .received_calls
                            .get(dep)
                            .map(|ranges| ranges.contains(&range_key))
                            .unwrap_or(false)
                    });

                    if deps_ready {
                        let calls = state
                            .calls_buffer
                            .get(&range_key)
                            .cloned()
                            .unwrap_or_default();
                        // Filter calls by contract start_block (inline to avoid borrowing self inside lock)
                        let calls: Vec<_> = calls
                            .into_iter()
                            .filter(|c| {
                                let start_block = self
                                    .contracts
                                    .get(&c.source_name)
                                    .and_then(|ct| ct.start_block.map(|u| u.to::<u64>()));
                                start_block.map_or(true, |sb| c.block_number >= sb)
                            })
                            .collect();
                        tracing::debug!(
                            "Handler {} deps ready for block {}, {} calls",
                            handler_key,
                            msg.range_start,
                            calls.len()
                        );
                        ready_handlers.push((handler.clone(), Arc::new(calls)));
                    } else {
                        tracing::warn!(
                            "Handler {} buffering block {}: waiting for eth_call deps {:?}",
                            handler_key,
                            msg.range_start,
                            call_deps
                        );
                        let pending = PendingEventData {
                            range_start: msg.range_start,
                            range_end: msg.range_end,
                            source_name: msg.source_name.clone(),
                            event_name: msg.event_name.clone(),
                            events: filtered_events.clone(),
                            required_calls: call_deps.clone(),
                        };
                        // Track when this pending event was first added for timeout detection
                        let timestamp_key = (msg.range_start, msg.range_end, handler_key.clone());
                        state
                            .pending_event_timestamps
                            .entry(timestamp_key)
                            .or_insert_with(Instant::now);
                        state
                            .pending_events
                            .entry(handler_key)
                            .or_default()
                            .push(pending);
                    }
                }
            }
        } // lock released

        if ready_handlers.is_empty() {
            return Ok(());
        }

        // Run ready handlers concurrently
        let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
        let mut join_set: JoinSet<Result<HandlerExecutionOutcome, TransformationError>> =
            JoinSet::new();
        let tx_addresses = self.read_receipt_addresses(msg.range_start, msg.range_end);
        let tx_addresses = Arc::new(tx_addresses);

        for (handler, calls) in ready_handlers {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let events = events.clone();
            let db_pool = self.db_pool.clone();
            let chain_name = self.chain_name.clone();
            let chain_id = self.chain_id;
            let historical = self.historical_reader.clone();
            let rpc = self.rpc_client.clone();
            let contracts = self.contracts.clone();
            let handler_name = handler.name();
            let handler_version = handler.version();
            let handler_key = handler.handler_key();
            let range_start = msg.range_start;
            let range_end = msg.range_end;
            let source_name = msg.source_name.clone();
            let event_name = msg.event_name.clone();
            let tx_addrs = tx_addresses.clone();

            join_set.spawn(async move {
                let _permit = permit;
                // Each handler gets its own context (shared events, handler-specific calls)
                let ctx = TransformationContext::new(
                    chain_name,
                    chain_id,
                    range_start,
                    range_end,
                    events,
                    calls,
                    Arc::try_unwrap(tx_addrs).unwrap_or_else(|arc| (*arc).clone()),
                    historical,
                    rpc,
                    contracts,
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops =
                                Self::inject_source_version(ops, handler_name, handler_version);
                            if let Err(e) = db_pool.execute_transaction(ops).await {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key,
                                    range_start,
                                    range_end,
                                    e
                                );
                                return Ok(HandlerExecutionOutcome::DbTransactionFailed {
                                    handler_key,
                                    range_start,
                                    range_end,
                                    error: e.to_string(),
                                });
                            }
                            Ok(HandlerExecutionOutcome::Succeeded {
                                handler_key,
                                range_start,
                                range_end,
                            })
                        } else {
                            Ok(HandlerExecutionOutcome::SucceededEmpty {
                                handler_key,
                                range_start,
                                range_end,
                            })
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for event {}/{}: {}",
                            handler_key,
                            source_name,
                            event_name,
                            e
                        );
                        Ok(HandlerExecutionOutcome::Failed {
                            handler_key,
                            range_start,
                            range_end,
                            error: e.to_string(),
                        })
                    }
                }
            });
        }

        // Collect outcomes and record progress only for successful handlers
        let mut outcomes = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(outcome)) => outcomes.push(outcome),
                Ok(Err(e)) => {
                    tracing::error!("Handler task returned error: {}", e);
                }
                Err(e) => {
                    tracing::error!("Handler task panicked: {}", e);
                }
            }
        }

        let (_successful, failed, _blocked) = aggregate_outcomes(&outcomes);
        let is_single_block = msg.range_end - msg.range_start == 1;

        // Record progress only for successful handlers
        for outcome in &outcomes {
            if outcome.is_success() {
                let handler_key = outcome.handler_key();
                let (rs, re) = outcome.range();
                self.record_completed_range_for_handler(handler_key, rs, re)
                    .await?;

                if is_single_block {
                    if let Some(ref tracker) = self.progress_tracker {
                        let mut t = tracker.lock().await;
                        if let Err(e) = t.mark_complete(rs, handler_key).await {
                            tracing::warn!(
                                "Failed to mark live progress for block {} handler {}: {}",
                                rs,
                                handler_key,
                                e
                            );
                        }
                    }
                }
            }
        }

        // Persist failed handlers to status file for retry (atomic update)
        if !failed.is_empty() && is_single_block {
            let storage = LiveStorage::new(&self.chain_name);
            if let Err(e) = storage.update_status_atomic(msg.range_start, |status| {
                status.failed_handlers.extend(failed.iter().cloned());
                for h in &failed {
                    status.completed_handlers.remove(h);
                }
            }) {
                match e {
                    StorageError::NotFound(_) => {
                        tracing::debug!(
                            "Status file not found for block {}, cannot persist failed handlers",
                            msg.range_start
                        );
                    }
                    _ => {
                        tracing::warn!(
                            "Failed to persist failed handlers for block {}: {}",
                            msg.range_start,
                            e
                        );
                    }
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

        tracing::debug!(
            "Received {} eth_calls for {}/{} block {}",
            msg.calls.len(),
            msg.source_name,
            msg.function_name,
            msg.range_start
        );

        // Mark this call type as received for this range and buffer the calls
        {
            let mut state = self.live_state.lock().await;
            state
                .received_calls
                .entry(call_key.clone())
                .or_default()
                .insert(range_key);
            state
                .calls_buffer
                .entry(range_key)
                .or_default()
                .extend(msg.calls.clone());
        }

        // Process call handlers (existing behavior) - filter calls by start_block
        let filtered_calls = self.filter_calls_by_start_block(msg.calls);
        self.process_range(msg.range_start, msg.range_end, Vec::new(), filtered_calls)
            .await?;

        // Check if any pending events can now be processed
        self.try_process_pending_events(range_key).await?;
        self.maybe_finalize_range(range_key).await?;

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
                                state
                                    .received_calls
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
                    tracing::info!(
                        "Handler {} unblocked for block {}: call deps now available",
                        handler_key,
                        range_key.0
                    );
                    let pending = state.pending_events.get_mut(&handler_key).unwrap();
                    // Extract in reverse order to preserve indices
                    for i in ready_indices.into_iter().rev() {
                        let event_data = pending.remove(i);
                        // Find the handler
                        let handlers = self
                            .registry
                            .handlers_for_event(&event_data.source_name, &event_data.event_name);
                        if let Some(handler) = handlers
                            .into_iter()
                            .find(|h| h.handler_key() == handler_key)
                        {
                            ready.push((handler_key.clone(), event_data, handler));
                        }
                    }
                }

                // Clean up empty entries
                if state
                    .pending_events
                    .get(&handler_key)
                    .map(|p| p.is_empty())
                    .unwrap_or(true)
                {
                    state.pending_events.remove(&handler_key);
                }
            }

            ready
        };

        if ready_events.is_empty() {
            return Ok(());
        }

        // Fetch calls once for this range and filter by start_block
        let calls = {
            let state = self.live_state.lock().await;
            let calls = state
                .calls_buffer
                .get(&range_key)
                .cloned()
                .unwrap_or_default();
            Arc::new(self.filter_calls_by_start_block(calls))
        };

        // Process ready events concurrently
        let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
        let mut join_set: JoinSet<Result<HandlerExecutionOutcome, TransformationError>> =
            JoinSet::new();

        for (handler_key, event_data, handler) in ready_events {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let calls = calls.clone();
            let db_pool = self.db_pool.clone();
            let chain_name = self.chain_name.clone();
            let chain_id = self.chain_id;
            let historical = self.historical_reader.clone();
            let rpc = self.rpc_client.clone();
            let contracts = self.contracts.clone();
            let handler_name = handler.name();
            let handler_version = handler.version();
            let tx_addresses =
                self.read_receipt_addresses(event_data.range_start, event_data.range_end);

            join_set.spawn(async move {
                let _permit = permit;
                tracing::debug!(
                    "Handler {} processing previously buffered events for range {}-{}",
                    handler_key,
                    event_data.range_start,
                    event_data.range_end
                );

                let source_name = event_data.source_name.clone();
                let event_name = event_data.event_name.clone();
                let rs = event_data.range_start;
                let re = event_data.range_end;

                let ctx = TransformationContext::new(
                    chain_name,
                    chain_id,
                    rs,
                    re,
                    Arc::new(event_data.events),
                    Arc::try_unwrap(calls)
                        .unwrap_or_else(|arc| (*arc).clone())
                        .into(),
                    tx_addresses,
                    historical,
                    rpc,
                    contracts,
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops =
                                Self::inject_source_version(ops, handler_name, handler_version);
                            if let Err(e) = db_pool.execute_transaction(ops).await {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key,
                                    rs,
                                    re,
                                    e
                                );
                                return Ok(HandlerExecutionOutcome::DbTransactionFailed {
                                    handler_key,
                                    range_start: rs,
                                    range_end: re,
                                    error: e.to_string(),
                                });
                            }
                            Ok(HandlerExecutionOutcome::Succeeded {
                                handler_key,
                                range_start: rs,
                                range_end: re,
                            })
                        } else {
                            Ok(HandlerExecutionOutcome::SucceededEmpty {
                                handler_key,
                                range_start: rs,
                                range_end: re,
                            })
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for buffered event {}/{}: {}",
                            handler_key,
                            source_name,
                            event_name,
                            e
                        );
                        Ok(HandlerExecutionOutcome::Failed {
                            handler_key,
                            range_start: rs,
                            range_end: re,
                            error: e.to_string(),
                        })
                    }
                }
            });
        }

        // Collect outcomes and record progress only for successful handlers
        let mut outcomes = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(outcome)) => outcomes.push(outcome),
                Ok(Err(e)) => {
                    tracing::error!("Handler task returned error: {}", e);
                }
                Err(e) => {
                    tracing::error!("Handler task panicked: {}", e);
                }
            }
        }

        let (_successful, failed, _blocked) = aggregate_outcomes(&outcomes);

        // Record progress only for successful handlers
        for outcome in &outcomes {
            if outcome.is_success() {
                let handler_key = outcome.handler_key();
                let (rs, re) = outcome.range();
                self.record_completed_range_for_handler(handler_key, rs, re)
                    .await?;

                if re - rs == 1 {
                    if let Some(ref tracker) = self.progress_tracker {
                        let mut t = tracker.lock().await;
                        if let Err(e) = t.mark_complete(rs, handler_key).await {
                            tracing::warn!(
                                "Failed to mark live progress for block {} handler {}: {}",
                                rs,
                                handler_key,
                                e
                            );
                        }
                    }
                }
            }
        }

        // Persist failed handlers to status file for retry (atomic update)
        if !failed.is_empty() {
            // All events in this batch have the same range (single-block in live mode)
            if let Some(first_outcome) = outcomes.first() {
                let (rs, re) = first_outcome.range();
                if re - rs == 1 {
                    let storage = LiveStorage::new(&self.chain_name);
                    if let Err(e) = storage.update_status_atomic(rs, |status| {
                        status.failed_handlers.extend(failed.iter().cloned());
                        for h in &failed {
                            status.completed_handlers.remove(h);
                        }
                    }) {
                        if !matches!(e, StorageError::NotFound(_)) {
                            tracing::warn!(
                                "Failed to persist failed handlers for block {}: {}",
                                rs,
                                e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process range completion signal.
    /// Records progress for ALL handlers (event + call) even when no events/calls matched their triggers.
    /// This ensures handlers don't unnecessarily re-process empty ranges on restart.
    async fn process_range_complete(
        &self,
        msg: RangeCompleteMessage,
    ) -> Result<(), TransformationError> {
        let range_key = (msg.range_start, msg.range_end);
        {
            let mut state = self.live_state.lock().await;
            state
                .completion
                .entry(range_key)
                .or_default()
                .mark(msg.kind);
        }

        self.maybe_finalize_range(range_key).await?;
        Ok(())
    }

    /// Process a reorg notification by:
    /// 1. Cleaning up pending events for orphaned blocks (memory)
    /// 2. Deleting committed rows from database tables
    /// 3. Cleaning up _live_progress entries
    ///
    /// When a reorg occurs, any events buffered for orphaned blocks should be discarded
    /// since those blocks are no longer part of the canonical chain. Additionally,
    /// data already committed to the database for orphaned blocks must be rolled back.
    async fn process_reorg(&self, msg: ReorgMessage) -> Result<(), TransformationError> {
        tracing::info!(
            "Processing reorg: common_ancestor={}, orphaned={:?}",
            msg.common_ancestor,
            msg.orphaned
        );

        if msg.orphaned.is_empty() {
            return Ok(());
        }

        // Phase 1: Clean up in-memory state
        let total_removed = self.cleanup_pending_state(&msg.orphaned).await;
        if total_removed > 0 {
            tracing::info!(
                "Reorg cleanup: removed {} pending events for {} orphaned blocks",
                total_removed,
                msg.orphaned.len()
            );
        }

        // Phase 2: Delete committed rows from database tables
        self.cleanup_reorg_tables(&msg.orphaned).await?;

        // Phase 3: Clean up _live_progress entries
        self.cleanup_live_progress(&msg.orphaned).await?;

        Ok(())
    }

    /// Clean up in-memory pending state for orphaned blocks.
    /// Returns the number of pending events removed.
    async fn cleanup_pending_state(&self, orphaned: &[u64]) -> usize {
        let mut state = self.live_state.lock().await;
        let mut total_removed = 0;

        for &orphaned_block in orphaned {
            // In live mode, each block is its own range: (block, block+1)
            let range_key = (orphaned_block, orphaned_block + 1);

            // Remove from calls_buffer
            if state.calls_buffer.remove(&range_key).is_some() {
                tracing::debug!("Removed calls buffer for orphaned range {:?}", range_key);
            }
            state.completion.remove(&range_key);

            // Remove from finalized_ranges to allow re-finalization if block is re-processed
            state.finalized_ranges.remove(&range_key);

            // Remove from received_calls
            for ranges in state.received_calls.values_mut() {
                ranges.remove(&range_key);
            }

            // Remove pending event timestamps for this range
            state
                .pending_event_timestamps
                .retain(|(rs, re, _), _| (*rs, *re) != range_key);

            // Remove pending events for this range from all handlers
            for (handler_key, pending_list) in state.pending_events.iter_mut() {
                let initial_len = pending_list.len();
                pending_list.retain(|pending| {
                    let matches = (pending.range_start, pending.range_end) == range_key;
                    if matches {
                        tracing::debug!(
                            "Removing pending event for handler {} at orphaned range {:?}",
                            handler_key,
                            range_key
                        );
                    }
                    !matches
                });
                total_removed += initial_len - pending_list.len();
            }
        }

        // Clean up empty entries
        state.pending_events.retain(|_, v| !v.is_empty());

        total_removed
    }

    /// Rollback committed rows for orphaned blocks using snapshots.
    ///
    /// Phase 2a: Read snapshots from storage and restore previous state:
    /// - Rows with previous_row = Some are restored via upsert
    /// - Rows with previous_row = None are deleted by key
    ///
    /// Phase 2b: Delete remaining rows without snapshots (fallback for rows
    /// that were INSERTed without update_columns, where we have no prior state).
    async fn cleanup_reorg_tables(&self, orphaned: &[u64]) -> Result<(), TransformationError> {
        // Collect all unique reorg tables from all handlers
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
                                // Row existed before - restore via upsert
                                let columns: Vec<String> =
                                    previous.iter().map(|(k, _)| k.clone()).collect();
                                let values: Vec<DbValue> =
                                    previous.iter().map(|(_, v)| v.to_db_value()).collect();
                                let conflict_cols: Vec<String> = snapshot
                                    .key_columns
                                    .iter()
                                    .map(|(k, _)| k.clone())
                                    .collect();

                                // Update all non-key columns
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
                                // Row didn't exist before - delete by key
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
                    // No snapshots for this block - will use fallback DELETE
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

        // Execute restore operations if any
        if !restore_ops.is_empty() {
            tracing::info!(
                "Reorg rollback: executing {} restore operations from snapshots",
                restore_ops.len()
            );
            self.db_pool.execute_transaction(restore_ops).await?;
        }

        // Phase 2b: Delete remaining rows without snapshots
        // Only delete from tables that didn't have snapshots (pure INSERTs)
        let block_list = orphaned
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let mut fallback_ops = Vec::new();
        for table in &tables_to_clean {
            // Skip tables that had snapshot-based restoration
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

    async fn maybe_finalize_range(&self, range_key: (u64, u64)) -> Result<(), TransformationError> {
        let (should_finalize, timed_out_handlers) = {
            let mut state = self.live_state.lock().await;
            let completion = state
                .completion
                .get(&range_key)
                .copied()
                .unwrap_or_default();

            // Check for timed-out pending events
            let timeout = std::time::Duration::from_secs(PENDING_EVENT_TIMEOUT_SECS);
            let now = Instant::now();
            let mut timed_out: Vec<String> = Vec::new();

            for (handler_key, pending_list) in &state.pending_events {
                for pending in pending_list {
                    if (pending.range_start, pending.range_end) == range_key {
                        let timestamp_key = (range_key.0, range_key.1, handler_key.clone());
                        if let Some(&first_seen) =
                            state.pending_event_timestamps.get(&timestamp_key)
                        {
                            if now.duration_since(first_seen) >= timeout {
                                tracing::error!(
                                    "Pending event TIMED OUT after {:?}: handler={} range={}-{} event={}/{} waiting_for={:?}. \
                                     Force-finalizing range to unblock progress.",
                                    now.duration_since(first_seen),
                                    handler_key,
                                    pending.range_start,
                                    pending.range_end,
                                    pending.source_name,
                                    pending.event_name,
                                    pending.required_calls
                                );
                                timed_out.push(handler_key.clone());
                            }
                        }
                    }
                }
            }

            // Remove timed-out pending events
            for handler_key in &timed_out {
                if let Some(pending_list) = state.pending_events.get_mut(handler_key) {
                    pending_list
                        .retain(|pending| (pending.range_start, pending.range_end) != range_key);
                }
                let timestamp_key = (range_key.0, range_key.1, handler_key.clone());
                state.pending_event_timestamps.remove(&timestamp_key);
            }
            state.pending_events.retain(|_, v| !v.is_empty());

            let has_pending = state.pending_events.values().any(|pending_list| {
                pending_list
                    .iter()
                    .any(|pending| (pending.range_start, pending.range_end) == range_key)
            });

            if has_pending && completion.logs_complete && completion.eth_calls_complete {
                for (handler_key, pending_list) in &state.pending_events {
                    for pending in pending_list {
                        if (pending.range_start, pending.range_end) == range_key {
                            tracing::warn!(
                                "Stuck pending event detected: handler={} range={}-{} event={}/{} waiting_for={:?}. \
                                 This may indicate missing eth_call configuration or RPC failure.",
                                handler_key,
                                pending.range_start,
                                pending.range_end,
                                pending.source_name,
                                pending.event_name,
                                pending.required_calls
                            );
                        }
                    }
                }
            }

            let ready = !has_pending
                && completion.is_ready(
                    self.expect_log_completion,
                    self.range_requires_eth_call_completion(range_key),
                );
            (ready, timed_out)
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

        self.finalize_range(range_key.0, range_key.1).await
    }

    async fn finalize_range(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> Result<(), TransformationError> {
        let range_key = (range_start, range_end);
        let is_single_block = range_end - range_start == 1;

        // Prevent double finalization by checking and marking atomically
        {
            let mut state = self.live_state.lock().await;
            if state.finalized_ranges.contains(&range_key) {
                tracing::debug!(
                    "Range {}-{} already finalized, skipping duplicate finalization",
                    range_start,
                    range_end
                );
                return Ok(());
            }
            state.finalized_ranges.insert(range_key);
        }

        // For single-block ranges (live mode), check which handlers failed
        // and exclude them from finalization
        let (failed_handlers, completed_handlers) = if is_single_block {
            let storage = LiveStorage::new(&self.chain_name);
            match storage.read_status(range_start) {
                Ok(status) => (status.failed_handlers, status.completed_handlers),
                Err(_) => (HashSet::new(), HashSet::new()),
            }
        } else {
            (HashSet::new(), HashSet::new())
        };

        // Mark handlers as complete only if they didn't fail
        // Handlers that weren't triggered at all (no matching events/calls) are marked complete
        // Handlers that were triggered and succeeded were already marked in the execution loops
        // Handlers that failed are NOT marked complete here
        let mut handlers_marked = 0;
        for handler in self.registry.all_handlers() {
            let handler_key = handler.handler_key();

            // Skip handlers that failed - they need to be retried
            if failed_handlers.contains(&handler_key) {
                tracing::debug!(
                    "Handler {} failed for block {}, not marking complete in finalize_range",
                    handler_key,
                    range_start
                );
                continue;
            }

            // Skip handlers that are already completed (to avoid duplicate DB writes)
            if completed_handlers.contains(&handler_key) {
                continue;
            }

            self.record_completed_range_for_handler(&handler_key, range_start, range_end)
                .await?;
            handlers_marked += 1;

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
            let mut state = self.live_state.lock().await;
            state.calls_buffer.remove(&range_key);
            state.completion.remove(&range_key);
            for ranges in state.received_calls.values_mut() {
                ranges.remove(&range_key);
            }
            // Clean up pending event timestamps for this range
            state
                .pending_event_timestamps
                .retain(|(rs, re, _), _| (*rs, *re) != range_key);
        }

        tracing::debug!(
            "Recorded progress for {} handlers on range {}-{} ({} skipped due to failure)",
            handlers_marked,
            range_start,
            range_end,
            failed_handlers.len()
        );

        // Only set transformed=true if there are no failed handlers (atomic update)
        // Also filter out stale handler keys that are no longer in the registry
        if is_single_block {
            let storage = LiveStorage::new(&self.chain_name);
            let registered_handlers: HashSet<String> = self
                .registry
                .all_handlers()
                .iter()
                .map(|h| h.handler_key())
                .collect();

            let mut marked_complete = false;
            let mut stale_removed = 0;
            let mut remaining_failures = Vec::new();

            if let Err(e) = storage.update_status_atomic(range_start, |status| {
                // Filter out stale handler keys (handlers no longer in registry)
                let original_len = status.failed_handlers.len();
                status
                    .failed_handlers
                    .retain(|k| registered_handlers.contains(k));
                stale_removed = original_len - status.failed_handlers.len();

                // Check if all remaining failures are resolved
                if status.failed_handlers.is_empty() {
                    status.transformed = true;
                    marked_complete = true;
                } else {
                    remaining_failures = status.failed_handlers.iter().cloned().collect();
                }
            }) {
                match e {
                    StorageError::NotFound(_) => {
                        tracing::debug!(
                            "Status file not found for block {}, skipping transformed=true",
                            range_start
                        );
                    }
                    _ => {
                        tracing::warn!("Failed to finalize status for block {}: {}", range_start, e);
                    }
                }
            } else {
                if stale_removed > 0 {
                    tracing::info!(
                        "Block {}: removed {} stale failed handler keys (handlers no longer registered)",
                        range_start,
                        stale_removed
                    );
                }
                if !remaining_failures.is_empty() {
                    tracing::info!(
                        "Block {} has {} failed handlers, not marking transformed=true: {:?}",
                        range_start,
                        remaining_failures.len(),
                        remaining_failures
                    );
                }
            }
        }

        Ok(())
    }

    /// Check if this range requires eth_call completion signal before finalization.
    ///
    /// Only single-block ranges (live mode) require waiting for eth_call completion.
    /// Historical batch ranges don't require this because:
    /// 1. Historical data is processed in larger batches where calls are pre-fetched
    /// 2. The batch processing handles call dependencies internally before finalization
    /// 3. Live mode needs block-by-block coordination since data arrives incrementally
    fn range_requires_eth_call_completion(&self, range_key: (u64, u64)) -> bool {
        self.expect_eth_call_completion && range_key.1.saturating_sub(range_key.0) == 1
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

    async fn process_transform_retry(
        &self,
        request: TransformRetryRequest,
    ) -> Result<(), TransformationError> {
        let block_number = request.block_number;
        let range_key = (block_number, block_number + 1);

        tracing::info!(
            "Processing direct transform retry for block {}",
            block_number
        );

        {
            let mut state = self.live_state.lock().await;
            state.calls_buffer.remove(&range_key);
            state.completion.remove(&range_key);
            state.finalized_ranges.remove(&range_key);
            state
                .pending_event_timestamps
                .retain(|(rs, re, _), _| (*rs, *re) != range_key);
            for ranges in state.received_calls.values_mut() {
                ranges.remove(&range_key);
            }
            for pending in state.pending_events.values_mut() {
                pending.retain(|entry| (entry.range_start, entry.range_end) != range_key);
            }
            state
                .pending_events
                .retain(|_, entries| !entries.is_empty());
        }

        let tracker_missing = if let Some(ref tracker) = self.progress_tracker {
            Some(tracker.lock().await.get_pending_handlers(block_number))
        } else {
            None
        };
        let all_handlers: HashSet<String> = self
            .registry
            .all_handlers()
            .iter()
            .map(|handler| handler.handler_key())
            .collect();
        let missing_handlers =
            resolve_retry_missing_handlers(request.missing_handlers, tracker_missing, all_handlers);

        if missing_handlers.is_empty() {
            return self.finalize_range(block_number, block_number + 1).await;
        }

        let (events, calls) = self.read_live_retry_data(block_number).await?;
        let events = self.filter_events_by_start_block(events);
        let calls = self.filter_calls_by_start_block(calls);

        let blocked_handlers = self
            .execute_live_retry_handlers(block_number, events, calls, &missing_handlers)
            .await?;

        if !blocked_handlers.is_empty() {
            tracing::warn!(
                "Live retry for block {} is still waiting on call dependencies for handlers {:?}",
                block_number,
                blocked_handlers
            );
            return Ok(());
        }

        self.finalize_range(block_number, block_number + 1).await
    }

    async fn read_live_retry_data(
        &self,
        block_number: u64,
    ) -> Result<(Vec<DecodedEvent>, Vec<DecodedCall>), TransformationError> {
        let storage = LiveStorage::new(&self.chain_name);
        let mut events = Vec::new();
        let mut calls = Vec::new();

        let (regular_matchers, factory_matchers) =
            build_event_matchers(&self.contracts, &self.factory_collections).map_err(|e| {
                TransformationError::DecodeError(format!(
                    "failed to build live retry event matchers: {}",
                    e
                ))
            })?;

        let mut event_schemas: HashMap<(String, String), ParsedEvent> = HashMap::new();
        for matcher in regular_matchers {
            event_schemas.insert(
                (matcher.name.clone(), matcher.event_name.clone()),
                matcher.event,
            );
        }
        for matchers in factory_matchers.values() {
            for matcher in matchers {
                event_schemas.insert(
                    (matcher.name.clone(), matcher.event_name.clone()),
                    matcher.event.clone(),
                );
            }
        }

        for (source_name, event_name) in storage.list_decoded_log_types(block_number)? {
            let parsed_event = event_schemas
                .get(&(source_name.clone(), event_name.clone()))
                .ok_or_else(|| {
                    TransformationError::MissingData(format!(
                        "missing event schema for live retry {}/{}",
                        source_name, event_name
                    ))
                })?;

            for log in storage.read_decoded_logs(block_number, &source_name, &event_name)? {
                events.push(Self::live_log_to_decoded_event(
                    &log,
                    parsed_event,
                    &source_name,
                    &event_name,
                )?);
            }
        }

        let (regular_configs, once_configs, event_configs) = build_decode_configs(&self.contracts);
        let regular_map: HashMap<(String, String), CallDecodeConfig> = regular_configs
            .into_iter()
            .map(|config| {
                (
                    (config.contract_name.clone(), config.function_name.clone()),
                    config,
                )
            })
            .collect();
        let once_map: HashMap<(String, String), CallDecodeConfig> = once_configs
            .into_iter()
            .map(|config| {
                (
                    (config.contract_name.clone(), config.function_name.clone()),
                    config,
                )
            })
            .collect();
        let event_map: HashMap<(String, String), EventCallDecodeConfig> = event_configs
            .into_iter()
            .map(|config| {
                (
                    (config.contract_name.clone(), config.function_name.clone()),
                    config,
                )
            })
            .collect();
        let regular_keys: HashSet<(String, String)> = regular_map.keys().cloned().collect();
        let event_keys: HashSet<(String, String)> = event_map.keys().cloned().collect();

        for (source_name, function_name) in storage.list_decoded_call_types(block_number)? {
            match classify_live_retry_call_artifact(
                &source_name,
                &function_name,
                &regular_keys,
                &event_keys,
            )? {
                LiveRetryCallArtifactKind::Regular => {
                    let config = regular_map
                        .get(&(source_name.clone(), function_name.clone()))
                        .ok_or_else(|| {
                            TransformationError::MissingData(format!(
                                "missing regular call schema for live retry {}/{}",
                                source_name, function_name
                            ))
                        })?;

                    for call in
                        storage.read_decoded_calls(block_number, &source_name, &function_name)?
                    {
                        calls.push(Self::live_call_to_decoded_call(
                            &call,
                            &source_name,
                            &function_name,
                            &config.output_type,
                        )?);
                    }
                }
                LiveRetryCallArtifactKind::EventTriggered { base_name } => {
                    let config = event_map
                        .get(&(source_name.clone(), base_name.clone()))
                        .ok_or_else(|| {
                            TransformationError::MissingData(format!(
                                "missing event call schema for live retry {}/{}",
                                source_name, base_name
                            ))
                        })?;

                    for call in
                        storage.read_decoded_event_calls(block_number, &source_name, &base_name)?
                    {
                        calls.push(Self::live_event_call_to_decoded_call(
                            &call,
                            &source_name,
                            &base_name,
                            &config.output_type,
                        )?);
                    }
                }
            }
        }

        let mut once_sources: HashSet<String> = self.contracts.keys().cloned().collect();
        for contract in self.contracts.values() {
            if let Some(factories) = &contract.factories {
                once_sources.extend(factories.iter().map(|factory| factory.collection.clone()));
            }
        }

        for source_name in once_sources {
            let Ok(once_calls) = storage.read_decoded_once_calls(block_number, &source_name) else {
                continue;
            };

            // Consolidate all function results per address into a single DecodedCall
            // with function_name = "once" and merged result map.
            // This mirrors the historical pipeline (src/decoding/eth_calls.rs:632-658)
            // and matches how handlers declare call_dependencies (e.g., ("DERC20", "once")).
            for call in once_calls {
                let mut merged_result = HashMap::new();
                for (function_name, value) in &call.decoded_values {
                    if let Some(config) =
                        once_map.get(&(source_name.clone(), function_name.clone()))
                    {
                        let decoded_value = Self::live_value_to_eth_decoded_value(value)?;
                        let partial_result =
                            build_result_map(&decoded_value, &config.output_type, function_name);
                        merged_result.extend(partial_result);
                    }
                }
                if !merged_result.is_empty() {
                    calls.push(DecodedCall {
                        block_number: call.block_number,
                        block_timestamp: call.block_timestamp,
                        contract_address: call.contract_address,
                        source_name: source_name.clone(),
                        function_name: "once".to_string(),
                        trigger_log_index: None,
                        result: merged_result,
                    });
                }
            }
        }

        Ok((events, calls))
    }

    async fn execute_live_retry_handlers(
        &self,
        block_number: u64,
        events: Vec<DecodedEvent>,
        calls: Vec<DecodedCall>,
        missing_handlers: &HashSet<String>,
    ) -> Result<HashSet<String>, TransformationError> {
        let range_start = block_number;
        let range_end = block_number + 1;
        let tx_addresses = Arc::new(self.read_live_receipt_addresses(block_number)?);
        let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
        let mut join_set: JoinSet<Result<HandlerExecutionOutcome, TransformationError>> =
            JoinSet::new();
        let mut blocked_handlers = HashSet::new();

        for handler_info in self.registry.unique_event_handlers() {
            let handler = handler_info.handler;
            let handler_key = handler.handler_key();
            if !missing_handlers.contains(&handler_key) {
                continue;
            }

            let triggers: HashSet<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|trigger| {
                    (
                        trigger.source.clone(),
                        extract_event_name(&trigger.event_signature),
                    )
                })
                .collect();
            let handler_events: Vec<DecodedEvent> = events
                .iter()
                .filter(|event| {
                    triggers.contains(&(event.source_name.clone(), event.event_name.clone()))
                })
                .cloned()
                .collect();

            if handler_events.is_empty() {
                continue;
            }

            let call_deps: HashSet<(String, String)> =
                handler.call_dependencies().into_iter().collect();
            let missing_deps = missing_retry_call_dependencies(&call_deps, &calls);
            if !missing_deps.is_empty() {
                tracing::warn!(
                    "Skipping live retry for handler {} on block {}: missing call dependencies {:?}",
                    handler_key,
                    block_number,
                    missing_deps
                );
                blocked_handlers.insert(handler_key);
                continue;
            }
            let handler_calls: Vec<DecodedCall> = calls
                .iter()
                .filter(|call| {
                    call_deps.contains(&(call.source_name.clone(), call.function_name.clone()))
                })
                .cloned()
                .collect();

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let db_pool = self.db_pool.clone();
            let chain_name = self.chain_name.clone();
            let chain_id = self.chain_id;
            let historical = self.historical_reader.clone();
            let rpc = self.rpc_client.clone();
            let contracts = self.contracts.clone();
            let tx_addresses = tx_addresses.clone();
            let handler_name = handler.name();
            let handler_version = handler.version();

            join_set.spawn(async move {
                let _permit = permit;
                let live_storage = LiveStorage::new(&chain_name);
                let ctx = TransformationContext::new(
                    chain_name.clone(),
                    chain_id,
                    range_start,
                    range_end,
                    Arc::new(handler_events),
                    Arc::new(handler_calls),
                    (*tx_addresses).clone(),
                    historical,
                    rpc,
                    contracts,
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops =
                                Self::inject_source_version(ops, handler_name, handler_version);
                            if let Err(e) = execute_with_snapshot_capture(
                                ops,
                                &db_pool,
                                Some(&live_storage),
                                range_start,
                                handler_name,
                                handler_version,
                            )
                            .await
                            {
                                return Ok(HandlerExecutionOutcome::DbTransactionFailed {
                                    handler_key,
                                    range_start,
                                    range_end,
                                    error: e.to_string(),
                                });
                            }
                            Ok(HandlerExecutionOutcome::Succeeded {
                                handler_key,
                                range_start,
                                range_end,
                            })
                        } else {
                            Ok(HandlerExecutionOutcome::SucceededEmpty {
                                handler_key,
                                range_start,
                                range_end,
                            })
                        }
                    }
                    Err(e) => Ok(HandlerExecutionOutcome::Failed {
                        handler_key,
                        range_start,
                        range_end,
                        error: e.to_string(),
                    }),
                }
            });
        }

        for handler_info in self.registry.unique_call_handlers() {
            let handler = handler_info.handler;
            let handler_key = handler.handler_key();
            if !missing_handlers.contains(&handler_key) {
                continue;
            }

            let triggers: HashSet<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|trigger| (trigger.source.clone(), trigger.function_name.clone()))
                .collect();
            let handler_calls: Vec<DecodedCall> = calls
                .iter()
                .filter(|call| {
                    triggers.contains(&(call.source_name.clone(), call.function_name.clone()))
                })
                .cloned()
                .collect();

            if handler_calls.is_empty() {
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let db_pool = self.db_pool.clone();
            let chain_name = self.chain_name.clone();
            let chain_id = self.chain_id;
            let historical = self.historical_reader.clone();
            let rpc = self.rpc_client.clone();
            let contracts = self.contracts.clone();
            let handler_name = handler.name();
            let handler_version = handler.version();

            join_set.spawn(async move {
                let _permit = permit;
                let live_storage = LiveStorage::new(&chain_name);
                let ctx = TransformationContext::new(
                    chain_name.clone(),
                    chain_id,
                    range_start,
                    range_end,
                    Arc::new(Vec::new()),
                    Arc::new(handler_calls),
                    HashMap::new(),
                    historical,
                    rpc,
                    contracts,
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops =
                                Self::inject_source_version(ops, handler_name, handler_version);
                            if let Err(e) = execute_with_snapshot_capture(
                                ops,
                                &db_pool,
                                Some(&live_storage),
                                range_start,
                                handler_name,
                                handler_version,
                            )
                            .await
                            {
                                return Ok(HandlerExecutionOutcome::DbTransactionFailed {
                                    handler_key,
                                    range_start,
                                    range_end,
                                    error: e.to_string(),
                                });
                            }
                            Ok(HandlerExecutionOutcome::Succeeded {
                                handler_key,
                                range_start,
                                range_end,
                            })
                        } else {
                            Ok(HandlerExecutionOutcome::SucceededEmpty {
                                handler_key,
                                range_start,
                                range_end,
                            })
                        }
                    }
                    Err(e) => Ok(HandlerExecutionOutcome::Failed {
                        handler_key,
                        range_start,
                        range_end,
                        error: e.to_string(),
                    }),
                }
            });
        }

        // Collect outcomes
        let mut outcomes = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(outcome)) => outcomes.push(outcome),
                Ok(Err(e)) => {
                    tracing::error!(
                        "Handler task returned error during live retry for block {}: {}",
                        block_number,
                        e
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Handler task panicked during live retry for block {}: {}",
                        block_number,
                        e
                    );
                }
            }
        }

        let (successful, failed, _blocked) = aggregate_outcomes(&outcomes);

        // Record progress for successful handlers
        for outcome in &outcomes {
            if outcome.is_success() {
                let handler_key = outcome.handler_key();
                if let Err(e) = self
                    .record_completed_range_for_handler(handler_key, range_start, range_end)
                    .await
                {
                    tracing::warn!(
                        "Failed to record completed range for handler {} on block {}: {}",
                        handler_key,
                        block_number,
                        e
                    );
                }

                if let Some(ref tracker) = self.progress_tracker {
                    let mut tracker = tracker.lock().await;
                    if let Err(e) = tracker.mark_complete(block_number, handler_key).await {
                        tracing::warn!(
                            "Failed to mark retry progress for block {} handler {}: {}",
                            block_number,
                            handler_key,
                            e
                        );
                    }
                }
            }
        }

        // Update status file atomically: move successful handlers from failed to completed,
        // keep failed handlers in failed set
        let storage = LiveStorage::new(&self.chain_name);
        let mut marked_complete = false;
        if let Err(e) = storage.update_status_atomic(block_number, |status| {
            // Move successful handlers from failed_handlers to completed_handlers
            for handler_key in &successful {
                status.failed_handlers.remove(handler_key);
                status.completed_handlers.insert(handler_key.clone());
            }

            // Keep failed handlers in failed_handlers (they'll be retried again)
            for handler_key in &failed {
                status.failed_handlers.insert(handler_key.clone());
                status.completed_handlers.remove(handler_key);
            }

            // Check if all handlers are now complete
            if status.failed_handlers.is_empty() {
                status.transformed = true;
                marked_complete = true;
            }
        }) {
            if !matches!(e, StorageError::NotFound(_)) {
                tracing::warn!(
                    "Failed to update status for block {} after retry: {}",
                    block_number,
                    e
                );
            }
        } else if marked_complete {
            tracing::info!(
                "Block {} transformation complete after retry (all handlers succeeded)",
                block_number
            );
        }

        Ok(blocked_handlers)
    }

    fn read_live_receipt_addresses(
        &self,
        block_number: u64,
    ) -> Result<HashMap<[u8; 32], TransactionAddresses>, TransformationError> {
        let storage = LiveStorage::new(&self.chain_name);
        let mut tx_addresses = HashMap::new();

        for receipt in storage.read_receipts(block_number)? {
            tx_addresses.insert(
                receipt.transaction_hash,
                TransactionAddresses {
                    from_address: receipt.from,
                    to_address: receipt.to,
                },
            );
        }

        Ok(tx_addresses)
    }

    fn live_log_to_decoded_event(
        log: &crate::live::LiveDecodedLog,
        parsed_event: &ParsedEvent,
        source_name: &str,
        event_name: &str,
    ) -> Result<DecodedEvent, TransformationError> {
        let mut params = HashMap::new();
        for (flattened, value) in parsed_event
            .flattened_fields
            .iter()
            .zip(log.decoded_values.iter())
        {
            params.insert(
                flattened.full_name.clone(),
                Self::live_value_to_transform_value(value)?,
            );
        }

        Ok(DecodedEvent {
            block_number: log.block_number,
            block_timestamp: log.block_timestamp,
            transaction_hash: log.transaction_hash,
            log_index: log.log_index,
            contract_address: log.contract_address,
            source_name: source_name.to_string(),
            event_name: event_name.to_string(),
            event_signature: parsed_event.signature.clone(),
            params,
        })
    }

    fn live_call_to_decoded_call(
        call: &crate::live::LiveDecodedCall,
        source_name: &str,
        function_name: &str,
        output_type: &EvmType,
    ) -> Result<DecodedCall, TransformationError> {
        let decoded_value = Self::live_value_to_eth_decoded_value(&call.decoded_value)?;
        Ok(DecodedCall {
            block_number: call.block_number,
            block_timestamp: call.block_timestamp,
            contract_address: call.contract_address,
            source_name: source_name.to_string(),
            function_name: function_name.to_string(),
            trigger_log_index: None,
            result: build_result_map(&decoded_value, output_type, function_name),
        })
    }

    fn live_event_call_to_decoded_call(
        call: &crate::live::LiveDecodedEventCall,
        source_name: &str,
        function_name: &str,
        output_type: &EvmType,
    ) -> Result<DecodedCall, TransformationError> {
        let decoded_value = Self::live_value_to_eth_decoded_value(&call.decoded_value)?;
        Ok(DecodedCall {
            block_number: call.block_number,
            block_timestamp: call.block_timestamp,
            contract_address: call.target_address,
            source_name: source_name.to_string(),
            function_name: function_name.to_string(),
            trigger_log_index: Some(call.log_index),
            result: build_result_map(&decoded_value, output_type, function_name),
        })
    }

    fn live_value_to_transform_value(
        value: &LiveDecodedValue,
    ) -> Result<super::context::DecodedValue, TransformationError> {
        macro_rules! convert_live_value {
            ($value:expr, $Target:path, $recurse:path) => {
                Ok(match $value {
                    LiveDecodedValue::Address(v) => <$Target>::Address(*v),
                    LiveDecodedValue::Uint256(v) => <$Target>::Uint256(
                        v.trim()
                            .parse::<U256>()
                            .map_err(|e| TransformationError::TypeConversion(e.to_string()))?,
                    ),
                    LiveDecodedValue::Int256(v) => <$Target>::Int256(
                        v.parse::<I256>()
                            .map_err(|e| TransformationError::TypeConversion(e.to_string()))?,
                    ),
                    LiveDecodedValue::Uint64(v) => <$Target>::Uint64(*v),
                    LiveDecodedValue::Int64(v) => <$Target>::Int64(*v),
                    LiveDecodedValue::Uint8(v) => <$Target>::Uint8(*v),
                    LiveDecodedValue::Int8(v) => <$Target>::Int8(*v),
                    LiveDecodedValue::Bool(v) => <$Target>::Bool(*v),
                    LiveDecodedValue::Bytes32(v) => <$Target>::Bytes32(*v),
                    LiveDecodedValue::Bytes(v) => <$Target>::Bytes(v.clone()),
                    LiveDecodedValue::String(v) => <$Target>::String(v.clone()),
                    LiveDecodedValue::NamedTuple(fields) => <$Target>::NamedTuple(
                        fields
                            .iter()
                            .map(|(name, val)| Ok((name.clone(), $recurse(val)?)))
                            .collect::<Result<_, TransformationError>>()?,
                    ),
                    LiveDecodedValue::UnnamedTuple(values) => <$Target>::UnnamedTuple(
                        values
                            .iter()
                            .map($recurse)
                            .collect::<Result<_, TransformationError>>()?,
                    ),
                    LiveDecodedValue::Array(values) => <$Target>::Array(
                        values
                            .iter()
                            .map($recurse)
                            .collect::<Result<_, TransformationError>>()?,
                    ),
                })
            };
        }

        convert_live_value!(value, super::context::DecodedValue, Self::live_value_to_transform_value)
    }

    fn live_value_to_eth_decoded_value(
        value: &LiveDecodedValue,
    ) -> Result<EthDecodedValue, TransformationError> {
        macro_rules! convert_live_value {
            ($value:expr, $Target:path, $recurse:path) => {
                Ok(match $value {
                    LiveDecodedValue::Address(v) => <$Target>::Address(*v),
                    LiveDecodedValue::Uint256(v) => <$Target>::Uint256(
                        v.trim()
                            .parse::<U256>()
                            .map_err(|e| TransformationError::TypeConversion(e.to_string()))?,
                    ),
                    LiveDecodedValue::Int256(v) => <$Target>::Int256(
                        v.parse::<I256>()
                            .map_err(|e| TransformationError::TypeConversion(e.to_string()))?,
                    ),
                    LiveDecodedValue::Uint64(v) => <$Target>::Uint64(*v),
                    LiveDecodedValue::Int64(v) => <$Target>::Int64(*v),
                    LiveDecodedValue::Uint8(v) => <$Target>::Uint8(*v),
                    LiveDecodedValue::Int8(v) => <$Target>::Int8(*v),
                    LiveDecodedValue::Bool(v) => <$Target>::Bool(*v),
                    LiveDecodedValue::Bytes32(v) => <$Target>::Bytes32(*v),
                    LiveDecodedValue::Bytes(v) => <$Target>::Bytes(v.clone()),
                    LiveDecodedValue::String(v) => <$Target>::String(v.clone()),
                    LiveDecodedValue::NamedTuple(fields) => <$Target>::NamedTuple(
                        fields
                            .iter()
                            .map(|(name, val)| Ok((name.clone(), $recurse(val)?)))
                            .collect::<Result<_, TransformationError>>()?,
                    ),
                    LiveDecodedValue::UnnamedTuple(values) => <$Target>::UnnamedTuple(
                        values
                            .iter()
                            .map($recurse)
                            .collect::<Result<_, TransformationError>>()?,
                    ),
                    LiveDecodedValue::Array(values) => <$Target>::Array(
                        values
                            .iter()
                            .map($recurse)
                            .collect::<Result<_, TransformationError>>()?,
                    ),
                })
            };
        }

        convert_live_value!(value, EthDecodedValue, Self::live_value_to_eth_decoded_value)
    }

    /// Process a block range with per-handler transactions.
    /// Each handler's operations execute in their own transaction and
    /// progress is recorded independently. Handlers run concurrently
    /// bounded by `handler_concurrency`.
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

        // Get unique triggers before moving data into Arc
        let event_triggers: HashSet<_> = events
            .iter()
            .map(|e| (e.source_name.clone(), e.event_name.clone()))
            .collect();
        let call_triggers: HashSet<_> = calls
            .iter()
            .map(|c| (c.source_name.clone(), c.function_name.clone()))
            .collect();

        // Create shared context for handlers
        let tx_addresses = self.read_receipt_addresses(range_start, range_end);
        let ctx = Arc::new(TransformationContext::new(
            self.chain_name.clone(),
            self.chain_id,
            range_start,
            range_end,
            Arc::new(events),
            Arc::new(calls),
            tx_addresses,
            self.historical_reader.clone(),
            self.rpc_client.clone(),
            self.contracts.clone(),
        ));

        let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
        let mut join_set: JoinSet<Result<HandlerExecutionOutcome, TransformationError>> =
            JoinSet::new();

        // Spawn event handlers concurrently
        // For live mode (single-block ranges), we capture snapshots for rollback
        let is_live_mode = range_end - range_start == 1;
        let chain_name_for_storage = self.chain_name.clone();

        for (source, event_name) in &event_triggers {
            for handler in self.registry.handlers_for_event(source, event_name) {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let ctx = ctx.clone();
                let db_pool = self.db_pool.clone();
                let handler_name = handler.name();
                let handler_version = handler.version();
                let handler_key = handler.handler_key();
                let source = source.clone();
                let event_name = event_name.clone();
                let chain_name = chain_name_for_storage.clone();

                join_set.spawn(async move {
                    let _permit = permit;
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
                                let ops =
                                    Self::inject_source_version(ops, handler_name, handler_version);

                                // Use snapshot-capturing execution for live mode
                                let storage = if is_live_mode {
                                    Some(LiveStorage::new(&chain_name))
                                } else {
                                    None
                                };

                                let result = execute_with_snapshot_capture(
                                    ops,
                                    &db_pool,
                                    storage.as_ref(),
                                    range_start,
                                    handler_name,
                                    handler_version,
                                )
                                .await;

                                if let Err(e) = result {
                                    tracing::error!(
                                        "Handler {} transaction failed for range {}-{}: {:?}",
                                        handler_key,
                                        range_start,
                                        range_end,
                                        e
                                    );
                                    return Ok(HandlerExecutionOutcome::DbTransactionFailed {
                                        handler_key,
                                        range_start,
                                        range_end,
                                        error: e.to_string(),
                                    });
                                }
                                Ok(HandlerExecutionOutcome::Succeeded {
                                    handler_key,
                                    range_start,
                                    range_end,
                                })
                            } else {
                                Ok(HandlerExecutionOutcome::SucceededEmpty {
                                    handler_key,
                                    range_start,
                                    range_end,
                                })
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Handler {} failed for event {}/{}: {}",
                                handler_key,
                                source,
                                event_name,
                                e
                            );
                            Ok(HandlerExecutionOutcome::Failed {
                                handler_key,
                                range_start,
                                range_end,
                                error: e.to_string(),
                            })
                        }
                    }
                });
            }
        }

        // Spawn call handlers concurrently
        for (source, function_name) in &call_triggers {
            for handler in self.registry.handlers_for_call(source, function_name) {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let ctx = ctx.clone();
                let db_pool = self.db_pool.clone();
                let handler_name = handler.name();
                let handler_version = handler.version();
                let handler_key = handler.handler_key();
                let source = source.clone();
                let function_name = function_name.clone();
                let chain_name = chain_name_for_storage.clone();

                join_set.spawn(async move {
                    let _permit = permit;
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
                                let ops =
                                    Self::inject_source_version(ops, handler_name, handler_version);

                                // Use snapshot-capturing execution for live mode
                                let storage = if is_live_mode {
                                    Some(LiveStorage::new(&chain_name))
                                } else {
                                    None
                                };

                                let result = execute_with_snapshot_capture(
                                    ops,
                                    &db_pool,
                                    storage.as_ref(),
                                    range_start,
                                    handler_name,
                                    handler_version,
                                )
                                .await;

                                if let Err(e) = result {
                                    tracing::error!(
                                        "Handler {} transaction failed for range {}-{}: {:?}",
                                        handler_key,
                                        range_start,
                                        range_end,
                                        e
                                    );
                                    return Ok(HandlerExecutionOutcome::DbTransactionFailed {
                                        handler_key,
                                        range_start,
                                        range_end,
                                        error: e.to_string(),
                                    });
                                }
                                Ok(HandlerExecutionOutcome::Succeeded {
                                    handler_key,
                                    range_start,
                                    range_end,
                                })
                            } else {
                                Ok(HandlerExecutionOutcome::SucceededEmpty {
                                    handler_key,
                                    range_start,
                                    range_end,
                                })
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Handler {} failed for call {}/{}: {}",
                                handler_key,
                                source,
                                function_name,
                                e
                            );
                            Ok(HandlerExecutionOutcome::Failed {
                                handler_key,
                                range_start,
                                range_end,
                                error: e.to_string(),
                            })
                        }
                    }
                });
            }
        }

        // Collect outcomes and record progress only for successful handlers
        let mut outcomes = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(outcome)) => outcomes.push(outcome),
                Ok(Err(e)) => {
                    tracing::error!("Handler task returned error: {}", e);
                }
                Err(e) => {
                    tracing::error!("Handler task panicked: {}", e);
                }
            }
        }

        let (_successful, failed, _blocked) = aggregate_outcomes(&outcomes);

        // Record progress only for successful handlers
        for outcome in &outcomes {
            if outcome.is_success() {
                let handler_key = outcome.handler_key();
                let (rs, re) = outcome.range();
                self.record_completed_range_for_handler(handler_key, rs, re)
                    .await?;

                if is_live_mode {
                    if let Some(ref tracker) = self.progress_tracker {
                        let mut t = tracker.lock().await;
                        if let Err(e) = t.mark_complete(rs, handler_key).await {
                            tracing::warn!(
                                "Failed to mark live progress for block {} handler {}: {}",
                                rs,
                                handler_key,
                                e
                            );
                        }
                    }
                }
            }
        }

        // Persist failed handlers to status file for retry (atomic update, live mode only)
        if !failed.is_empty() && is_live_mode {
            let storage = LiveStorage::new(&self.chain_name);
            if let Err(e) = storage.update_status_atomic(range_start, |status| {
                status.failed_handlers.extend(failed.iter().cloned());
                for h in &failed {
                    status.completed_handlers.remove(h);
                }
            }) {
                if !matches!(e, StorageError::NotFound(_)) {
                    tracing::warn!(
                        "Failed to persist failed handlers for block {}: {}",
                        range_start,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    // ─── Start Block Filtering ──────────────────────────────────────────

    /// Filter events by contract start_block.
    /// Events from contracts with a start_block are excluded if the event's
    /// block_number is before that start_block.
    fn filter_events_by_start_block(&self, events: Vec<DecodedEvent>) -> Vec<DecodedEvent> {
        events
            .into_iter()
            .filter(|e| {
                let start_block = self
                    .contracts
                    .get(&e.source_name)
                    .and_then(|c| c.start_block.map(|u| u.to::<u64>()));
                start_block.map_or(true, |sb| e.block_number >= sb)
            })
            .collect()
    }

    /// Filter calls by contract start_block.
    /// Calls from contracts with a start_block are excluded if the call's
    /// block_number is before that start_block.
    fn filter_calls_by_start_block(&self, calls: Vec<DecodedCall>) -> Vec<DecodedCall> {
        calls
            .into_iter()
            .filter(|c| {
                let start_block = self
                    .contracts
                    .get(&c.source_name)
                    .and_then(|ct| ct.start_block.map(|u| u.to::<u64>()));
                start_block.map_or(true, |sb| c.block_number >= sb)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::{
        classify_live_retry_call_artifact, missing_retry_call_dependencies,
        resolve_retry_missing_handlers, LiveRetryCallArtifactKind, RangeCompleteKind,
        RangeCompletionState,
    };
    use crate::transformations::context::{DecodedCall, DecodedValue};

    #[test]
    fn range_completion_requires_both_streams_when_calls_expected() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::Logs);
        assert!(!state.is_ready(true, true));
        state.mark(RangeCompleteKind::EthCalls);
        assert!(state.is_ready(true, true));
    }

    #[test]
    fn range_completion_only_requires_logs_without_calls() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::Logs);
        assert!(state.is_ready(true, false));
    }

    #[test]
    fn range_completion_can_finalize_call_only_ranges() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::EthCalls);
        assert!(state.is_ready(false, true));
    }

    #[test]
    fn retry_missing_handlers_prefers_current_tracker_state() {
        let requested = Some(HashSet::from([
            "handler_a_v1".to_string(),
            "handler_b_v1".to_string(),
        ]));
        let tracker = Some(HashSet::from(["handler_b_v1".to_string()]));
        let all_handlers = HashSet::from([
            "handler_a_v1".to_string(),
            "handler_b_v1".to_string(),
            "handler_c_v1".to_string(),
        ]);

        let resolved = resolve_retry_missing_handlers(requested, tracker, all_handlers);
        assert_eq!(resolved, HashSet::from(["handler_b_v1".to_string()]));
    }

    #[test]
    fn retry_dependency_check_detects_missing_calls() {
        let required = HashSet::from([
            ("Pool".to_string(), "slot0".to_string()),
            ("Pool".to_string(), "liquidity".to_string()),
        ]);
        let calls = vec![DecodedCall {
            block_number: 100,
            block_timestamp: 1200,
            contract_address: [0; 20],
            source_name: "Pool".to_string(),
            function_name: "slot0".to_string(),
            trigger_log_index: None,
            result: HashMap::from([("result".to_string(), DecodedValue::Uint64(1))]),
        }];

        let missing = missing_retry_call_dependencies(&required, &calls);
        assert_eq!(
            missing,
            HashSet::from([("Pool".to_string(), "liquidity".to_string())])
        );
    }

    #[test]
    fn live_retry_call_artifact_prefers_regular_name_over_event_suffix() {
        let regular = HashSet::from([("Pool".to_string(), "foo_event".to_string())]);
        let event = HashSet::from([("Pool".to_string(), "foo".to_string())]);

        let kind =
            classify_live_retry_call_artifact("Pool", "foo_event", &regular, &event).unwrap();

        assert_eq!(kind, LiveRetryCallArtifactKind::Regular);
    }

    #[test]
    fn live_retry_call_artifact_still_recognizes_event_triggered_suffix() {
        let regular = HashSet::new();
        let event = HashSet::from([("Pool".to_string(), "foo".to_string())]);

        let kind =
            classify_live_retry_call_artifact("Pool", "foo_event", &regular, &event).unwrap();

        assert_eq!(
            kind,
            LiveRetryCallArtifactKind::EventTriggered {
                base_name: "foo".to_string()
            }
        );
    }

    // ─── Handler Execution Outcome Tests ─────────────────────────────────

    use super::{aggregate_outcomes, HandlerExecutionOutcome};

    #[test]
    fn outcome_is_success_returns_true_for_succeeded() {
        let outcome = HandlerExecutionOutcome::Succeeded {
            handler_key: "test_v1".to_string(),
            range_start: 100,
            range_end: 101,
        };
        assert!(outcome.is_success());
        assert!(!outcome.is_failure());
        assert!(!outcome.is_blocked());
    }

    #[test]
    fn outcome_is_success_returns_true_for_succeeded_empty() {
        let outcome = HandlerExecutionOutcome::SucceededEmpty {
            handler_key: "test_v1".to_string(),
            range_start: 100,
            range_end: 101,
        };
        assert!(outcome.is_success());
        assert!(!outcome.is_failure());
    }

    #[test]
    fn outcome_is_failure_returns_true_for_failed() {
        let outcome = HandlerExecutionOutcome::Failed {
            handler_key: "test_v1".to_string(),
            range_start: 100,
            range_end: 101,
            error: "handler error".to_string(),
        };
        assert!(outcome.is_failure());
        assert!(!outcome.is_success());
    }

    #[test]
    fn outcome_is_failure_returns_true_for_db_transaction_failed() {
        let outcome = HandlerExecutionOutcome::DbTransactionFailed {
            handler_key: "test_v1".to_string(),
            range_start: 100,
            range_end: 101,
            error: "db error".to_string(),
        };
        assert!(outcome.is_failure());
        assert!(!outcome.is_success());
    }

    #[test]
    fn outcome_is_blocked_returns_true_for_blocked() {
        let outcome = HandlerExecutionOutcome::Blocked {
            handler_key: "test_v1".to_string(),
            range_start: 100,
            range_end: 101,
            missing_dependencies: vec![("Pool".to_string(), "slot0".to_string())],
        };
        assert!(outcome.is_blocked());
        assert!(!outcome.is_success());
        assert!(!outcome.is_failure());
    }

    #[test]
    fn outcome_handler_key_returns_correct_key() {
        let outcome = HandlerExecutionOutcome::Succeeded {
            handler_key: "my_handler_v2".to_string(),
            range_start: 100,
            range_end: 101,
        };
        assert_eq!(outcome.handler_key(), "my_handler_v2");
    }

    #[test]
    fn outcome_range_returns_correct_range() {
        let outcome = HandlerExecutionOutcome::Failed {
            handler_key: "test_v1".to_string(),
            range_start: 500,
            range_end: 600,
            error: "error".to_string(),
        };
        assert_eq!(outcome.range(), (500, 600));
    }

    #[test]
    fn aggregate_outcomes_separates_success_failure_blocked() {
        let outcomes = vec![
            HandlerExecutionOutcome::Succeeded {
                handler_key: "handler_a_v1".to_string(),
                range_start: 100,
                range_end: 101,
            },
            HandlerExecutionOutcome::SucceededEmpty {
                handler_key: "handler_b_v1".to_string(),
                range_start: 100,
                range_end: 101,
            },
            HandlerExecutionOutcome::Failed {
                handler_key: "handler_c_v1".to_string(),
                range_start: 100,
                range_end: 101,
                error: "error".to_string(),
            },
            HandlerExecutionOutcome::DbTransactionFailed {
                handler_key: "handler_d_v1".to_string(),
                range_start: 100,
                range_end: 101,
                error: "db error".to_string(),
            },
            HandlerExecutionOutcome::Blocked {
                handler_key: "handler_e_v1".to_string(),
                range_start: 100,
                range_end: 101,
                missing_dependencies: vec![],
            },
        ];

        let (successful, failed, blocked) = aggregate_outcomes(&outcomes);

        assert_eq!(
            successful,
            HashSet::from(["handler_a_v1".to_string(), "handler_b_v1".to_string()])
        );
        assert_eq!(
            failed,
            HashSet::from(["handler_c_v1".to_string(), "handler_d_v1".to_string()])
        );
        assert_eq!(blocked, HashSet::from(["handler_e_v1".to_string()]));
    }

    #[test]
    fn aggregate_outcomes_handles_empty_list() {
        let outcomes: Vec<HandlerExecutionOutcome> = vec![];
        let (successful, failed, blocked) = aggregate_outcomes(&outcomes);

        assert!(successful.is_empty());
        assert!(failed.is_empty());
        assert!(blocked.is_empty());
    }

    #[test]
    fn aggregate_outcomes_all_successful() {
        let outcomes = vec![
            HandlerExecutionOutcome::Succeeded {
                handler_key: "a_v1".to_string(),
                range_start: 100,
                range_end: 101,
            },
            HandlerExecutionOutcome::Succeeded {
                handler_key: "b_v1".to_string(),
                range_start: 100,
                range_end: 101,
            },
        ];

        let (successful, failed, blocked) = aggregate_outcomes(&outcomes);

        assert_eq!(successful.len(), 2);
        assert!(failed.is_empty());
        assert!(blocked.is_empty());
    }

    #[test]
    fn aggregate_outcomes_all_failed() {
        let outcomes = vec![
            HandlerExecutionOutcome::Failed {
                handler_key: "a_v1".to_string(),
                range_start: 100,
                range_end: 101,
                error: "error".to_string(),
            },
            HandlerExecutionOutcome::DbTransactionFailed {
                handler_key: "b_v1".to_string(),
                range_start: 100,
                range_end: 101,
                error: "db error".to_string(),
            },
        ];

        let (successful, failed, blocked) = aggregate_outcomes(&outcomes);

        assert!(successful.is_empty());
        assert_eq!(failed.len(), 2);
        assert!(blocked.is_empty());
    }
}

/// Execute a transaction with optional snapshot capture for reorg rollback.
///
/// For live mode (single-block ranges), this function:
/// 1. Queries current row state for upserts with update_columns
/// 2. Writes snapshots to storage (before transaction, for crash safety)
/// 3. Executes the transaction
///
/// Writing snapshots before the transaction ensures that if a crash occurs after
/// the transaction commits, the snapshot is already persisted. Orphan snapshots
/// from failed transactions are harmless and cleaned up during compaction.
async fn execute_with_snapshot_capture(
    ops: Vec<DbOperation>,
    db_pool: &DbPool,
    storage: Option<&LiveStorage>,
    block_number: u64,
    handler_source: &str,
    handler_version: u32,
) -> Result<(), crate::db::DbError> {
    // If no storage provided, just execute directly (historical mode)
    let storage = match storage {
        Some(s) => s,
        None => return db_pool.execute_transaction(ops).await,
    };

    // Collect snapshots for upserts with update_columns (these modify existing rows)
    let mut snapshots = Vec::new();

    for op in &ops {
        if let DbOperation::Upsert {
            table,
            columns,
            values,
            conflict_columns,
            update_columns,
        } = op
        {
            // Only capture snapshots for upserts that update existing rows
            if update_columns.is_empty() {
                continue;
            }

            // Build key columns from conflict_columns
            let mut key_columns: Vec<(String, DbValue)> = Vec::new();
            for conflict_col in conflict_columns {
                // Find the value for this conflict column
                if let Some(idx) = columns.iter().position(|c| c == conflict_col) {
                    key_columns.push((conflict_col.clone(), values[idx].clone()));
                }
            }

            // Query current row state
            let previous_row = db_pool.query_row(table, &key_columns).await?;

            // Convert to LiveDbValue
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
                table: table.clone(),
                source: handler_source.to_string(),
                source_version: handler_version,
                key_columns: live_key_columns,
                previous_row: live_previous_row,
            });
        }
    }

    // Write snapshots to storage BEFORE transaction (for crash safety)
    // If we crash after transaction but before snapshot write, we'd lose rollback ability.
    // Orphan snapshots from failed transactions are cleaned up during compaction.
    if !snapshots.is_empty() {
        // Read existing snapshots and append new ones
        let mut all_snapshots = storage.read_snapshots(block_number).unwrap_or_default();
        all_snapshots.extend(snapshots);

        if let Err(e) = storage.write_snapshots(block_number, &all_snapshots) {
            tracing::warn!(
                "Failed to write upsert snapshots for block {}: {}",
                block_number,
                e
            );
            // Continue with transaction even if snapshot write fails - fallback DELETE still works
        }
    }

    // Execute the transaction
    db_pool.execute_transaction(ops).await
}
