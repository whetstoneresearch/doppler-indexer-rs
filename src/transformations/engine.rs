//! Transformation engine that orchestrates handler execution.
//!
//! The engine receives decoded events and calls, invokes registered handlers,
//! and writes results to PostgreSQL. It tracks progress per handler and performs
//! per-handler catchup from decoded parquet files on startup.
//!
//! Sub-components handle focused responsibilities:
//! - [`executor`](super::executor): Handler spawn-loop, source/version injection, snapshot capture
//! - [`finalizer`](super::finalizer): Range finalization, reorg cleanup, progress tracking
//! - [`retry`](super::retry): Live retry processing from bincode storage
//! - [`live_state`](super::live_state): Pending event buffering and completion tracking

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use super::context::{DecodedCall, DecodedEvent, TransactionAddresses, TransformationContext};
use super::error::TransformationError;
use super::executor::{inject_source_version, DbExecMode, HandlerExecutor, HandlerTask};
use super::finalizer::RangeFinalizer;
use super::historical::HistoricalDataReader;
use super::live_state::{LiveProcessingState, PendingEventData};
use super::registry::{extract_event_name, TransformationRegistry};
use super::retry::{filter_calls_by_start_block, filter_events_by_start_block, RetryProcessor};
use crate::db::DbPool;
use crate::live::{LiveProgressTracker, LiveStorage, StorageError, TransformRetryRequest};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::contract::{Contracts, FactoryCollections};

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
#[derive(Default)]
pub enum ExecutionMode {
    /// Process data as it arrives (for live/real-time data).
    #[default]
    Streaming,
    /// Process in larger batches (for historical catchup).
    Batch { batch_size: usize },
}

/// Configuration for creating a TransformationEngine.
///
/// Groups the chain-specific and behavior-related parameters that configure
/// the engine, reducing the argument count of `TransformationEngine::new`.
pub struct TransformationEngineConfig {
    pub chain_name: String,
    pub chain_id: u64,
    pub mode: ExecutionMode,
    pub contracts: Contracts,
    pub factory_collections: FactoryCollections,
    pub handler_concurrency: usize,
    pub expect_log_completion: bool,
    pub expect_eth_call_completion: bool,
}

/// Result type for individual handler task execution during catchup.
type HandlerTaskResult = Result<Option<(String, u64, u64)>, TransformationError>;

/// A handler paired with the decoded calls it needs to process.
type ReadyHandler = (
    Arc<dyn super::traits::TransformationHandler>,
    Arc<Vec<DecodedCall>>,
);

/// Discriminates between event-based and call-based handler processing.
///
/// Used by the unified catchup and retry logic to handle the three divergent
/// points: trigger extraction, call dependency checking, and context construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerKind {
    Event,
    Call,
}

/// Uniform representation of a handler for catchup processing.
///
/// Bridges `EventHandlerInfo` and `CallHandlerInfo` so the catchup loop
/// can iterate a single collection regardless of handler kind.
struct CatchupHandler {
    handler: Arc<dyn super::traits::TransformationHandler>,
    /// (source, name) pairs — event names for Event, function names for Call.
    triggers: Vec<(String, String)>,
    /// Call dependencies (Event handlers only; empty for Call handlers).
    call_deps: Vec<(String, String)>,
    kind: HandlerKind,
}

/// The transformation engine processes decoded data and invokes handlers.
///
/// Progress is tracked per handler (keyed by `handler_key()`) in the
/// `_handler_progress` table, enabling independent catchup and versioning.
pub struct TransformationEngine {
    registry: Arc<TransformationRegistry>,
    db_pool: Arc<DbPool>,
    historical_reader: Arc<HistoricalDataReader>,
    chain_name: String,
    chain_id: u64,
    mode: ExecutionMode,
    decoded_logs_dir: PathBuf,
    decoded_calls_dir: PathBuf,
    raw_receipts_dir: PathBuf,
    contracts: Arc<Contracts>,
    handler_concurrency: usize,
    // Sub-components
    executor: Arc<HandlerExecutor>,
    finalizer: Arc<RangeFinalizer>,
    retry_processor: RetryProcessor,
    live_state: Mutex<LiveProcessingState>,
    /// Live mode progress tracker for marking block completion.
    progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
}

impl TransformationEngine {
    /// Create a new transformation engine.
    pub async fn new(
        registry: Arc<TransformationRegistry>,
        db_pool: Arc<DbPool>,
        rpc_client: Arc<UnifiedRpcClient>,
        config: TransformationEngineConfig,
        progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
    ) -> Result<Self, TransformationError> {
        let chain_name = config.chain_name;
        let chain_id = config.chain_id;
        let mode = config.mode;
        let handler_concurrency = config.handler_concurrency;

        let historical_reader = Arc::new(HistoricalDataReader::new(&chain_name)?);
        let decoded_logs_dir = crate::storage::paths::decoded_logs_dir(&chain_name);
        let decoded_calls_dir = crate::storage::paths::decoded_eth_calls_dir(&chain_name);
        let raw_receipts_dir = crate::storage::paths::raw_receipts_dir(&chain_name);

        let contracts = Arc::new(config.contracts);
        let factory_collections = Arc::new(config.factory_collections);

        let executor = Arc::new(HandlerExecutor {
            db_pool: db_pool.clone(),
            historical_reader: historical_reader.clone(),
            rpc_client: rpc_client.clone(),
            contracts: contracts.clone(),
            chain_name: chain_name.clone(),
            chain_id,
            handler_concurrency,
        });

        let finalizer = Arc::new(RangeFinalizer {
            registry: registry.clone(),
            db_pool: db_pool.clone(),
            chain_name: chain_name.clone(),
            chain_id,
            progress_tracker: progress_tracker.clone(),
            expect_log_completion: config.expect_log_completion,
            expect_eth_call_completion: config.expect_eth_call_completion,
        });

        let retry_processor = RetryProcessor {
            registry: registry.clone(),
            db_pool: db_pool.clone(),
            rpc_client: rpc_client.clone(),
            historical_reader: historical_reader.clone(),
            contracts: contracts.clone(),
            factory_collections: factory_collections.clone(),
            chain_name: chain_name.clone(),
            chain_id,
            handler_concurrency,
            progress_tracker: progress_tracker.clone(),
        };

        Ok(Self {
            registry,
            db_pool,
            historical_reader,
            chain_name,
            chain_id,
            mode,
            decoded_logs_dir,
            decoded_calls_dir,
            raw_receipts_dir,
            contracts,
            handler_concurrency,
            executor,
            finalizer,
            retry_processor,
            live_state: Mutex::new(LiveProcessingState::default()),
            progress_tracker,
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
    async fn run_handler_migrations(&self) -> Result<(), TransformationError> {
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

        let pool = self.db_pool.inner();
        let applied: HashSet<String> = {
            let client = pool.get().await?;
            let rows = client.query("SELECT name FROM _migrations", &[]).await?;
            rows.iter().map(|r| r.get(0)).collect()
        };

        let mut sql_entries: Vec<(PathBuf, String)> = Vec::new();

        for path in &migration_paths {
            if !path.exists() {
                tracing::warn!("Handler migration path does not exist: {}", path.display());
                continue;
            }

            if path.is_file() {
                let migration_name = format!("{}", path.display());
                sql_entries.push((path.clone(), migration_name));
            } else if path.is_dir() {
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

    // ─── Range Scanning ──────────────────────────────────────────────

    fn scan_available_ranges(
        &self,
        base_dir: &Path,
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
    pub async fn run_catchup(&self) -> Result<(), TransformationError> {
        self.run_handler_catchup(HandlerKind::Event).await?;
        self.run_handler_catchup(HandlerKind::Call).await?;
        Ok(())
    }

    /// Spawn a single catchup handler task into the provided JoinSet.
    ///
    /// Acquires a semaphore permit, clones all shared state, and spawns an
    /// async task that invokes the handler and executes the resulting database
    /// operations.
    #[allow(clippy::too_many_arguments)]
    async fn spawn_catchup_handler_task<H>(
        &self,
        join_set: &mut JoinSet<HandlerTaskResult>,
        semaphore: &Arc<Semaphore>,
        handler: &Arc<H>,
        handler_key: &str,
        range_start: u64,
        range_end: u64,
        events: Vec<DecodedEvent>,
        calls: Vec<DecodedCall>,
        tx_addresses: HashMap<[u8; 32], TransactionAddresses>,
    ) where
        H: super::traits::TransformationHandler + ?Sized,
    {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let handler = handler.clone();
        let db_pool = self.db_pool.clone();
        let handler_key = handler_key.to_string();
        let handler_name = handler.name();
        let handler_version = handler.version();
        let chain_name = self.chain_name.clone();
        let chain_id = self.chain_id;
        let historical = self.historical_reader.clone();
        let rpc = self.executor.rpc_client.clone();
        let contracts = self.contracts.clone();

        join_set.spawn(async move {
            let _permit = permit;
            let ctx = TransformationContext::new(
                chain_name,
                chain_id,
                range_start,
                range_end,
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
                        let ops = inject_source_version(ops, handler_name, handler_version);
                        db_pool.execute_transaction(ops).await?;
                    }
                    Ok(Some((handler_key, range_start, range_end)))
                }
                Err(e) => Err(e),
            }
        });
    }

    /// Drain completed tasks from a catchup JoinSet, recording completed
    /// ranges and collecting failures.
    ///
    /// Returns `true` if at least one handler errored or panicked.
    async fn drain_catchup_results(
        &self,
        join_set: &mut JoinSet<HandlerTaskResult>,
        handler_key: &str,
        failed_handlers: &mut Vec<(String, String)>,
    ) -> Result<bool, TransformationError> {
        let mut errored = false;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(Some((hk, rs, re)))) => {
                    self.finalizer
                        .record_completed_range_for_handler(&hk, rs, re)
                        .await?;
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    tracing::error!(
                        "Handler {} failed during catchup: {}. Stopping catchup for this handler.",
                        handler_key,
                        e
                    );
                    failed_handlers.push((handler_key.to_string(), e.to_string()));
                    errored = true;
                    break;
                }
                Err(e) => {
                    tracing::error!("Handler {} catchup task panicked: {}", handler_key, e);
                    failed_handlers
                        .push((handler_key.to_string(), format!("task panicked: {}", e)));
                    errored = true;
                    break;
                }
            }
        }
        Ok(errored)
    }

    /// Format and return a catchup failure error from accumulated handler failures.
    fn catchup_failure_error(
        failed_handlers: &[(String, String)],
    ) -> Result<(), TransformationError> {
        if failed_handlers.is_empty() {
            return Ok(());
        }
        let msg = failed_handlers
            .iter()
            .map(|(k, e)| format!("{}: {}", k, e))
            .collect::<Vec<_>>()
            .join("; ");
        Err(TransformationError::HandlerError {
            handler_name: "catchup".to_string(),
            message: format!("{} handler(s) failed: {}", failed_handlers.len(), msg),
        })
    }

    async fn run_handler_catchup(&self, kind: HandlerKind) -> Result<(), TransformationError> {
        let base_dir = match kind {
            HandlerKind::Event => &self.decoded_logs_dir,
            HandlerKind::Call => &self.decoded_calls_dir,
        };
        let kind_label = match kind {
            HandlerKind::Event => "Event",
            HandlerKind::Call => "Call",
        };

        let available = self.scan_available_ranges(base_dir)?;

        if available.is_empty() {
            tracing::info!(
                "{} handler catchup: no parquet ranges found for chain {}",
                kind_label,
                self.chain_name
            );
            return Ok(());
        }

        // Collect handler descriptors uniformly across both kinds.
        let mut handlers: Vec<CatchupHandler> = match kind {
            HandlerKind::Event => self
                .registry
                .unique_event_handlers()
                .into_iter()
                .map(|info| {
                    let triggers: Vec<(String, String)> = info
                        .triggers
                        .iter()
                        .map(|t| (t.source.clone(), extract_event_name(&t.event_signature)))
                        .collect();
                    let call_deps = info.handler.call_dependencies();
                    CatchupHandler {
                        handler: info.handler as Arc<dyn super::traits::TransformationHandler>,
                        triggers,
                        call_deps,
                        kind: HandlerKind::Event,
                    }
                })
                .collect(),
            HandlerKind::Call => self
                .registry
                .unique_call_handlers()
                .into_iter()
                .map(|info| {
                    let triggers: Vec<(String, String)> = info
                        .triggers
                        .iter()
                        .map(|t| (t.source.clone(), t.function_name.clone()))
                        .collect();
                    CatchupHandler {
                        handler: info.handler as Arc<dyn super::traits::TransformationHandler>,
                        triggers,
                        call_deps: Vec::new(),
                        kind: HandlerKind::Call,
                    }
                })
                .collect(),
        };

        // Sort event handlers by topological order so dependencies are processed first
        if kind == HandlerKind::Event {
            let topo_order = self.registry.handler_topological_order();
            let position: HashMap<&str, usize> = topo_order
                .iter()
                .enumerate()
                .map(|(i, name)| (name.as_str(), i))
                .collect();
            handlers.sort_by_key(|ch| {
                position.get(ch.handler.name()).copied().unwrap_or(usize::MAX)
            });
        }

        let mut failed_handlers: Vec<(String, String)> = vec![];

        for ch in &handlers {
            let handler = &ch.handler;
            let handler_key = handler.handler_key();
            let triggers = &ch.triggers;
            let call_deps = &ch.call_deps;

            let completed = self
                .finalizer
                .get_completed_ranges_for_handler(&handler_key)
                .await?;

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
                "Handler {} catchup: processing {} ranges (triggers: {:?}{})",
                handler_key,
                to_process.len(),
                triggers,
                if call_deps.is_empty() {
                    String::new()
                } else {
                    format!(", call_deps: {:?}", call_deps)
                }
            );

            let mut ranges_to_attempt = to_process;
            let mut pass = 0u32;
            let mut total_processed = 0usize;
            let mut handler_errored = false;

            loop {
                pass += 1;
                if ranges_to_attempt.is_empty() {
                    break;
                }

                if pass > 1 {
                    tracing::info!(
                        "Handler {} catchup pass {}: retrying {} ranges waiting for call dependencies",
                        handler_key,
                        pass,
                        ranges_to_attempt.len()
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                // Re-scan available call ranges from filesystem each pass (event handlers only)
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

                let total = ranges_to_attempt.len();
                let mut processed = 0;
                let mut skipped_ranges: Vec<(u64, u64)> = Vec::new();

                let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
                let mut join_set: JoinSet<HandlerTaskResult> = JoinSet::new();

                for (range_start, range_end) in &ranges_to_attempt {
                    // Read primary data: events for event handlers, calls for call handlers
                    let (events, primary_data_empty) = match ch.kind {
                        HandlerKind::Event => {
                            let evts = self
                                .read_decoded_events_for_triggers(
                                    *range_start,
                                    *range_end,
                                    triggers,
                                )
                                .await?;
                            let evts = filter_events_by_start_block(&self.contracts, evts);
                            let empty = evts.is_empty();
                            (evts, empty)
                        }
                        HandlerKind::Call => (Vec::new(), false),
                    };

                    // For event handlers: check call dependency readiness and load calls
                    if !primary_data_empty && !call_deps.is_empty() {
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
                            skipped_ranges.push((*range_start, *range_end));
                            continue;
                        }
                    }

                    let calls = match ch.kind {
                        HandlerKind::Event => {
                            if !events.is_empty() && !call_deps.is_empty() {
                                let calls = self
                                    .read_decoded_calls_for_triggers(
                                        *range_start,
                                        *range_end,
                                        call_deps,
                                    )
                                    .await?;
                                filter_calls_by_start_block(&self.contracts, calls)
                            } else {
                                Vec::new()
                            }
                        }
                        HandlerKind::Call => {
                            let calls = self
                                .read_decoded_calls_for_triggers(*range_start, *range_end, triggers)
                                .await?;
                            filter_calls_by_start_block(&self.contracts, calls)
                        }
                    };

                    processed += 1;

                    // Determine whether there is data to process
                    let has_data = match ch.kind {
                        HandlerKind::Event => !events.is_empty(),
                        HandlerKind::Call => !calls.is_empty(),
                    };

                    if has_data {
                        // Event handlers get tx_addresses; call handlers pass empty map
                        let tx_addresses = match ch.kind {
                            HandlerKind::Event => {
                                self.read_receipt_addresses(*range_start, *range_end)
                            }
                            HandlerKind::Call => HashMap::new(),
                        };
                        self.spawn_catchup_handler_task(
                            &mut join_set,
                            &semaphore,
                            handler,
                            &handler_key,
                            *range_start,
                            *range_end,
                            events,
                            calls,
                            tx_addresses,
                        )
                        .await;
                    } else {
                        self.finalizer
                            .record_completed_range_for_handler(
                                &handler_key,
                                *range_start,
                                *range_end,
                            )
                            .await?;
                    }

                    if processed % 50 == 0 {
                        tracing::info!(
                            "Handler {} catchup pass {} progress: {}/{} (skipped {} waiting for calls)",
                            handler_key,
                            pass,
                            processed,
                            total,
                            skipped_ranges.len()
                        );
                    }
                }

                let handler_errored_this_pass = self
                    .drain_catchup_results(&mut join_set, &handler_key, &mut failed_handlers)
                    .await?;

                total_processed += processed;

                if !handler_errored_this_pass {
                    if skipped_ranges.is_empty() {
                        tracing::info!(
                            "Handler {} catchup complete in {} pass(es): processed {} ranges",
                            handler_key,
                            pass,
                            total_processed
                        );
                        break;
                    }

                    if skipped_ranges.len() == ranges_to_attempt.len() {
                        tracing::warn!(
                            "Handler {} catchup: no progress on {} ranges still waiting for call dependencies after {} pass(es). Giving up.",
                            handler_key,
                            skipped_ranges.len(),
                            pass
                        );
                        break;
                    }

                    // Progress was made, retry the skipped ranges
                    ranges_to_attempt = skipped_ranges;
                } else {
                    // Handler errored, skip retry-skipped-ranges and move to next handler
                    handler_errored = true;
                    break;
                }
            }

            if handler_errored {
                continue;
            }
        }

        Self::catchup_failure_error(&failed_handlers)
    }

    // ─── Receipt Address Reading ────────────────────────────────────

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

            let tx_hash_col = match batch.column_by_name("transaction_hash") {
                Some(col) => match col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    Some(arr) => arr.clone(),
                    None => continue,
                },
                None => continue,
            };

            let from_col = match batch.column_by_name("from_address") {
                Some(col) => match col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    Some(arr) => arr.clone(),
                    None => continue,
                },
                None => continue,
            };

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

    /// Read decoded parquet files for a set of triggers, spawning one blocking
    /// read task per trigger.
    ///
    /// `resolve_paths` turns each `(source, name)` trigger into zero or more
    /// file paths to read.  `read_file` performs the actual parquet read.
    async fn read_decoded_parquet_for_triggers<T>(
        &self,
        triggers: &[(String, String)],
        resolve_paths: impl Fn(&str, &str) -> Vec<PathBuf>,
        read_file: impl Fn(Arc<HistoricalDataReader>, &Path, &str, &str) -> Result<Vec<T>, TransformationError>
            + Send
            + Sync
            + 'static,
    ) -> Result<Vec<T>, TransformationError>
    where
        T: Send + 'static,
    {
        let mut read_tasks = JoinSet::new();
        let read_fn = Arc::new(read_file);

        for (source_name, secondary_name) in triggers {
            let paths = resolve_paths(source_name, secondary_name);

            for file_path in paths {
                if !file_path.exists() {
                    continue;
                }

                let reader = self.historical_reader.clone();
                let src = source_name.clone();
                let name = secondary_name.clone();
                let read_fn = read_fn.clone();

                read_tasks.spawn_blocking(move || {
                    tracing::debug!("Reading decoded data from {}", file_path.display());
                    match read_fn(reader, &file_path, &src, &name) {
                        Ok(items) => {
                            tracing::debug!(
                                "Read {} items from {}",
                                items.len(),
                                file_path.display()
                            );
                            Ok(items)
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to read decoded data from {}: {}",
                                file_path.display(),
                                e
                            );
                            Ok(Vec::new())
                        }
                    }
                });
            }
        }

        let mut all_items = Vec::new();
        while let Some(result) = read_tasks.join_next().await {
            match result {
                Ok(Ok(items)) => all_items.extend(items),
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(TransformationError::IoError(std::io::Error::other(
                        e.to_string(),
                    )))
                }
            }
        }

        Ok(all_items)
    }

    async fn read_decoded_events_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        event_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let logs_dir = self.decoded_logs_dir.clone();

        self.read_decoded_parquet_for_triggers(
            event_triggers,
            |source, event| vec![logs_dir.join(source).join(event).join(&file_name)],
            |reader, path, src, evt| reader.read_events_from_file(path, src, evt),
        )
        .await
    }

    async fn read_decoded_calls_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        call_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let calls_dir = self.decoded_calls_dir.clone();

        self.read_decoded_parquet_for_triggers(
            call_triggers,
            |source, function| {
                let base_dir = calls_dir.join(source).join(function);
                // Use first-match semantics: only the first existing path is returned,
                // matching the original behavior where base > on_events > once priority
                // prevents duplicate data when multiple paths exist.
                [
                    base_dir.join(&file_name),
                    base_dir.join("on_events").join(&file_name),
                    base_dir.join("once").join(&file_name),
                ]
                .into_iter()
                .find(|p| p.exists())
                .into_iter()
                .collect()
            },
            |reader, path, src, func| reader.read_calls_from_file(path, src, func),
        )
        .await
    }

    // ─── Live Processing ─────────────────────────────────────────────

    /// Run the transformation engine, processing messages from channels.
    pub async fn run(
        &self,
        mut events_rx: Receiver<DecodedEventsMessage>,
        mut calls_rx: Receiver<DecodedCallsMessage>,
        mut complete_rx: Receiver<RangeCompleteMessage>,
        mut reorg_rx: Option<Receiver<ReorgMessage>>,
        mut retry_rx: Option<Receiver<TransformRetryRequest>>,
        decode_catchup_done_rx: Option<oneshot::Receiver<()>>,
    ) -> Result<(), TransformationError> {
        if let Some(rx) = decode_catchup_done_rx {
            tracing::info!(
                "Waiting for eth_call decode catchup to complete before transformation catchup..."
            );
            let _ = rx.await;
            tracing::info!(
                "Eth_call decode catchup complete, proceeding with transformation catchup"
            );
        }

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
                    self.process_events_message(msg).await?;
                }

                Some(msg) = calls_rx.recv() => {
                    if msg.calls.is_empty() {
                        continue;
                    }
                    self.process_calls_message(msg).await?;
                }

                Some(msg) = complete_rx.recv() => {
                    let range_key = (msg.range_start, msg.range_end);

                    // When all log events have been sent, mark untriggered dependency
                    // handlers as completed-no-op so dependent handlers can proceed.
                    if msg.kind == RangeCompleteKind::Logs {
                        let dep_names = self.registry.dependency_handler_names();
                        if !dep_names.is_empty() {
                            let mut state = self.live_state.lock().await;
                            let completed = state.completed_handlers
                                .entry(range_key)
                                .or_default();
                            for name in dep_names {
                                if !completed.contains(name) {
                                    tracing::debug!(
                                        "Marking handler {} as completed-no-op for block {} (not triggered)",
                                        name, range_key.0
                                    );
                                    completed.insert(name.clone());
                                }
                            }
                        }
                        self.try_process_pending_events(range_key).await?;
                    }

                    self.finalizer
                        .process_range_complete(range_key, msg.kind, &self.live_state)
                        .await?;
                }

                Some(msg) = async {
                    match reorg_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.finalizer
                        .process_reorg(msg.common_ancestor, &msg.orphaned, &self.live_state)
                        .await?;
                }

                Some(msg) = async {
                    match retry_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.retry_processor
                        .process_transform_retry(msg, &self.live_state, self)
                        .await?;
                }

                else => {
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

        let filtered_events = filter_events_by_start_block(&self.contracts, msg.events.clone());
        let events = Arc::new(filtered_events.clone());

        // Categorize handlers: ready to run vs needs buffering
        let range_key = (msg.range_start, msg.range_end);
        let mut ready_handlers: Vec<ReadyHandler> = Vec::new();
        {
            let mut state = self.live_state.lock().await;
            for handler in &handlers {
                let call_deps = handler.call_dependencies();
                let handler_deps: Vec<String> = handler
                    .handler_dependencies()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                let handler_key = handler.handler_key();

                let call_deps_ready = call_deps.is_empty()
                    || call_deps.iter().all(|dep| {
                        state
                            .received_calls
                            .get(dep)
                            .map(|ranges| ranges.contains(&range_key))
                            .unwrap_or(false)
                    });

                let handler_deps_ready = handler_deps.is_empty()
                    || handler_deps.iter().all(|dep| {
                        state
                            .completed_handlers
                            .get(&range_key)
                            .map(|completed| completed.contains(dep))
                            .unwrap_or(false)
                    });

                if call_deps_ready && handler_deps_ready {
                    // Ready to execute
                    if call_deps.is_empty() {
                        ready_handlers.push((handler.clone(), Arc::new(Vec::new())));
                    } else {
                        let calls = state.get_buffered_calls(range_key);
                        let calls: Vec<_> = calls
                            .into_iter()
                            .filter(|c| {
                                let start_block =
                                    self.contracts.get(&c.source_name).and_then(|ct| {
                                        ct.start_block.map(|u| u.try_into().unwrap_or(u64::MAX))
                                    });
                                start_block.is_none_or(|sb| c.block_number >= sb)
                            })
                            .collect();
                        tracing::debug!(
                            "Handler {} deps ready for block {}, {} calls",
                            handler_key,
                            msg.range_start,
                            calls.len()
                        );
                        ready_handlers.push((handler.clone(), Arc::new(calls)));
                    }
                } else {
                    // Buffer — needs either call deps or handler deps (or both)
                    let waiting_for = if !call_deps_ready && !handler_deps_ready {
                        format!(
                            "eth_call deps {:?} and handler deps {:?}",
                            call_deps, handler_deps
                        )
                    } else if !call_deps_ready {
                        format!("eth_call deps {:?}", call_deps)
                    } else {
                        format!("handler deps {:?}", handler_deps)
                    };
                    tracing::warn!(
                        "Handler {} buffering block {}: waiting for {}",
                        handler_key,
                        msg.range_start,
                        waiting_for
                    );
                    let pending = PendingEventData {
                        range_start: msg.range_start,
                        range_end: msg.range_end,
                        source_name: msg.source_name.clone(),
                        event_name: msg.event_name.clone(),
                        events: filtered_events.clone(),
                        required_calls: call_deps.clone(),
                        required_handlers: handler_deps,
                    };
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
        } // lock released

        if ready_handlers.is_empty() {
            return Ok(());
        }

        // Build HandlerTasks and use executor
        let tx_addresses = self.read_receipt_addresses(msg.range_start, msg.range_end);
        let tasks: Vec<HandlerTask> = ready_handlers
            .into_iter()
            .map(|(handler, calls)| HandlerTask {
                handler,
                events: events.clone(),
                calls,
                tx_addresses: tx_addresses.clone(),
            })
            .collect();

        let submitted_keys: HashSet<String> =
            tasks.iter().map(|t| t.handler.handler_key()).collect();

        let outcomes = self
            .executor
            .execute_handlers(tasks, msg.range_start, msg.range_end, &DbExecMode::Direct)
            .await;

        let succeeded_keys: HashSet<String> =
            outcomes.iter().map(|o| o.handler_key.clone()).collect();

        for outcome in &outcomes {
            self.finalizer
                .record_completed_range_for_handler(
                    &outcome.handler_key,
                    outcome.range_start,
                    outcome.range_end,
                )
                .await?;
        }

        self.record_handler_outcomes(
            msg.range_start,
            msg.range_end,
            &submitted_keys,
            &succeeded_keys,
            &outcomes,
        )
        .await;

        // Record handler completions for dependency tracking
        {
            let mut state = self.live_state.lock().await;
            for outcome in &outcomes {
                state
                    .completed_handlers
                    .entry(range_key)
                    .or_default()
                    .insert(outcome.handler_name.clone());
            }
        }

        // Try to unblock handlers waiting on these completions
        self.try_process_pending_events(range_key).await?;

        Ok(())
    }

    // ─── Shared outcome recording ────────────────────────────────────

    /// Mark a single block as complete for a handler in the live progress tracker.
    async fn record_live_progress(&self, block_number: u64, handler_key: &str) {
        if let Some(ref tracker) = self.progress_tracker {
            let mut t = tracker.lock().await;
            if let Err(e) = t.mark_complete(block_number, handler_key).await {
                tracing::warn!(
                    "Failed to mark live progress for block {} handler {}: {}",
                    block_number,
                    handler_key,
                    e
                );
            }
        }
    }

    /// Persist the set of failed handlers into the live status file for a block.
    async fn persist_failed_handlers(
        &self,
        block_number: u64,
        submitted_keys: &HashSet<String>,
        succeeded_keys: &HashSet<String>,
    ) {
        let failed_keys: HashSet<String> =
            submitted_keys.difference(succeeded_keys).cloned().collect();
        if !failed_keys.is_empty() {
            let storage = LiveStorage::new(&self.chain_name);
            if let Err(e) = storage.update_status_atomic(block_number, |status| {
                status.failed_handlers.extend(failed_keys.iter().cloned());
                for h in &failed_keys {
                    status.completed_handlers.remove(h);
                }
            }) {
                if !matches!(e, StorageError::NotFound(_)) {
                    tracing::warn!(
                        "Failed to persist failed handlers for block {}: {}",
                        block_number,
                        e
                    );
                }
            }
        }
    }

    /// Record progress and persist failures for handler outcomes from a single
    /// block range execution.  For single-block (live) ranges this records
    /// per-handler live progress and writes failures to the status file.
    async fn record_handler_outcomes(
        &self,
        range_start: u64,
        range_end: u64,
        submitted_keys: &HashSet<String>,
        succeeded_keys: &HashSet<String>,
        outcomes: &[super::executor::HandlerOutcome],
    ) {
        if range_end - range_start == 1 {
            for outcome in outcomes {
                self.record_live_progress(outcome.range_start, &outcome.handler_key)
                    .await;
            }
            self.persist_failed_handlers(range_start, submitted_keys, succeeded_keys)
                .await;
        }
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

        // Log reverted calls to the database for debugging
        let reverted: Vec<_> = msg.calls.iter().filter(|c| c.is_reverted).collect();
        if !reverted.is_empty() {
            tracing::warn!(
                "{} reverted eth_calls for {}/{} in range {}-{}",
                reverted.len(),
                msg.source_name,
                msg.function_name,
                msg.range_start,
                msg.range_end
            );
            if let Err(e) = self.log_reverted_calls(&reverted, msg.range_start).await {
                tracing::warn!("Failed to log reverted calls to DB: {}", e);
            }
        }

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

        let filtered_calls = filter_calls_by_start_block(&self.contracts, msg.calls);
        self.process_range(msg.range_start, msg.range_end, Vec::new(), filtered_calls)
            .await?;

        self.try_process_pending_events(range_key).await?;
        self.finalizer
            .maybe_finalize_range(range_key, &self.live_state)
            .await?;

        Ok(())
    }

    /// Batch-insert reverted call information into the `_call_revert_log` table.
    async fn log_reverted_calls(
        &self,
        calls: &[&DecodedCall],
        _range_start: u64,
    ) -> Result<(), TransformationError> {
        if calls.is_empty() {
            return Ok(());
        }

        let pool = self.db_pool.inner();
        let client = pool.get().await?;

        for call in calls {
            let chain_id = self.chain_id as i64;
            let block_number = call.block_number as i64;
            let log_index = call.trigger_log_index.map(|i| i as i32);
            let source_name = &call.source_name;
            let function_name = &call.function_name;
            let target_address = call.contract_address.as_slice();
            let revert_reason = call.revert_reason.as_deref();

            client
                .execute(
                    "INSERT INTO _call_revert_log (chain_id, block_number, log_index, source_name, function_name, target_address, revert_reason)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &chain_id,
                        &block_number,
                        &log_index,
                        &source_name,
                        &function_name,
                        &target_address,
                        &revert_reason,
                    ],
                )
                .await?;
        }

        Ok(())
    }

    /// Try to process pending events for a range now that new calls or handler
    /// completions arrived. Uses a cascading loop: after executing newly-ready
    /// handlers, their completions may unblock further handlers in the chain.
    async fn try_process_pending_events(
        &self,
        range_key: (u64, u64),
    ) -> Result<(), TransformationError> {
        loop {
            let ready_events: Vec<(
                String,
                PendingEventData,
                Arc<dyn super::traits::TransformationHandler>,
            )> = {
                let mut state = self.live_state.lock().await;
                let mut ready = Vec::new();

                let handler_keys: Vec<_> = state.pending_events.keys().cloned().collect();

                for handler_key in handler_keys {
                    let ready_indices: Vec<usize> = {
                        let pending = state.pending_events.get(&handler_key).unwrap();

                        pending
                            .iter()
                            .enumerate()
                            .filter(|(_, event_data)| {
                                (event_data.range_start, event_data.range_end) == range_key
                            })
                            .filter(|(_, event_data)| {
                                let calls_ready = event_data.required_calls.iter().all(|dep| {
                                    state
                                        .received_calls
                                        .get(dep)
                                        .map(|ranges| ranges.contains(&range_key))
                                        .unwrap_or(false)
                                });
                                let handlers_ready =
                                    event_data.required_handlers.iter().all(|dep| {
                                        state
                                            .completed_handlers
                                            .get(&range_key)
                                            .map(|completed| completed.contains(dep))
                                            .unwrap_or(false)
                                    });
                                calls_ready && handlers_ready
                            })
                            .map(|(i, _)| i)
                            .collect()
                    };

                    if !ready_indices.is_empty() {
                        tracing::info!(
                            "Handler {} unblocked for block {}: deps now available",
                            handler_key,
                            range_key.0
                        );
                        let pending = state.pending_events.get_mut(&handler_key).unwrap();
                        for i in ready_indices.into_iter().rev() {
                            let event_data = pending.remove(i);
                            let handlers = self.registry.handlers_for_event(
                                &event_data.source_name,
                                &event_data.event_name,
                            );
                            if let Some(handler) = handlers
                                .into_iter()
                                .find(|h| h.handler_key() == handler_key)
                            {
                                let handler: Arc<dyn super::traits::TransformationHandler> =
                                    handler;
                                ready.push((handler_key.clone(), event_data, handler));
                            }
                        }
                    }

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

            let calls = {
                let state = self.live_state.lock().await;
                let calls = state.get_buffered_calls(range_key);
                Arc::new(filter_calls_by_start_block(&self.contracts, calls))
            };

            // Build HandlerTasks and use executor
            let mut tasks = Vec::new();
            for (_handler_key, event_data, handler) in ready_events {
                let tx_addresses =
                    self.read_receipt_addresses(event_data.range_start, event_data.range_end);
                tasks.push(HandlerTask {
                    handler,
                    events: Arc::new(event_data.events),
                    calls: calls.clone(),
                    tx_addresses,
                });
            }

            let submitted_keys: HashSet<String> =
                tasks.iter().map(|t| t.handler.handler_key()).collect();

            let outcomes = self
                .executor
                .execute_handlers(tasks, range_key.0, range_key.1, &DbExecMode::Direct)
                .await;

            let succeeded_keys: HashSet<String> =
                outcomes.iter().map(|o| o.handler_key.clone()).collect();

            for outcome in &outcomes {
                self.finalizer
                    .record_completed_range_for_handler(
                        &outcome.handler_key,
                        outcome.range_start,
                        outcome.range_end,
                    )
                    .await?;
            }

            self.record_handler_outcomes(
                range_key.0,
                range_key.1,
                &submitted_keys,
                &succeeded_keys,
                &outcomes,
            )
            .await;

            // Record handler completions for cascading
            {
                let mut state = self.live_state.lock().await;
                for outcome in &outcomes {
                    state
                        .completed_handlers
                        .entry(range_key)
                        .or_default()
                        .insert(outcome.handler_name.clone());
                }
            }
            // Loop to check if more handlers are now unblocked
        }
    }

    /// Build handler tasks for all event and call triggers in a block range.
    fn build_handler_tasks(
        &self,
        event_triggers: &[(String, String)],
        call_triggers: &[(String, String)],
        events: Arc<Vec<DecodedEvent>>,
        calls: Arc<Vec<DecodedCall>>,
        tx_addresses: HashMap<[u8; 32], TransactionAddresses>,
    ) -> Vec<HandlerTask> {
        let mut tasks = Vec::new();

        for (source, event_name) in event_triggers {
            for handler in self.registry.handlers_for_event(source, event_name) {
                let handler: Arc<dyn super::traits::TransformationHandler> = handler;
                tasks.push(HandlerTask {
                    handler,
                    events: events.clone(),
                    calls: calls.clone(),
                    tx_addresses: tx_addresses.clone(),
                });
            }
        }

        for (source, function_name) in call_triggers {
            for handler in self.registry.handlers_for_call(source, function_name) {
                let handler: Arc<dyn super::traits::TransformationHandler> = handler;
                tasks.push(HandlerTask {
                    handler,
                    events: events.clone(),
                    calls: calls.clone(),
                    tx_addresses: tx_addresses.clone(),
                });
            }
        }

        tasks
    }

    /// Process a block range with per-handler transactions.
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

        let event_triggers: HashSet<_> = events
            .iter()
            .map(|e| (e.source_name.clone(), e.event_name.clone()))
            .collect();
        let call_triggers: HashSet<_> = calls
            .iter()
            .map(|c| (c.source_name.clone(), c.function_name.clone()))
            .collect();

        let tx_addresses = self.read_receipt_addresses(range_start, range_end);
        let events = Arc::new(events);
        let calls = Arc::new(calls);

        let is_live_mode = range_end - range_start == 1;
        let db_exec_mode = if is_live_mode {
            DbExecMode::WithSnapshotCapture {
                chain_name: self.chain_name.clone(),
            }
        } else {
            DbExecMode::Direct
        };

        let tasks = self.build_handler_tasks(
            &event_triggers.into_iter().collect::<Vec<_>>(),
            &call_triggers.into_iter().collect::<Vec<_>>(),
            events,
            calls,
            tx_addresses,
        );

        let submitted_keys: HashSet<String> =
            tasks.iter().map(|t| t.handler.handler_key()).collect();

        let outcomes = self
            .executor
            .execute_handlers(tasks, range_start, range_end, &db_exec_mode)
            .await;

        let succeeded_keys: HashSet<String> =
            outcomes.iter().map(|o| o.handler_key.clone()).collect();

        for outcome in &outcomes {
            self.finalizer
                .record_completed_range_for_handler(
                    &outcome.handler_key,
                    outcome.range_start,
                    outcome.range_end,
                )
                .await?;
        }

        self.record_handler_outcomes(
            range_start,
            range_end,
            &submitted_keys,
            &succeeded_keys,
            &outcomes,
        )
        .await;

        Ok(())
    }
}

// ─── RecordAndFinalize implementation ──────────────────────────────

#[async_trait::async_trait]
impl super::retry::RecordAndFinalize for TransformationEngine {
    async fn finalize_range(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> Result<(), TransformationError> {
        self.finalizer
            .finalize_range(range_start, range_end, &self.live_state)
            .await
    }
}
