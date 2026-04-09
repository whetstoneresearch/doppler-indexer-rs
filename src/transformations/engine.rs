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

use metrics::{counter, gauge, histogram};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

use super::context::{DecodedAccountState, DecodedCall, DecodedEvent, TransactionAddresses};
use super::error::TransformationError;
use super::executor::{
    run_handler_task, DbExecMode, HandlerExecutor, HandlerTask, ProcessRangePayload,
};
use super::finalizer::RangeFinalizer;
use super::historical::HistoricalDataReader;
use super::live_state::{LiveProcessingState, PendingEventData};
use super::registry::{extract_event_name, TransformationRegistry};
use super::retry::{filter_calls_by_start_block, filter_events_by_start_block, RetryProcessor};
use super::scheduler::dag::{DagScheduler, OutcomeStatus, WorkItem};
use super::scheduler::loader::{read_receipt_addresses, CatchupLoader, CatchupPayload};
use super::scheduler::tracker::CompletionTracker;
use crate::db::DbPool;
use crate::live::{LiveProgressTracker, LiveStorage, StorageError, TransformRetryRequest};
use crate::rpc::UnifiedRpcClient;
use crate::storage::contract_index::{
    build_expected_factory_contracts_for_range, get_missing_contracts, range_key,
    read_contract_index, ExpectedContracts,
};
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

/// Message containing decoded account states for a block range.
#[derive(Debug)]
pub struct DecodedAccountStatesMessage {
    pub range_start: u64,
    pub range_end: u64,
    pub source_name: String,
    pub account_type: String,
    pub account_states: Vec<DecodedAccountState>,
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
    AccountStates,
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
    pub expect_account_state_completion: bool,
}

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
    /// Handler dependencies: handler name() values that must complete first.
    handler_deps: Vec<String>,
    kind: HandlerKind,
    /// When true, the scheduler processes ranges one at a time in ascending order.
    sequential: bool,
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
    raw_eth_calls_dir: PathBuf,
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
        let raw_eth_calls_dir = crate::storage::paths::raw_eth_calls_dir(&chain_name);
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
            expect_account_state_completion: config.expect_account_state_completion,
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
            raw_eth_calls_dir,
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

    /// Initialize the engine: register sources, then initialize handlers.
    ///
    /// Handler migrations must have already been run before calling this
    /// (either globally in main or via `DbPool::run_handler_migrations`).
    pub async fn initialize(&self) -> Result<(), TransformationError> {
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

    async fn scan_available_ranges(
        &self,
        base_dir: &Path,
    ) -> Result<Vec<(u64, u64)>, TransformationError> {
        let base_dir = base_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
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

            scan_recursive(&base_dir, &mut ranges);

            let mut sorted: Vec<_> = ranges.into_iter().collect();
            sorted.sort_by_key(|(start, _)| *start);

            Ok(sorted)
        })
        .await
        .map_err(|e| TransformationError::IoError(std::io::Error::other(e.to_string())))?
    }

    async fn scan_available_call_dependency_ranges(
        &self,
        source: &str,
        function_name: &str,
    ) -> Result<HashSet<(u64, u64)>, TransformationError> {
        let source = source.to_string();
        let function_name = function_name.to_string();
        let decoded_base = self.decoded_calls_dir.join(&source).join(&function_name);
        let raw_base = self.raw_eth_calls_dir.join(&source).join(&function_name);
        let contracts = self.contracts.clone();

        tokio::task::spawn_blocking(move || -> std::io::Result<HashSet<(u64, u64)>> {
            fn scan_recursive(
                dir: &Path,
                decoded_base: &Path,
                raw_base: &Path,
                source: &str,
                function_name: &str,
                contracts: &Contracts,
                ranges: &mut HashSet<(u64, u64)>,
            ) -> std::io::Result<()> {
                if !dir.exists() {
                    return Ok(());
                }

                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        scan_recursive(
                            &path,
                            decoded_base,
                            raw_base,
                            source,
                            function_name,
                            contracts,
                            ranges,
                        )?;
                        continue;
                    }

                    if !path.extension().is_some_and(|ext| ext == "parquet") {
                        continue;
                    }

                    let Some((range_start, range_end_inclusive)) =
                        crate::storage::paths::parse_range_from_filename(&path)
                    else {
                        continue;
                    };

                    let range_end = range_end_inclusive + 1;
                    let expected =
                        build_expected_factory_contracts_for_range(contracts, range_end);
                    let parent_dir = path.parent().unwrap_or(decoded_base);
                    let relative_parent = parent_dir
                        .strip_prefix(decoded_base)
                        .ok()
                        .filter(|rel| !rel.as_os_str().is_empty());
                    let raw_index_dir = match relative_parent {
                        Some(rel) => raw_base.join(rel),
                        None => raw_base.to_path_buf(),
                    };

                    if !call_dependency_contract_index_complete(
                        &raw_index_dir,
                        source,
                        range_start,
                        range_end,
                        &expected,
                    ) {
                        tracing::debug!(
                            "Deferring call dependency {}/{} range {}-{} until raw contract index is complete",
                            source,
                            function_name,
                            range_start,
                            range_end_inclusive
                        );
                        continue;
                    }

                    ranges.insert((range_start, range_end));
                }

                Ok(())
            }

            let mut ranges = HashSet::new();
            if !decoded_base.exists() {
                return Ok(ranges);
            }

            scan_recursive(
                &decoded_base,
                &decoded_base,
                &raw_base,
                &source,
                &function_name,
                contracts.as_ref(),
                &mut ranges,
            )?;

            Ok(ranges)
        })
        .await
        .map_err(|e| TransformationError::IoError(std::io::Error::other(e.to_string())))?
        .map_err(TransformationError::IoError)
    }

    fn raw_call_dependency_index_dir(
        &self,
        source: &str,
        function_name: &str,
        decoded_file_path: &Path,
    ) -> PathBuf {
        let decoded_base = self.decoded_calls_dir.join(source).join(function_name);
        let raw_base = self.raw_eth_calls_dir.join(source).join(function_name);
        let relative_parent = decoded_file_path
            .parent()
            .and_then(|parent| parent.strip_prefix(&decoded_base).ok())
            .filter(|rel| !rel.as_os_str().is_empty());

        match relative_parent {
            Some(rel) => raw_base.join(rel),
            None => raw_base,
        }
    }

    fn call_dependency_path_ready(
        &self,
        source: &str,
        function_name: &str,
        range_key: (u64, u64),
        decoded_file_path: &Path,
    ) -> bool {
        let expected =
            build_expected_factory_contracts_for_range(self.contracts.as_ref(), range_key.1);
        let raw_index_dir =
            self.raw_call_dependency_index_dir(source, function_name, decoded_file_path);

        call_dependency_contract_index_complete(
            &raw_index_dir,
            source,
            range_key.0,
            range_key.1,
            &expected,
        )
    }

    // ─── Per-Handler Catchup ─────────────────────────────────────────

    /// Run catchup phase: process decoded parquet files per handler.
    pub async fn run_catchup(&self) -> Result<(), TransformationError> {
        self.run_handler_catchup(HandlerKind::Event).await?;
        self.run_handler_catchup(HandlerKind::Call).await?;
        Ok(())
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

        let available = self.scan_available_ranges(base_dir).await?;

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
                    let handler_deps: Vec<String> = info
                        .handler
                        .handler_dependencies()
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                    let sequential = info.handler.requires_sequential();
                    CatchupHandler {
                        handler: info.handler as Arc<dyn super::traits::TransformationHandler>,
                        triggers,
                        call_deps,
                        handler_deps,
                        kind: HandlerKind::Event,
                        sequential,
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
                    let sequential = info.handler.requires_sequential();
                    CatchupHandler {
                        handler: info.handler as Arc<dyn super::traits::TransformationHandler>,
                        triggers,
                        call_deps: Vec::new(),
                        handler_deps: Vec::new(),
                        kind: HandlerKind::Call,
                        sequential,
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
                position
                    .get(ch.handler.name())
                    .copied()
                    .unwrap_or(usize::MAX)
            });
        }

        // Catchup walks the union of all decoded ranges so dependency handlers
        // can no-op complete unrelated ranges. Call deps, however, should only
        // gate ranges where this handler actually has trigger parquet data.
        let mut trigger_range_sets: HashMap<String, HashSet<(u64, u64)>> = HashMap::new();
        for ch in &handlers {
            let mut ranges: HashSet<(u64, u64)> = HashSet::new();
            for (source, trigger_name) in &ch.triggers {
                let dir = base_dir.join(source).join(trigger_name);
                ranges.extend(self.scan_available_ranges(&dir).await?);
            }
            trigger_range_sets.insert(ch.handler.name().to_string(), ranges);
        }

        // Seed CompletionTracker from _handler_progress so downstream handlers
        // can gate on upstream completion per range via the DAG scheduler.
        let tracker = Arc::new(CompletionTracker::new());
        // self_completed: handler_name → set of range_starts already processed.
        // Used to skip building WorkItems for ranges already done.
        let mut self_completed: HashMap<String, HashSet<u64>> = HashMap::new();
        for ch in &handlers {
            let completed = self
                .finalizer
                .get_completed_ranges_for_handler(&ch.handler.handler_key())
                .await?;
            tracker
                .seed_completed(ch.handler.name(), completed.iter().copied())
                .await;
            self_completed.insert(ch.handler.name().to_string(), completed);
        }

        let total_todo: usize = handlers
            .iter()
            .map(|ch| {
                let done = self_completed
                    .get(ch.handler.name())
                    .map(|s| s.len())
                    .unwrap_or(0);
                available.len().saturating_sub(done)
            })
            .sum();

        if total_todo == 0 {
            tracing::info!("{} handler catchup: all handlers up to date", kind_label);
            return Ok(());
        }

        tracing::info!(
            "{} handler catchup: {} work items to process across {} handlers",
            kind_label,
            total_todo,
            handlers.len()
        );

        gauge!(
            "transformation_catchup_ranges_remaining",
            "kind" => kind_label,
        )
        .set(total_todo as f64);

        let loader = Arc::new(CatchupLoader {
            decoded_logs_dir: self.decoded_logs_dir.clone(),
            decoded_calls_dir: self.decoded_calls_dir.clone(),
            raw_receipts_dir: self.raw_receipts_dir.clone(),
            historical_reader: self.historical_reader.clone(),
            rpc_client: self.executor.rpc_client.clone(),
            contracts: self.contracts.clone(),
            chain_name: self.chain_name.clone(),
            chain_id: self.chain_id,
            db_pool: self.db_pool.clone(),
            finalizer: self.finalizer.clone(),
        });

        let scheduler = DagScheduler::new(tracker.clone(), self.handler_concurrency);

        // (handler_key, range_start, error_message) for each item-level failure.
        let mut failed_items: Vec<(String, u64, String)> = vec![];

        // Call-dep retry loop.
        //
        // `ranges_pending` is None on the first pass (try every available range).
        // On subsequent passes it holds only the ranges that were skipped because
        // the call-dep parquet files weren't on disk yet.
        let mut ranges_pending: Option<HashMap<String, Vec<(u64, u64)>>> = None;
        let mut pass = 0u32;
        // Bail only after several consecutive passes make zero progress, so that
        // slowly-arriving call-dep files don't get abandoned.
        const MAX_CONSECUTIVE_NO_PROGRESS: u32 = 3;
        let mut consecutive_no_progress: u32 = 0;

        loop {
            pass += 1;

            let mut items: Vec<WorkItem> = Vec::new();
            let mut next_pending: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
            // Parallel index into next_pending for O(1) cascade lookups.
            // Updated in lock-step with next_pending whenever we defer a range.
            let mut deferred_starts: HashMap<String, HashSet<u64>> = HashMap::new();
            // Per-handler per-pass counters for observability.
            // (submitted, call_dep_deferred, cascade_deferred).
            let mut per_handler_counts: HashMap<String, (usize, usize, usize)> = HashMap::new();
            // First missing call dep paths per handler (for summary log).
            let mut per_handler_missing_call_deps: HashMap<String, Vec<String>> = HashMap::new();

            for ch in &handlers {
                let name = ch.handler.name().to_string();
                let completed = self_completed.get(&name).cloned().unwrap_or_default();

                // On retry passes only re-try handlers that had pending ranges.
                let candidate_ranges: Vec<(u64, u64)> = if let Some(ref pending) = ranges_pending {
                    match pending.get(&name) {
                        Some(r) => r.clone(),
                        None => continue,
                    }
                } else {
                    available.clone()
                };

                // Scan call-dep file availability once per handler per pass.
                let mut call_range_sets: Vec<HashSet<(u64, u64)>> =
                    Vec::with_capacity(ch.call_deps.len());
                for (source, func) in &ch.call_deps {
                    let ranges = self
                        .scan_available_call_dependency_ranges(source, func)
                        .await
                        .unwrap_or_default();
                    call_range_sets.push(ranges);
                }

                for (range_start, range_end) in candidate_ranges {
                    if completed.contains(&range_start) {
                        continue;
                    }

                    let trigger_range_present = trigger_range_sets
                        .get(&name)
                        .is_some_and(|ranges| ranges.contains(&(range_start, range_end)));

                    // Skip range if call-dep files aren't on disk yet, but
                    // only when this handler actually has trigger data in the
                    // range. Otherwise let it no-op complete and unblock
                    // same-range dependents.
                    if trigger_range_present && !ch.call_deps.is_empty() {
                        let missing_deps: Vec<&str> = ch
                            .call_deps
                            .iter()
                            .zip(call_range_sets.iter())
                            .filter(|(_, set)| !set.contains(&(range_start, range_end)))
                            .map(|((source, func), _)| {
                                // Leak-free: just reference the source since it
                                // lives as long as the handler.
                                if func.is_empty() {
                                    source.as_str()
                                } else {
                                    // Can't return a formatted &str, so store in
                                    // per_handler_missing_deps below instead.
                                    source.as_str()
                                }
                            })
                            .collect();
                        if !missing_deps.is_empty() {
                            // Track which call deps are missing for the summary log.
                            let missing_formatted: Vec<String> = ch
                                .call_deps
                                .iter()
                                .zip(call_range_sets.iter())
                                .filter(|(_, set)| !set.contains(&(range_start, range_end)))
                                .map(|((source, func), _)| format!("{}/{}", source, func))
                                .collect();
                            per_handler_missing_call_deps
                                .entry(name.clone())
                                .or_insert_with(|| missing_formatted);
                            next_pending
                                .entry(name.clone())
                                .or_default()
                                .push((range_start, range_end));
                            deferred_starts
                                .entry(name.clone())
                                .or_default()
                                .insert(range_start);
                            per_handler_counts.entry(name.clone()).or_default().1 += 1;
                            continue;
                        }
                    }

                    // Cascade: if any handler_dep is already deferred for this
                    // range_start, defer this (handler, range) too. Submitting
                    // it would hang the scheduler because its dep will never
                    // be marked in the tracker for this pass. Topological
                    // handler iteration (sorted above) guarantees deps are
                    // processed before dependents in the same pass, so a
                    // single-level check composes into full transitive cascade.
                    if let Some(blocking) = ch.handler_deps.iter().find(|dep_name| {
                        deferred_starts
                            .get(dep_name.as_str())
                            .is_some_and(|starts| starts.contains(&range_start))
                    }) {
                        tracing::debug!(
                            "Handler {} deferring range {}-{}: upstream dep '{}' deferred",
                            ch.handler.handler_key(),
                            range_start,
                            range_end,
                            blocking
                        );
                        next_pending
                            .entry(name.clone())
                            .or_default()
                            .push((range_start, range_end));
                        deferred_starts
                            .entry(name.clone())
                            .or_default()
                            .insert(range_start);
                        per_handler_counts.entry(name.clone()).or_default().2 += 1;
                        continue;
                    }

                    // Skip if any handler dep already failed in a previous
                    // pass — the tracker retains failure state across
                    // scheduler.execute() calls, so submitting this item
                    // would immediately cascade-fail.
                    {
                        let mut dep_failed = false;
                        for dep in &ch.handler_deps {
                            if tracker.is_failed(dep, range_start).await {
                                dep_failed = true;
                                break;
                            }
                        }
                        if dep_failed {
                            continue;
                        }
                    }

                    per_handler_counts.entry(name.clone()).or_default().0 += 1;
                    items.push(WorkItem {
                        handler_name: name.clone(),
                        range_start,
                        range_end,
                        dep_names: ch.handler_deps.clone(),
                        sequential: ch.sequential,
                        payload: Box::new(CatchupPayload {
                            handler: ch.handler.clone(),
                            handler_key: ch.handler.handler_key(),
                            handler_name: ch.handler.name(),
                            handler_version: ch.handler.version(),
                            triggers: ch.triggers.clone(),
                            call_deps: ch.call_deps.clone(),
                            kind: ch.kind,
                        }),
                    });
                }
            }

            // Per-handler summary of this pass's work (submitted / deferred).
            // Iterate `handlers` for deterministic topological ordering.
            for ch in &handlers {
                let name = ch.handler.name();
                let (submitted, call_dep_def, cascade_def) =
                    per_handler_counts.get(name).copied().unwrap_or((0, 0, 0));
                if submitted == 0 && call_dep_def == 0 && cascade_def == 0 {
                    continue;
                }
                if call_dep_def == 0 && cascade_def == 0 {
                    tracing::info!(
                        "Handler {} catchup pass {}: submitting {} range(s)",
                        ch.handler.handler_key(),
                        pass,
                        submitted
                    );
                } else {
                    let missing_info = per_handler_missing_call_deps
                        .get(ch.handler.name())
                        .map(|deps| format!(" [missing: {}]", deps.join(", ")))
                        .unwrap_or_default();
                    tracing::info!(
                        "Handler {} catchup pass {}: submitting {} range(s), \
                         deferring {} (call_deps not ready) + {} (upstream deferred){}",
                        ch.handler.handler_key(),
                        pass,
                        submitted,
                        call_dep_def,
                        cascade_def,
                        missing_info
                    );
                }
            }

            if !items.is_empty() {
                tracing::info!(
                    "{} catchup pass {}: executing {} work items",
                    kind_label,
                    pass,
                    items.len()
                );

                let pass_start = Instant::now();
                let loader_ref = loader.clone();
                let outcomes = scheduler
                    .execute(items, move |item| {
                        let loader = loader_ref.clone();
                        Box::pin(async move { loader.run(item).await.map_err(|e| e.to_string()) })
                    })
                    .await;

                histogram!(
                    "transformation_catchup_pass_duration_seconds",
                    "kind" => kind_label,
                )
                .record(pass_start.elapsed().as_secs_f64());

                let mut succeeded = 0usize;
                let mut cascade_failed = 0usize;
                // Per-handler: (succeeded, failed, cascade_failed, panicked).
                let mut per_handler_outcomes: HashMap<String, (usize, usize, usize, usize)> =
                    HashMap::new();

                for outcome in &outcomes {
                    let counts = per_handler_outcomes
                        .entry(outcome.handler_name.clone())
                        .or_default();
                    match &outcome.status {
                        OutcomeStatus::Succeeded => {
                            self_completed
                                .entry(outcome.handler_name.clone())
                                .or_default()
                                .insert(outcome.range_start);
                            succeeded += 1;
                            counts.0 += 1;
                            let key = handlers
                                .iter()
                                .find(|ch| ch.handler.name() == outcome.handler_name)
                                .map(|ch| ch.handler.handler_key())
                                .unwrap_or_else(|| outcome.handler_name.clone());
                            counter!(
                                "transformation_catchup_ranges_completed_total",
                                "handler_key" => key,
                                "kind" => kind_label,
                            )
                            .increment(1);
                        }
                        OutcomeStatus::HandlerFailed { reason } => {
                            tracing::error!(
                                "Handler {} failed on range {}-{}: {}",
                                outcome.handler_name,
                                outcome.range_start,
                                outcome.range_end,
                                reason
                            );
                            let key = handlers
                                .iter()
                                .find(|ch| ch.handler.name() == outcome.handler_name)
                                .map(|ch| ch.handler.handler_key())
                                .unwrap_or_else(|| outcome.handler_name.clone());
                            failed_items.push((key, outcome.range_start, reason.clone()));
                            counts.1 += 1;
                        }
                        OutcomeStatus::DepCascadeFailed { dep_name } => {
                            tracing::warn!(
                                "Handler {} cascade-failed on range {} due to dep '{}'",
                                outcome.handler_name,
                                outcome.range_start,
                                dep_name
                            );
                            let key = handlers
                                .iter()
                                .find(|ch| ch.handler.name() == outcome.handler_name)
                                .map(|ch| ch.handler.handler_key())
                                .unwrap_or_else(|| outcome.handler_name.clone());
                            failed_items.push((
                                key,
                                outcome.range_start,
                                format!("cascade-failed: dep '{}' failed", dep_name),
                            ));
                            cascade_failed += 1;
                            counts.2 += 1;
                        }
                        OutcomeStatus::Panicked => {
                            tracing::error!(
                                "Handler {} panicked on range {}",
                                outcome.handler_name,
                                outcome.range_start
                            );
                            let key = handlers
                                .iter()
                                .find(|ch| ch.handler.name() == outcome.handler_name)
                                .map(|ch| ch.handler.handler_key())
                                .unwrap_or_else(|| outcome.handler_name.clone());
                            failed_items.push((
                                key,
                                outcome.range_start,
                                "task panicked".to_string(),
                            ));
                            counts.3 += 1;
                        }
                    }
                }

                // Per-handler outcome summary. Log anything with non-success
                // activity at info, clean runs at debug to reduce log noise.
                for ch in &handlers {
                    let name = ch.handler.name();
                    let Some(&(ok, failed, cascade, panicked)) = per_handler_outcomes.get(name)
                    else {
                        continue;
                    };
                    let total = ok + failed + cascade + panicked;
                    if failed > 0 || cascade > 0 || panicked > 0 {
                        tracing::info!(
                            "Handler {} catchup pass {} result: {}/{} succeeded \
                             ({} failed, {} cascade-failed, {} panicked)",
                            ch.handler.handler_key(),
                            pass,
                            ok,
                            total,
                            failed,
                            cascade,
                            panicked
                        );
                    } else {
                        tracing::debug!(
                            "Handler {} catchup pass {} result: {} range(s) ok",
                            ch.handler.handler_key(),
                            pass,
                            ok
                        );
                    }
                }

                tracing::info!(
                    "{} catchup pass {} complete: {} succeeded, {} cascade-failed",
                    kind_label,
                    pass,
                    succeeded,
                    cascade_failed
                );

                // Update remaining gauge: recompute from self_completed
                let remaining: usize = handlers
                    .iter()
                    .map(|ch| {
                        let done = self_completed
                            .get(ch.handler.name())
                            .map(|s| s.len())
                            .unwrap_or(0);
                        available.len().saturating_sub(done)
                    })
                    .sum();
                gauge!(
                    "transformation_catchup_ranges_remaining",
                    "kind" => kind_label,
                )
                .set(remaining as f64);
            }

            if next_pending.is_empty() {
                break;
            }

            // Check for progress on call-dep-waiting ranges; bail only after
            // repeated passes with strictly zero progress.
            if let Some(ref prev) = ranges_pending {
                let prev_count: usize = prev.values().map(|v| v.len()).sum();
                let next_count: usize = next_pending.values().map(|v| v.len()).sum();
                if next_count >= prev_count {
                    consecutive_no_progress += 1;
                    if consecutive_no_progress >= MAX_CONSECUTIVE_NO_PROGRESS {
                        tracing::warn!(
                            "{} catchup: {} ranges still blocked by call dependencies, \
                             no progress for {} consecutive passes after pass {}. Giving up.",
                            kind_label,
                            next_count,
                            consecutive_no_progress,
                            pass
                        );
                        break;
                    }
                } else {
                    consecutive_no_progress = 0;
                }
            }

            let pending_count: usize = next_pending.values().map(|v| v.len()).sum();
            tracing::info!(
                "{} catchup pass {}: {} ranges pending call dependencies, retrying in 1s...",
                kind_label,
                pass,
                pending_count
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            ranges_pending = Some(next_pending);
        }

        if failed_items.is_empty() {
            return Ok(());
        }

        let failed_handlers: Vec<(String, String)> = failed_items
            .iter()
            .map(|(key, range, reason)| (key.clone(), format!("range {}: {}", range, reason)))
            .collect();
        Self::catchup_failure_error(&failed_handlers)
    }

    fn resolve_decoded_call_path(
        &self,
        range_key: (u64, u64),
        source: &str,
        function_name: &str,
    ) -> Option<PathBuf> {
        let file_name = format!("{}-{}.parquet", range_key.0, range_key.1 - 1);
        let base_dir = self.decoded_calls_dir.join(source).join(function_name);

        [
            base_dir.join(&file_name),
            base_dir.join("on_events").join(&file_name),
            base_dir.join("once").join(&file_name),
        ]
        .into_iter()
        .find(|path| path.exists())
    }

    async fn hydrate_call_dependency_from_disk(
        &self,
        range_key: (u64, u64),
        dep: &(String, String),
    ) -> Result<bool, TransformationError> {
        {
            let state = self.live_state.lock().await;
            if state
                .received_calls
                .get(dep)
                .map(|ranges| ranges.contains(&range_key))
                .unwrap_or(false)
            {
                return Ok(true);
            }
        }

        let Some(file_path) = self.resolve_decoded_call_path(range_key, &dep.0, &dep.1) else {
            return Ok(false);
        };
        if !self.call_dependency_path_ready(&dep.0, &dep.1, range_key, &file_path) {
            return Ok(false);
        }

        let historical_reader = self.historical_reader.clone();
        let source_name = dep.0.clone();
        let function_name = dep.1.clone();
        let file_path_for_read = file_path.clone();
        let calls = tokio::task::spawn_blocking(move || {
            historical_reader.read_calls_from_file(
                &file_path_for_read,
                &source_name,
                &function_name,
            )
        })
        .await
        .map_err(|e| TransformationError::IoError(std::io::Error::other(e.to_string())))??;

        let mut state = self.live_state.lock().await;
        let ranges = state.received_calls.entry(dep.clone()).or_default();
        if !ranges.insert(range_key) {
            return Ok(true);
        }
        if !calls.is_empty() {
            state
                .calls_buffer
                .entry(range_key)
                .or_default()
                .extend(calls);
        }

        tracing::debug!(
            "Hydrated call dependency {}/{} for range {}-{} from {}",
            dep.0,
            dep.1,
            range_key.0,
            range_key.1,
            file_path.display()
        );

        Ok(true)
    }

    async fn hydrate_missing_call_deps_from_disk(
        &self,
        range_key: (u64, u64),
        deps: impl IntoIterator<Item = (String, String)>,
    ) -> Result<(), TransformationError> {
        for dep in deps {
            let _ = self
                .hydrate_call_dependency_from_disk(range_key, &dep)
                .await?;
        }
        Ok(())
    }

    // ─── Receipt Address Reading ────────────────────────────────────

    async fn read_receipt_addresses(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> HashMap<[u8; 32], TransactionAddresses> {
        read_receipt_addresses(&self.raw_receipts_dir, range_start, range_end).await
    }

    // ─── Live Processing ─────────────────────────────────────────────

    /// Run the transformation engine, processing messages from channels.
    pub async fn run(
        &self,
        mut events_rx: Receiver<DecodedEventsMessage>,
        mut calls_rx: Receiver<DecodedCallsMessage>,
        mut account_states_rx: Option<Receiver<DecodedAccountStatesMessage>>,
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

                Some(msg) = async {
                    match account_states_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if msg.account_states.is_empty() {
                        continue;
                    }
                    self.process_account_states_message(msg).await?;
                }

                Some(msg) = complete_rx.recv() => {
                    let range_key = (msg.range_start, msg.range_end);

                    // When all log events have been sent, mark logs complete and
                    // mark untriggered dependency handlers as completed-no-op so
                    // dependent handlers can proceed.
                    if msg.kind == RangeCompleteKind::Logs {
                        let dep_names = self.registry.dependency_handler_names();

                        // Mark logs complete before processing pending events so
                        // that handlers unblocked below are properly recorded in
                        // completed_handlers (the recording logic gates dep-handler
                        // completion on logs_complete being true).
                        let mut state = self.live_state.lock().await;
                        state
                            .completion
                            .entry(range_key)
                            .or_default()
                            .mark(RangeCompleteKind::Logs);

                        if !dep_names.is_empty() {
                            // Compute handler names that are pending (triggered
                            // but waiting on their own deps). These must NOT be
                            // marked as no-op — they still need to execute.
                            let pending_names: HashSet<&str> = state
                                .pending_events
                                .iter()
                                .filter(|(_, entries)| {
                                    entries.iter().any(|e| {
                                        (e.range_start, e.range_end) == range_key
                                    })
                                })
                                .filter_map(|(handler_key, _)| {
                                    self.registry.handler_name_for_key(handler_key)
                                })
                                .collect();

                            let failed_names: HashSet<String> = state
                                .failed_handlers
                                .get(&range_key)
                                .cloned()
                                .unwrap_or_default();

                            let completed = state.completed_handlers
                                .entry(range_key)
                                .or_default();
                            for name in dep_names {
                                if !completed.contains(name)
                                    && !pending_names.contains(name.as_str())
                                    && !failed_names.contains(name)
                                {
                                    tracing::debug!(
                                        "Marking dep handler {} as completed for block {} (all batches dispatched)",
                                        name, range_key.0
                                    );
                                    completed.insert(name.clone());
                                }
                            }
                        }
                        drop(state);
                    }

                    if matches!(msg.kind, RangeCompleteKind::Logs | RangeCompleteKind::EthCalls) {
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

        if !filtered_events.is_empty() {
            counter!(
                "transformation_events_processed_total",
                "source_name" => msg.source_name.clone(),
                "event_name" => msg.event_name.clone(),
            )
            .increment(filtered_events.len() as u64);
        }

        let events = Arc::new(filtered_events.clone());
        let range_key = (msg.range_start, msg.range_end);

        let missing_call_deps: HashSet<(String, String)> = {
            let state = self.live_state.lock().await;
            handlers
                .iter()
                .flat_map(|handler| handler.call_dependencies())
                .filter(|dep| {
                    !state
                        .received_calls
                        .get(dep)
                        .map(|ranges| ranges.contains(&range_key))
                        .unwrap_or(false)
                })
                .collect()
        };
        if !missing_call_deps.is_empty() {
            self.hydrate_missing_call_deps_from_disk(range_key, missing_call_deps)
                .await?;
        }

        // Categorize handlers: ready to run vs needs buffering
        let mut ready_handlers: Vec<ReadyHandler> = Vec::new();
        let mut dep_failed_names: Vec<String> = Vec::new();
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
                let handler_name = handler.name().to_string();

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

                let self_already_failed = state
                    .failed_handlers
                    .get(&range_key)
                    .map(|failed| failed.contains(&handler_name))
                    .unwrap_or(false);
                if self_already_failed {
                    tracing::debug!(
                        "Handler {} skipped for block {}: already failed for this range",
                        handler_key,
                        msg.range_start,
                    );
                    continue;
                }

                // If any handler dependency has already failed for this
                // range, this handler can never run — skip it immediately.
                let dep_already_failed = if handler_deps.is_empty() {
                    false
                } else {
                    let failed = state
                        .failed_handlers
                        .get(&range_key)
                        .map(|f| handler_deps.iter().any(|dep| f.contains(dep)))
                        .unwrap_or(false);
                    if failed {
                        tracing::warn!(
                            "Handler {} skipped for block {}: upstream handler dep already failed",
                            handler_key,
                            msg.range_start,
                        );
                        state
                            .failed_handlers
                            .entry(range_key)
                            .or_default()
                            .insert(handler_name.clone());
                        dep_failed_names.push(handler_name);
                    }
                    failed
                };

                if dep_already_failed {
                    // Don't buffer — handler is already doomed for this range
                } else if call_deps_ready && handler_deps_ready {
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
                        .entry(handler_key.clone())
                        .or_default()
                        .push(pending);
                    gauge!(
                        "transformation_pending_events",
                        "handler_key" => handler_key,
                    )
                    .increment(1.0);
                }
            }
        } // lock released

        // Handlers skipped because an upstream dep already failed: persist
        // and cascade so their own dependents are also failed immediately.
        if !dep_failed_names.is_empty() {
            if range_key.1 - range_key.0 == 1 {
                let failed_keys: HashSet<String> = dep_failed_names
                    .iter()
                    .filter_map(|name| {
                        self.registry
                            .handler_key_for_name(name)
                            .map(|k| k.to_string())
                    })
                    .collect();
                let storage = LiveStorage::new(&self.chain_name);
                if let Err(e) = storage.update_status_atomic(range_key.0, |status| {
                    status.failed_handlers.extend(failed_keys.iter().cloned());
                    for k in &failed_keys {
                        status.completed_handlers.remove(k);
                    }
                    status.transformed = false;
                }) {
                    if !matches!(e, StorageError::NotFound(_)) {
                        tracing::warn!(
                            "Failed to persist dep-failed handlers for block {}: {}",
                            range_key.0,
                            e
                        );
                    }
                }
            }
            self.cascade_handler_failures(&dep_failed_names, range_key)
                .await;
        }

        if ready_handlers.is_empty() {
            return Ok(());
        }

        // Build HandlerTasks and use executor
        let tx_addresses = self
            .read_receipt_addresses(msg.range_start, msg.range_end)
            .await;
        let tasks: Vec<HandlerTask> = ready_handlers
            .into_iter()
            .map(|(handler, calls)| HandlerTask {
                handler,
                events: events.clone(),
                calls,
                account_states: Arc::new(Vec::new()),
                tx_addresses: tx_addresses.clone(),
            })
            .collect();

        let submitted_keys: HashSet<String> =
            tasks.iter().map(|t| t.handler.handler_key()).collect();

        let (snapshot_tasks, direct_tasks): (Vec<_>, Vec<_>) = tasks
            .into_iter()
            .partition(|t| !self.registry.is_multi_trigger(&t.handler.handler_key()));

        let mut outcomes = self
            .executor
            .execute_handlers(
                snapshot_tasks,
                msg.range_start,
                msg.range_end,
                &DbExecMode::WithSnapshotCapture {
                    chain_name: self.chain_name.clone(),
                },
            )
            .await;
        outcomes.extend(
            self.executor
                .execute_handlers(
                    direct_tasks,
                    msg.range_start,
                    msg.range_end,
                    &DbExecMode::Direct,
                )
                .await,
        );

        let succeeded_keys: HashSet<String> =
            outcomes.iter().map(|o| o.handler_key.clone()).collect();

        // Only persist success for single-trigger handlers.  Multi-trigger
        // handlers are deferred to finalize_range() because this method runs
        // once per (source, event_name) batch and a multi-trigger handler
        // would be marked complete before all its batches are dispatched.
        let persistable_success_keys: HashSet<String> = succeeded_keys
            .iter()
            .filter(|k| !self.registry.is_multi_trigger(k))
            .cloned()
            .collect();

        for key in &persistable_success_keys {
            self.finalizer
                .record_completed_range_for_handler(key, msg.range_start, msg.range_end)
                .await?;
        }

        self.record_handler_outcomes(
            msg.range_start,
            msg.range_end,
            &submitted_keys,
            &succeeded_keys,
            &persistable_success_keys,
        )
        .await;

        // Record handler completions and failures for dependency tracking.
        // Dependency handlers are NOT marked as completed here because this
        // method runs per (source, event_name) batch — a multi-trigger dep
        // handler would prematurely unblock dependents after its first batch.
        // Dep handlers are completed at RangeCompleteKind::Logs time when all
        // event batches for the block have been dispatched.
        let newly_failed_names: Vec<String> = {
            let dep_names = self.registry.dependency_handler_names();
            let mut state = self.live_state.lock().await;
            let failed_names: HashSet<String> = state
                .failed_handlers
                .get(&range_key)
                .cloned()
                .unwrap_or_default();
            for outcome in &outcomes {
                if !dep_names.contains(&outcome.handler_name)
                    && !failed_names.contains(&outcome.handler_name)
                {
                    state
                        .completed_handlers
                        .entry(range_key)
                        .or_default()
                        .insert(outcome.handler_name.clone());
                }
            }
            let mut newly_failed = Vec::new();
            for key in submitted_keys.difference(&succeeded_keys) {
                if let Some(name) = self.registry.handler_name_for_key(key) {
                    state
                        .failed_handlers
                        .entry(range_key)
                        .or_default()
                        .insert(name.to_string());
                    newly_failed.push(name.to_string());
                }
            }
            newly_failed
        };

        self.cascade_handler_failures(&newly_failed_names, range_key)
            .await;

        // Try to unblock handlers waiting on call deps (handler dep
        // unblocking for dep handlers is deferred until Logs fires).
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
                status.transformed = false;
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
    ///
    /// `persistable_success_keys` controls which handlers get their success
    /// written to `_live_progress` and the status file.  This is a subset of
    /// `succeeded_keys` that excludes multi-trigger handlers whose remaining
    /// batches have not yet been dispatched.  Failure persistence always uses
    /// the real `submitted_keys` / `succeeded_keys` to avoid false positives.
    async fn record_handler_outcomes(
        &self,
        range_start: u64,
        range_end: u64,
        submitted_keys: &HashSet<String>,
        succeeded_keys: &HashSet<String>,
        persistable_success_keys: &HashSet<String>,
    ) {
        if range_end - range_start == 1 {
            // Only record live progress for handlers in persistable_success_keys.
            // Skip handlers that already failed for this block to prevent
            // re-completing them outside explicit retry.
            let failed_for_block: HashSet<String> = {
                let state = self.live_state.lock().await;
                state
                    .failed_handlers
                    .get(&(range_start, range_end))
                    .cloned()
                    .unwrap_or_default()
            };
            for key in persistable_success_keys {
                let is_already_failed = self
                    .registry
                    .handler_name_for_key(key)
                    .map(|n| failed_for_block.contains(n))
                    .unwrap_or(false);
                if !is_already_failed {
                    self.record_live_progress(range_start, key).await;
                }
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

        // Decoded call files can be rewritten/backfilled in place, especially for
        // `once` calls. Invalidate cached historical lookups so handlers re-read
        // the freshest parquet contents after new decode messages arrive.
        self.historical_reader
            .invalidate_call_cache(&msg.source_name, &msg.function_name);

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

        if !filtered_calls.is_empty() {
            counter!(
                "transformation_calls_processed_total",
                "source_name" => msg.source_name.clone(),
                "function_name" => msg.function_name.clone(),
            )
            .increment(filtered_calls.len() as u64);
        }

        self.process_range(
            msg.range_start,
            msg.range_end,
            Vec::new(),
            filtered_calls,
            Vec::new(),
        )
        .await?;

        self.try_process_pending_events(range_key).await?;
        self.finalizer
            .maybe_finalize_range(range_key, &self.live_state)
            .await?;

        Ok(())
    }

    /// Process an account-state message.
    async fn process_account_states_message(
        &self,
        msg: DecodedAccountStatesMessage,
    ) -> Result<(), TransformationError> {
        let handlers = self
            .registry
            .handlers_for_account_state(&msg.source_name, &msg.account_type);
        if handlers.is_empty() {
            tracing::debug!(
                "No account state handlers registered for {}/{}",
                msg.source_name,
                msg.account_type
            );
            return Ok(());
        }

        tracing::debug!(
            "Processing {} account states for {}/{} block {} with {} handlers",
            msg.account_states.len(),
            msg.source_name,
            msg.account_type,
            msg.range_start,
            handlers.len()
        );

        counter!(
            "transformation_account_states_processed_total",
            "source_name" => msg.source_name.clone(),
            "account_type" => msg.account_type.clone(),
        )
        .increment(msg.account_states.len() as u64);

        self.process_range(
            msg.range_start,
            msg.range_end,
            Vec::new(),
            Vec::new(),
            msg.account_states,
        )
        .await
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
            let missing_call_deps: HashSet<(String, String)> = {
                let state = self.live_state.lock().await;
                state
                    .pending_events
                    .values()
                    .flat_map(|entries| entries.iter())
                    .filter(|event_data| {
                        (event_data.range_start, event_data.range_end) == range_key
                    })
                    .flat_map(|event_data| event_data.required_calls.iter().cloned())
                    .filter(|dep| {
                        !state
                            .received_calls
                            .get(dep)
                            .map(|ranges| ranges.contains(&range_key))
                            .unwrap_or(false)
                    })
                    .collect()
            };
            if !missing_call_deps.is_empty() {
                self.hydrate_missing_call_deps_from_disk(range_key, missing_call_deps)
                    .await?;
            }

            let ready_events: Vec<(
                String,
                PendingEventData,
                Arc<dyn super::traits::TransformationHandler>,
            )> = {
                let mut state = self.live_state.lock().await;
                let mut ready = Vec::new();

                let handler_keys: Vec<_> = state.pending_events.keys().cloned().collect();

                for handler_key in handler_keys {
                    let handler_failed = self
                        .registry
                        .handler_name_for_key(&handler_key)
                        .map(|name| {
                            state
                                .failed_handlers
                                .get(&range_key)
                                .map(|failed| failed.contains(name))
                                .unwrap_or(false)
                        })
                        .unwrap_or(false);
                    if handler_failed {
                        let remove_entry =
                            if let Some(pending) = state.pending_events.get_mut(&handler_key) {
                                let before = pending.len();
                                pending.retain(|p| (p.range_start, p.range_end) != range_key);
                                let removed = before - pending.len();
                                if removed > 0 {
                                    gauge!(
                                        "transformation_pending_events",
                                        "handler_key" => handler_key.clone(),
                                    )
                                    .decrement(removed as f64);
                                }
                                pending.is_empty()
                            } else {
                                false
                            };
                        if remove_entry {
                            state.pending_events.remove(&handler_key);
                        }
                        state.pending_event_timestamps.remove(&(
                            range_key.0,
                            range_key.1,
                            handler_key.clone(),
                        ));
                        continue;
                    }

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
                            gauge!(
                                "transformation_pending_events",
                                "handler_key" => handler_key.clone(),
                            )
                            .decrement(1.0);
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
                let tx_addresses = self
                    .read_receipt_addresses(event_data.range_start, event_data.range_end)
                    .await;
                tasks.push(HandlerTask {
                    handler,
                    events: Arc::new(event_data.events),
                    calls: calls.clone(),
                    account_states: Arc::new(Vec::new()),
                    tx_addresses,
                });
            }

            // Count submissions per handler to detect partial failures.
            // A handler is "fully succeeded" only if ALL its batches succeeded.
            let mut submitted_counts: HashMap<String, usize> = HashMap::new();
            for t in &tasks {
                *submitted_counts.entry(t.handler.handler_key()).or_default() += 1;
            }

            let (snapshot_tasks, direct_tasks): (Vec<_>, Vec<_>) = tasks
                .into_iter()
                .partition(|t| !self.registry.is_multi_trigger(&t.handler.handler_key()));

            let mut outcomes = self
                .executor
                .execute_handlers(
                    snapshot_tasks,
                    range_key.0,
                    range_key.1,
                    &DbExecMode::WithSnapshotCapture {
                        chain_name: self.chain_name.clone(),
                    },
                )
                .await;
            outcomes.extend(
                self.executor
                    .execute_handlers(direct_tasks, range_key.0, range_key.1, &DbExecMode::Direct)
                    .await,
            );

            let mut succeeded_counts: HashMap<String, usize> = HashMap::new();
            for o in &outcomes {
                *succeeded_counts.entry(o.handler_key.clone()).or_default() += 1;
            }
            let submitted_keys: HashSet<String> = submitted_counts.keys().cloned().collect();
            let succeeded_keys: HashSet<String> = submitted_counts
                .iter()
                .filter(|(k, &count)| succeeded_counts.get(*k).copied().unwrap_or(0) == count)
                .map(|(k, _)| k.clone())
                .collect();

            // Compute which succeeded keys are safe to persist now.
            // A handler's success is persistable only when:
            //  - it is not multi-trigger, OR logs_complete is true (all
            //    event batches dispatched), AND
            //  - it has no remaining pending entries for this range.
            let (persistable_success_keys, logs_complete) = {
                let state = self.live_state.lock().await;
                let logs_complete = state
                    .completion
                    .get(&range_key)
                    .map(|c| c.logs_complete)
                    .unwrap_or(false);
                let keys: HashSet<String> = succeeded_keys
                    .iter()
                    .filter(|key| {
                        let is_multi = self.registry.is_multi_trigger(key);
                        if is_multi && !logs_complete {
                            return false;
                        }
                        let has_remaining_pending = state
                            .pending_events
                            .get(*key)
                            .map(|entries| {
                                entries
                                    .iter()
                                    .any(|e| (e.range_start, e.range_end) == range_key)
                            })
                            .unwrap_or(false);
                        !has_remaining_pending
                    })
                    .cloned()
                    .collect();
                (keys, logs_complete)
            };

            for key in &persistable_success_keys {
                self.finalizer
                    .record_completed_range_for_handler(key, range_key.0, range_key.1)
                    .await?;
            }

            self.record_handler_outcomes(
                range_key.0,
                range_key.1,
                &submitted_keys,
                &succeeded_keys,
                &persistable_success_keys,
            )
            .await;

            // Record handler completions and failures for cascading.
            // A handler is only marked completed in-memory when:
            //  - it succeeded (all batches in this execution round)
            //  - for dep handlers: logs_complete is true (all event batches dispatched)
            //  - it has no remaining pending entries for this range
            //  - it is not already failed
            let newly_failed_names: Vec<String> = {
                let dep_names = self.registry.dependency_handler_names();
                let mut state = self.live_state.lock().await;
                let failed_names: HashSet<String> = state
                    .failed_handlers
                    .get(&range_key)
                    .cloned()
                    .unwrap_or_default();
                for outcome in &outcomes {
                    let has_remaining_pending = state
                        .pending_events
                        .get(&outcome.handler_key)
                        .map(|entries| {
                            entries
                                .iter()
                                .any(|e| (e.range_start, e.range_end) == range_key)
                        })
                        .unwrap_or(false);
                    if succeeded_keys.contains(&outcome.handler_key)
                        && (logs_complete || !dep_names.contains(&outcome.handler_name))
                        && !has_remaining_pending
                        && !failed_names.contains(&outcome.handler_name)
                    {
                        state
                            .completed_handlers
                            .entry(range_key)
                            .or_default()
                            .insert(outcome.handler_name.clone());
                    }
                }
                let mut newly_failed_names: Vec<String> = Vec::new();
                for key in submitted_keys.difference(&succeeded_keys) {
                    if let Some(name) = self.registry.handler_name_for_key(key) {
                        state
                            .failed_handlers
                            .entry(range_key)
                            .or_default()
                            .insert(name.to_string());
                        newly_failed_names.push(name.to_string());
                    }
                }
                newly_failed_names
            };

            self.cascade_handler_failures(&newly_failed_names, range_key)
                .await;

            // Loop to check if more handlers are now unblocked
        }
    }

    /// Cascade failures from newly-failed handlers to all their transitive
    /// dependents.  Marks dependents as failed in `state`, removes their
    /// pending events, and persists the cascaded failures to the status file
    /// for single-block (live) ranges.
    ///
    /// `newly_failed_names` should contain the handler `name()` strings that
    /// just failed — **not** handler keys.
    ///
    /// Must be called while the caller does **not** hold the `live_state` lock.
    async fn cascade_handler_failures(&self, newly_failed_names: &[String], range_key: (u64, u64)) {
        if newly_failed_names.is_empty() {
            return;
        }

        let mut cascaded: HashSet<String> = HashSet::new();
        for failed_name in newly_failed_names {
            for dep_name in self.registry.transitive_dependents_of(failed_name) {
                cascaded.insert(dep_name);
            }
        }

        let newly_failed_keys: Vec<String> = newly_failed_names
            .iter()
            .filter_map(|name| self.registry.handler_key_for_name(name).map(str::to_string))
            .collect();
        let cascaded_keys: Vec<String> = cascaded
            .iter()
            .filter_map(|name| {
                self.registry
                    .handler_key_for_name(name)
                    .map(|k| k.to_string())
            })
            .collect();

        {
            let mut state = self.live_state.lock().await;
            for key in &newly_failed_keys {
                let remove_entry = if let Some(pending) = state.pending_events.get_mut(key) {
                    pending.retain(|p| (p.range_start, p.range_end) != range_key);
                    pending.is_empty()
                } else {
                    false
                };
                if remove_entry {
                    state.pending_events.remove(key);
                }
                state
                    .pending_event_timestamps
                    .remove(&(range_key.0, range_key.1, key.clone()));
            }

            for name in &cascaded {
                state
                    .failed_handlers
                    .entry(range_key)
                    .or_default()
                    .insert(name.clone());
                if let Some(completed) = state.completed_handlers.get_mut(&range_key) {
                    completed.remove(name);
                }
            }

            for key in &cascaded_keys {
                if let Some(pending) = state.pending_events.get_mut(key) {
                    pending.retain(|p| (p.range_start, p.range_end) != range_key);
                    if pending.is_empty() {
                        state.pending_events.remove(key);
                    }
                }
                state
                    .pending_event_timestamps
                    .remove(&(range_key.0, range_key.1, key.clone()));
            }
        }

        if cascaded.is_empty() {
            return;
        }

        tracing::warn!(
            "Cascaded failure to {} dependent handler(s) for block {}: {:?}",
            cascaded.len(),
            range_key.0,
            cascaded
        );

        // Persist cascaded failures to status file for single-block (live) ranges.
        if range_key.1 - range_key.0 == 1 {
            let cascaded_key_set: HashSet<String> = cascaded_keys.into_iter().collect();
            let storage = LiveStorage::new(&self.chain_name);
            if let Err(e) = storage.update_status_atomic(range_key.0, |status| {
                status
                    .failed_handlers
                    .extend(cascaded_key_set.iter().cloned());
                for k in &cascaded_key_set {
                    status.completed_handlers.remove(k);
                }
                status.transformed = false;
            }) {
                if !matches!(e, StorageError::NotFound(_)) {
                    tracing::warn!(
                        "Failed to persist cascaded failures for block {}: {}",
                        range_key.0,
                        e
                    );
                }
            }
        }
    }

    /// Build WorkItems and a `handler_name → handler_key` map for a single
    /// `(range_start, range_end)` execution via the [`DagScheduler`].
    ///
    /// Deduplicates by `handler_key` so multi-trigger handlers produce exactly
    /// one `WorkItem` even when several triggers match the same block. Each
    /// `WorkItem` carries `dep_names` derived from `handler_dependencies()` so
    /// the scheduler gates execution on those deps completing first.
    fn build_process_range_items(
        &self,
        event_triggers: &[(String, String)],
        call_triggers: &[(String, String)],
        account_state_triggers: &[(String, String)],
        events: Arc<Vec<DecodedEvent>>,
        calls: Arc<Vec<DecodedCall>>,
        account_states: Arc<Vec<DecodedAccountState>>,
        tx_addresses: HashMap<[u8; 32], TransactionAddresses>,
        range_start: u64,
        range_end: u64,
        snapshot_chain: Option<String>,
    ) -> (Vec<WorkItem>, HashMap<String, String>) {
        let mut items: Vec<WorkItem> = Vec::new();
        let mut seen_keys: HashSet<String> = HashSet::new();
        let mut name_to_key: HashMap<String, String> = HashMap::new();

        for (source, event_name) in event_triggers {
            for handler in self.registry.handlers_for_event(source, event_name) {
                // Collect dep_names from EventHandler before upcasting — the
                // method lives on EventHandler, not on TransformationHandler.
                let dep_names: Vec<String> = handler
                    .handler_dependencies()
                    .iter()
                    .map(|s: &&str| s.to_string())
                    .collect();
                let handler: Arc<dyn super::traits::TransformationHandler> = handler;
                let key = handler.handler_key();
                if !seen_keys.insert(key.clone()) {
                    continue;
                }
                let name = handler.name().to_string();
                name_to_key.insert(name.clone(), key);
                items.push(WorkItem {
                    handler_name: name,
                    range_start,
                    range_end,
                    dep_names,
                    sequential: false,
                    payload: Box::new(ProcessRangePayload {
                        handler,
                        events: events.clone(),
                        calls: calls.clone(),
                        account_states: account_states.clone(),
                        tx_addresses: tx_addresses.clone(),
                        snapshot_chain: snapshot_chain.clone(),
                    }),
                });
            }
        }

        for (source, function_name) in call_triggers {
            for handler in self.registry.handlers_for_call(source, function_name) {
                // EthCallHandler has no handler_dependencies; call handlers
                // never declare handler-level ordering constraints.
                let handler: Arc<dyn super::traits::TransformationHandler> = handler;
                let key = handler.handler_key();
                if !seen_keys.insert(key.clone()) {
                    continue;
                }
                let name = handler.name().to_string();
                name_to_key.insert(name.clone(), key);
                items.push(WorkItem {
                    handler_name: name,
                    range_start,
                    range_end,
                    dep_names: vec![],
                    sequential: false,
                    payload: Box::new(ProcessRangePayload {
                        handler,
                        events: events.clone(),
                        calls: calls.clone(),
                        account_states: account_states.clone(),
                        tx_addresses: tx_addresses.clone(),
                        snapshot_chain: snapshot_chain.clone(),
                    }),
                });
            }
        }

        for (source, account_type) in account_state_triggers {
            for handler in self
                .registry
                .handlers_for_account_state(source, account_type)
            {
                let handler: Arc<dyn super::traits::TransformationHandler> = handler;
                let key = handler.handler_key();
                if !seen_keys.insert(key.clone()) {
                    continue;
                }
                let name = handler.name().to_string();
                name_to_key.insert(name.clone(), key);
                items.push(WorkItem {
                    handler_name: name,
                    range_start,
                    range_end,
                    dep_names: vec![],
                    sequential: false,
                    payload: Box::new(ProcessRangePayload {
                        handler,
                        events: events.clone(),
                        calls: calls.clone(),
                        account_states: account_states.clone(),
                        tx_addresses: tx_addresses.clone(),
                        snapshot_chain: snapshot_chain.clone(),
                    }),
                });
            }
        }

        (items, name_to_key)
    }

    /// Process a block range with dep-aware concurrent per-handler transactions.
    ///
    /// Routes all triggered handlers through the [`DagScheduler`], which gates
    /// each handler on its `handler_dependencies()` completing first. Replaces
    /// the old `HandlerExecutor::execute_handlers` call that ran handlers in
    /// parallel with no dependency ordering, which was incorrect for handlers
    /// with `handler_dependencies` in the retry/reorg path.
    async fn process_range(
        &self,
        range_start: u64,
        range_end: u64,
        events: Vec<DecodedEvent>,
        calls: Vec<DecodedCall>,
        account_states: Vec<DecodedAccountState>,
    ) -> Result<(), TransformationError> {
        tracing::debug!(
            "Processing range {}-{} with {} events, {} calls, and {} account states",
            range_start,
            range_end,
            events.len(),
            calls.len(),
            account_states.len()
        );

        let event_triggers: Vec<(String, String)> = events
            .iter()
            .map(|e| (e.source_name.clone(), e.event_name.clone()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let call_triggers: Vec<(String, String)> = calls
            .iter()
            .map(|c| (c.source_name.clone(), c.function_name.clone()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let account_state_triggers: Vec<(String, String)> = account_states
            .iter()
            .map(|account_state| {
                (
                    account_state.source_name.clone(),
                    account_state.account_type.clone(),
                )
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let tx_addresses = self.read_receipt_addresses(range_start, range_end).await;
        let events = Arc::new(events);
        let calls = Arc::new(calls);
        let account_states = Arc::new(account_states);

        let is_live_mode = range_end - range_start == 1;
        let snapshot_chain = if is_live_mode {
            Some(self.chain_name.clone())
        } else {
            None
        };

        let (items, name_to_key) = self.build_process_range_items(
            &event_triggers,
            &call_triggers,
            &account_state_triggers,
            events,
            calls,
            account_states,
            tx_addresses,
            range_start,
            range_end,
            snapshot_chain,
        );

        if items.is_empty() {
            return Ok(());
        }

        let submitted_keys: HashSet<String> = name_to_key.values().cloned().collect();

        // Fresh tracker per call: single range. Dep handlers that have no
        // triggers in this batch are seeded as completed so their dependents
        // don't hang waiting on a mark_completed that will never arrive.
        let tracker = Arc::new(CompletionTracker::new());
        {
            let item_names: HashSet<&str> = items.iter().map(|i| i.handler_name.as_str()).collect();
            let missing_deps: HashSet<&str> = items
                .iter()
                .flat_map(|i| i.dep_names.iter().map(|d| d.as_str()))
                .filter(|dep| !item_names.contains(dep))
                .collect();
            for dep in missing_deps {
                tracker.seed_completed(dep, [range_start]).await;
            }
        }
        let scheduler = DagScheduler::new(tracker, self.handler_concurrency);

        let db_pool = self.db_pool.clone();
        let historical = self.historical_reader.clone();
        let rpc_client = self.executor.rpc_client.clone();
        let contracts = self.contracts.clone();
        let chain_name = self.chain_name.clone();
        let chain_id = self.chain_id;

        let outcomes = scheduler
            .execute(items, move |item: WorkItem| {
                let db_pool = db_pool.clone();
                let historical = historical.clone();
                let rpc_client = rpc_client.clone();
                let contracts = contracts.clone();
                let chain_name = chain_name.clone();
                Box::pin(async move {
                    let WorkItem {
                        range_start,
                        range_end,
                        payload,
                        ..
                    } = item;
                    let payload = *payload
                        .downcast::<ProcessRangePayload>()
                        .expect("process_range WorkItem payload type mismatch");
                    let db_exec_mode = match payload.snapshot_chain {
                        Some(ref cn) => DbExecMode::WithSnapshotCapture {
                            chain_name: cn.clone(),
                        },
                        None => DbExecMode::Direct,
                    };
                    run_handler_task(
                        payload.handler,
                        payload.events,
                        payload.calls,
                        payload.account_states,
                        payload.tx_addresses,
                        chain_name,
                        chain_id,
                        range_start,
                        range_end,
                        historical,
                        rpc_client,
                        contracts,
                        db_pool,
                        &db_exec_mode,
                    )
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
                })
            })
            .await;

        let succeeded_keys: HashSet<String> = outcomes
            .iter()
            .filter(|o| matches!(o.status, OutcomeStatus::Succeeded))
            .filter_map(|o| name_to_key.get(&o.handler_name).cloned())
            .collect();

        // Only persist success for single-trigger handlers.  Multi-trigger
        // call handlers (e.g. PriceHandler) are deferred to finalize_range()
        // because process_calls_message dispatches one call-trigger batch at
        // a time.
        let persistable_success_keys: HashSet<String> = succeeded_keys
            .iter()
            .filter(|k| !self.registry.is_multi_trigger(k))
            .cloned()
            .collect();

        for key in &persistable_success_keys {
            self.finalizer
                .record_completed_range_for_handler(key, range_start, range_end)
                .await?;
        }

        self.record_handler_outcomes(
            range_start,
            range_end,
            &submitted_keys,
            &succeeded_keys,
            &persistable_success_keys,
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

fn call_dependency_contract_index_complete(
    raw_index_dir: &Path,
    source: &str,
    range_start: u64,
    range_end_exclusive: u64,
    expected_factory_contracts: &HashMap<String, ExpectedContracts>,
) -> bool {
    if !raw_index_dir.join("contract_index.json").exists() {
        // Backward-compatible fallback for older ranges that predate the sidecar.
        return true;
    }

    let Some(expected) = expected_factory_contracts.get(source) else {
        return true;
    };

    let index = read_contract_index(raw_index_dir);
    let rk = range_key(range_start, range_end_exclusive - 1);
    get_missing_contracts(&index, &rk, expected).is_empty()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::tempdir;

    use super::call_dependency_contract_index_complete;
    use crate::storage::contract_index::{range_key, update_contract_index, write_contract_index};

    #[test]
    fn contract_index_gate_defaults_open_without_sidecar() {
        let dir = tempdir().unwrap();
        let expected = HashMap::from([(
            "DERC20".to_string(),
            HashMap::from([(
                "Airlock".to_string(),
                vec!["0x660eaaedebc968f8f3694354fa8ec0b4c5ba8d12".to_string()],
            )]),
        )]);

        assert!(call_dependency_contract_index_complete(
            dir.path(),
            "DERC20",
            100,
            200,
            &expected
        ));
    }

    #[test]
    fn contract_index_gate_blocks_until_range_coverage_is_complete() {
        let dir = tempdir().unwrap();
        let expected_for_source = HashMap::from([(
            "Airlock".to_string(),
            vec!["0x660eaaedebc968f8f3694354fa8ec0b4c5ba8d12".to_string()],
        )]);
        let expected = HashMap::from([("DERC20".to_string(), expected_for_source.clone())]);

        let empty_index: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        write_contract_index(dir.path(), &empty_index).unwrap();
        assert!(!call_dependency_contract_index_complete(
            dir.path(),
            "DERC20",
            100,
            200,
            &expected
        ));

        let mut complete_index = HashMap::new();
        update_contract_index(
            &mut complete_index,
            &range_key(100, 199),
            &expected_for_source,
        );
        write_contract_index(dir.path(), &complete_index).unwrap();
        assert!(call_dependency_contract_index_complete(
            dir.path(),
            "DERC20",
            100,
            200,
            &expected
        ));
    }
}
