//! CatchupLoader: per-WorkItem data loading, handler invocation, and DB write.
//!
//! This module contains:
//! - Free functions shared by `CatchupLoader` and the engine's live-path methods
//!   (`read_decoded_parquet`, `read_receipt_addresses`).
//! - [`CatchupPayload`]: opaque payload carried in a [`WorkItem`].
//! - [`CatchupLoader`]: executes one [`WorkItem`] end-to-end.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::task::JoinSet;

use super::dag::WorkItem;
use crate::db::DbPool;
use crate::rpc::UnifiedRpcClient;
use crate::transformations::context::{DecodedCall, DecodedEvent, TransactionAddresses};
use crate::transformations::engine::HandlerKind;
use crate::transformations::error::TransformationError;
use crate::transformations::executor::{run_handler_task, DbExecMode};
use crate::transformations::finalizer::RangeFinalizer;
use crate::transformations::historical::HistoricalDataReader;
use crate::transformations::retry::{filter_calls_by_start_block, filter_events_by_start_block};
use crate::transformations::traits::TransformationHandler;
use crate::types::config::contract::Contracts;

// ─── Free functions ──────────────────────────────────────────────────────────

/// Generic parquet reader shared by CatchupLoader and engine's live-path methods.
///
/// Spawns one blocking task per file path returned by `resolve_paths`, collects
/// all items, and returns the merged result.
pub(crate) async fn read_decoded_parquet<T>(
    historical_reader: Arc<HistoricalDataReader>,
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
    let mut read_tasks: JoinSet<Result<Vec<T>, TransformationError>> = JoinSet::new();
    let read_fn = Arc::new(read_file);

    for (source_name, secondary_name) in triggers {
        let paths = resolve_paths(source_name, secondary_name);

        for file_path in paths {
            if !file_path.exists() {
                continue;
            }

            let reader = historical_reader.clone();
            let src = source_name.clone();
            let name = secondary_name.clone();
            let read_fn = read_fn.clone();

            read_tasks.spawn_blocking(move || {
                tracing::debug!("Reading decoded data from {}", file_path.display());
                match read_fn(reader, &file_path, &src, &name) {
                    Ok(items) => {
                        tracing::debug!("Read {} items from {}", items.len(), file_path.display());
                        Ok(items)
                    }
                    Err(e) => {
                        let is_not_found = matches!(
                            &e,
                            TransformationError::IoError(io)
                                if io.kind() == std::io::ErrorKind::NotFound
                        );
                        if is_not_found {
                            tracing::debug!("File not found, skipping: {}", file_path.display());
                            Ok(Vec::new())
                        } else {
                            tracing::warn!(
                                "Failed to read decoded data from {}: {}",
                                file_path.display(),
                                e
                            );
                            Err(e)
                        }
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

/// Read transaction addresses from a receipts parquet file.
///
/// Returns a map from tx_hash → `TransactionAddresses`. Returns an empty map
/// if the file is missing or cannot be read. The parquet I/O is offloaded to
/// a blocking thread via [`tokio::task::spawn_blocking`].
pub(crate) async fn read_receipt_addresses(
    raw_receipts_dir: &Path,
    range_start: u64,
    range_end: u64,
) -> HashMap<[u8; 32], TransactionAddresses> {
    let raw_receipts_dir = raw_receipts_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        read_receipt_addresses_sync(&raw_receipts_dir, range_start, range_end)
    })
    .await
    .unwrap_or_else(|e| {
        tracing::error!(
            "read_receipt_addresses panicked for range {}-{}: {}",
            range_start,
            range_end,
            e
        );
        HashMap::new()
    })
}

fn read_receipt_addresses_sync(
    raw_receipts_dir: &Path,
    range_start: u64,
    range_end: u64,
) -> HashMap<[u8; 32], TransactionAddresses> {
    use arrow::array::{Array, FixedSizeBinaryArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file_name = format!("receipts_{}-{}.parquet", range_start, range_end - 1);
    let file_path = raw_receipts_dir.join(&file_name);

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

// ─── CatchupPayload ──────────────────────────────────────────────────────────

/// Opaque payload carried inside a [`WorkItem`] for catchup execution.
pub(crate) struct CatchupPayload {
    pub handler: Arc<dyn TransformationHandler>,
    pub handler_key: String,
    pub handler_name: &'static str,
    pub handler_version: u32,
    pub triggers: Vec<(String, String)>,
    pub call_deps: Vec<(String, String)>,
    pub kind: HandlerKind,
}

// ─── CatchupLoader ───────────────────────────────────────────────────────────

/// Shared state needed to execute one [`WorkItem`] during catchup.
///
/// Constructed once per `run_handler_catchup` invocation and shared via `Arc`
/// across all spawned tasks inside [`DagScheduler::execute`].
pub(crate) struct CatchupLoader {
    pub decoded_logs_dir: PathBuf,
    pub decoded_calls_dir: PathBuf,
    pub raw_receipts_dir: PathBuf,
    pub historical_reader: Arc<HistoricalDataReader>,
    pub rpc_client: Arc<UnifiedRpcClient>,
    pub contracts: Arc<Contracts>,
    pub chain_name: String,
    pub chain_id: u64,
    pub db_pool: Arc<DbPool>,
    pub finalizer: Arc<RangeFinalizer>,
}

impl CatchupLoader {
    /// Load data, invoke handler, execute DB ops, and record progress for one [`WorkItem`].
    pub(crate) async fn run(&self, item: WorkItem) -> Result<(), TransformationError> {
        let range_start = item.range_start;
        let range_end = item.range_end;

        let payload = item
            .payload
            .downcast::<CatchupPayload>()
            .expect("CatchupPayload type mismatch");

        let handler_key = &payload.handler_key;

        let (events, calls) = match payload.kind {
            HandlerKind::Event => {
                let evts = self
                    .load_events(range_start, range_end, &payload.triggers)
                    .await?;
                let evts = filter_events_by_start_block(&self.contracts, evts);

                let calls = if !evts.is_empty() && !payload.call_deps.is_empty() {
                    let raw_calls = self
                        .load_calls(range_start, range_end, &payload.call_deps)
                        .await?;
                    filter_calls_by_start_block(&self.contracts, raw_calls)
                } else {
                    Vec::new()
                };

                (evts, calls)
            }
            HandlerKind::Call => {
                let raw_calls = self
                    .load_calls(range_start, range_end, &payload.triggers)
                    .await?;
                let calls = filter_calls_by_start_block(&self.contracts, raw_calls);
                (Vec::new(), calls)
            }
        };

        let has_data = match payload.kind {
            HandlerKind::Event => !events.is_empty(),
            HandlerKind::Call => !calls.is_empty(),
        };

        if !has_data {
            self.finalizer
                .record_completed_range_for_handler(handler_key, range_start, range_end)
                .await?;
            return Ok(());
        }

        let tx_addresses = match payload.kind {
            HandlerKind::Event => {
                read_receipt_addresses(&self.raw_receipts_dir, range_start, range_end).await
            }
            HandlerKind::Call => HashMap::new(),
        };

        run_handler_task(
            payload.handler,
            Arc::new(events),
            Arc::new(calls),
            tx_addresses,
            self.chain_name.clone(),
            self.chain_id,
            range_start,
            range_end,
            self.historical_reader.clone(),
            self.rpc_client.clone(),
            self.contracts.clone(),
            self.db_pool.clone(),
            &DbExecMode::Direct,
        )
        .await?;

        self.finalizer
            .record_completed_range_for_handler(handler_key, range_start, range_end)
            .await?;

        Ok(())
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    async fn load_events(
        &self,
        range_start: u64,
        range_end: u64,
        triggers: &[(String, String)],
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let logs_dir = self.decoded_logs_dir.clone();

        read_decoded_parquet(
            self.historical_reader.clone(),
            triggers,
            move |source, event| vec![logs_dir.join(source).join(event).join(&file_name)],
            |reader, path, src, evt| reader.read_events_from_file(path, src, evt),
        )
        .await
    }

    async fn load_calls(
        &self,
        range_start: u64,
        range_end: u64,
        triggers: &[(String, String)],
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let calls_dir = self.decoded_calls_dir.clone();

        read_decoded_parquet(
            self.historical_reader.clone(),
            triggers,
            move |source, function| {
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
}

// ─── CallDepScanner ────────────────────────────────────────────────────────

use std::collections::HashSet;

use crate::storage::contract_index::build_expected_factory_contracts_for_range;
use crate::transformations::engine::call_dependency_contract_index_complete;
use crate::transformations::scheduler::tracker::CompletionTracker;

/// Standalone call-dependency file scanner.
///
/// Periodically scans the decoded-calls directory for each unique `(source,
/// function)` pair to discover which parquet ranges are available on disk.
/// Registers newly-found ranges with the [`CompletionTracker`] so that
/// `wait_ready_extended` can unblock items whose call deps have arrived.
pub(crate) struct CallDepScanner {
    decoded_calls_dir: PathBuf,
    raw_eth_calls_dir: PathBuf,
    contracts: Arc<Contracts>,
    /// Unique `(source, function)` pairs to scan for.
    call_dep_pairs: Vec<(String, String)>,
}

impl CallDepScanner {
    pub(crate) fn new(
        decoded_calls_dir: PathBuf,
        raw_eth_calls_dir: PathBuf,
        contracts: Arc<Contracts>,
        call_dep_pairs: Vec<(String, String)>,
    ) -> Self {
        Self {
            decoded_calls_dir,
            raw_eth_calls_dir,
            contracts,
            call_dep_pairs,
        }
    }

    /// Scan all `(source, function)` pairs and return available ranges.
    pub(crate) async fn scan_all(
        &self,
    ) -> HashMap<(String, String), HashSet<(u64, u64)>> {
        let mut results = HashMap::new();
        for (source, function) in &self.call_dep_pairs {
            match self.scan_one(source, function).await {
                Ok(ranges) => {
                    if !ranges.is_empty() {
                        results.insert((source.clone(), function.clone()), ranges);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Call-dep scan failed for {}/{}: {}",
                        source,
                        function,
                        e
                    );
                }
            }
        }
        results
    }

    /// Scan one `(source, function)` pair for available decoded call parquet files.
    ///
    /// This is the same logic as `TransformationEngine::scan_available_call_dependency_ranges`
    /// but extracted to avoid borrowing `&self` across a spawn boundary.
    async fn scan_one(
        &self,
        source: &str,
        function_name: &str,
    ) -> Result<HashSet<(u64, u64)>, std::io::Error> {
        let source = source.to_string();
        let function_name = function_name.to_string();
        let decoded_base = self.decoded_calls_dir.join(&source).join(&function_name);
        let raw_base = self.raw_eth_calls_dir.join(&source).join(&function_name);
        let contracts = self.contracts.clone();

        tokio::task::spawn_blocking(move || {
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
        .map_err(|e| std::io::Error::other(e.to_string()))?
    }
}

/// Background scanner loop that periodically discovers call-dep files and
/// registers them with the tracker.
///
/// The loop runs until the `cancel` watch receives `true`, or the task is
/// aborted. Callers typically abort the returned `JoinHandle` when catchup
/// finishes.
pub(crate) async fn run_call_dep_scanner_loop(
    scanner: CallDepScanner,
    tracker: Arc<CompletionTracker>,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let results = scanner.scan_all().await;
        for ((source, func), ranges) in results {
            tracker.register_call_dep_ranges(&source, &func, ranges).await;
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {},
            _ = cancel.changed() => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::dag::{DagScheduler, OutcomeStatus, WorkItem, WorkItemRunResult};
    use super::super::tracker::CompletionTracker;
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;

    /// (handler_name, range_start, kind) — kind is "start" or "end".
    type EventLog = Arc<TokioMutex<Vec<(String, u64, &'static str)>>>;

    struct Recorder {
        events: EventLog,
        fail_on: HashSet<(String, u64)>,
        hold_ms: u64,
    }

    impl Recorder {
        fn new(hold_ms: u64) -> Arc<Self> {
            Arc::new(Self {
                events: Arc::new(TokioMutex::new(Vec::new())),
                fail_on: HashSet::new(),
                hold_ms,
            })
        }

        fn with_fails(hold_ms: u64, fail_on: &[(&str, u64)]) -> Arc<Self> {
            Arc::new(Self {
                events: Arc::new(TokioMutex::new(Vec::new())),
                fail_on: fail_on.iter().map(|(n, r)| (n.to_string(), *r)).collect(),
                hold_ms,
            })
        }

        fn runner(
            self: &Arc<Self>,
        ) -> impl Fn(WorkItem) -> std::pin::Pin<Box<dyn Future<Output = WorkItemRunResult> + Send>>
               + Send
               + Sync
               + Clone
               + 'static {
            let rec = self.clone();
            move |item: WorkItem| {
                let rec = rec.clone();
                Box::pin(async move {
                    let key = (item.handler_name.clone(), item.range_start);
                    rec.events.lock().await.push((
                        item.handler_name.clone(),
                        item.range_start,
                        "start",
                    ));

                    if rec.hold_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(rec.hold_ms)).await;
                    }

                    rec.events.lock().await.push((
                        item.handler_name.clone(),
                        item.range_start,
                        "end",
                    ));

                    if rec.fail_on.contains(&key) {
                        WorkItemRunResult::Failed(format!(
                            "test-forced failure at {}:{}",
                            key.0, key.1
                        ))
                    } else {
                        WorkItemRunResult::Succeeded
                    }
                })
            }
        }
    }

    fn item(name: &str, range_start: u64, deps: &[&str]) -> WorkItem {
        WorkItem {
            handler_name: name.to_string(),
            range_start,
            range_end: range_start + 1000,
            dep_names: deps.iter().map(|s| s.to_string()).collect(),
            contiguous_dep_names: Vec::new(),
            call_dep_keys: Vec::new(),
            sequential: false,
            payload: Box::new(()),
        }
    }

    fn idx_start(events: &[(String, u64, &'static str)], name: &str, range: u64) -> usize {
        events
            .iter()
            .position(|(n, r, k)| n == name && *r == range && *k == "start")
            .unwrap_or_else(|| panic!("no start event for {}:{}", name, range))
    }

    fn idx_end(events: &[(String, u64, &'static str)], name: &str, range: u64) -> usize {
        events
            .iter()
            .position(|(n, r, k)| n == name && *r == range && *k == "end")
            .unwrap_or_else(|| panic!("no end event for {}:{}", name, range))
    }

    /// Simulates the engine's multi-pass build-items-with-deferral logic.
    ///
    /// Each "handler descriptor" is (name, handler_deps, has_call_deps).
    /// `call_deps_ready` is the set of range_starts whose call-dep files
    /// are available this pass.
    fn build_items_with_deferral(
        handlers: &[(&str, &[&str], bool)],
        candidate_ranges: &[(u64, u64)],
        call_deps_ready: &HashSet<u64>,
        completed: &HashMap<String, HashSet<u64>>,
    ) -> (Vec<WorkItem>, HashMap<String, Vec<(u64, u64)>>) {
        let mut items = Vec::new();
        let mut next_pending: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
        let mut deferred_starts: HashMap<String, HashSet<u64>> = HashMap::new();

        for &(name, handler_deps, has_call_deps) in handlers {
            let handler_completed = completed.get(name).cloned().unwrap_or_default();

            for &(range_start, range_end) in candidate_ranges {
                if handler_completed.contains(&range_start) {
                    continue;
                }

                // Check call-dep readiness.
                if has_call_deps && !call_deps_ready.contains(&range_start) {
                    next_pending
                        .entry(name.to_string())
                        .or_default()
                        .push((range_start, range_end));
                    deferred_starts
                        .entry(name.to_string())
                        .or_default()
                        .insert(range_start);
                    continue;
                }

                // Cascade: if any handler dep is deferred this pass, defer self.
                if let Some(_blocking) = handler_deps.iter().find(|dep| {
                    deferred_starts
                        .get(**dep)
                        .is_some_and(|starts| starts.contains(&range_start))
                }) {
                    next_pending
                        .entry(name.to_string())
                        .or_default()
                        .push((range_start, range_end));
                    deferred_starts
                        .entry(name.to_string())
                        .or_default()
                        .insert(range_start);
                    continue;
                }

                items.push(item(name, range_start, handler_deps));
            }
        }

        (items, next_pending)
    }

    /// Multi-pass catchup with call-dep deferral and cascade.
    ///
    /// Handlers: A (has call_deps), B (depends on A), C (independent).
    /// Pass 1: call-deps not ready for ranges 100, 101 → A and B deferred,
    ///         A:102 and B:102 submitted (B waits for A via tracker), C all submitted.
    /// Pass 2: call-deps now ready → A:100,101 and B:100,101 submitted.
    #[tokio::test]
    async fn catchup_multi_pass_with_call_dep_deferral() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker.clone(), 8);
        let rec = Recorder::new(5);

        // Handlers in topological order: A first, then B (depends on A), then C.
        let handlers: Vec<(&str, &[&str], bool)> = vec![
            ("A", &[], true),     // has call_deps
            ("B", &["A"], false), // depends on A, no call_deps
            ("C", &[], false),    // independent
        ];
        let ranges: Vec<(u64, u64)> = vec![(100, 1100), (101, 1101), (102, 1102)];
        let completed: HashMap<String, HashSet<u64>> = HashMap::new();

        // Pass 1: call-deps ready only for range 102.
        let call_deps_ready: HashSet<u64> = [102].into_iter().collect();
        let (items, next_pending) =
            build_items_with_deferral(&handlers, &ranges, &call_deps_ready, &completed);

        // Verify deferral: A:100, A:101 deferred; B:100, B:101 cascade-deferred.
        assert!(next_pending.contains_key("A"));
        assert!(next_pending.contains_key("B"));
        assert!(!next_pending.contains_key("C"));
        assert_eq!(next_pending["A"].len(), 2);
        assert_eq!(next_pending["B"].len(), 2);

        // Items submitted: A:102, B:102, C:100, C:101, C:102 = 5 items.
        assert_eq!(items.len(), 5);
        let item_keys: HashSet<(String, u64)> = items
            .iter()
            .map(|i| (i.handler_name.clone(), i.range_start))
            .collect();
        assert!(item_keys.contains(&("A".to_string(), 102)));
        assert!(item_keys.contains(&("B".to_string(), 102)));
        assert!(item_keys.contains(&("C".to_string(), 100)));
        assert!(item_keys.contains(&("C".to_string(), 101)));
        assert!(item_keys.contains(&("C".to_string(), 102)));

        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 5);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded, "{:?}", o);
        }

        // Verify ordering: A:102 finishes before B:102 starts.
        let events = rec.events.lock().await.clone();
        assert!(idx_end(&events, "A", 102) < idx_start(&events, "B", 102));

        // Update completed from pass 1 outcomes.
        let mut completed = completed;
        for o in &outcomes {
            completed
                .entry(o.handler_name.clone())
                .or_default()
                .insert(o.range_start);
        }

        // Pass 2: all call-deps ready now.
        let call_deps_ready_all: HashSet<u64> = [100, 101, 102].into_iter().collect();
        let pending_ranges: Vec<(u64, u64)> = next_pending
            .values()
            .flatten()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        // Build only for pending handlers.
        let pending_handlers: Vec<(&str, &[&str], bool)> = handlers
            .iter()
            .filter(|(name, _, _)| next_pending.contains_key(*name))
            .copied()
            .collect();

        let (items2, next_pending2) = build_items_with_deferral(
            &pending_handlers,
            &pending_ranges,
            &call_deps_ready_all,
            &completed,
        );

        assert!(next_pending2.is_empty(), "all should be submitted now");
        assert_eq!(items2.len(), 4); // A:100, A:101, B:100, B:101

        let rec2 = Recorder::new(5);
        let outcomes2 = scheduler.execute(items2, rec2.runner()).await;
        assert_eq!(outcomes2.len(), 4);
        for o in &outcomes2 {
            assert_eq!(o.status, OutcomeStatus::Succeeded, "{:?}", o);
        }

        // Verify ordering in pass 2: A finishes before B for each range.
        let events2 = rec2.events.lock().await.clone();
        assert!(idx_end(&events2, "A", 100) < idx_start(&events2, "B", 100));
        assert!(idx_end(&events2, "A", 101) < idx_start(&events2, "B", 101));
    }

    /// Diamond DAG with cascade failure across ranges.
    ///
    /// A → B, A → C, B → D, C → D. A fails on range 101.
    /// Ranges 100 and 102 should all succeed. Range 101: B, C, D cascade-fail.
    #[tokio::test]
    async fn catchup_cascade_failure_across_ranges() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 8);
        let rec = Recorder::with_fails(5, &[("A", 101)]);

        let handlers: Vec<(&str, &[&str], bool)> = vec![
            ("A", &[], false),
            ("B", &["A"], false),
            ("C", &["A"], false),
            ("D", &["B", "C"], false),
        ];
        let ranges: Vec<(u64, u64)> = vec![(100, 1100), (101, 1101), (102, 1102)];
        let completed: HashMap<String, HashSet<u64>> = HashMap::new();
        let all_ready: HashSet<u64> = [100, 101, 102].into_iter().collect();

        let (items, next_pending) =
            build_items_with_deferral(&handlers, &ranges, &all_ready, &completed);
        assert!(next_pending.is_empty());
        assert_eq!(items.len(), 12); // 4 handlers × 3 ranges

        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 12);

        let get = |name: &str, r: u64| {
            outcomes
                .iter()
                .find(|o| o.handler_name == name && o.range_start == r)
                .cloned()
                .unwrap_or_else(|| panic!("no outcome for {}:{}", name, r))
        };

        // Range 100: all succeed.
        assert_eq!(get("A", 100).status, OutcomeStatus::Succeeded);
        assert_eq!(get("B", 100).status, OutcomeStatus::Succeeded);
        assert_eq!(get("C", 100).status, OutcomeStatus::Succeeded);
        assert_eq!(get("D", 100).status, OutcomeStatus::Succeeded);

        // Range 101: A fails, B/C/D cascade.
        assert!(matches!(
            get("A", 101).status,
            OutcomeStatus::HandlerFailed { .. }
        ));
        assert_eq!(
            get("B", 101).status,
            OutcomeStatus::DepCascadeFailed {
                dep_name: "A".to_string()
            }
        );
        assert_eq!(
            get("C", 101).status,
            OutcomeStatus::DepCascadeFailed {
                dep_name: "A".to_string()
            }
        );
        // D:101 cascades from B or C (whichever the tracker reports first).
        assert!(matches!(
            get("D", 101).status,
            OutcomeStatus::DepCascadeFailed { .. }
        ));

        // Range 102: all succeed.
        assert_eq!(get("A", 102).status, OutcomeStatus::Succeeded);
        assert_eq!(get("B", 102).status, OutcomeStatus::Succeeded);
        assert_eq!(get("C", 102).status, OutcomeStatus::Succeeded);
        assert_eq!(get("D", 102).status, OutcomeStatus::Succeeded);
    }
}
