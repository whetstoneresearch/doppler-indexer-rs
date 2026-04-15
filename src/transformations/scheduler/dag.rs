//! Pipelined DAG scheduler — executes [`WorkItem`]s with per-range dependency gating.
//!
//! See module docs in [`super`]. Each item is spawned as an independent Tokio
//! task. Each task:
//!
//!   1. awaits [`CompletionTracker::wait_ready`] on its `(dep_names, range_start)`,
//!   2. acquires a permit from the global concurrency semaphore,
//!   3. invokes a caller-supplied runner closure with the item,
//!   4. marks itself completed, blocked, or failed on the tracker, waking dependents.
//!
//! Waiting happens *before* permit acquisition to prevent a permit-deadlock
//! where all permits are held by waiters.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::{watch, Mutex, Semaphore};
use tokio::task::{Id, JoinSet};

use super::tracker::CompletionTracker;

/// One atomic unit of work: run one handler on one range.
///
/// The `payload` is opaque to the scheduler — the caller supplies a runner
/// closure that downcasts it to its concrete type. This keeps the scheduler
/// decoupled from handler traits, DB types, and context construction.
pub(crate) struct WorkItem {
    pub handler_name: String,
    pub range_start: u64,
    pub range_end: u64,
    pub dep_names: Arc<Vec<String>>,
    /// Handler names whose contiguous watermark must be >= `range_start` before
    /// this item can execute. Used for `contiguous_handler_dependencies`.
    pub contiguous_dep_names: Arc<Vec<String>>,
    /// `(source, function)` pairs whose decoded call parquet files must be
    /// available on disk for `(range_start, range_end)` before this item can
    /// execute. Empty if no call deps or no trigger data in this range.
    pub call_dep_keys: Arc<Vec<(String, String)>>,
    /// When `true`, the scheduler enforces one-at-a-time FIFO execution for this
    /// handler via a per-handler capacity-1 semaphore. Items must be submitted in
    /// ascending `range_start` order for the FIFO guarantee to hold.
    pub sequential: bool,
    pub payload: Box<dyn Any + Send + Sync>,
}

/// Terminal result of one [`WorkItem`] execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkItemOutcome {
    pub handler_name: String,
    pub range_start: u64,
    pub range_end: u64,
    pub status: OutcomeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutcomeStatus {
    /// Runner returned `Ok(())`.
    Succeeded,
    /// Runner returned a transient blocked result; item should be retried.
    Blocked { reason: String },
    /// Runner returned `Err(reason)`.
    HandlerFailed { reason: String },
    /// A dependency was marked blocked, so this item was cascade-blocked.
    DepCascadeBlocked { dep_name: String },
    /// A dependency was marked failed, so this item was cascade-skipped.
    DepCascadeFailed { dep_name: String },
    /// Runner panicked or the task was cancelled.
    Panicked,
}

/// Runner result for one work item execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkItemRunResult {
    Succeeded,
    Failed(String),
    Blocked(String),
}

/// Per-handler sequential execution state.
///
/// Uses a capacity-1 semaphore for mutual exclusion and a watch channel to
/// enforce strict ascending range_start ordering. The watch tracks how many
/// ranges have completed; each task knows its ordinal and waits its turn.
struct HandlerSequentialState {
    semaphore: Arc<Semaphore>,
    blocked_range: Arc<Mutex<Option<u64>>>,
    /// Sorted range_starts for this handler (ascending order).
    sorted_ranges: Arc<Vec<u64>>,
    /// Broadcasts the index of the next range allowed to execute.
    /// Tasks wait until this equals their position in `sorted_ranges`.
    next_index: watch::Sender<usize>,
}

/// Pure DAG scheduler — owns the [`CompletionTracker`] and a concurrency semaphore.
///
/// DB writes, handler invocation, and context construction all live in the
/// runner closure passed to [`execute`]. Phase 2 wires a catchup runner on
/// top of this; tests use a trivial recording runner.
///
/// [`execute`]: DagScheduler::execute
pub(crate) struct DagScheduler {
    tracker: Arc<CompletionTracker>,
    global_permits: Arc<Semaphore>,
}

impl DagScheduler {
    pub(crate) fn new(tracker: Arc<CompletionTracker>, concurrency: usize) -> Self {
        Self {
            tracker,
            global_permits: Arc::new(Semaphore::new(concurrency.max(1))),
        }
    }

    /// Spawn one task per item, returning an outcome for every item.
    ///
    /// Items whose deps are already satisfied start immediately (modulo the
    /// concurrency bound). Items with unsatisfied deps park in
    /// [`CompletionTracker::wait_ready`] until their deps complete, block, or fail.
    /// Cascade failures/blocks short-circuit downstream items with
    /// [`OutcomeStatus::DepCascadeFailed`] / [`OutcomeStatus::DepCascadeBlocked`].
    pub(crate) async fn execute<R, Fut>(
        &self,
        items: Vec<WorkItem>,
        runner: R,
    ) -> Vec<WorkItemOutcome>
    where
        R: Fn(WorkItem) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = WorkItemRunResult> + Send + 'static,
    {
        if items.is_empty() {
            return Vec::new();
        }

        // Build per-handler sequential state. Collect and sort range_starts per
        // handler to enable deterministic ordering via watch channel.
        let seq_state: Arc<HashMap<String, HandlerSequentialState>> = {
            let mut ranges_per_handler: HashMap<String, Vec<u64>> = HashMap::new();
            for item in &items {
                if item.sequential {
                    ranges_per_handler
                        .entry(item.handler_name.clone())
                        .or_default()
                        .push(item.range_start);
                }
            }
            let mut m = HashMap::new();
            for (name, mut ranges) in ranges_per_handler {
                ranges.sort_unstable();
                ranges.dedup();
                let (tx, _rx) = watch::channel(0usize);
                m.insert(
                    name,
                    HandlerSequentialState {
                        semaphore: Arc::new(Semaphore::new(1)),
                        blocked_range: Arc::new(Mutex::new(None)),
                        sorted_ranges: Arc::new(ranges),
                        next_index: tx,
                    },
                );
            }
            Arc::new(m)
        };

        let mut join_set: JoinSet<WorkItemOutcome> = JoinSet::new();
        // Tracks identity per Tokio task ID so we can report a full outcome
        // even if the task panics (JoinError carries no user payload).
        let mut identity: HashMap<Id, (String, u64, u64)> = HashMap::new();

        for item in items {
            let name = item.handler_name.clone();
            let range_start = item.range_start;
            let range_end = item.range_end;
            let dep_names = item.dep_names.clone();
            let contiguous_dep_names = item.contiguous_dep_names.clone();
            let call_dep_keys = item.call_dep_keys.clone();
            let tracker = self.tracker.clone();
            let permits = self.global_permits.clone();
            let seq_state = seq_state.clone();
            let runner = runner.clone();

            // Data captured by the spawned task:
            let name_for_task = name.clone();
            let handle = join_set.spawn(async move {
                // 1. Gate on dependencies (handler deps + contiguous deps + call deps).
                if let Err(dep_wait) = tracker
                    .wait_ready_extended(
                        &dep_names,
                        &contiguous_dep_names,
                        &call_dep_keys,
                        range_start,
                    )
                    .await
                {
                    match dep_wait {
                        super::tracker::DepWaitError::DepFailed { dep_name, .. } => {
                            // Cascade: our own dependents must also short-circuit.
                            tracker.mark_failed(&name_for_task, range_start).await;
                            return WorkItemOutcome {
                                handler_name: name_for_task,
                                range_start,
                                range_end,
                                status: OutcomeStatus::DepCascadeFailed { dep_name },
                            };
                        }
                        super::tracker::DepWaitError::DepBlocked { dep_name, .. } => {
                            tracker.mark_blocked(&name_for_task, range_start).await;
                            return WorkItemOutcome {
                                handler_name: name_for_task,
                                range_start,
                                range_end,
                                status: OutcomeStatus::DepCascadeBlocked { dep_name },
                            };
                        }
                    }
                }

                // 2. Per-handler sequential gate.
                //    Uses a watch channel to enforce strict ascending range_start
                //    ordering, then acquires a capacity-1 semaphore for mutual
                //    exclusion. The watch ensures tasks execute in range_start
                //    order regardless of dep-wait wake ordering.
                let handler_seq = seq_state.get(&name_for_task);
                if let Some(state) = handler_seq {
                    // Find our ordinal position in the sorted range list.
                    let my_index = state
                        .sorted_ranges
                        .binary_search(&range_start)
                        .unwrap_or(0);

                    // Wait until it's our turn.
                    let mut rx = state.next_index.subscribe();
                    loop {
                        if *rx.borrow() >= my_index {
                            break;
                        }
                        if rx.changed().await.is_err() {
                            break; // Sender dropped, proceed anyway.
                        }
                    }

                    // Check for earlier blocked range.
                    let blocking_range = *state.blocked_range.lock().await;
                    if let Some(blocking_range) = blocking_range.filter(|r| *r < range_start) {
                        let reason = format!(
                            "waiting on earlier blocked range {} before processing {}",
                            blocking_range, range_start
                        );
                        tracker.mark_blocked(&name_for_task, range_start).await;
                        // Advance to next index so subsequent tasks don't hang.
                        let _ = state.next_index.send(my_index + 1);
                        return WorkItemOutcome {
                            handler_name: name_for_task,
                            range_start,
                            range_end,
                            status: OutcomeStatus::Blocked { reason },
                        };
                    }
                }
                let _seq_permit = match handler_seq {
                    Some(state) => Some(
                        state
                            .semaphore
                            .clone()
                            .acquire_owned()
                            .await
                            .expect("sequential semaphore never closed"),
                    ),
                    None => None,
                };

                // 3. Acquire global permit AFTER waiting to avoid permit-deadlock.
                let _permit = permits
                    .acquire_owned()
                    .await
                    .expect("semaphore never closed");

                // 4. Invoke runner.
                let result = runner(item).await;

                // 5. Propagate result to tracker + return outcome.
                //    Advance sequential next_index after each result so the
                //    next range can proceed.
                let outcome = match result {
                    WorkItemRunResult::Succeeded => {
                        tracker.mark_completed(&name_for_task, range_start).await;
                        WorkItemOutcome {
                            handler_name: name_for_task.clone(),
                            range_start,
                            range_end,
                            status: OutcomeStatus::Succeeded,
                        }
                    }
                    WorkItemRunResult::Failed(reason) => {
                        tracker.mark_failed(&name_for_task, range_start).await;
                        WorkItemOutcome {
                            handler_name: name_for_task.clone(),
                            range_start,
                            range_end,
                            status: OutcomeStatus::HandlerFailed { reason },
                        }
                    }
                    WorkItemRunResult::Blocked(reason) => {
                        if let Some(state) = seq_state.get(&name_for_task) {
                            let mut blocked_range = state.blocked_range.lock().await;
                            if blocked_range.is_none_or(|existing| range_start < existing) {
                                *blocked_range = Some(range_start);
                            }
                        }
                        tracker.mark_blocked(&name_for_task, range_start).await;
                        WorkItemOutcome {
                            handler_name: name_for_task.clone(),
                            range_start,
                            range_end,
                            status: OutcomeStatus::Blocked { reason },
                        }
                    }
                };

                // Advance the sequential next_index for the next range.
                if let Some(state) = seq_state.get(&name_for_task) {
                    if let Ok(idx) = state.sorted_ranges.binary_search(&range_start) {
                        let _ = state.next_index.send(idx + 1);
                    }
                }

                outcome
            });
            identity.insert(handle.id(), (name, range_start, range_end));
        }

        let mut outcomes = Vec::new();
        while let Some(res) = join_set.join_next_with_id().await {
            match res {
                Ok((id, outcome)) => {
                    identity.remove(&id);
                    outcomes.push(outcome);
                }
                Err(join_error) => {
                    let id = join_error.id();
                    let (name, range_start, range_end) = identity
                        .remove(&id)
                        .expect("every spawned task's id was recorded");
                    tracing::error!(
                        "WorkItem task panicked: handler={} range_start={} err={}",
                        name,
                        range_start,
                        join_error
                    );
                    // Panic means the task never reached mark_completed/mark_failed,
                    // so downstream waiters would hang. Mark failed here.
                    self.tracker.mark_failed(&name, range_start).await;
                    outcomes.push(WorkItemOutcome {
                        handler_name: name,
                        range_start,
                        range_end,
                        status: OutcomeStatus::Panicked,
                    });
                }
            }
        }

        outcomes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use tokio::sync::Mutex as TokioMutex;

    /// (handler_name, range_start, kind) — kind is "start" or "end".
    type EventLog = Arc<TokioMutex<Vec<(String, u64, &'static str)>>>;

    /// Observable side-effects of a test runner. Shared across all spawned tasks
    /// so assertions can inspect ordering, concurrency, and timing.
    struct Recorder {
        events: EventLog,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        fail_on: HashSet<(String, u64)>,
        block_on: HashSet<(String, u64)>,
        panic_on: HashSet<(String, u64)>,
        hold_ms: u64,
    }

    impl Recorder {
        fn new(hold_ms: u64) -> Arc<Self> {
            Arc::new(Self {
                events: Arc::new(TokioMutex::new(Vec::new())),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                fail_on: HashSet::new(),
                block_on: HashSet::new(),
                panic_on: HashSet::new(),
                hold_ms,
            })
        }

        fn with_fails(hold_ms: u64, fail_on: &[(&str, u64)]) -> Arc<Self> {
            Arc::new(Self {
                events: Arc::new(TokioMutex::new(Vec::new())),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                fail_on: fail_on.iter().map(|(n, r)| (n.to_string(), *r)).collect(),
                block_on: HashSet::new(),
                panic_on: HashSet::new(),
                hold_ms,
            })
        }

        fn with_blocks(hold_ms: u64, block_on: &[(&str, u64)]) -> Arc<Self> {
            Arc::new(Self {
                events: Arc::new(TokioMutex::new(Vec::new())),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                fail_on: HashSet::new(),
                block_on: block_on.iter().map(|(n, r)| (n.to_string(), *r)).collect(),
                panic_on: HashSet::new(),
                hold_ms,
            })
        }

        fn with_panics(hold_ms: u64, panic_on: &[(&str, u64)]) -> Arc<Self> {
            Arc::new(Self {
                events: Arc::new(TokioMutex::new(Vec::new())),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                fail_on: HashSet::new(),
                block_on: HashSet::new(),
                panic_on: panic_on.iter().map(|(n, r)| (n.to_string(), *r)).collect(),
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

                    let now = rec.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    rec.max_in_flight.fetch_max(now, Ordering::SeqCst);

                    if rec.hold_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(rec.hold_ms)).await;
                    }

                    if rec.panic_on.contains(&key) {
                        panic!("test-forced panic at {}:{}", key.0, key.1);
                    }

                    rec.in_flight.fetch_sub(1, Ordering::SeqCst);
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
                    } else if rec.block_on.contains(&key) {
                        WorkItemRunResult::Blocked(format!(
                            "test-forced block at {}:{}",
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
            range_end: range_start + 1,
            dep_names: Arc::new(deps.iter().map(|s| s.to_string()).collect()),
            contiguous_dep_names: Arc::new(Vec::new()),
            call_dep_keys: Arc::new(Vec::new()),
            sequential: false,
            payload: Box::new(()),
        }
    }

    fn seq_item(name: &str, range_start: u64, deps: &[&str]) -> WorkItem {
        WorkItem {
            sequential: true,
            ..item(name, range_start, deps)
        }
    }

    /// Return the index in `events` of the first (name, range, "start") entry,
    /// or panic if not found.
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

    #[tokio::test]
    async fn empty_items_returns_empty_outcomes() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::new(0);
        let outcomes = scheduler.execute(Vec::new(), rec.runner()).await;
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn outcomes_returned_for_every_item() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::new(0);
        let items = vec![
            item("A", 100, &[]),
            item("A", 101, &[]),
            item("B", 100, &[]),
        ];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 3);
        let mut keys: Vec<_> = outcomes
            .iter()
            .map(|o| (o.handler_name.clone(), o.range_start))
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                ("A".to_string(), 100),
                ("A".to_string(), 101),
                ("B".to_string(), 100),
            ]
        );
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded);
        }
    }

    #[tokio::test]
    async fn linear_chain_respects_order() {
        // A -> B -> C on a single range.
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::new(10);
        let items = vec![
            item("A", 100, &[]),
            item("B", 100, &["A"]),
            item("C", 100, &["B"]),
        ];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 3);

        let events = rec.events.lock().await.clone();
        assert!(idx_end(&events, "A", 100) < idx_start(&events, "B", 100));
        assert!(idx_end(&events, "B", 100) < idx_start(&events, "C", 100));
    }

    #[tokio::test]
    async fn diamond_partial_ordering() {
        // A -> B, A -> C, B -> D, C -> D. 5 ranges.
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 16);
        let rec = Recorder::new(5);
        let mut items = Vec::new();
        for r in [100u64, 101, 102, 103, 104] {
            items.push(item("A", r, &[]));
            items.push(item("B", r, &["A"]));
            items.push(item("C", r, &["A"]));
            items.push(item("D", r, &["B", "C"]));
        }
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 20);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded, "{:?}", o);
        }
        let events = rec.events.lock().await.clone();
        for r in [100u64, 101, 102, 103, 104] {
            // A before B/C/D
            assert!(idx_end(&events, "A", r) < idx_start(&events, "B", r));
            assert!(idx_end(&events, "A", r) < idx_start(&events, "C", r));
            // B and C before D
            assert!(idx_end(&events, "B", r) < idx_start(&events, "D", r));
            assert!(idx_end(&events, "C", r) < idx_start(&events, "D", r));
        }
    }

    #[tokio::test]
    async fn independent_handlers_run_in_parallel() {
        // 4 handlers, no deps, concurrency=4, hold=50ms.
        // Total wall-clock should be ~50ms, not ~200ms.
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::new(50);
        let items = vec![
            item("A", 100, &[]),
            item("B", 100, &[]),
            item("C", 100, &[]),
            item("D", 100, &[]),
        ];
        let start = Instant::now();
        let outcomes = scheduler.execute(items, rec.runner()).await;
        let elapsed = start.elapsed();
        assert_eq!(outcomes.len(), 4);
        assert!(
            elapsed < Duration::from_millis(150),
            "expected parallel (~50ms), got {:?}",
            elapsed
        );
        assert_eq!(rec.max_in_flight.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn concurrency_bound_honored() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 2);
        let rec = Recorder::new(20);
        // 10 independent items with concurrency=2.
        let items: Vec<_> = (0..10).map(|i| item("H", 100 + i, &[])).collect();
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 10);
        assert_eq!(rec.max_in_flight.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cascade_failure_short_circuits_downstream() {
        // A -> B -> C on 3 ranges. A fails on range 101. B and C should
        // cascade-fail on range 101, and succeed on 100 and 102.
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 8);
        let rec = Recorder::with_fails(5, &[("A", 101)]);
        let mut items = Vec::new();
        for r in [100u64, 101, 102] {
            items.push(item("A", r, &[]));
            items.push(item("B", r, &["A"]));
            items.push(item("C", r, &["B"]));
        }
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 9);

        let get = |name: &str, r: u64| {
            outcomes
                .iter()
                .find(|o| o.handler_name == name && o.range_start == r)
                .cloned()
                .unwrap()
        };

        assert_eq!(get("A", 100).status, OutcomeStatus::Succeeded);
        assert_eq!(get("B", 100).status, OutcomeStatus::Succeeded);
        assert_eq!(get("C", 100).status, OutcomeStatus::Succeeded);

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
                dep_name: "B".to_string()
            }
        );

        assert_eq!(get("A", 102).status, OutcomeStatus::Succeeded);
        assert_eq!(get("B", 102).status, OutcomeStatus::Succeeded);
        assert_eq!(get("C", 102).status, OutcomeStatus::Succeeded);
    }

    #[tokio::test]
    async fn pipelined_per_range_gating() {
        // A -> B on 3 ranges. A's range-100 is slow (100ms); A's 101/102 are
        // fast (10ms). B's range-101 should start BEFORE A's range-100 ends,
        // proving per-range gating (not per-handler barrier).
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 8);

        // Custom runner that varies hold time per item.
        struct VarRec {
            events: EventLog,
        }
        let vrec = Arc::new(VarRec {
            events: Arc::new(TokioMutex::new(Vec::new())),
        });
        let runner = {
            let vrec = vrec.clone();
            move |item: WorkItem| {
                let vrec = vrec.clone();
                Box::pin(async move {
                    vrec.events.lock().await.push((
                        item.handler_name.clone(),
                        item.range_start,
                        "start",
                    ));
                    let hold = if item.handler_name == "A" && item.range_start == 100 {
                        100u64
                    } else {
                        10u64
                    };
                    tokio::time::sleep(Duration::from_millis(hold)).await;
                    vrec.events.lock().await.push((
                        item.handler_name.clone(),
                        item.range_start,
                        "end",
                    ));
                    WorkItemRunResult::Succeeded
                })
                    as std::pin::Pin<Box<dyn Future<Output = WorkItemRunResult> + Send>>
            }
        };

        let items = vec![
            item("A", 100, &[]),
            item("A", 101, &[]),
            item("A", 102, &[]),
            item("B", 100, &["A"]),
            item("B", 101, &["A"]),
            item("B", 102, &["A"]),
        ];
        let outcomes = scheduler.execute(items, runner).await;
        assert_eq!(outcomes.len(), 6);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded);
        }

        let events = vrec.events.lock().await.clone();
        // B's range-101 must start before A's range-100 ends (pipelined).
        let b_101_start = idx_start(&events, "B", 101);
        let a_100_end = idx_end(&events, "A", 100);
        assert!(
            b_101_start < a_100_end,
            "expected B:101 start ({}) before A:100 end ({})  events: {:?}",
            b_101_start,
            a_100_end,
            events
        );
    }

    #[tokio::test]
    async fn panic_in_runner_reports_panicked_outcome() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::with_panics(5, &[("A", 101)]);
        let items = vec![
            item("A", 100, &[]),
            item("A", 101, &[]),
            item("A", 102, &[]),
        ];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 3);

        let get = |r: u64| {
            outcomes
                .iter()
                .find(|o| o.handler_name == "A" && o.range_start == r)
                .cloned()
                .unwrap()
        };
        assert_eq!(get(100).status, OutcomeStatus::Succeeded);
        assert_eq!(get(101).status, OutcomeStatus::Panicked);
        assert_eq!(get(102).status, OutcomeStatus::Succeeded);
    }

    #[tokio::test]
    async fn panic_cascades_to_dependents() {
        // A's range-100 panics; B (dep on A) at range-100 should cascade-fail
        // (proving we call mark_failed in the panic-handling branch).
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::with_panics(5, &[("A", 100)]);
        let items = vec![item("A", 100, &[]), item("B", 100, &["A"])];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 2);

        let get = |name: &str, r: u64| {
            outcomes
                .iter()
                .find(|o| o.handler_name == name && o.range_start == r)
                .cloned()
                .unwrap()
        };
        assert_eq!(get("A", 100).status, OutcomeStatus::Panicked);
        assert_eq!(
            get("B", 100).status,
            OutcomeStatus::DepCascadeFailed {
                dep_name: "A".to_string()
            }
        );
    }

    #[tokio::test]
    async fn seeded_tracker_unblocks_items_immediately() {
        // Pre-seed tracker so B's deps are already satisfied at submit time.
        let tracker = Arc::new(CompletionTracker::new());
        tracker.seed_completed("A", [100, 101]).await;
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::new(0);
        let items = vec![item("B", 100, &["A"]), item("B", 101, &["A"])];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 2);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded);
        }
    }

    // ─── Sequential handler tests ───────────────────────────────────────

    #[tokio::test]
    async fn sequential_handler_executes_in_order() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 8);
        let rec = Recorder::new(20);
        let items = vec![
            seq_item("S", 100, &[]),
            seq_item("S", 101, &[]),
            seq_item("S", 102, &[]),
            seq_item("S", 103, &[]),
            seq_item("S", 104, &[]),
        ];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 5);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded);
        }

        let events = rec.events.lock().await.clone();
        // Each range must finish before the next starts.
        for r in 100..104 {
            let end_r = idx_end(&events, "S", r);
            let start_next = idx_start(&events, "S", r + 1);
            assert!(
                end_r < start_next,
                "expected S:{} end ({}) before S:{} start ({})\nevents: {:?}",
                r,
                end_r,
                r + 1,
                start_next,
                events
            );
        }
        assert_eq!(rec.max_in_flight.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn sequential_does_not_starve_parallel() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        // Sequential handler S holds for 30ms per range, parallel handler P
        // holds for 10ms. With concurrency=4, P items should run in parallel
        // while S consumes only 1 permit at a time.
        let rec = Arc::new(Recorder {
            events: Arc::new(TokioMutex::new(Vec::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::new(AtomicUsize::new(0)),
            fail_on: HashSet::new(),
            block_on: HashSet::new(),
            panic_on: HashSet::new(),
            hold_ms: 10,
        });
        // We need separate hold_ms for S vs P, so use the default 10ms for all
        // and rely on concurrency observation: if S were consuming all permits,
        // P items would not overlap.
        let mut items = Vec::new();
        for r in 100..105 {
            items.push(seq_item("S", r, &[]));
        }
        for r in 100..105 {
            items.push(item("P", r, &[]));
        }
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 10);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded);
        }

        // S uses only 1 permit, leaving 3 for P. P should be able to achieve
        // at least 2 in-flight (practically all 3 or 4, but 2 is a safe lower bound).
        // max_in_flight tracks the global peak, which includes S + P.
        // Since S never has more than 1 in-flight, max_in_flight >= 2 proves P runs in parallel.
        assert!(
            rec.max_in_flight.load(Ordering::SeqCst) >= 2,
            "expected parallel handler P to achieve at least 2 in-flight; max was {}",
            rec.max_in_flight.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn sequential_with_dep_on_parallel() {
        // A (parallel, no deps) → S (sequential, depends on A).
        // S must still execute its ranges in order even though A may complete
        // out of order across ranges.
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::new(10);
        let mut items = Vec::new();
        for r in 100..103 {
            items.push(item("A", r, &[]));
            items.push(seq_item("S", r, &["A"]));
        }
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 6);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded);
        }

        let events = rec.events.lock().await.clone();
        // S's ranges must be strictly ordered.
        for r in 100..102 {
            let end_r = idx_end(&events, "S", r);
            let start_next = idx_start(&events, "S", r + 1);
            assert!(
                end_r < start_next,
                "expected S:{} end ({}) before S:{} start ({})\nevents: {:?}",
                r,
                end_r,
                r + 1,
                start_next,
                events
            );
        }
        // A must complete before S for each range (dep gating).
        for r in 100..103 {
            let a_end = idx_end(&events, "A", r);
            let s_start = idx_start(&events, "S", r);
            assert!(
                a_end < s_start,
                "expected A:{} end ({}) before S:{} start ({})\nevents: {:?}",
                r,
                a_end,
                r,
                s_start,
                events
            );
        }
    }

    #[tokio::test]
    async fn two_independent_sequential_handlers() {
        // S1 and S2 are both sequential but independent. Each must be in order
        // internally, but they should overlap with each other.
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::new(15);
        let mut items = Vec::new();
        for r in 100..103 {
            items.push(seq_item("S1", r, &[]));
            items.push(seq_item("S2", r, &[]));
        }
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 6);
        for o in &outcomes {
            assert_eq!(o.status, OutcomeStatus::Succeeded);
        }

        let events = rec.events.lock().await.clone();
        // Each sequential handler's ranges are strictly ordered.
        for name in &["S1", "S2"] {
            for r in 100..102 {
                let end_r = idx_end(&events, name, r);
                let start_next = idx_start(&events, name, r + 1);
                assert!(
                    end_r < start_next,
                    "expected {}:{} end ({}) before {}:{} start ({})\nevents: {:?}",
                    name,
                    r,
                    end_r,
                    name,
                    r + 1,
                    start_next,
                    events
                );
            }
        }
        // Global max_in_flight should be > 1 since S1 and S2 can overlap.
        assert!(
            rec.max_in_flight.load(Ordering::SeqCst) >= 2,
            "expected S1 and S2 to overlap; max_in_flight was {}",
            rec.max_in_flight.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn sequential_failure_releases_semaphore() {
        // S has 3 ranges; range 101 fails. Range 102 must still execute (the
        // permit is released on failure because the task drops its OwnedSemaphorePermit).
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker, 4);
        let rec = Recorder::with_fails(10, &[("S", 101)]);
        let items = vec![
            seq_item("S", 100, &[]),
            seq_item("S", 101, &[]),
            seq_item("S", 102, &[]),
        ];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 3);

        let get = |r: u64| {
            outcomes
                .iter()
                .find(|o| o.handler_name == "S" && o.range_start == r)
                .cloned()
                .unwrap()
        };
        assert_eq!(get(100).status, OutcomeStatus::Succeeded);
        assert!(matches!(
            get(101).status,
            OutcomeStatus::HandlerFailed { .. }
        ));
        assert_eq!(get(102).status, OutcomeStatus::Succeeded);
    }

    #[tokio::test]
    async fn blocked_dep_cascades_and_sequential_ranges_hold() {
        let tracker = Arc::new(CompletionTracker::new());
        let scheduler = DagScheduler::new(tracker.clone(), 4);
        let rec = Recorder::with_blocks(10, &[("S", 100)]);
        let items = vec![
            seq_item("S", 100, &[]),
            seq_item("S", 101, &[]),
            item("D", 100, &["S"]),
            item("D", 101, &["S"]),
        ];
        let outcomes = scheduler.execute(items, rec.runner()).await;
        assert_eq!(outcomes.len(), 4);

        let get = |name: &str, r: u64| {
            outcomes
                .iter()
                .find(|o| o.handler_name == name && o.range_start == r)
                .cloned()
                .unwrap()
        };

        assert!(matches!(
            get("S", 100).status,
            OutcomeStatus::Blocked { .. }
        ));
        assert!(matches!(
            get("S", 101).status,
            OutcomeStatus::Blocked { .. }
        ));
        assert_eq!(
            get("D", 100).status,
            OutcomeStatus::DepCascadeBlocked {
                dep_name: "S".to_string()
            }
        );
        assert_eq!(
            get("D", 101).status,
            OutcomeStatus::DepCascadeBlocked {
                dep_name: "S".to_string()
            }
        );
        assert!(tracker.is_blocked("S", 100).await);
        assert!(tracker.is_blocked("S", 101).await);
        assert!(tracker.is_blocked("D", 100).await);
        assert!(tracker.is_blocked("D", 101).await);
    }
}
