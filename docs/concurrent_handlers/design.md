# Concurrent Handler Execution — Design

## Problem

Handler dependency chains exist (`EventHandler::handler_dependencies()`), but the three execution paths treat them very differently, and one of them — **catchup** — serializes all dependent handlers end-to-end.

| Path | File / Entry Point | Current behavior | Dep respect |
|---|---|---|---|
| **Catchup** | `engine.rs::run_handler_catchup` (L480) | `for ch in &handlers { ... }` — one handler at a time, in topological order. Each handler processes its ranges concurrently under a `handler_concurrency` semaphore. | Correct, but handlers are fully serialized: downstream handler cannot start its first range until upstream has finished *all* ranges. |
| **`process_range`** | `engine.rs::process_range` (L2236), used by retry/reorg | `build_handler_tasks` → `HandlerExecutor::execute_handlers` submits all tasks to a single semaphore pool with **no dep gating**. | Broken for any handler with `handler_dependencies` in the retry/reorg path. |
| **Live** | `engine.rs::process_events_message` (L1297) + `live_state::LiveProcessingState` | Ready handlers run concurrently via `execute_handlers`; unmet-dep handlers buffer their events and retry via `try_process_pending_events`. | Correct, with bespoke buffering machinery. |

The current `HandlerExecutor::execute_handlers(tasks, range_start, range_end, db_exec_mode)` is **per-range** and **dep-unaware**. Its atomic unit is `(handler, single range)`.

## Goal

Run transformation handlers **concurrently across ranges and across handlers**, while guaranteeing that for every dependency edge `B depends on A`, no execution of handler `B` on range `R` starts until handler `A` on range `R` has completed successfully.

Non-goals:
- Changing the handler trait or how handlers are registered.
- Changing the `_handler_progress` table schema.
- Changing live-mode's call-dependency or timeout/cascade-failure semantics (addressed in Phase 4).

## Approach: hybrid DAG-aware execution

The design has **two composable pieces**:

1. **`CompletionTracker`** — a small, standalone shared state object that tracks per-`(handler_name, range_start)` completion and failure, with a `Notify`-based wake-up for waiters. Seeded from the `_handler_progress` table at startup.

2. **`DagExecutor`** — a scheduler that accepts a batch of `WorkItem`s (each carrying a `(handler, range, dep_names)` triple), spawns each as a Tokio task, and has each task self-gate on the tracker before acquiring a concurrency permit and running the handler.

These get introduced **incrementally** across phases, so each phase is independently mergeable and delivers measurable value. See `roadmap.md`.

## Core types

```rust
// src/transformations/scheduler/tracker.rs

/// Shared in-memory completion state for (handler_name, range_start) pairs.
/// Seeded from `_handler_progress` at startup. Updated as WorkItems finish.
pub struct CompletionTracker {
    // handler_name -> set of completed range_starts
    completed: RwLock<HashMap<String, BTreeSet<u64>>>,
    // handler_name -> set of failed range_starts (for cascade)
    failed:    RwLock<HashMap<String, BTreeSet<u64>>>,
    notify: Notify,
}

impl CompletionTracker {
    pub fn new() -> Self;

    /// Seed from persisted progress. Called once at startup.
    pub fn seed_completed(&self, handler_name: &str, ranges: impl IntoIterator<Item = u64>);

    /// Block until every (dep, range) is completed, or return Err if any dep failed.
    pub async fn wait_ready(
        &self,
        deps: &[String],
        range_start: u64,
    ) -> Result<(), DepFailed>;

    /// Non-blocking check; useful for catchup's initial filter.
    pub fn is_ready(&self, deps: &[String], range_start: u64) -> DepState;

    /// Mark a (handler_name, range) as completed and wake all waiters.
    pub fn mark_completed(&self, handler_name: &str, range_start: u64);

    /// Mark a (handler_name, range) as failed and wake all waiters.
    pub fn mark_failed(&self, handler_name: &str, range_start: u64);
}

pub enum DepState {
    Ready,
    Waiting,
    DepFailed { dep_name: String },
}

pub struct DepFailed {
    pub dep_name: String,
    pub range_start: u64,
}
```

```rust
// src/transformations/scheduler/executor.rs

/// A single atomic unit of work: run one handler on one range.
pub struct WorkItem {
    pub handler: Arc<dyn TransformationHandler>,
    pub handler_name: String,
    pub handler_key: String,
    pub range_start: u64,
    pub range_end: u64,
    pub dep_names: Vec<String>,
    /// Lazy loader, called inside the spawned task after deps are satisfied.
    /// Returns (events, calls, tx_addresses) or None to indicate no-op.
    pub loader: Arc<dyn WorkItemLoader>,
}

#[async_trait]
pub trait WorkItemLoader: Send + Sync {
    async fn load(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> Result<WorkItemData, TransformationError>;
}

pub struct WorkItemData {
    pub events: Vec<DecodedEvent>,
    pub calls: Vec<DecodedCall>,
    pub tx_addresses: HashMap<[u8; 32], TransactionAddresses>,
}

pub struct DagExecutor {
    tracker: Arc<CompletionTracker>,
    global_permits: Arc<Semaphore>,
    db_pool: Arc<DbPool>,
    historical_reader: Arc<HistoricalDataReader>,
    rpc_client: Arc<UnifiedRpcClient>,
    contracts: Arc<Contracts>,
    chain_name: String,
    chain_id: u64,
    finalizer: Arc<RangeFinalizer>,
}

impl DagExecutor {
    /// Submit a batch of items. Spawns one task per item; each task self-gates
    /// on `tracker.wait_ready`, then acquires a permit, loads data, runs the
    /// handler, executes ops, and updates the tracker + progress table.
    ///
    /// Returns when every item has either completed, failed, or been
    /// cascade-skipped.
    pub async fn execute(
        &self,
        items: Vec<WorkItem>,
        db_exec_mode: DbExecMode,
    ) -> Vec<HandlerOutcome>;
}
```

## Per-item execution flow

Each `WorkItem` becomes one spawned Tokio task. The task's lifecycle:

```
1. wait_ready(deps, range_start) on the tracker
   ├─ all deps have that range in `completed` → proceed
   ├─ any dep has that range in `failed`      → cascade: mark_failed(self, range), return
   └─ otherwise → await notify.notified(), re-check (loop)

2. global_permits.acquire_owned().await    // AFTER waiting to avoid permit deadlock

3. loader.load(range_start, range_end).await
   ├─ empty events AND empty calls → record_completed_range_for_handler, mark_completed, return
   └─ has data → proceed

4. Build TransformationContext

5. handler.handle(&ctx).await
   ├─ Err → log, tracker.mark_failed(name, range), return
   └─ Ok(ops) → proceed

6. inject_source_version(ops)

7. execute_transaction (or execute_with_snapshot_capture for live)
   ├─ Err → log, tracker.mark_failed(name, range), return
   └─ Ok → proceed

8. finalizer.record_completed_range_for_handler(key, start, end).await

9. tracker.mark_completed(name, range)     // wakes downstream waiters
```

### Why wait *before* acquiring a permit

If a waiter held a permit while awaiting its deps, and deps' tasks couldn't acquire permits because waiters were holding them, the scheduler would deadlock. Waiting first, permitting second, is deadlock-free.

### Cascade failure

When a handler task marks itself failed, it notifies all waiters. Any downstream waiter whose current dep set now contains a failed range short-circuits with `DepFailed`, marks *itself* failed (propagating further), and returns. The overall `execute()` call reports those items as failed rather than hanging.

## Concurrency limits

**v1**: single global `Semaphore` with permits = configured `handler_concurrency` (existing config field).

**Future**: optional per-handler sub-semaphore for handlers known to be heavy (e.g., a handler that does many RPC calls). Not required for initial delivery; easy to add as `HashMap<String, Arc<Semaphore>>` inside `DagExecutor`.

## Data-loading parallelism

Today, `run_handler_catchup` loads events/calls from parquet on its main task *before* dispatching each per-range spawn. In the new model, the `WorkItemLoader` is called **inside** the spawned task, after dep readiness. Consequences:

- **Pro:** parquet reads are parallelized alongside handler execution.
- **Pro:** tasks that end up skipped due to cascade failure never waste a parquet read.
- **Con:** a parquet-read failure surfaces as a task-level error (instead of aborting the whole catchup pass before dispatch). This matches the retry/partial-failure model we already tolerate.

## Dependency key: `name` vs `handler_key`

Dependencies are declared by `handler_name` (e.g., `"PoolsHandler"`), not by `handler_key` (`"PoolsHandler_v3"`). The tracker indexes on `handler_name` so that when a handler's version bumps, its dependents automatically gate on the *active* version's progress for that name. This matches how the catchup loop already interprets `handler_dependencies()` and how `handler_topological_order()` is built (name-keyed).

Completed-ranges are still persisted to `_handler_progress` keyed by `handler_key` (version-specific). At seed time we load progress for each handler's current `handler_key` and insert under its `name`.

## Integrating with the three paths

### Catchup (Phase 2)

Replaces the outer `for ch in &handlers` loop. Pseudocode:

```rust
let tracker = Arc::new(CompletionTracker::new());

// Seed tracker from _handler_progress
for handler in all_event_handlers {
    let completed = finalizer.get_completed_ranges_for_handler(&handler.handler_key()).await?;
    tracker.seed_completed(handler.name(), completed);
}

// Build WorkItems for every (handler, range) not yet completed
let mut items = Vec::new();
for handler in event_handlers {
    let completed = tracker.completed_for(handler.name());
    for (start, end) in &available_ranges {
        if completed.contains(start) { continue; }
        items.push(WorkItem {
            handler: handler.clone(),
            handler_name: handler.name().to_string(),
            handler_key: handler.handler_key(),
            range_start: *start,
            range_end: *end,
            dep_names: handler.handler_dependencies().iter().map(|s| s.to_string()).collect(),
            loader: Arc::new(CatchupEventLoader { ... }),
        });
    }
}
// plus call handlers, same pattern, dep_names = vec![]

let outcomes = dag_executor.execute(items, DbExecMode::Direct).await;
```

The call-dependency retry loop (Phase 2 keeps this logic, Phase 4 folds it in) stays in the `CatchupEventLoader::load` implementation: if call deps for the range aren't decoded yet, the loader returns a `CallDepsMissing` variant that the task treats as a transient skip; the catchup code wraps the executor call in a retry loop over unresolved items.

### `process_range` (Phase 3)

Build one `WorkItem` per handler for the single `(range_start, range_end)` pair, submit to the executor, await. The existing snapshot-capture mode (`DbExecMode::WithSnapshotCapture`) is passed through.

### Live mode (Phase 4)

`LiveProcessingState`'s event buffering is replaced: each arriving event batch produces `WorkItem`s that are submitted to the executor. Handler-dep gating happens via the tracker. The additional live-specific readiness conditions get layered as extra await points inside the WorkItem's wait sequence:

- **Call dependencies**: currently tracked in `LiveProcessingState::received_calls`. Becomes a second `wait_ready`-style condition on call arrival.
- **Timeout + stuck-event logging**: replicated by a `tokio::select!` on the tracker wait with a timeout arm.
- **Cascade failure status-file persistence**: the task's failure path calls `LiveStorage::update_status_atomic` when in live mode, mirroring today's `cascade_handler_failures`.

## Testing strategy

**`CompletionTracker` (Phase 1)**: unit tests in isolation.
- Seed + `wait_ready` resolves immediately when seeded state already satisfies deps.
- Single waiter is woken when its dep is marked completed.
- Multiple waiters on the same dep are all woken.
- `mark_failed` causes waiters to return `DepFailed`.
- Cascading: waiter whose dep fails marks itself failed, wakes *its* downstream waiters who also fail.
- Concurrent `mark_completed`/`wait_ready` calls don't lose wakeups (test with `tokio::test` and many tasks).

**`DagExecutor` (Phase 1/2)**: integration tests with mock handlers.
- Diamond DAG (A→B, A→C, B→D, C→D) with 5 ranges executes with correct partial ordering.
- Handler B fails on range 3: D skips range 3, B still runs other ranges, C/D run for other ranges.
- Concurrency bound is honored: with `handler_concurrency=2` and 10 independent items, never more than 2 running simultaneously (assertion via shared atomic counter in mock handler).
- Empty-data WorkItems complete without spawning handler work.

**Catchup (Phase 2)**: end-to-end test with synthetic parquet files and real handlers (or mocks registered via the registry test harness).

## File layout

New module tree under `src/transformations/scheduler/`:

```
src/transformations/scheduler/
├── mod.rs            # re-exports, module docs
├── tracker.rs        # CompletionTracker, DepState, DepFailed
├── executor.rs       # DagExecutor, WorkItem, WorkItemLoader, WorkItemData
└── loader.rs         # Standard loaders: CatchupEventLoader, CatchupCallLoader, LiveLoader
```

The existing `src/transformations/executor.rs` (current `HandlerExecutor`) stays during Phases 1–2, is rewired to delegate to `DagExecutor` in Phase 3, and is removed (or becomes a thin adapter) by end of Phase 4.

## Config

No new required config. Existing `handler_concurrency` (default 4, in `src/types/config/defaults.rs`) continues to control global parallelism.

Optional future fields (not shipping in v1):
- `per_handler_concurrency: HashMap<String, usize>` — per-handler caps.
- `dep_cascade_on_failure: bool` — opt out of cascade, retry failed items on next run.

## Open questions deferred to implementation

- Exact size of the `Notify`-based wake storm under heavy cascade failures — may warrant swapping `Notify` for a per-`(name, range)` `oneshot` or per-`name` `watch<BTreeSet<u64>>` if contention shows up in profiling.
- Whether to persist "cascade-skipped" items to `_handler_progress` with a marker, or rely on next startup's re-scan (current behavior). Leaning toward the latter for simplicity.
