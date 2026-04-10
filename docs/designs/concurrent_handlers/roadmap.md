# Concurrent Handler Execution — Roadmap

Four independently-mergeable phases. Each ships value on its own and can be reviewed/reverted without tangling the others.

## Phase 1 — Build `CompletionTracker` + `DagExecutor` in isolation

**Goal**: stand up the new scheduler module with no callers yet. Purely additive code; zero behavior change in production paths.

**Deliverables**
- `src/transformations/scheduler/mod.rs`
- `src/transformations/scheduler/tracker.rs` — `CompletionTracker`, `DepState`, `DepFailed`
- `src/transformations/scheduler/executor.rs` — `DagExecutor`, `WorkItem`, `WorkItemLoader`, `WorkItemData`
- Unit tests for `CompletionTracker` (single/multi waiters, completion/failure/cascade, wakeup ordering under concurrency).
- Integration tests for `DagExecutor` using mock handlers registered via the test harness:
  - Diamond DAG, 5 ranges, correct partial ordering
  - Cascade failure short-circuits downstream
  - Concurrency bound honored
  - Empty-data items complete without running handler work

**Out of scope**
- Wiring into catchup, `process_range`, or live mode.
- Real parquet loaders — tests use in-memory mock loaders.
- Snapshot-capture mode (tests run in `DbExecMode::Direct` with mock DB).

**Value delivered on merge**
- New testable scheduler primitive available for subsequent phases.
- Zero risk: no production code path touches it.

**Review surface**: ~1 new module (~500–700 lines), ~200–300 lines of tests. Fully self-contained diff.

**Branch**: `feat/dag-scheduler-primitives`

---

## Phase 2 — Rewire catchup to use `DagExecutor`

**Goal**: replace the sequential `for ch in &handlers` loop in `run_handler_catchup` with pipelined dep-gated execution. This is the user-visible speedup.

**Deliverables**
- `src/transformations/scheduler/loader.rs` — `CatchupEventLoader`, `CatchupCallLoader` (wrapping existing `read_decoded_events_for_triggers` / `read_decoded_calls_for_triggers` / `read_receipt_addresses`).
- `engine.rs::run_handler_catchup` rewritten: seed tracker from `_handler_progress`, build WorkItems for every missing `(handler, range)`, call `dag_executor.execute(items, DbExecMode::Direct).await`.
- Retained: call-dependency retry loop — loader returns a `CallDepsMissing` sentinel and catchup wraps the executor call in a retry loop over unresolved items until no progress is made.
- Metrics/logging parity with existing catchup (pass counts, per-handler progress lines, blocked-by-deps warnings).

**What stays unchanged**
- `HandlerExecutor` (old) still exists and is used by `process_range` and live mode.
- `_handler_progress` schema.
- Handler trait surface.

**Test plan**
- Unit: mock registry with A→B dep, 3 ranges. Verify B's range-1 starts before A's range-3 finishes.
- Integration: run catchup against a test fixture with real handlers + synthetic parquet; assert speedup vs. baseline and identical final DB state.
- Regression: existing catchup integration tests pass unchanged.

**Value delivered on merge**
- Dependent handlers run concurrently end-to-end. Expected speedup ≈ (depth of handler DAG) × (num ranges) vs. current serial-by-handler behavior, capped by `handler_concurrency` and I/O.
- Fixes stall scenarios where one slow upstream handler blocks all downstream work.

**Risks & mitigations**
- **Wake-storm under many ranges**: monitored in tests; if pathological, swap `Notify` for `watch<BTreeSet<u64>>` per handler (isolated change inside tracker).
- **Loader failure semantics**: previously, a parquet-read failure aborted the whole catchup pass; now it fails just that item. Add a catchup-level summary that aggregates item failures so the error surface is equivalent.

**Branch**: `feat/dag-catchup`

---

## Phase 3 — Rewire `process_range` to use `DagExecutor`

**Goal**: fix the dep-unaware bug in the retry/reorg path by routing it through the same scheduler. Also retire `HandlerExecutor` as a separate abstraction.

**Deliverables**
- `process_range` rebuilt: construct WorkItems for the single `(range_start, range_end)` across all triggered handlers, call `dag_executor.execute(items, db_exec_mode).await`.
- `DbExecMode::WithSnapshotCapture` path inside `DagExecutor` (wrap the existing `execute_with_snapshot_capture` call inside the per-item flow).
- Delete or reduce `src/transformations/executor.rs::HandlerExecutor` to a thin adapter, depending on whether live mode has been migrated yet.
- Route `retry.rs::RetryProcessor` calls through the new executor.

**Test plan**
- Regression test: replay a range with handlers `A` and `B depends on A` via `process_range`, assert A's transaction commits before B's handler is invoked. This test would have **failed on main today**.
- Existing retry-path integration tests pass unchanged.
- Snapshot-capture live-retry tests pass unchanged.

**Value delivered on merge**
- Correctness fix for dep-aware retry/reorg behavior (previously `process_range` ran all handler tasks in parallel with no gating).
- Single executor abstraction for catchup + retry + reorg; lower cognitive overhead.

**Risks & mitigations**
- **Snapshot-capture layering**: easy to get wrong. Wrap existing `execute_with_snapshot_capture` unchanged; only the call site moves. Add a unit test that snapshots are written for all upsert items in live-retry mode.

**Branch**: `feat/dag-process-range`

---

## Phase 4 — Fold live mode into `DagExecutor`

**Goal**: retire `LiveProcessingState`'s bespoke buffering in favor of the unified scheduler with layered readiness conditions.

**Deliverables**
- Live-specific readiness conditions added to `WorkItem`:
  - `call_deps: Vec<(String, String)>` + `wait_for_calls(tracker, call_deps, range)` extension to `wait_ready`.
  - Per-item timeout with stuck-event warning (replicates `LiveProcessingState`'s timeout + detailed log).
- Cascade-failure status-file persistence in live-mode failure arm (`LiveStorage::update_status_atomic` mirroring `cascade_handler_failures`).
- `LiveProcessingState::pending_events`, `pending_event_timestamps`, and `try_process_pending_events` deleted.
- `received_calls` tracking moves into a secondary call-readiness tracker (or a second map on `CompletionTracker`).
- `process_events_message` and `process_calls_message` become thin shims that build WorkItems and submit to the executor.

**Test plan**
- Every existing live-mode test runs unchanged and passes.
- Timeout test: stuck pending WorkItem emits the same diagnostic as today's `LiveProcessingState` timeout.
- Cascade test: upstream fails → downstream fails → status file reflects both.
- Load test: end-to-end live-mode replay at realistic block rates, comparing latency distributions vs. pre-phase-4 baseline.

**Value delivered on merge**
- Single, consistent implementation of dep-gating across all three paths.
- `LiveProcessingState` drops ~200 lines of buffering logic.
- Future handlers with new kinds of dependencies only need one integration point.

**Risks & mitigations**
- **Largest behavior-change surface** of the four phases. Landed last intentionally. Gatekept on full live-mode integration test suite + manual replay on recent mainnet data before merge.
- **Persistence ordering**: today, handler failures are persisted to status files synchronously from `process_events_message`. Must preserve the same "persist-before-returning-from-message" guarantee in the new flow.

**Pick up Phase 2's call-dep deadlock workaround — IMPLEMENTED (catchup only)**

The catchup portion of this sub-goal has been completed. The per-pass
`deferred_starts` / `next_pending` retry loop from Phase 2 has been
removed. `run_handler_catchup` now builds all `(handler, range)` work
items upfront and submits them in a single `DagScheduler::execute`
call. Call-dep readiness is gated inside the scheduler via a
`call_dep_keys` field on `WorkItem` and a background
`CallDepScanner` loop that periodically scans the filesystem and
registers newly-available call-dep ranges on the `CompletionTracker`.
Items unblock as soon as their call-dep files appear on disk, not at
the next pass boundary.

Remaining (not yet implemented): the live-mode side of this, where
`wait_for_calls` uses a different signal source (received-calls
tracker rather than filesystem polling) for the same readiness
condition.

**Branch**: `feat/dag-live-mode`

---

## Dependencies between phases

```
Phase 1 (primitives)
   │
   ├──► Phase 2 (catchup)  ──────┐
   │                              │
   └──► Phase 3 (process_range) ──┤
                                  │
                     Phase 4 (live) ◄── depends on 1; benefits from 3 being in
```

Phase 2 and Phase 3 can land in either order after Phase 1. Phase 4 can begin after Phase 1 but is safer to land after both 2 and 3 so that the retiring of old `HandlerExecutor` happens against a stable set of callers.

## Not in roadmap

- Per-handler concurrency bounds — trivially added later.
- Priority-based scheduling (e.g., older ranges first) — not required for correctness or the target speedup.
- Replacing the catchup progress-reporting cadence or log format — orthogonal cleanup.
