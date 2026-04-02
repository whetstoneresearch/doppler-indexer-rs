# Handler Dependency Chains

## Problem

Handlers currently execute concurrently with no ordering guarantees relative to each other. Some handlers produce data that downstream handlers depend on — for example, a pool creation handler must populate a shared metadata cache before a swap handler can look up pool metadata for the same block. Today there is no way to express or enforce this relationship.

## Design

### Trait Change

Add a `handler_dependencies()` method to `EventHandler` with a default empty implementation:

```rust
pub trait EventHandler: TransformationHandler {
    fn triggers(&self) -> Vec<EventTrigger>;

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![]
    }

    /// Handler names (via `TransformationHandler::name()`) that must complete
    /// before this handler can execute for a given block range.
    fn handler_dependencies(&self) -> Vec<&'static str> {
        vec![]
    }
}
```

Dependencies reference the `name()` of other handlers (e.g., `"V3CreateHandler"`), not the versioned `handler_key()`. This means bumping a dependency's version doesn't require updating dependents.

### Startup Validation

During registry construction, validate the dependency graph:

1. **Missing dependency check** — if a handler declares a dependency on a name that isn't registered, panic with a descriptive error. Same pattern as the existing `validate_call_dependencies()`.
2. **Cycle detection** — perform a topological sort of the handler dependency graph. If a cycle is detected, panic with the cycle path. This catches `A -> B -> A` and longer cycles.
3. **Store the resolved dependency graph** — the registry holds a mapping from each handler name to its resolved dependency handler names, plus a precomputed topological ordering for use in catchup mode.

### Live Mode Execution

Live mode processes single-block ranges. Events arrive as `DecodedEventsMessage` batches per `(source, event_name)`. The existing call-dependency buffering pattern in `LiveProcessingState` is extended to also track handler dependencies.

#### New State

```rust
// In PendingEventData (extended, not a new struct):
pub required_handlers: Vec<String>,  // handler name() strings

// In LiveProcessingState:
pub completed_handlers: HashMap<(u64, u64), HashSet<String>>,

// In HandlerOutcome (extended):
pub handler_name: String,  // handler name() for completion tracking
```

Handler deps are tracked in the **same** `pending_events` map as call deps (via the extended `PendingEventData`), not a separate map. This avoids the "which map does a handler with both dep types go in?" problem.

#### Dispatch Flow

When `process_events_message` receives events for a block:

1. Find all handlers registered for the event.
2. For each handler, check **both** call dependencies (existing) and handler dependencies (new):
   - **Call deps**: all `(source, function)` pairs present in `received_calls` for this range.
   - **Handler deps**: all dependency handler `name()`s present in `completed_handlers` for this range.
3. If both are satisfied, the handler is ready — add to the execution batch.
4. If either is unsatisfied, buffer in `pending_events` with both `required_calls` and `required_handlers` populated.

When a handler completes execution:

1. Insert its `name()` into `completed_handlers` for the range.
2. Call `try_process_pending_events` which scans the unified `pending_events` map for handlers whose call deps AND handler deps are now all met.
3. Dispatch any newly-unblocked handlers.
4. Cascade: loop steps 1-3 until no more handlers are unblocked (handles A→B→C chains).

This means independent chains and dependency-free handlers run concurrently, while dependent handlers wait only for their specific dependencies — not an entire "level."

#### No-op Completion for Untriggered Handlers

If Handler A is declared as a dependency but has no events in a block, it is never dispatched and never completes — blocking dependent handlers forever.

**Resolution:** When `RangeCompleteKind::Logs` arrives for a range, the engine knows all event messages for that block have been sent. At this point:

1. Compute the set of all handlers that are declared as dependencies by any other handler (from the registry's dependency graph).
2. For each such handler that was **not** triggered for this range (not in `completed_handlers` and not currently executing), mark it as completed-no-op in `completed_handlers`.
3. Run the standard pending-handler-dep check to unblock any waiting handlers.

This integrates into the existing `process_range_complete` flow, between marking `logs_complete` and calling `maybe_finalize_range`.

#### Unified Readiness Check

A handler is ready to execute when ALL of the following are true:
- All `call_dependencies()` are available for the range
- All `handler_dependencies()` are completed (or no-op'd) for the range

Both dep types are stored on the same `PendingEventData` and checked in the same filter in `try_process_pending_events`. The check runs on two triggers: call arrival (existing) and handler completion (new).

#### Failure Cascading

When a handler fails, all of its transitive dependents are immediately failed as well (they can never satisfy their `required_handlers`). The cascade:

1. Computes transitive dependents via `registry.transitive_dependents_of(failed_name)`.
2. Marks each dependent as failed in `state.failed_handlers` for the range.
3. Removes their pending events from `state.pending_events` so they don't sit waiting for a 5-minute timeout.
4. Persists the cascaded failures to the status file so the block is retryable.

This replaces the previous behavior where dependents of a failed handler would eventually time out after 5 minutes. Failure cascading is immediate.

#### Finalization

The existing finalization flow is unchanged. `check_finalization_readiness` already gates on "no pending events." Since handler-dep pending events use the same `pending_events` map, they are automatically included in the finalization gate and the 5-minute timeout.

### Catchup / Historical Mode

In catchup mode, handlers already run sequentially in `run_handler_catchup` (a `for ch in &handlers` loop). The change is:

1. **Order handlers by topological sort** — process handlers in dependency order instead of registration order.
2. **No per-event interleaving** — each handler processes its entire range before the next handler starts. This is correct because catchup is throughput-sensitive, not latency-sensitive.

Independent handlers (no dependency relationship between them) can remain in any relative order since they don't interact. The topological sort just ensures that if B depends on A, A runs before B.

### Cross-Handler Data Visibility

Handler dependencies enforce **execution ordering only**. Cross-handler data sharing is handled via an in-memory shared cache (implemented separately). The dependency system guarantees that when Handler B runs, Handler A has already populated the cache for events up to and including B's block range.

The database transaction model is unchanged: each handler commits independently. Handler B does not need to read Handler A's committed rows — it reads from the shared cache.

## Invariants

1. A handler never executes for a range until all of its declared dependencies have completed (or been marked no-op) for that range.
2. The dependency graph is a DAG — cycles are detected and rejected at startup.
3. All handler names referenced in `handler_dependencies()` must correspond to a registered handler — missing references are rejected at startup.
4. Independent dependency chains execute concurrently. A handler blocks only on its own declared dependencies.
5. Catchup mode processes handlers in topological order. Live mode dispatches handlers as soon as their dependencies complete.
6. When a handler fails, all transitive dependents are immediately failed for that range — they are not left pending.
7. A handler is not marked completed (in-memory or persisted) while it still has pending entries for the range. This prevents a handler with multiple batches (e.g., from multiple triggers or split call-dep arrivals) from prematurely unblocking its dependents or being skipped on crash recovery.
8. Multi-trigger handler success is not persisted to `_handler_progress` or `_live_progress` until the corresponding completion signal (`RangeCompleteKind::Logs` for event handlers) confirms all batches have been dispatched. Single-trigger handlers persist immediately for crash-recovery efficiency. `finalize_range()` handles deferred persistence for multi-trigger handlers.

## Files to Modify

| File | Change |
|------|--------|
| `src/transformations/traits.rs` | Add `handler_dependencies()` to `EventHandler` trait |
| `src/transformations/registry.rs` | Dependency graph construction, topological sort, cycle detection, missing-dep validation |
| `src/transformations/live_state.rs` | Add `completed_handlers` to `LiveProcessingState`, extend `PendingEventData` with `required_handlers`, extend cleanup methods |
| `src/transformations/executor.rs` | Add `handler_name` to `HandlerOutcome` |
| `src/transformations/engine.rs` | Unified dep check in `process_events_message`, handler-completion recording, cascading dispatch in `try_process_pending_events`, integrate no-op completion into `process_range_complete` flow |
| `src/transformations/finalizer.rs` | Extend `process_range_complete` to trigger no-op marking before finalization check |
| `src/transformations/event/v3/create.rs` | No change (this is the dependency target) |
| Swap/metrics handlers | Add `handler_dependencies()` returning `vec!["V3CreateHandler"]` |

## Roadmap

### Phase 1: Trait and Registry (COMPLETE)

- Added `handler_dependencies()` to `EventHandler` with default empty vec.
- Built dependency graph in `TransformationRegistry` during handler registration.
- Implemented topological sort with cycle detection — panic on cycles.
- Validated all declared dependency names resolve to registered handlers — panic on missing.
- Stored topological ordering for catchup mode.
- Unit tests: cycle detection, missing dep detection, topological sort correctness, diamond dependencies.
- PRs: #80 (impl), #81 (tests)

### Phase 2: Catchup Mode (COMPLETE)

- Sorted event handlers by topological order in `run_handler_catchup()` before the processing loop.
- Only applies to `HandlerKind::Event` — call handlers have no dependency ordering.
- Committed directly to `feat/handler-dependency-chains`.

### Phase 3: Live Mode Dispatch (COMPLETE)

- Extended `PendingEventData` with `required_handlers` field (unified pending map, not a second map).
- Added `completed_handlers` to `LiveProcessingState` for tracking handler completions per range.
- Added `handler_name` to `HandlerOutcome` for completion tracking without reverse-lookup.
- Extended `process_events_message` with unified call+handler dep check.
- Record handler completions after execution, then cascade via `try_process_pending_events`.
- `try_process_pending_events` now checks both `required_calls` and `required_handlers`, wrapped in a loop for cascading (A→B→C chains).
- Extended all cleanup methods (`cleanup_after_finalize`, `cleanup_for_orphaned_blocks`, `cleanup_for_retry`) to clear `completed_handlers`.
- Timeout logging now shows both `waiting_for_calls` and `waiting_for_handlers`.
- PRs: #82 (state), #83 (dispatch)

### Phase 4: No-op Completion for Untriggered Handlers

- On `RangeCompleteKind::Logs`, compute untriggered dependency handlers and mark them completed-no-op in `completed_handlers`.
- Unblock any pending handlers that were waiting on the no-op'd handlers via `try_process_pending_events`.
- Without this phase, a handler depending on an untriggered handler will timeout after 5 minutes (the existing timeout safety net).

**Key details for implementation:**

**File:** `src/transformations/engine.rs` or `src/transformations/finalizer.rs`

**Trigger:** `process_range_complete()` in `finalizer.rs` (line 92) is called when `RangeCompleteKind::Logs` arrives. After marking `logs_complete`, but before calling `maybe_finalize_range`, insert the no-op logic.

**Logic:**
1. Get the set of all handler names that are declared as dependencies (from `registry.handler_dependency_graph()` — values are the dep names).
2. Lock `live_state`, get `completed_handlers[range_key]` (what has already completed).
3. For each dependency handler name not in the completed set, insert it as completed-no-op.
4. Call `try_process_pending_events(range_key)` to unblock any waiting handlers.
5. Then proceed to `maybe_finalize_range`.

**Registry helper needed:** A method like `all_dependency_handler_names() -> HashSet<String>` that returns the union of all values in `handler_dependency_graph`. This can be precomputed during `validate_and_sort_handler_dependencies()` and stored as a field. Or computed on the fly from `handler_dependency_graph()`.

**Interaction with `process_range_complete`:** Currently `process_range_complete` just marks completion and calls `maybe_finalize_range`. The no-op + unblock step goes between. Since `process_range_complete` is on `RangeFinalizer` (not the engine), it doesn't have access to `try_process_pending_events`. Options:
- Move the no-op logic to the engine's `run()` loop where `complete_rx` messages are handled (the engine has access to both finalizer and live_state)
- Pass a callback or the engine ref to the finalizer

The cleanest approach is to handle it in the engine's `run()` loop: when receiving a `RangeCompleteKind::Logs` message, do the no-op marking and pending dispatch BEFORE calling `finalizer.process_range_complete`.

### Phase 5: Wire Up Handlers

- Add `handler_dependencies()` to swap/metrics handlers that depend on create handlers.
- Integration test: block with pool creation and swap in same block, verify create handler runs before swap handler.
