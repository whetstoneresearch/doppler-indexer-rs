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
// In LiveProcessingState:

/// Handlers that have completed for a given range.
/// Key: (range_start, range_end), Value: set of handler names (from name(), not handler_key())
pub completed_handlers: HashMap<(u64, u64), HashSet<String>>,

/// Events buffered because handler dependencies are not yet met.
/// Key: handler_key, Value: pending event data (similar to existing call-dep pending)
pub pending_handler_dep_events: HashMap<String, Vec<PendingEventData>>,
```

#### Dispatch Flow

When `process_events_message` receives events for a block:

1. Find all handlers registered for the event.
2. For each handler, check **both** call dependencies (existing) and handler dependencies (new):
   - **Call deps**: all `(source, function)` pairs present in `received_calls` for this range.
   - **Handler deps**: all dependency handler `name()`s present in `completed_handlers` for this range.
3. If both are satisfied, the handler is ready — add to the execution batch.
4. If either is unsatisfied, buffer in the appropriate pending structure.

When a handler completes execution:

1. Insert its `name()` into `completed_handlers` for the range.
2. Scan `pending_handler_dep_events` for handlers whose dependencies are now fully met.
3. Dispatch any newly-unblocked handlers (they may still need call-dep checks).

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

The pending state can be unified or kept as two separate maps checked together. Two separate maps is cleaner since the unblocking triggers are different (call arrival vs. handler completion), but the dispatch check gates on both.

#### Finalization

The existing finalization flow is unchanged. `check_finalization_readiness` already gates on "no pending events." Handler-dep pending events are an additional set of pending events that must drain before finalization. The 5-minute timeout applies to handler-dep pending events as well, as a safety net.

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

## Files to Modify

| File | Change |
|------|--------|
| `src/transformations/traits.rs` | Add `handler_dependencies()` to `EventHandler` trait |
| `src/transformations/registry.rs` | Dependency graph construction, topological sort, cycle detection, missing-dep validation |
| `src/transformations/live_state.rs` | Add `completed_handlers` and `pending_handler_dep_events` to `LiveProcessingState`, handler-dep readiness check |
| `src/transformations/engine.rs` | Extend `process_events_message` to check handler deps, add handler-completion dispatch, integrate no-op completion into `process_range_complete` flow |
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

### Phase 3: Live Mode Dispatch

- Add `completed_handlers` and `pending_handler_dep_events` to `LiveProcessingState`.
- Extend `process_events_message` to check handler dependencies alongside call dependencies when categorizing handlers as ready vs. pending.
- On handler completion, insert into `completed_handlers` and scan pending handlers for newly-unblocked ones.
- Dispatch unblocked handlers through the existing executor path.
- Cleanup `completed_handlers` and `pending_handler_dep_events` in `cleanup_after_finalize`.

### Phase 4: No-op Completion and Finalization

- On `RangeCompleteKind::Logs`, compute untriggered dependency handlers and mark them completed-no-op.
- Unblock any pending handlers that were waiting on the no-op'd handlers.
- Extend `check_finalization_readiness` to also gate on handler-dep pending events being drained.
- Apply the existing 5-minute timeout to handler-dep pending events.

### Phase 5: Wire Up Handlers

- Add `handler_dependencies()` to swap/metrics handlers that depend on create handlers.
- Integration test: block with pool creation and swap in same block, verify create handler runs before swap handler.
