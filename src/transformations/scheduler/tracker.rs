//! Completion tracking with per-handler watch channels for dependency waiters.
//!
//! [`CompletionTracker`] is the synchronization primitive used by [`DagScheduler`]
//! to gate each `(handler, range_start)` work item on its dependencies.
//!
//! # Wake mechanism
//!
//! Per-handler `tokio::sync::watch<()>` channels notify only the waiters that
//! depend on a specific handler, avoiding the O(total_waiters) wake-storm that
//! a single global `Notify` would cause. A separate global `Notify` is used for
//! call-dep file registration events (infrequent, from the background scanner).
//!
//! Waiters subscribe to their dep handlers' watch channels before probing state.
//! `watch::Receiver::changed()` is cancellation-safe and captures any send that
//! occurs between subscription and `.await`, preserving the race-free invariant.
//!
//! # Lock ordering
//!
//! Three independent RwLocks: `state`, `contiguous`, `call_dep_ranges`.
//!
//! **Invariant**: never hold `state` write lock and `contiguous` write lock
//! simultaneously. `advance_contiguous_watermark` reads `state` then writes
//! `contiguous` — safe ordering. `call_dep_ranges` is independent.
//!
//! `handler_watches` uses `std::sync::RwLock` for synchronous access (never held
//! across `.await`).
//!
//! [`DagScheduler`]: super::dag::DagScheduler

use std::collections::{HashMap, HashSet};

use tokio::sync::{watch, Notify, RwLock};

/// Per-handler completed/failed/blocked range state, grouped under one lock.
struct HandlerRangeState {
    completed: HashMap<String, HashSet<u64>>,
    failed: HashMap<String, HashSet<u64>>,
    blocked: HashMap<String, HashSet<u64>>,
}

/// Per-handler contiguous watermark state, grouped under one lock.
struct ContiguousState {
    watermarks: HashMap<String, Option<u64>>,
    positions: HashMap<String, usize>,
}

/// In-memory state of per-`(handler_name, range_start)` completion/failure.
///
/// Seeded at startup from `_handler_progress`, then updated as [`WorkItem`]s
/// finish. Waiters register interest via [`wait_ready`] and are woken on every
/// state transition.
///
/// In continuous-scheduler mode, also tracks:
/// - **Contiguous watermarks**: per-handler highest `range_start` with no gap
///   from the first available range. Used to gate handlers with
///   `contiguous_handler_dependencies`.
/// - **Call-dep ranges**: per-`(source, function)` set of `range_start`
///   values where decoded call parquet files are available on disk.
///   Matched by range_start only (not range_end) since log and call-dep
///   parquet files may use different range sizes.
///   Updated by a background scanner task.
///
/// [`WorkItem`]: super::dag::WorkItem
/// [`wait_ready`]: CompletionTracker::wait_ready
pub(crate) struct CompletionTracker {
    state: RwLock<HandlerRangeState>,
    contiguous: RwLock<ContiguousState>,
    /// Per-`(source, function)` set of available call-dep range_starts.
    /// Matched by range_start only (log and call-dep files may have different range sizes).
    /// Updated by the background `CallDepScanner`.
    call_dep_ranges: RwLock<HashMap<(String, String), HashSet<u64>>>,
    /// Per-handler watch channels. Sends on completion/failure/block wake only
    /// the waiters that depend on that specific handler. Lazily populated via
    /// `get_or_create_watch`. Uses `std::sync::RwLock` (never held across await).
    handler_watches: std::sync::RwLock<HashMap<String, watch::Sender<()>>>,
    /// Global notify for call-dep file registration (background scanner).
    global_notify: Notify,
    /// Sorted list of all available range_starts (immutable after construction).
    available_starts: Vec<u64>,
    /// Range_start value at or below which all `completed` entries have been
    /// pruned to reclaim memory. All handlers completed these ranges
    /// contiguously, so queries treat them as implicitly completed.
    /// Uses `std::sync::RwLock` because the critical section is trivial and
    /// never held across `.await`.
    pruned_up_to: std::sync::RwLock<Option<u64>>,
}

/// Snapshot view of whether a set of dependencies is satisfied for a range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DepState {
    /// All deps have the given range marked completed.
    Ready,
    /// At least one dep has not yet marked the range completed or failed.
    Waiting,
    /// At least one dep has the range marked failed.
    DepFailed { dep_name: String },
    /// At least one dep is transiently blocked for this pass.
    DepBlocked { dep_name: String },
}

/// Returned by [`wait_ready`] when a dependency cannot be satisfied this pass.
///
/// [`wait_ready`]: CompletionTracker::wait_ready
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DepWaitError {
    #[error("dependency '{dep_name}' failed for range_start {range_start}")]
    DepFailed { dep_name: String, range_start: u64 },
    #[error("dependency '{dep_name}' blocked for range_start {range_start}")]
    DepBlocked { dep_name: String, range_start: u64 },
}

impl CompletionTracker {
    pub(crate) fn new() -> Self {
        Self {
            state: RwLock::new(HandlerRangeState {
                completed: HashMap::new(),
                failed: HashMap::new(),
                blocked: HashMap::new(),
            }),
            contiguous: RwLock::new(ContiguousState {
                watermarks: HashMap::new(),
                positions: HashMap::new(),
            }),
            call_dep_ranges: RwLock::new(HashMap::new()),
            handler_watches: std::sync::RwLock::new(HashMap::new()),
            global_notify: Notify::new(),
            available_starts: Vec::new(),
            pruned_up_to: std::sync::RwLock::new(None),
        }
    }

    /// Create a tracker with a known set of available range starts.
    ///
    /// The `starts` must be sorted ascending. This enables contiguous watermark
    /// tracking for handlers with `contiguous_handler_dependencies`.
    pub(crate) fn with_available_starts(starts: Vec<u64>) -> Self {
        debug_assert!(
            starts.windows(2).all(|w| w[0] < w[1]),
            "starts must be sorted"
        );
        Self {
            state: RwLock::new(HandlerRangeState {
                completed: HashMap::new(),
                failed: HashMap::new(),
                blocked: HashMap::new(),
            }),
            contiguous: RwLock::new(ContiguousState {
                watermarks: HashMap::new(),
                positions: HashMap::new(),
            }),
            call_dep_ranges: RwLock::new(HashMap::new()),
            handler_watches: std::sync::RwLock::new(HashMap::new()),
            global_notify: Notify::new(),
            available_starts: starts,
            pruned_up_to: std::sync::RwLock::new(None),
        }
    }

    // ─── Per-handler watch helpers ─────────────────────────────────────

    /// Send a notification on the handler's watch channel.
    /// Lazily creates the channel if it doesn't exist.
    fn notify_handler(&self, handler_name: &str) {
        // Fast path: read lock
        {
            let watches = self.handler_watches.read().unwrap();
            if let Some(tx) = watches.get(handler_name) {
                let _ = tx.send(());
                return;
            }
        }
        // Slow path: create and send
        let mut watches = self.handler_watches.write().unwrap();
        let tx = watches
            .entry(handler_name.to_string())
            .or_insert_with(|| watch::channel(()).0);
        let _ = tx.send(());
    }

    /// Subscribe to a handler's watch channel. Returns `None` if the handler
    /// has no watch yet (caller should fall back to global_notify).
    fn subscribe_handler(&self, handler_name: &str) -> watch::Receiver<()> {
        // Fast path: read lock
        {
            let watches = self.handler_watches.read().unwrap();
            if let Some(tx) = watches.get(handler_name) {
                return tx.subscribe();
            }
        }
        // Slow path: create watch and subscribe
        let mut watches = self.handler_watches.write().unwrap();
        let tx = watches
            .entry(handler_name.to_string())
            .or_insert_with(|| watch::channel(()).0);
        tx.subscribe()
    }

    /// Pre-populate completed ranges for a handler. Intended for startup
    /// seeding from `_handler_progress`. Calls `notify_waiters` so that any
    /// waiters that happen to be active (e.g. Phase 4 live-mode overlap)
    /// see the newly-seeded state.
    ///
    /// Also computes the initial contiguous watermark for this handler.
    pub(crate) async fn seed_completed(
        &self,
        handler_name: &str,
        ranges: impl IntoIterator<Item = u64>,
    ) {
        {
            let mut state = self.state.write().await;
            let entry = state
                .completed
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new);
            for range in ranges {
                entry.insert(range);
            }
        }
        // Compute initial contiguous watermark from seeded data.
        if !self.available_starts.is_empty() {
            self.recompute_contiguous_watermark(handler_name).await;
        }
        self.notify_handler(handler_name);
    }

    /// Snapshot check: are all `deps` completed for `range_start`?
    ///
    /// Takes ONE read lock on `state` for all failed/blocked/completed checks.
    /// Ranges at or below the pruned watermark are implicitly completed.
    #[allow(dead_code)]
    pub(crate) async fn probe(&self, deps: &[String], range_start: u64) -> DepState {
        if self.is_pruned(range_start) {
            return DepState::Ready;
        }
        let state = self.state.read().await;
        // Check failed first so a failed dep is reported even if others are completed.
        for dep in deps {
            if state
                .failed
                .get(dep)
                .is_some_and(|ranges| ranges.contains(&range_start))
            {
                return DepState::DepFailed {
                    dep_name: dep.clone(),
                };
            }
        }
        for dep in deps {
            if state
                .blocked
                .get(dep)
                .is_some_and(|ranges| ranges.contains(&range_start))
            {
                return DepState::DepBlocked {
                    dep_name: dep.clone(),
                };
            }
        }
        for dep in deps {
            let has = state
                .completed
                .get(dep)
                .is_some_and(|ranges| ranges.contains(&range_start));
            if !has {
                return DepState::Waiting;
            }
        }
        DepState::Ready
    }

    /// Await until every `(dep, range_start)` pair is completed, or return
    /// [`DepFailed`] if any dep has that range marked failed.
    ///
    /// Construct `notified()` **before** probing state so that any
    /// `notify_waiters()` call that lands between the probe read and the
    /// first poll of `notified` still unblocks this waiter. Tokio's
    /// `Notified` captures the `notify_waiters` call count at construction;
    /// `poll_notified` compares it to the current count and, if it changed,
    /// transitions directly to `Done`. That's what makes the bare
    /// `notified()` / `probe()` / `.await` sequence race-free here.
    #[allow(dead_code)]
    pub(crate) async fn wait_ready(
        &self,
        deps: &[String],
        range_start: u64,
    ) -> Result<(), DepWaitError> {
        // Subscribe to each dep handler's watch channel before the first probe.
        // `watch::Receiver::changed()` captures any send after subscription,
        // preserving the same race-free invariant as the old `notified()` pattern.
        let mut receivers: Vec<watch::Receiver<()>> =
            deps.iter().map(|d| self.subscribe_handler(d)).collect();

        loop {
            match self.probe(deps, range_start).await {
                DepState::Ready => return Ok(()),
                DepState::DepFailed { dep_name } => {
                    return Err(DepWaitError::DepFailed {
                        dep_name,
                        range_start,
                    })
                }
                DepState::DepBlocked { dep_name } => {
                    return Err(DepWaitError::DepBlocked {
                        dep_name,
                        range_start,
                    })
                }
                DepState::Waiting => {
                    wait_any_changed(&mut receivers).await;
                }
            }
        }
    }

    /// Mark `(handler_name, range_start)` as completed and wake all waiters.
    ///
    /// Takes ONE write lock on `state` for both the completed insert and the
    /// blocked cleanup (was 2 separate locks).
    ///
    /// Also advances the contiguous watermark for this handler if applicable.
    pub(crate) async fn mark_completed(&self, handler_name: &str, range_start: u64) {
        {
            let mut state = self.state.write().await;
            state
                .completed
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
            if let Some(ranges) = state.blocked.get_mut(handler_name) {
                ranges.remove(&range_start);
                if ranges.is_empty() {
                    state.blocked.remove(handler_name);
                }
            }
        }
        if !self.available_starts.is_empty() {
            self.advance_contiguous_watermark(handler_name).await;
        }
        self.notify_handler(handler_name);
    }

    /// Check whether `(handler_name, range_start)` has been marked as completed.
    pub(crate) async fn is_completed(&self, handler_name: &str, range_start: u64) -> bool {
        if self.is_pruned(range_start) {
            return true;
        }
        let state = self.state.read().await;
        state
            .completed
            .get(handler_name)
            .is_some_and(|ranges| ranges.contains(&range_start))
    }

    /// Check whether `(handler_name, range_start)` has been marked as failed.
    ///
    /// Used by the engine's item-building loop to skip WorkItems whose handler
    /// deps failed in a previous scheduler pass, avoiding wasted submissions
    /// that would immediately cascade-fail.
    pub(crate) async fn is_failed(&self, handler_name: &str, range_start: u64) -> bool {
        let state = self.state.read().await;
        state
            .failed
            .get(handler_name)
            .is_some_and(|ranges| ranges.contains(&range_start))
    }

    /// Mark `(handler_name, range_start)` as failed and wake all waiters.
    ///
    /// Takes ONE write lock on `state` for both the failed insert and the
    /// blocked cleanup (was 2 separate locks).
    pub(crate) async fn mark_failed(&self, handler_name: &str, range_start: u64) {
        {
            let mut state = self.state.write().await;
            state
                .failed
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
            if let Some(ranges) = state.blocked.get_mut(handler_name) {
                ranges.remove(&range_start);
                if ranges.is_empty() {
                    state.blocked.remove(handler_name);
                }
            }
        }
        self.notify_handler(handler_name);
    }

    /// Mark `(handler_name, range_start)` as transiently blocked for this pass.
    pub(crate) async fn mark_blocked(&self, handler_name: &str, range_start: u64) {
        {
            let mut state = self.state.write().await;
            state
                .blocked
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
        }
        self.notify_handler(handler_name);
    }

    /// Clear a transient blocked mark before the next scheduler pass.
    #[allow(dead_code)]
    pub(crate) async fn clear_blocked(&self, handler_name: &str, range_start: u64) {
        let mut state = self.state.write().await;
        if let Some(ranges) = state.blocked.get_mut(handler_name) {
            ranges.remove(&range_start);
            if ranges.is_empty() {
                state.blocked.remove(handler_name);
            }
        }
    }

    /// Check whether `(handler_name, range_start)` is transiently blocked.
    #[cfg(test)]
    pub(crate) async fn is_blocked(&self, handler_name: &str, range_start: u64) -> bool {
        let state = self.state.read().await;
        state
            .blocked
            .get(handler_name)
            .is_some_and(|ranges| ranges.contains(&range_start))
    }

    // ─── Contiguous watermark tracking ──────────────────────────────────

    /// Full recomputation of contiguous watermark from position 0.
    /// Used during `seed_completed`.
    ///
    /// Reads `state` (read lock) then writes `contiguous` (write lock).
    async fn recompute_contiguous_watermark(&self, handler_name: &str) {
        let state = self.state.read().await;
        let handler_completed = state.completed.get(handler_name);
        let mut pos = 0usize;
        let mut watermark: Option<u64> = None;
        for (i, &start) in self.available_starts.iter().enumerate() {
            if handler_completed.is_some_and(|c| c.contains(&start)) {
                watermark = Some(start);
                pos = i;
            } else {
                break;
            }
        }
        drop(state);
        let mut contiguous = self.contiguous.write().await;
        contiguous
            .watermarks
            .insert(handler_name.to_string(), watermark);
        contiguous.positions.insert(handler_name.to_string(), pos);
    }

    /// Incrementally advance the contiguous watermark after a new completion.
    ///
    /// Starting from the current position index, walks forward through
    /// `available_starts` while each successive range_start is in
    /// `completed[handler_name]`. O(k) amortized where k = newly-contiguous
    /// ranges (typically 0 or 1).
    ///
    /// Reads `state` (read lock), then reads/writes `contiguous` as needed.
    /// Never holds both write locks simultaneously.
    async fn advance_contiguous_watermark(&self, handler_name: &str) {
        let state = self.state.read().await;
        let handler_completed = match state.completed.get(handler_name) {
            Some(c) => c,
            None => return,
        };

        let contiguous_read = self.contiguous.read().await;
        let current_watermark = contiguous_read
            .watermarks
            .get(handler_name)
            .copied()
            .flatten();
        let current_pos = contiguous_read
            .positions
            .get(handler_name)
            .copied()
            .unwrap_or(0);
        drop(contiguous_read);

        // Determine the starting index for the walk. If there is no watermark
        // yet, start from 0 (the handler might now have completed the first
        // range). Otherwise start from the position after the current watermark.
        let start_idx = if current_watermark.is_some() {
            current_pos + 1
        } else {
            0
        };

        let mut new_pos = current_pos;
        let mut new_watermark = current_watermark;
        for i in start_idx..self.available_starts.len() {
            if handler_completed.contains(&self.available_starts[i]) {
                new_pos = i;
                new_watermark = Some(self.available_starts[i]);
            } else {
                break;
            }
        }

        // Only take write lock if something changed.
        if new_watermark != current_watermark {
            drop(state);
            let mut contiguous = self.contiguous.write().await;
            contiguous
                .watermarks
                .insert(handler_name.to_string(), new_watermark);
            contiguous
                .positions
                .insert(handler_name.to_string(), new_pos);
        }
    }

    /// Read the current contiguous watermark for a handler.
    #[allow(dead_code)]
    pub(crate) async fn contiguous_watermark(&self, handler_name: &str) -> Option<u64> {
        let contiguous = self.contiguous.read().await;
        contiguous.watermarks.get(handler_name).copied().flatten()
    }

    // ─── Call-dep range tracking ────────────────────────────────────────

    /// Bulk-register available call-dep ranges for a `(source, function)` pair.
    ///
    /// Accepts `(range_start, range_end)` tuples from the scanner but stores
    /// only `range_start` values — call-dep and log parquet files may use
    /// different range sizes, so matching on range_start alone is correct.
    ///
    /// Called by the background `CallDepScanner`. Only calls `notify_waiters`
    /// if at least one new range was added.
    pub(crate) async fn register_call_dep_ranges(
        &self,
        source: &str,
        function: &str,
        ranges: HashSet<(u64, u64)>,
    ) {
        let key = (source.to_string(), function.to_string());
        let mut call_deps = self.call_dep_ranges.write().await;
        let entry = call_deps.entry(key).or_insert_with(HashSet::new);
        let old_len = entry.len();
        entry.extend(ranges.into_iter().map(|(start, _end)| start));
        let grew = entry.len() > old_len;
        drop(call_deps);
        if grew {
            self.global_notify.notify_waiters();
        }
    }

    // ─── Compaction ──────────────────────────────────────────────────────

    /// Check whether `range_start` falls at or below the pruned watermark.
    ///
    /// Entries below this threshold have been removed from the per-handler
    /// `completed` sets but are implicitly treated as completed by all
    /// handlers.
    fn is_pruned(&self, range_start: u64) -> bool {
        self.pruned_up_to
            .read()
            .unwrap()
            .is_some_and(|wm| range_start <= wm)
    }

    /// Prune `completed` entries that all handlers have resolved
    /// contiguously, reclaiming memory from ranges that will never be
    /// queried again.
    ///
    /// The pruning threshold is the minimum contiguous watermark across
    /// the supplied `handler_names`. Every handler completed all ranges
    /// from the first available up to that watermark, so:
    ///
    /// - `is_completed` / `probe` / `probe_extended` treat pruned
    ///   range_starts as implicitly completed (via [`is_pruned`]).
    /// - `advance_contiguous_watermark` starts its walk from
    ///   `current_pos + 1`, which is always above the pruned threshold
    ///   (since the handler's own watermark >= the minimum).
    ///
    /// Only effective when `available_starts` is non-empty (catchup
    /// trackers created with [`with_available_starts`]).
    ///
    /// [`is_pruned`]: Self::is_pruned
    /// [`with_available_starts`]: Self::with_available_starts
    pub(crate) async fn compact(&self, handler_names: &[&str]) {
        if self.available_starts.is_empty() || handler_names.is_empty() {
            return;
        }

        // Find the minimum contiguous watermark across all handlers.
        let contiguous = self.contiguous.read().await;
        let min_watermark = handler_names
            .iter()
            .filter_map(|name| {
                contiguous
                    .watermarks
                    .get(*name)
                    .copied()
                    .unwrap_or(None)
            })
            .min();
        drop(contiguous);

        let Some(min_wm) = min_watermark else {
            return;
        };

        // Only advance the pruned watermark, never regress.
        {
            let current = *self.pruned_up_to.read().unwrap();
            if current.is_some_and(|c| c >= min_wm) {
                return;
            }
        }

        // Prune completed entries at or below the watermark.
        let mut state = self.state.write().await;
        for completed in state.completed.values_mut() {
            completed.retain(|r| *r > min_wm);
        }
        drop(state);

        *self.pruned_up_to.write().unwrap() = Some(min_wm);

        tracing::debug!(
            "CompletionTracker compacted: pruned completed entries at or below range_start {}",
            min_wm
        );
    }

    // ─── Extended wait (handler deps + contiguous deps + call deps) ─────

    /// Snapshot check for the extended readiness condition.
    ///
    /// Takes ONE read lock on `state` for all failed/blocked/completed checks
    /// (was 3 separate locks).
    pub(crate) async fn probe_extended(
        &self,
        handler_deps: &[String],
        contiguous_deps: &[String],
        call_dep_keys: &[(String, String)],
        range_start: u64,
    ) -> DepState {
        let pruned = self.is_pruned(range_start);

        // 1. Check handler deps failed/blocked/waiting under one read lock.
        //    Pruned ranges are implicitly completed by all handlers, so handler
        //    deps and contiguous deps are satisfied. Skip to call-dep checks.
        if !pruned {
            {
                let state = self.state.read().await;
                for dep in handler_deps {
                    if state
                        .failed
                        .get(dep)
                        .is_some_and(|r| r.contains(&range_start))
                    {
                        return DepState::DepFailed {
                            dep_name: dep.clone(),
                        };
                    }
                }
                for dep in handler_deps {
                    if state
                        .blocked
                        .get(dep)
                        .is_some_and(|r| r.contains(&range_start))
                    {
                        return DepState::DepBlocked {
                            dep_name: dep.clone(),
                        };
                    }
                }
                for dep in handler_deps {
                    if !state
                        .completed
                        .get(dep)
                        .is_some_and(|r| r.contains(&range_start))
                    {
                        return DepState::Waiting;
                    }
                }
            }

            // 2. Check contiguous handler deps.
            if !contiguous_deps.is_empty() {
                let contiguous = self.contiguous.read().await;
                for dep in contiguous_deps {
                    let watermark = contiguous.watermarks.get(dep).copied().flatten();
                    if watermark.is_none_or(|w| w < range_start) {
                        return DepState::Waiting;
                    }
                }
            }
        }

        // 3. Check call-dep file availability (matched by range_start only).
        if !call_dep_keys.is_empty() {
            let call_deps = self.call_dep_ranges.read().await;
            for (source, function) in call_dep_keys {
                let key = (source.clone(), function.clone());
                if !call_deps
                    .get(&key)
                    .is_some_and(|r| r.contains(&range_start))
                {
                    return DepState::Waiting;
                }
            }
        }

        DepState::Ready
    }

    /// Await until handler deps, contiguous watermark deps, and call-dep files
    /// are all satisfied for `range_start`.
    ///
    /// Call-dep availability is matched by `range_start` only (not range_end)
    /// since log and call-dep parquet files may use different range sizes.
    ///
    /// Subscribes to per-handler watch channels for handler deps and contiguous
    /// deps before the first probe. `watch::Receiver::changed()` is
    /// cancellation-safe and captures any send after subscription, preserving
    /// the race-free invariant. Call-dep changes use the global `Notify`.
    pub(crate) async fn wait_ready_extended(
        &self,
        handler_deps: &[String],
        contiguous_deps: &[String],
        call_dep_keys: &[(String, String)],
        range_start: u64,
    ) -> Result<(), DepWaitError> {
        // Collect unique dep handler names from both handler and contiguous deps.
        let mut unique_dep_names: Vec<&str> = Vec::new();
        for dep in handler_deps {
            if !unique_dep_names.contains(&dep.as_str()) {
                unique_dep_names.push(dep);
            }
        }
        for dep in contiguous_deps {
            if !unique_dep_names.contains(&dep.as_str()) {
                unique_dep_names.push(dep);
            }
        }

        // Subscribe to each dep handler's watch channel once before any probing.
        let mut receivers: Vec<watch::Receiver<()>> = unique_dep_names
            .iter()
            .map(|d| self.subscribe_handler(d))
            .collect();

        let has_call_deps = !call_dep_keys.is_empty();

        loop {
            // For call-dep changes, use global notify (created before probe).
            let global_notified = if has_call_deps {
                Some(self.global_notify.notified())
            } else {
                None
            };

            match self
                .probe_extended(handler_deps, contiguous_deps, call_dep_keys, range_start)
                .await
            {
                DepState::Ready => return Ok(()),
                DepState::DepFailed { dep_name } => {
                    return Err(DepWaitError::DepFailed {
                        dep_name,
                        range_start,
                    })
                }
                DepState::DepBlocked { dep_name } => {
                    return Err(DepWaitError::DepBlocked {
                        dep_name,
                        range_start,
                    })
                }
                DepState::Waiting => match global_notified {
                    Some(global) => {
                        tokio::select! {
                            _ = wait_any_changed(&mut receivers) => {},
                            _ = global => {},
                        }
                    }
                    None => {
                        wait_any_changed(&mut receivers).await;
                    }
                },
            }
        }
    }

    // ─── Observability ──────────────────────────────────────────────────

    /// Snapshot of per-handler progress for periodic logging.
    ///
    /// Returns `(completed_count, failed_count, blocked_count)` per handler.
    /// Takes ONE read lock on `state` (was 3 separate locks).
    pub(crate) async fn snapshot_progress(&self) -> HashMap<String, (usize, usize, usize)> {
        let state = self.state.read().await;

        let mut result: HashMap<String, (usize, usize, usize)> = HashMap::new();
        for (name, ranges) in state.completed.iter() {
            result.entry(name.clone()).or_default().0 = ranges.len();
        }
        for (name, ranges) in state.failed.iter() {
            result.entry(name.clone()).or_default().1 = ranges.len();
        }
        for (name, ranges) in state.blocked.iter() {
            result.entry(name.clone()).or_default().2 = ranges.len();
        }
        result
    }
}

/// Wait until any of the given watch receivers reports a change.
///
/// Returns immediately if `receivers` is empty (no deps to wait on).
/// Cancellation-safe: cancelled `changed()` futures do not lose events.
async fn wait_any_changed(receivers: &mut [watch::Receiver<()>]) {
    if receivers.is_empty() {
        // No deps: yield once and return (avoids infinite pending).
        tokio::task::yield_now().await;
        return;
    }
    if receivers.len() == 1 {
        // Fast path: single dep, no select overhead.
        let _ = receivers[0].changed().await;
        return;
    }
    // General case: wait for any receiver to report a change.
    let futures: Vec<_> = receivers
        .iter_mut()
        .map(|rx| Box::pin(rx.changed()))
        .collect();
    let _ = futures::future::select_all(futures).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn names(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn empty_deps_is_ready() {
        let tracker = CompletionTracker::new();
        // Empty deps: should resolve immediately for any range.
        tracker.wait_ready(&[], 100).await.unwrap();
        tracker.wait_ready(&[], 999).await.unwrap();
        assert_eq!(tracker.probe(&[], 100).await, DepState::Ready);
    }

    #[tokio::test]
    async fn seed_then_wait_returns_immediately() {
        let tracker = CompletionTracker::new();
        tracker.seed_completed("A", [100, 101, 102]).await;
        // Must return without blocking even if we never call mark_completed.
        tokio::time::timeout(
            Duration::from_millis(50),
            tracker.wait_ready(&names(&["A"]), 100),
        )
        .await
        .expect("wait_ready should return immediately after seed")
        .unwrap();
    }

    #[tokio::test]
    async fn wait_then_complete_wakes_waiter() {
        let tracker = Arc::new(CompletionTracker::new());
        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            tracker2.wait_ready(&names(&["A"]), 100).await.unwrap();
        });
        // Yield so the spawned task has a chance to register.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());
        tracker.mark_completed("A", 100).await;
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve within timeout")
            .unwrap();
    }

    #[tokio::test]
    async fn multiple_waiters_on_same_dep_all_wake() {
        let tracker = Arc::new(CompletionTracker::new());
        let mut handles = Vec::new();
        for _ in 0..5 {
            let tracker = tracker.clone();
            handles.push(tokio::spawn(async move {
                tracker.wait_ready(&names(&["A"]), 100).await.unwrap();
            }));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        tracker.mark_completed("A", 100).await;
        for handle in handles {
            tokio::time::timeout(Duration::from_millis(100), handle)
                .await
                .expect("every waiter should resolve")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn wait_returns_dep_failed_on_failure() {
        let tracker = CompletionTracker::new();
        tracker.mark_failed("A", 100).await;
        let err = tracker.wait_ready(&names(&["A"]), 100).await.unwrap_err();
        assert_eq!(
            err,
            DepWaitError::DepFailed {
                dep_name: "A".to_string(),
                range_start: 100,
            }
        );
    }

    #[tokio::test]
    async fn failed_dep_wakes_waiters_immediately() {
        let tracker = Arc::new(CompletionTracker::new());
        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move { tracker2.wait_ready(&names(&["A"]), 100).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());
        tracker.mark_failed("A", 100).await;
        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve")
            .unwrap();
        assert_eq!(
            result.unwrap_err(),
            DepWaitError::DepFailed {
                dep_name: "A".to_string(),
                range_start: 100,
            }
        );
    }

    #[tokio::test]
    async fn multiple_deps_all_must_complete() {
        let tracker = Arc::new(CompletionTracker::new());
        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            tracker2.wait_ready(&names(&["A", "B"]), 100).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        tracker.mark_completed("A", 100).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished(), "should still wait for B");

        tracker.mark_completed("B", 100).await;
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve after both deps complete")
            .unwrap();
    }

    #[tokio::test]
    async fn unrelated_marks_dont_resolve_waiter() {
        let tracker = Arc::new(CompletionTracker::new());
        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            tracker2.wait_ready(&names(&["A"]), 100).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Mark a different range for A.
        tracker.mark_completed("A", 200).await;
        // Mark the same range but for a different handler.
        tracker.mark_completed("B", 100).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !handle.is_finished(),
            "unrelated marks must not resolve waiter"
        );

        // Now mark the actual dep.
        tracker.mark_completed("A", 100).await;
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve after correct mark")
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_mark_and_wait_no_lost_wakeup() {
        // Stress test: many waiters spawned concurrently with marks, all should
        // resolve. This exercises the register-before-probe invariant.
        let tracker = Arc::new(CompletionTracker::new());
        let n = 50usize;

        let mut waiters = Vec::new();
        for i in 0..n {
            let tracker = tracker.clone();
            waiters.push(tokio::spawn(async move {
                let name = format!("H{}", i);
                let range = (i as u64) * 10;
                tokio::time::timeout(Duration::from_secs(2), tracker.wait_ready(&[name], range))
                    .await
                    .expect("no lost wakeup")
                    .unwrap();
            }));
        }

        // Interleave marks with the waiter spawns completing registration.
        for i in 0..n {
            let tracker = tracker.clone();
            let name = format!("H{}", i);
            let range = (i as u64) * 10;
            tokio::spawn(async move {
                tracker.mark_completed(&name, range).await;
            });
        }

        for w in waiters {
            w.await.unwrap();
        }
    }

    #[tokio::test]
    async fn probe_matches_wait_outcome() {
        let tracker = CompletionTracker::new();

        // Waiting.
        assert_eq!(tracker.probe(&names(&["A"]), 100).await, DepState::Waiting);

        // Ready.
        tracker.mark_completed("A", 100).await;
        assert_eq!(tracker.probe(&names(&["A"]), 100).await, DepState::Ready);
        tracker.wait_ready(&names(&["A"]), 100).await.unwrap();

        // DepFailed.
        tracker.mark_failed("B", 200).await;
        assert_eq!(
            tracker.probe(&names(&["B"]), 200).await,
            DepState::DepFailed {
                dep_name: "B".to_string()
            }
        );
        assert_eq!(
            tracker.wait_ready(&names(&["B"]), 200).await.unwrap_err(),
            DepWaitError::DepFailed {
                dep_name: "B".to_string(),
                range_start: 200,
            }
        );
    }

    #[tokio::test]
    async fn failed_dep_reported_even_if_others_complete() {
        // A is completed, B is failed: wait_ready should return DepFailed for B.
        let tracker = CompletionTracker::new();
        tracker.mark_completed("A", 100).await;
        tracker.mark_failed("B", 100).await;
        let err = tracker
            .wait_ready(&names(&["A", "B"]), 100)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            DepWaitError::DepFailed {
                dep_name: "B".to_string(),
                range_start: 100,
            }
        );
    }

    #[tokio::test]
    async fn is_failed_reflects_mark_failed() {
        let tracker = CompletionTracker::new();
        assert!(!tracker.is_failed("A", 100).await);
        tracker.mark_failed("A", 100).await;
        assert!(tracker.is_failed("A", 100).await);
        // Different range is not failed.
        assert!(!tracker.is_failed("A", 200).await);
        // Different handler is not failed.
        assert!(!tracker.is_failed("B", 100).await);
        // Completed handler is not failed.
        tracker.mark_completed("C", 100).await;
        assert!(!tracker.is_failed("C", 100).await);
    }

    #[tokio::test]
    async fn seed_merges_with_subsequent_marks() {
        let tracker = CompletionTracker::new();
        tracker.seed_completed("A", [100, 101]).await;
        tracker.mark_completed("A", 102).await;
        tracker.wait_ready(&names(&["A"]), 100).await.unwrap();
        tracker.wait_ready(&names(&["A"]), 101).await.unwrap();
        tracker.wait_ready(&names(&["A"]), 102).await.unwrap();
    }

    #[tokio::test]
    async fn wait_returns_dep_blocked_on_block() {
        let tracker = CompletionTracker::new();
        tracker.mark_blocked("A", 100).await;
        assert_eq!(
            tracker.wait_ready(&names(&["A"]), 100).await.unwrap_err(),
            DepWaitError::DepBlocked {
                dep_name: "A".to_string(),
                range_start: 100,
            }
        );
    }

    #[tokio::test]
    async fn clear_blocked_removes_transient_state() {
        let tracker = CompletionTracker::new();
        tracker.mark_blocked("A", 100).await;
        assert!(tracker.is_blocked("A", 100).await);
        tracker.clear_blocked("A", 100).await;
        assert!(!tracker.is_blocked("A", 100).await);
        assert_eq!(tracker.probe(&names(&["A"]), 100).await, DepState::Waiting);
    }

    // ─── Contiguous watermark tests ─────────────────────────────────────

    #[tokio::test]
    async fn contiguous_watermark_seed_computes_initial() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300, 400, 500]);
        // Seed with a contiguous prefix.
        tracker.seed_completed("A", [100, 200, 300]).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(300));
    }

    #[tokio::test]
    async fn contiguous_watermark_seed_with_gap() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300, 400, 500]);
        // Gap at 200 means watermark stops at 100.
        tracker.seed_completed("A", [100, 300, 400]).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(100));
    }

    #[tokio::test]
    async fn contiguous_watermark_seed_missing_first() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300]);
        // First range not completed.
        tracker.seed_completed("A", [200, 300]).await;
        assert_eq!(tracker.contiguous_watermark("A").await, None);
    }

    #[tokio::test]
    async fn contiguous_watermark_advances_on_mark_completed() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300, 400]);
        tracker.seed_completed("A", [100, 200]).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(200));

        // Complete 300 — watermark should advance.
        tracker.mark_completed("A", 300).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(300));
    }

    #[tokio::test]
    async fn contiguous_watermark_fills_gap() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300, 400]);
        tracker.seed_completed("A", [100, 300, 400]).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(100));

        // Fill the gap at 200 — watermark should jump to 400.
        tracker.mark_completed("A", 200).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(400));
    }

    #[tokio::test]
    async fn contiguous_watermark_no_available_starts() {
        let tracker = CompletionTracker::new(); // No available_starts.
        tracker.seed_completed("A", [100, 200]).await;
        // Should return None — no available_starts to track against.
        assert_eq!(tracker.contiguous_watermark("A").await, None);
    }

    // ─── Extended wait tests ────────────────────────────────────────────

    #[tokio::test]
    async fn wait_ready_extended_contiguous_dep_gates() {
        let tracker = Arc::new(CompletionTracker::with_available_starts(vec![
            100, 200, 300,
        ]));
        // A has completed 100 only; contiguous watermark = 100.
        tracker.seed_completed("A", [100]).await;

        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            tracker2
                .wait_ready_extended(&[], &names(&["A"]), &[], 200)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !handle.is_finished(),
            "should wait for A's watermark to reach 200"
        );

        // Complete A:200 → watermark advances to 200 → waiter unblocks.
        tracker.mark_completed("A", 200).await;
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve")
            .unwrap();
    }

    #[tokio::test]
    async fn wait_ready_extended_call_dep_gates() {
        let tracker = Arc::new(CompletionTracker::with_available_starts(vec![100, 200]));

        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            let call_deps = vec![("src".to_string(), "func".to_string())];
            tracker2
                .wait_ready_extended(&[], &[], &call_deps, 100)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished(), "should wait for call-dep");

        // Register the call-dep range (range_end differs from log's range_end — only range_start matters).
        let mut ranges = HashSet::new();
        ranges.insert((100u64, 250u64)); // Different range_end than log's (100, 200)
        tracker
            .register_call_dep_ranges("src", "func", ranges)
            .await;

        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve after call-dep registered (matched by range_start only)")
            .unwrap();
    }

    #[tokio::test]
    async fn wait_ready_extended_combined_gating() {
        let tracker = Arc::new(CompletionTracker::with_available_starts(vec![
            100, 200, 300,
        ]));
        // Need: handler dep B completed for range 200, contiguous dep A >= 200,
        // call dep (src, func) available for range_start 200.
        tracker.seed_completed("A", [100]).await;

        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            let call_deps = vec![("src".to_string(), "func".to_string())];
            tracker2
                .wait_ready_extended(&names(&["B"]), &names(&["A"]), &call_deps, 200)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());

        // Satisfy handler dep B.
        tracker.mark_completed("B", 200).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !handle.is_finished(),
            "still waiting on contiguous + call dep"
        );

        // Satisfy contiguous dep A.
        tracker.mark_completed("A", 200).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished(), "still waiting on call dep");

        // Satisfy call dep (call-dep file has different range_end — only range_start matters).
        let mut ranges = HashSet::new();
        ranges.insert((200u64, 400u64));
        tracker
            .register_call_dep_ranges("src", "func", ranges)
            .await;
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve after all deps satisfied")
            .unwrap();
    }

    #[tokio::test]
    async fn probe_extended_reports_failed_dep() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200]);
        tracker.mark_failed("A", 100).await;
        let state = tracker.probe_extended(&names(&["A"]), &[], &[], 100).await;
        assert_eq!(
            state,
            DepState::DepFailed {
                dep_name: "A".to_string()
            }
        );
    }

    #[tokio::test]
    async fn snapshot_progress_returns_counts() {
        let tracker = CompletionTracker::new();
        tracker.mark_completed("A", 100).await;
        tracker.mark_completed("A", 200).await;
        tracker.mark_failed("B", 100).await;
        tracker.mark_blocked("A", 300).await;

        let snap = tracker.snapshot_progress().await;
        assert_eq!(snap.get("A"), Some(&(2, 0, 1)));
        assert_eq!(snap.get("B"), Some(&(0, 1, 0)));
    }

    // ─── Compaction tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn compact_prunes_completed_entries_below_min_watermark() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300, 400, 500]);
        tracker.seed_completed("A", [100, 200, 300, 400, 500]).await;
        tracker.seed_completed("B", [100, 200, 300]).await;

        // A's watermark = 500, B's watermark = 300, min = 300.
        tracker.compact(&["A", "B"]).await;

        // Entries at or below 300 are pruned but still report as completed.
        assert!(tracker.is_completed("A", 100).await);
        assert!(tracker.is_completed("A", 200).await);
        assert!(tracker.is_completed("A", 300).await);
        assert!(tracker.is_completed("B", 100).await);
        assert!(tracker.is_completed("B", 200).await);
        assert!(tracker.is_completed("B", 300).await);

        // Entries above 300 are still in the set.
        assert!(tracker.is_completed("A", 400).await);
        assert!(tracker.is_completed("A", 500).await);
        // B never completed 400, so it's not completed.
        assert!(!tracker.is_completed("B", 400).await);

        // Verify the actual HashSet was pruned.
        let state = tracker.state.read().await;
        let a_completed = state.completed.get("A").unwrap();
        assert!(!a_completed.contains(&100));
        assert!(!a_completed.contains(&200));
        assert!(!a_completed.contains(&300));
        assert!(a_completed.contains(&400));
        assert!(a_completed.contains(&500));
    }

    #[tokio::test]
    async fn compact_pruned_ranges_satisfy_deps() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300]);
        tracker.seed_completed("A", [100, 200, 300]).await;
        tracker.seed_completed("B", [100, 200, 300]).await;

        tracker.compact(&["A", "B"]).await;

        // Pruned ranges should satisfy dependency checks.
        assert_eq!(tracker.probe(&names(&["A"]), 100).await, DepState::Ready);
        assert_eq!(tracker.probe(&names(&["A"]), 200).await, DepState::Ready);
        assert_eq!(
            tracker
                .probe_extended(&names(&["A"]), &[], &[], 100)
                .await,
            DepState::Ready
        );
    }

    #[tokio::test]
    async fn compact_does_not_prune_past_failure() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300, 400]);
        tracker.seed_completed("A", [100, 200, 300, 400]).await;
        // B completed 100, failed at 200 → contiguous watermark = 100.
        tracker.seed_completed("B", [100]).await;
        tracker.mark_failed("B", 200).await;

        // min watermark = min(400, 100) = 100.
        tracker.compact(&["A", "B"]).await;

        // Only range 100 pruned.
        assert!(tracker.is_completed("A", 100).await); // pruned, implicit
        assert!(tracker.is_completed("A", 200).await); // still in set
        assert!(tracker.is_failed("B", 200).await); // failure preserved

        let state = tracker.state.read().await;
        let a_completed = state.completed.get("A").unwrap();
        assert!(!a_completed.contains(&100)); // pruned
        assert!(a_completed.contains(&200)); // not pruned
    }

    #[tokio::test]
    async fn compact_contiguous_watermark_advances_after_prune() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200, 300, 400]);
        tracker.seed_completed("A", [100, 200]).await;
        tracker.seed_completed("B", [100, 200]).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(200));

        // Compact prunes up to 200.
        tracker.compact(&["A", "B"]).await;

        // Now complete 300 — watermark should advance from 200 to 300.
        tracker.mark_completed("A", 300).await;
        assert_eq!(tracker.contiguous_watermark("A").await, Some(300));
    }

    #[tokio::test]
    async fn compact_noop_without_available_starts() {
        let tracker = CompletionTracker::new();
        tracker.mark_completed("A", 100).await;
        tracker.compact(&["A"]).await;

        // No pruning should occur.
        let state = tracker.state.read().await;
        assert!(state.completed.get("A").unwrap().contains(&100));
    }
}
