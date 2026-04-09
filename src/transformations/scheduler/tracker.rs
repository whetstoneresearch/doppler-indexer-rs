//! Completion tracking with notify-based wake-up for dependency waiters.
//!
//! [`CompletionTracker`] is the synchronization primitive used by [`DagScheduler`]
//! to gate each `(handler, range_start)` work item on its dependencies. Each
//! waiter constructs a `Notify::notified()` future, checks shared state, and
//! awaits the future if any dep is still pending. Every `mark_completed` /
//! `mark_failed` call wakes all waiters so they can re-check. The ordering
//! "construct notified() *before* probe" is load-bearing: tokio's Notified
//! captures the notify_waiters call count at construction and honors any
//! call that happens before first poll, so no wake can be lost between
//! probe and `.await`.
//!
//! # Wake mechanism
//!
//! [`tokio::sync::Notify`] with `notify_waiters` on every completion/failure.
//! The codebase has no other `Notify` usage, but here it's the natural primitive:
//! each waiter re-checks its predicate against shared state after being woken.
//! Under heavy cascade-failure the wake-storm could become noisy; a per-handler
//! `watch<HashSet<u64>>` would be an isolated drop-in swap if profiling shows
//! contention.
//!
//! [`DagScheduler`]: super::dag::DagScheduler

use std::collections::{HashMap, HashSet};

use tokio::sync::{Notify, RwLock};

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
/// - **Call-dep ranges**: per-`(source, function)` set of `(range_start,
///   range_end)` pairs where decoded call parquet files are available on disk.
///   Updated by a background scanner task.
///
/// [`WorkItem`]: super::dag::WorkItem
/// [`wait_ready`]: CompletionTracker::wait_ready
pub(crate) struct CompletionTracker {
    completed: RwLock<HashMap<String, HashSet<u64>>>,
    failed: RwLock<HashMap<String, HashSet<u64>>>,
    blocked: RwLock<HashMap<String, HashSet<u64>>>,
    notify: Notify,
    /// Sorted list of all available range_starts (immutable after construction).
    available_starts: Vec<u64>,
    /// Per-handler contiguous watermark: highest `range_start` where all ranges
    /// from the first available through this one are completed with no gaps.
    contiguous_watermarks: RwLock<HashMap<String, Option<u64>>>,
    /// Per-handler index into `available_starts` for O(1) amortized watermark advance.
    contiguous_positions: RwLock<HashMap<String, usize>>,
    /// Per-`(source, function)` set of available call-dep range keys.
    /// Updated by the background `CallDepScanner`.
    call_dep_ranges: RwLock<HashMap<(String, String), HashSet<(u64, u64)>>>,
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
            completed: RwLock::new(HashMap::new()),
            failed: RwLock::new(HashMap::new()),
            blocked: RwLock::new(HashMap::new()),
            notify: Notify::new(),
            available_starts: Vec::new(),
            contiguous_watermarks: RwLock::new(HashMap::new()),
            contiguous_positions: RwLock::new(HashMap::new()),
            call_dep_ranges: RwLock::new(HashMap::new()),
        }
    }

    /// Create a tracker with a known set of available range starts.
    ///
    /// The `starts` must be sorted ascending. This enables contiguous watermark
    /// tracking for handlers with `contiguous_handler_dependencies`.
    pub(crate) fn with_available_starts(starts: Vec<u64>) -> Self {
        debug_assert!(starts.windows(2).all(|w| w[0] < w[1]), "starts must be sorted");
        Self {
            completed: RwLock::new(HashMap::new()),
            failed: RwLock::new(HashMap::new()),
            blocked: RwLock::new(HashMap::new()),
            notify: Notify::new(),
            available_starts: starts,
            contiguous_watermarks: RwLock::new(HashMap::new()),
            contiguous_positions: RwLock::new(HashMap::new()),
            call_dep_ranges: RwLock::new(HashMap::new()),
        }
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
            let mut completed = self.completed.write().await;
            let entry = completed
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
        self.notify.notify_waiters();
    }

    /// Snapshot check: are all `deps` completed for `range_start`?
    pub(crate) async fn probe(&self, deps: &[String], range_start: u64) -> DepState {
        // Check failed first so a failed dep is reported even if others are completed.
        {
            let failed = self.failed.read().await;
            for dep in deps {
                if failed
                    .get(dep)
                    .map(|ranges| ranges.contains(&range_start))
                    .unwrap_or(false)
                {
                    return DepState::DepFailed {
                        dep_name: dep.clone(),
                    };
                }
            }
        }
        {
            let blocked = self.blocked.read().await;
            for dep in deps {
                if blocked
                    .get(dep)
                    .map(|ranges| ranges.contains(&range_start))
                    .unwrap_or(false)
                {
                    return DepState::DepBlocked {
                        dep_name: dep.clone(),
                    };
                }
            }
        }
        let completed = self.completed.read().await;
        for dep in deps {
            let has = completed
                .get(dep)
                .map(|ranges| ranges.contains(&range_start))
                .unwrap_or(false);
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
    pub(crate) async fn wait_ready(
        &self,
        deps: &[String],
        range_start: u64,
    ) -> Result<(), DepWaitError> {
        loop {
            // MUST be created before probing: the stored notify_waiters_calls
            // counter is the mechanism that catches a mark that fires after
            // our probe read but before we park on notified.
            let notified = self.notify.notified();
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
                DepState::Waiting => notified.await,
            }
        }
    }

    /// Mark `(handler_name, range_start)` as completed and wake all waiters.
    ///
    /// Also advances the contiguous watermark for this handler if applicable.
    pub(crate) async fn mark_completed(&self, handler_name: &str, range_start: u64) {
        {
            let mut completed = self.completed.write().await;
            completed
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
        }
        {
            let mut blocked = self.blocked.write().await;
            if let Some(ranges) = blocked.get_mut(handler_name) {
                ranges.remove(&range_start);
                if ranges.is_empty() {
                    blocked.remove(handler_name);
                }
            }
        }
        if !self.available_starts.is_empty() {
            self.advance_contiguous_watermark(handler_name).await;
        }
        self.notify.notify_waiters();
    }

    /// Check whether `(handler_name, range_start)` has been marked as failed.
    ///
    /// Used by the engine's item-building loop to skip WorkItems whose handler
    /// deps failed in a previous scheduler pass, avoiding wasted submissions
    /// that would immediately cascade-fail.
    pub(crate) async fn is_failed(&self, handler_name: &str, range_start: u64) -> bool {
        let failed = self.failed.read().await;
        failed
            .get(handler_name)
            .map(|ranges| ranges.contains(&range_start))
            .unwrap_or(false)
    }

    /// Mark `(handler_name, range_start)` as failed and wake all waiters.
    pub(crate) async fn mark_failed(&self, handler_name: &str, range_start: u64) {
        {
            let mut failed = self.failed.write().await;
            failed
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
        }
        {
            let mut blocked = self.blocked.write().await;
            if let Some(ranges) = blocked.get_mut(handler_name) {
                ranges.remove(&range_start);
                if ranges.is_empty() {
                    blocked.remove(handler_name);
                }
            }
        }
        self.notify.notify_waiters();
    }

    /// Mark `(handler_name, range_start)` as transiently blocked for this pass.
    pub(crate) async fn mark_blocked(&self, handler_name: &str, range_start: u64) {
        {
            let mut blocked = self.blocked.write().await;
            blocked
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
        }
        self.notify.notify_waiters();
    }

    /// Clear a transient blocked mark before the next scheduler pass.
    pub(crate) async fn clear_blocked(&self, handler_name: &str, range_start: u64) {
        let mut blocked = self.blocked.write().await;
        if let Some(ranges) = blocked.get_mut(handler_name) {
            ranges.remove(&range_start);
            if ranges.is_empty() {
                blocked.remove(handler_name);
            }
        }
    }

    /// Check whether `(handler_name, range_start)` is transiently blocked.
    #[cfg(test)]
    pub(crate) async fn is_blocked(&self, handler_name: &str, range_start: u64) -> bool {
        let blocked = self.blocked.read().await;
        blocked
            .get(handler_name)
            .map(|ranges| ranges.contains(&range_start))
            .unwrap_or(false)
    }

    // ─── Contiguous watermark tracking ──────────────────────────────────

    /// Full recomputation of contiguous watermark from position 0.
    /// Used during `seed_completed`.
    async fn recompute_contiguous_watermark(&self, handler_name: &str) {
        let completed = self.completed.read().await;
        let handler_completed = completed.get(handler_name);
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
        drop(completed);
        let mut watermarks = self.contiguous_watermarks.write().await;
        watermarks.insert(handler_name.to_string(), watermark);
        let mut positions = self.contiguous_positions.write().await;
        positions.insert(handler_name.to_string(), pos);
    }

    /// Incrementally advance the contiguous watermark after a new completion.
    ///
    /// Starting from the current position index, walks forward through
    /// `available_starts` while each successive range_start is in
    /// `completed[handler_name]`. O(k) amortized where k = newly-contiguous
    /// ranges (typically 0 or 1).
    async fn advance_contiguous_watermark(&self, handler_name: &str) {
        let completed = self.completed.read().await;
        let handler_completed = match completed.get(handler_name) {
            Some(c) => c,
            None => return,
        };

        let watermarks_read = self.contiguous_watermarks.read().await;
        let current_watermark = watermarks_read
            .get(handler_name)
            .copied()
            .flatten();
        let positions_read = self.contiguous_positions.read().await;
        let current_pos = positions_read
            .get(handler_name)
            .copied()
            .unwrap_or(0);
        drop(positions_read);
        drop(watermarks_read);

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

        // Only take write locks if something changed.
        if new_watermark != current_watermark {
            drop(completed);
            let mut watermarks = self.contiguous_watermarks.write().await;
            watermarks.insert(handler_name.to_string(), new_watermark);
            let mut positions = self.contiguous_positions.write().await;
            positions.insert(handler_name.to_string(), new_pos);
        }
    }

    /// Read the current contiguous watermark for a handler.
    pub(crate) async fn contiguous_watermark(&self, handler_name: &str) -> Option<u64> {
        let watermarks = self.contiguous_watermarks.read().await;
        watermarks.get(handler_name).copied().flatten()
    }

    // ─── Call-dep range tracking ────────────────────────────────────────

    /// Bulk-register available call-dep ranges for a `(source, function)` pair.
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
        entry.extend(ranges);
        let grew = entry.len() > old_len;
        drop(call_deps);
        if grew {
            self.notify.notify_waiters();
        }
    }

    // ─── Extended wait (handler deps + contiguous deps + call deps) ─────

    /// Snapshot check for the extended readiness condition.
    pub(crate) async fn probe_extended(
        &self,
        handler_deps: &[String],
        contiguous_deps: &[String],
        call_dep_keys: &[(String, String)],
        range_start: u64,
        range_key: (u64, u64),
    ) -> DepState {
        // 1. Check handler deps failed/blocked/waiting (existing logic).
        {
            let failed = self.failed.read().await;
            for dep in handler_deps {
                if failed.get(dep).is_some_and(|r| r.contains(&range_start)) {
                    return DepState::DepFailed { dep_name: dep.clone() };
                }
            }
        }
        {
            let blocked = self.blocked.read().await;
            for dep in handler_deps {
                if blocked.get(dep).is_some_and(|r| r.contains(&range_start)) {
                    return DepState::DepBlocked { dep_name: dep.clone() };
                }
            }
        }
        {
            let completed = self.completed.read().await;
            for dep in handler_deps {
                if !completed.get(dep).is_some_and(|r| r.contains(&range_start)) {
                    return DepState::Waiting;
                }
            }
        }

        // 2. Check contiguous handler deps.
        if !contiguous_deps.is_empty() {
            let watermarks = self.contiguous_watermarks.read().await;
            for dep in contiguous_deps {
                let watermark = watermarks.get(dep).copied().flatten();
                if !watermark.is_some_and(|w| w >= range_start) {
                    return DepState::Waiting;
                }
            }
        }

        // 3. Check call-dep file availability.
        if !call_dep_keys.is_empty() {
            let call_deps = self.call_dep_ranges.read().await;
            for (source, function) in call_dep_keys {
                let key = (source.clone(), function.clone());
                if !call_deps.get(&key).is_some_and(|r| r.contains(&range_key)) {
                    return DepState::Waiting;
                }
            }
        }

        DepState::Ready
    }

    /// Await until handler deps, contiguous watermark deps, and call-dep files
    /// are all satisfied for `(range_start, range_end)`.
    ///
    /// Same wake-safety invariant as [`wait_ready`]: `notified()` is constructed
    /// before probing.
    pub(crate) async fn wait_ready_extended(
        &self,
        handler_deps: &[String],
        contiguous_deps: &[String],
        call_dep_keys: &[(String, String)],
        range_start: u64,
        range_key: (u64, u64),
    ) -> Result<(), DepWaitError> {
        loop {
            let notified = self.notify.notified();
            match self
                .probe_extended(handler_deps, contiguous_deps, call_dep_keys, range_start, range_key)
                .await
            {
                DepState::Ready => return Ok(()),
                DepState::DepFailed { dep_name } => {
                    return Err(DepWaitError::DepFailed { dep_name, range_start })
                }
                DepState::DepBlocked { dep_name } => {
                    return Err(DepWaitError::DepBlocked { dep_name, range_start })
                }
                DepState::Waiting => notified.await,
            }
        }
    }

    // ─── Observability ──────────────────────────────────────────────────

    /// Snapshot of per-handler progress for periodic logging.
    ///
    /// Returns `(completed_count, failed_count, blocked_count)` per handler.
    pub(crate) async fn snapshot_progress(
        &self,
    ) -> HashMap<String, (usize, usize, usize)> {
        let completed = self.completed.read().await;
        let failed = self.failed.read().await;
        let blocked = self.blocked.read().await;

        let mut result: HashMap<String, (usize, usize, usize)> = HashMap::new();
        for (name, ranges) in completed.iter() {
            result.entry(name.clone()).or_default().0 = ranges.len();
        }
        for (name, ranges) in failed.iter() {
            result.entry(name.clone()).or_default().1 = ranges.len();
        }
        for (name, ranges) in blocked.iter() {
            result.entry(name.clone()).or_default().2 = ranges.len();
        }
        result
    }
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
        let tracker = Arc::new(CompletionTracker::with_available_starts(vec![100, 200, 300]));
        // A has completed 100 only; contiguous watermark = 100.
        tracker.seed_completed("A", [100]).await;

        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            tracker2
                .wait_ready_extended(&[], &names(&["A"]), &[], 200, (200, 300))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished(), "should wait for A's watermark to reach 200");

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
                .wait_ready_extended(&[], &[], &call_deps, 100, (100, 200))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished(), "should wait for call-dep");

        // Register the call-dep range.
        let mut ranges = HashSet::new();
        ranges.insert((100u64, 200u64));
        tracker.register_call_dep_ranges("src", "func", ranges).await;

        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve after call-dep registered")
            .unwrap();
    }

    #[tokio::test]
    async fn wait_ready_extended_combined_gating() {
        let tracker = Arc::new(CompletionTracker::with_available_starts(vec![100, 200, 300]));
        // Need: handler dep B completed for range 200, contiguous dep A >= 200,
        // call dep (src, func) available for (200, 300).
        tracker.seed_completed("A", [100]).await;

        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            let call_deps = vec![("src".to_string(), "func".to_string())];
            tracker2
                .wait_ready_extended(
                    &names(&["B"]),
                    &names(&["A"]),
                    &call_deps,
                    200,
                    (200, 300),
                )
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());

        // Satisfy handler dep B.
        tracker.mark_completed("B", 200).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished(), "still waiting on contiguous + call dep");

        // Satisfy contiguous dep A.
        tracker.mark_completed("A", 200).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished(), "still waiting on call dep");

        // Satisfy call dep.
        let mut ranges = HashSet::new();
        ranges.insert((200u64, 300u64));
        tracker.register_call_dep_ranges("src", "func", ranges).await;
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter should resolve after all deps satisfied")
            .unwrap();
    }

    #[tokio::test]
    async fn probe_extended_reports_failed_dep() {
        let tracker = CompletionTracker::with_available_starts(vec![100, 200]);
        tracker.mark_failed("A", 100).await;
        let state = tracker
            .probe_extended(&names(&["A"]), &[], &[], 100, (100, 200))
            .await;
        assert_eq!(
            state,
            DepState::DepFailed { dep_name: "A".to_string() }
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
}
