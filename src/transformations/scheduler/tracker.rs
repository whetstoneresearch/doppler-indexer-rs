//! Completion tracking with notify-based wake-up for dependency waiters.
//!
//! [`CompletionTracker`] is the synchronization primitive used by [`DagScheduler`]
//! to gate each `(handler, range_start)` work item on its dependencies. Each
//! waiter registers a single `Notify::notified()` future, checks shared state,
//! and awaits the future if any dep is still pending. Every `mark_completed` /
//! `mark_failed` call wakes all waiters so they can re-check.
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

use tokio::sync::{Mutex, Notify};

/// In-memory state of per-`(handler_name, range_start)` completion/failure.
///
/// Seeded at startup from `_handler_progress`, then updated as [`WorkItem`]s
/// finish. Waiters register interest via [`wait_ready`] and are woken on every
/// state transition.
///
/// [`WorkItem`]: super::dag::WorkItem
/// [`wait_ready`]: CompletionTracker::wait_ready
pub(crate) struct CompletionTracker {
    completed: Mutex<HashMap<String, HashSet<u64>>>,
    failed: Mutex<HashMap<String, HashSet<u64>>>,
    notify: Notify,
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
}

/// Returned by [`wait_ready`] when a dependency has failed.
///
/// [`wait_ready`]: CompletionTracker::wait_ready
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("dependency '{dep_name}' failed for range_start {range_start}")]
pub(crate) struct DepFailed {
    pub dep_name: String,
    pub range_start: u64,
}

impl CompletionTracker {
    pub(crate) fn new() -> Self {
        Self {
            completed: Mutex::new(HashMap::new()),
            failed: Mutex::new(HashMap::new()),
            notify: Notify::new(),
        }
    }

    /// Pre-populate completed ranges for a handler. Intended for startup
    /// seeding from `_handler_progress`; safe to call before any waiters exist.
    pub(crate) async fn seed_completed(
        &self,
        handler_name: &str,
        ranges: impl IntoIterator<Item = u64>,
    ) {
        let mut completed = self.completed.lock().await;
        let entry = completed
            .entry(handler_name.to_string())
            .or_insert_with(HashSet::new);
        for range in ranges {
            entry.insert(range);
        }
    }

    /// Snapshot check: are all `deps` completed for `range_start`?
    pub(crate) async fn probe(&self, deps: &[String], range_start: u64) -> DepState {
        // Check failed first so a failed dep is reported even if others are completed.
        {
            let failed = self.failed.lock().await;
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
        let completed = self.completed.lock().await;
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
    /// The loop registers the `notified()` future *before* probing state so
    /// wake-ups cannot be lost between the check and the await.
    pub(crate) async fn wait_ready(
        &self,
        deps: &[String],
        range_start: u64,
    ) -> Result<(), DepFailed> {
        loop {
            // Register interest BEFORE probing — otherwise a mark() between
            // probe and await could leave this waiter parked forever.
            let notified = self.notify.notified();
            match self.probe(deps, range_start).await {
                DepState::Ready => return Ok(()),
                DepState::DepFailed { dep_name } => {
                    return Err(DepFailed {
                        dep_name,
                        range_start,
                    })
                }
                DepState::Waiting => notified.await,
            }
        }
    }

    /// Mark `(handler_name, range_start)` as completed and wake all waiters.
    pub(crate) async fn mark_completed(&self, handler_name: &str, range_start: u64) {
        {
            let mut completed = self.completed.lock().await;
            completed
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
        }
        self.notify.notify_waiters();
    }

    /// Mark `(handler_name, range_start)` as failed and wake all waiters.
    pub(crate) async fn mark_failed(&self, handler_name: &str, range_start: u64) {
        {
            let mut failed = self.failed.lock().await;
            failed
                .entry(handler_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(range_start);
        }
        self.notify.notify_waiters();
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
        assert_eq!(err.dep_name, "A");
        assert_eq!(err.range_start, 100);
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
        let err = result.unwrap_err();
        assert_eq!(err.dep_name, "A");
    }

    #[tokio::test]
    async fn multiple_deps_all_must_complete() {
        let tracker = Arc::new(CompletionTracker::new());
        let tracker2 = tracker.clone();
        let handle = tokio::spawn(async move {
            tracker2
                .wait_ready(&names(&["A", "B"]), 100)
                .await
                .unwrap();
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
                tokio::time::timeout(
                    Duration::from_secs(2),
                    tracker.wait_ready(&[name], range),
                )
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
        assert_eq!(
            tracker.probe(&names(&["A"]), 100).await,
            DepState::Waiting
        );

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
        let err = tracker.wait_ready(&names(&["B"]), 200).await.unwrap_err();
        assert_eq!(err.dep_name, "B");
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
        assert_eq!(err.dep_name, "B");
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
}
