//! Live processing state for buffering events with call dependencies.
//!
//! Manages pending events, call buffers, range completion tracking,
//! and finalization state for the transformation engine's live mode.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::context::{DecodedCall, DecodedEvent};
use super::engine::RangeCompleteKind;

/// Buffered event data waiting for call dependencies and/or handler dependencies.
#[derive(Debug)]
pub(crate) struct PendingEventData {
    pub range_start: u64,
    pub range_end: u64,
    pub source_name: String,
    pub event_name: String,
    pub events: Vec<DecodedEvent>,
    /// Call dependencies needed: (source, function_name)
    pub required_calls: Vec<(String, String)>,
    /// Handler dependencies needed: handler name() strings
    pub required_handlers: Vec<String>,
}

/// A pending handler/range entry that timed out during finalization.
#[derive(Debug, Clone)]
pub(crate) struct TimedOutPendingHandler {
    pub handler_key: String,
    pub source_name: String,
    pub event_name: String,
    pub required_calls: Vec<(String, String)>,
    pub required_handlers: Vec<String>,
    pub timed_out_after_secs: u64,
}

/// Default timeout for stuck pending events (5 minutes).
pub(crate) const PENDING_EVENT_TIMEOUT_SECS: u64 = 300;

/// Live processing state for buffering events with call and handler dependencies.
#[derive(Default)]
pub(crate) struct LiveProcessingState {
    /// Track which (source, function) calls have arrived for which ranges.
    /// Key: (source_name, function_name), Value: set of (range_start, range_end)
    pub received_calls: HashMap<(String, String), HashSet<(u64, u64)>>,
    /// Accumulated calls per range for event handlers with dependencies.
    /// Key: (range_start, range_end), Value: accumulated calls
    pub calls_buffer: HashMap<(u64, u64), Vec<DecodedCall>>,
    /// Buffer events waiting for calls, keyed by handler_key.
    pub pending_events: HashMap<String, Vec<PendingEventData>>,
    /// Tracks which decode streams have completed for a range.
    pub completion: HashMap<(u64, u64), RangeCompletionState>,
    /// Track when events first become pending for timeout detection.
    /// Key: (range_start, range_end, handler_key), Value: when first added
    pub pending_event_timestamps: HashMap<(u64, u64, String), Instant>,
    /// Ranges that have been finalized (to prevent double finalization).
    pub finalized_ranges: HashSet<(u64, u64)>,
    /// Handlers that have completed for a given range.
    /// Key: (range_start, range_end), Value: set of handler names (from name(), not handler_key())
    pub completed_handlers: HashMap<(u64, u64), HashSet<String>>,
    /// Handlers that were dispatched and failed for a given range.
    /// Key: (range_start, range_end), Value: set of handler names (from name(), not handler_key())
    pub failed_handlers: HashMap<(u64, u64), HashSet<String>>,
}

impl LiveProcessingState {
    /// Clean up in-memory state for orphaned blocks.
    /// Returns the number of pending events removed.
    pub fn cleanup_for_orphaned_blocks(&mut self, orphaned: &[u64]) -> usize {
        let mut total_removed = 0;

        for &orphaned_block in orphaned {
            let range_key = (orphaned_block, orphaned_block + 1);

            // Remove from calls_buffer
            if self.calls_buffer.remove(&range_key).is_some() {
                tracing::debug!("Removed calls buffer for orphaned range {:?}", range_key);
            }
            self.completion.remove(&range_key);
            self.completed_handlers.remove(&range_key);
            self.failed_handlers.remove(&range_key);

            // Remove from finalized_ranges to allow re-finalization if block is re-processed
            self.finalized_ranges.remove(&range_key);

            // Remove from received_calls
            for ranges in self.received_calls.values_mut() {
                ranges.remove(&range_key);
            }

            // Remove pending event timestamps for this range
            self.pending_event_timestamps
                .retain(|(rs, re, _), _| (*rs, *re) != range_key);

            // Remove pending events for this range from all handlers
            for (handler_key, pending_list) in self.pending_events.iter_mut() {
                let initial_len = pending_list.len();
                pending_list.retain(|pending| {
                    let matches = (pending.range_start, pending.range_end) == range_key;
                    if matches {
                        tracing::debug!(
                            "Removing pending event for handler {} at orphaned range {:?}",
                            handler_key,
                            range_key
                        );
                    }
                    !matches
                });
                total_removed += initial_len - pending_list.len();
            }
        }

        // Clean up empty entries
        self.pending_events.retain(|_, v| !v.is_empty());

        total_removed
    }

    /// Clean up state for a retry request on a given range.
    pub fn cleanup_for_retry(&mut self, range_key: (u64, u64)) {
        self.calls_buffer.remove(&range_key);
        self.completion.remove(&range_key);
        self.finalized_ranges.remove(&range_key);
        self.completed_handlers.remove(&range_key);
        self.failed_handlers.remove(&range_key);
        self.pending_event_timestamps
            .retain(|(rs, re, _), _| (*rs, *re) != range_key);
        for ranges in self.received_calls.values_mut() {
            ranges.remove(&range_key);
        }
        for pending in self.pending_events.values_mut() {
            pending.retain(|entry| (entry.range_start, entry.range_end) != range_key);
        }
        self.pending_events.retain(|_, entries| !entries.is_empty());
    }

    /// Check if a range is ready for finalization.
    /// Returns (should_finalize, timed_out_handler_keys).
    pub fn check_finalization_readiness(
        &mut self,
        range_key: (u64, u64),
        expect_logs: bool,
        expect_eth_calls: bool,
        expect_account_states: bool,
    ) -> (bool, Vec<TimedOutPendingHandler>) {
        let completion = self.completion.get(&range_key).copied().unwrap_or_default();

        // Check for timed-out pending events
        let timeout = Duration::from_secs(PENDING_EVENT_TIMEOUT_SECS);
        let now = Instant::now();
        let mut timed_out: HashMap<String, TimedOutPendingHandler> = HashMap::new();

        for (handler_key, pending_list) in &self.pending_events {
            for pending in pending_list {
                if (pending.range_start, pending.range_end) == range_key {
                    let timestamp_key = (range_key.0, range_key.1, handler_key.clone());
                    if let Some(&first_seen) = self.pending_event_timestamps.get(&timestamp_key) {
                        let elapsed = now.duration_since(first_seen);
                        if elapsed >= timeout {
                            tracing::error!(
                                "Pending event TIMED OUT after {:?}: handler={} range={}-{} event={}/{} waiting_for_calls={:?} waiting_for_handlers={:?}. \
                                 Force-finalizing range to unblock progress.",
                                elapsed,
                                handler_key,
                                pending.range_start,
                                pending.range_end,
                                pending.source_name,
                                pending.event_name,
                                pending.required_calls,
                                pending.required_handlers
                            );
                            timed_out.entry(handler_key.clone()).or_insert_with(|| {
                                TimedOutPendingHandler {
                                    handler_key: handler_key.clone(),
                                    source_name: pending.source_name.clone(),
                                    event_name: pending.event_name.clone(),
                                    required_calls: pending.required_calls.clone(),
                                    required_handlers: pending.required_handlers.clone(),
                                    timed_out_after_secs: elapsed.as_secs(),
                                }
                            });
                        }
                    }
                }
            }
        }

        // Remove timed-out pending events
        for timed_out_handler in timed_out.values() {
            if let Some(pending_list) = self.pending_events.get_mut(&timed_out_handler.handler_key)
            {
                pending_list
                    .retain(|pending| (pending.range_start, pending.range_end) != range_key);
            }
            let timestamp_key = (
                range_key.0,
                range_key.1,
                timed_out_handler.handler_key.clone(),
            );
            self.pending_event_timestamps.remove(&timestamp_key);
        }
        self.pending_events.retain(|_, v| !v.is_empty());

        let has_pending = self.pending_events.values().any(|pending_list| {
            pending_list
                .iter()
                .any(|pending| (pending.range_start, pending.range_end) == range_key)
        });

        if has_pending
            && completion.has_required_completions(
                expect_logs,
                expect_eth_calls,
                expect_account_states,
            )
        {
            for (handler_key, pending_list) in &self.pending_events {
                for pending in pending_list {
                    if (pending.range_start, pending.range_end) == range_key {
                        tracing::warn!(
                            "Stuck pending event detected: handler={} range={}-{} event={}/{} waiting_for_calls={:?} waiting_for_handlers={:?}. \
                             This may indicate missing eth_call configuration, handler dependency issue, or RPC failure.",
                            handler_key,
                            pending.range_start,
                            pending.range_end,
                            pending.source_name,
                            pending.event_name,
                            pending.required_calls,
                            pending.required_handlers
                        );
                    }
                }
            }
        }

        let ready = !has_pending
            && completion.is_ready(expect_logs, expect_eth_calls, expect_account_states);
        (ready, timed_out.into_values().collect())
    }

    /// Mark a range as finalized. Returns false if already finalized.
    pub fn mark_finalized(&mut self, range_key: (u64, u64)) -> bool {
        if self.finalized_ranges.contains(&range_key) {
            tracing::debug!(
                "Range {}-{} already finalized, skipping duplicate finalization",
                range_key.0,
                range_key.1
            );
            return false;
        }
        self.finalized_ranges.insert(range_key);
        true
    }

    /// Clean up state after finalizing a range.
    pub fn cleanup_after_finalize(&mut self, range_key: (u64, u64)) {
        self.calls_buffer.remove(&range_key);
        self.completion.remove(&range_key);
        self.completed_handlers.remove(&range_key);
        self.failed_handlers.remove(&range_key);
        for ranges in self.received_calls.values_mut() {
            ranges.remove(&range_key);
        }
        for pending in self.pending_events.values_mut() {
            pending.retain(|entry| (entry.range_start, entry.range_end) != range_key);
        }
        self.pending_events.retain(|_, entries| !entries.is_empty());
        self.pending_event_timestamps
            .retain(|(rs, re, _), _| (*rs, *re) != range_key);
    }

    /// Get buffered calls for a range.
    pub fn get_buffered_calls(&self, range_key: (u64, u64)) -> Vec<DecodedCall> {
        self.calls_buffer
            .get(&range_key)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RangeCompletionState {
    pub logs_complete: bool,
    pub eth_calls_complete: bool,
    pub account_states_complete: bool,
}

impl RangeCompletionState {
    pub fn mark(&mut self, kind: RangeCompleteKind) {
        match kind {
            RangeCompleteKind::Logs => self.logs_complete = true,
            RangeCompleteKind::EthCalls => self.eth_calls_complete = true,
            RangeCompleteKind::AccountStates => self.account_states_complete = true,
        }
    }

    fn has_required_completions(
        self,
        expect_logs: bool,
        expect_eth_calls: bool,
        expect_account_states: bool,
    ) -> bool {
        (!expect_logs || self.logs_complete)
            && (!expect_eth_calls || self.eth_calls_complete)
            && (!expect_account_states || self.account_states_complete)
    }

    pub fn is_ready(
        self,
        expect_logs: bool,
        expect_eth_calls: bool,
        expect_account_states: bool,
    ) -> bool {
        self.has_required_completions(expect_logs, expect_eth_calls, expect_account_states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_completion_requires_both_streams_when_calls_expected() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::Logs);
        assert!(!state.is_ready(true, true, false));
        state.mark(RangeCompleteKind::EthCalls);
        assert!(state.is_ready(true, true, false));
    }

    #[test]
    fn range_completion_only_requires_logs_without_calls() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::Logs);
        assert!(state.is_ready(true, false, false));
    }

    #[test]
    fn range_completion_can_finalize_call_only_ranges() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::EthCalls);
        assert!(state.is_ready(false, true, false));
    }

    #[test]
    fn range_completion_can_require_account_states() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::AccountStates);
        assert!(state.is_ready(false, false, true));
    }

    #[test]
    fn range_completion_does_not_require_account_states_when_not_expected() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::Logs);
        state.mark(RangeCompleteKind::EthCalls);

        assert!(state.has_required_completions(true, true, false));
        assert!(!state.has_required_completions(true, true, true));
    }

    /// Helper: check whether a handler has remaining pending entries for a range.
    /// This mirrors the `has_remaining_pending` check in
    /// `TransformationEngine::try_process_pending_events`.
    fn has_remaining_pending(
        state: &LiveProcessingState,
        handler_key: &str,
        range_key: (u64, u64),
    ) -> bool {
        state
            .pending_events
            .get(handler_key)
            .map(|entries| {
                entries
                    .iter()
                    .any(|e| (e.range_start, e.range_end) == range_key)
            })
            .unwrap_or(false)
    }

    /// Simulates the in-memory completion condition in try_process_pending_events:
    /// a handler is only marked complete when it succeeded, logs_complete (for
    /// dep handlers), has NO remaining pending entries, and is not already failed.
    fn should_mark_complete_in_memory(
        state: &LiveProcessingState,
        handler_key: &str,
        handler_name: &str,
        range_key: (u64, u64),
        succeeded: bool,
        is_dep_handler: bool,
    ) -> bool {
        let logs_complete = state
            .completion
            .get(&range_key)
            .map(|c| c.logs_complete)
            .unwrap_or(false);
        let is_failed = state
            .failed_handlers
            .get(&range_key)
            .map(|f| f.contains(handler_name))
            .unwrap_or(false);
        let remaining = has_remaining_pending(state, handler_key, range_key);

        succeeded && (logs_complete || !is_dep_handler) && !remaining && !is_failed
    }

    /// Simulates the persistable_success_keys computation in
    /// try_process_pending_events: multi-trigger handlers require logs_complete,
    /// and all handlers require no remaining pending entries.
    fn should_persist_success(
        state: &LiveProcessingState,
        handler_key: &str,
        range_key: (u64, u64),
        succeeded: bool,
        is_multi_trigger: bool,
    ) -> bool {
        let logs_complete = state
            .completion
            .get(&range_key)
            .map(|c| c.logs_complete)
            .unwrap_or(false);
        let remaining = has_remaining_pending(state, handler_key, range_key);

        succeeded && !(is_multi_trigger && !logs_complete) && !remaining
    }

    fn make_pending(
        range_key: (u64, u64),
        required_calls: Vec<(String, String)>,
        required_handlers: Vec<String>,
    ) -> PendingEventData {
        PendingEventData {
            range_start: range_key.0,
            range_end: range_key.1,
            source_name: "Test".to_string(),
            event_name: "Event".to_string(),
            events: vec![],
            required_calls,
            required_handlers,
        }
    }

    // ── Regression: dep handler with two pending batches ──────────────

    #[test]
    fn dep_handler_not_completed_while_pending_entries_remain() {
        let range_key = (100u64, 101u64);
        let mut state = LiveProcessingState::default();

        // Handler B is a dep handler with two pending batches.
        // Batch 1 needs call C; batch 2 needs call D.
        state.pending_events.insert(
            "B_v1".to_string(),
            vec![
                make_pending(range_key, vec![("Pool".into(), "callC".into())], vec![]),
                make_pending(range_key, vec![("Pool".into(), "callD".into())], vec![]),
            ],
        );

        // Mark logs_complete (all event batches dispatched).
        state
            .completion
            .entry(range_key)
            .or_default()
            .mark(RangeCompleteKind::Logs);

        // Simulate: call C arrives, batch 1 becomes ready, is removed from
        // pending_events.  Batch 2 still waiting for call D.
        let pending = state.pending_events.get_mut("B_v1").unwrap();
        pending.remove(0); // batch 1 removed (ready & executed)

        // After batch 1 succeeds: B must NOT be marked complete because
        // batch 2 is still pending.
        assert!(
            has_remaining_pending(&state, "B_v1", range_key),
            "B still has a pending entry for the range"
        );
        assert!(
            !should_mark_complete_in_memory(&state, "B_v1", "B", range_key, true, true),
            "dep handler B must NOT be marked complete while a batch is pending"
        );
        assert!(
            !should_persist_success(&state, "B_v1", range_key, true, false),
            "B's success must NOT be persisted while a batch is pending"
        );

        // Now call D arrives, batch 2 removed (ready & executed).
        let pending = state.pending_events.get_mut("B_v1").unwrap();
        pending.remove(0);
        // Entry is now empty; clean up like the engine does.
        state.pending_events.remove("B_v1");

        // Now B should be completable.
        assert!(
            !has_remaining_pending(&state, "B_v1", range_key),
            "B has no more pending entries"
        );
        assert!(
            should_mark_complete_in_memory(&state, "B_v1", "B", range_key, true, true),
            "dep handler B should be completable now"
        );
        assert!(
            should_persist_success(&state, "B_v1", range_key, true, false),
            "B's success should be persistable now"
        );
    }

    #[test]
    fn dep_handler_not_completed_before_logs_complete() {
        let range_key = (100u64, 101u64);
        let state = LiveProcessingState::default();
        // logs_complete is false (default)

        // Handler B succeeded and has no pending entries, but logs_complete
        // is false — must NOT be marked complete (more event batches could arrive).
        assert!(
            !should_mark_complete_in_memory(&state, "B_v1", "B", range_key, true, true),
            "dep handler must wait for logs_complete"
        );

        // Non-dep handler is fine without logs_complete.
        assert!(
            should_mark_complete_in_memory(&state, "X_v1", "X", range_key, true, false),
            "non-dep handler can complete without logs_complete"
        );
    }

    // ── Regression: multi-trigger call handler ────────────────────────

    #[test]
    fn multi_trigger_handler_not_persisted_before_logs_complete() {
        let range_key = (100u64, 101u64);
        let state = LiveProcessingState::default();
        // logs_complete is false

        // Multi-trigger handler succeeded with no pending entries, but
        // logs_complete is false — persistence must be deferred.
        assert!(
            !should_persist_success(&state, "price_v1", range_key, true, true),
            "multi-trigger handler must defer persistence until logs_complete"
        );

        // Single-trigger handler in the same situation should persist.
        assert!(
            should_persist_success(&state, "single_v1", range_key, true, false),
            "single-trigger handler can persist immediately"
        );
    }

    #[test]
    fn multi_trigger_handler_persisted_after_logs_complete() {
        let range_key = (100u64, 101u64);
        let mut state = LiveProcessingState::default();
        state
            .completion
            .entry(range_key)
            .or_default()
            .mark(RangeCompleteKind::Logs);

        // Multi-trigger handler with logs_complete and no pending entries.
        assert!(
            should_persist_success(&state, "price_v1", range_key, true, true),
            "multi-trigger handler should persist after logs_complete"
        );
    }

    #[test]
    fn multi_trigger_handler_not_persisted_while_pending() {
        let range_key = (100u64, 101u64);
        let mut state = LiveProcessingState::default();
        state
            .completion
            .entry(range_key)
            .or_default()
            .mark(RangeCompleteKind::Logs);

        // Even with logs_complete, if pending entries remain, don't persist.
        state.pending_events.insert(
            "price_v1".to_string(),
            vec![make_pending(
                range_key,
                vec![("Pool".into(), "slot0".into())],
                vec![],
            )],
        );
        assert!(
            !should_persist_success(&state, "price_v1", range_key, true, true),
            "must not persist while pending entries remain, even with logs_complete"
        );
    }

    #[test]
    fn failed_handler_never_marked_complete() {
        let range_key = (100u64, 101u64);
        let mut state = LiveProcessingState::default();
        state
            .completion
            .entry(range_key)
            .or_default()
            .mark(RangeCompleteKind::Logs);

        // Mark handler as failed.
        state
            .failed_handlers
            .entry(range_key)
            .or_default()
            .insert("B".to_string());

        assert!(
            !should_mark_complete_in_memory(&state, "B_v1", "B", range_key, true, true),
            "failed handler must never be marked complete"
        );
    }
}
