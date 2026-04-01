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
    ) -> (bool, Vec<String>) {
        let completion = self.completion.get(&range_key).copied().unwrap_or_default();

        // Check for timed-out pending events
        let timeout = Duration::from_secs(PENDING_EVENT_TIMEOUT_SECS);
        let now = Instant::now();
        let mut timed_out: Vec<String> = Vec::new();

        for (handler_key, pending_list) in &self.pending_events {
            for pending in pending_list {
                if (pending.range_start, pending.range_end) == range_key {
                    let timestamp_key = (range_key.0, range_key.1, handler_key.clone());
                    if let Some(&first_seen) = self.pending_event_timestamps.get(&timestamp_key) {
                        if now.duration_since(first_seen) >= timeout {
                            tracing::error!(
                                "Pending event TIMED OUT after {:?}: handler={} range={}-{} event={}/{} waiting_for_calls={:?} waiting_for_handlers={:?}. \
                                 Force-finalizing range to unblock progress.",
                                now.duration_since(first_seen),
                                handler_key,
                                pending.range_start,
                                pending.range_end,
                                pending.source_name,
                                pending.event_name,
                                pending.required_calls,
                                pending.required_handlers
                            );
                            timed_out.push(handler_key.clone());
                        }
                    }
                }
            }
        }

        // Remove timed-out pending events
        for handler_key in &timed_out {
            if let Some(pending_list) = self.pending_events.get_mut(handler_key) {
                pending_list
                    .retain(|pending| (pending.range_start, pending.range_end) != range_key);
            }
            let timestamp_key = (range_key.0, range_key.1, handler_key.clone());
            self.pending_event_timestamps.remove(&timestamp_key);
        }
        self.pending_events.retain(|_, v| !v.is_empty());

        let has_pending = self.pending_events.values().any(|pending_list| {
            pending_list
                .iter()
                .any(|pending| (pending.range_start, pending.range_end) == range_key)
        });

        if has_pending && completion.logs_complete && completion.eth_calls_complete {
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

        let ready = !has_pending && completion.is_ready(expect_logs, expect_eth_calls);
        (ready, timed_out)
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
}

impl RangeCompletionState {
    pub fn mark(&mut self, kind: RangeCompleteKind) {
        match kind {
            RangeCompleteKind::Logs => self.logs_complete = true,
            RangeCompleteKind::EthCalls => self.eth_calls_complete = true,
        }
    }

    pub fn is_ready(self, expect_logs: bool, expect_eth_calls: bool) -> bool {
        (!expect_logs || self.logs_complete) && (!expect_eth_calls || self.eth_calls_complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_completion_requires_both_streams_when_calls_expected() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::Logs);
        assert!(!state.is_ready(true, true));
        state.mark(RangeCompleteKind::EthCalls);
        assert!(state.is_ready(true, true));
    }

    #[test]
    fn range_completion_only_requires_logs_without_calls() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::Logs);
        assert!(state.is_ready(true, false));
    }

    #[test]
    fn range_completion_can_finalize_call_only_ranges() {
        let mut state = RangeCompletionState::default();
        state.mark(RangeCompleteKind::EthCalls);
        assert!(state.is_ready(false, true));
    }
}
