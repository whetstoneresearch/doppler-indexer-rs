use std::collections::HashSet;

use crate::raw_data::historical::receipts::EventTriggerData;

/// Mutable local state for the eth_calls collector event loop.
pub(super) struct CollectorState {
    pub(super) block_rx_closed: bool,
    pub(super) event_trigger_rx_closed: bool,
    pub(super) pending_event_triggers: Vec<EventTriggerData>,
    pub(super) completed_event_ranges: HashSet<u64>,
    pub(super) emitted_complete_ranges: HashSet<u64>,
}

impl CollectorState {
    pub(super) fn new(has_event_triggered_calls: bool) -> Self {
        Self {
            block_rx_closed: false,
            event_trigger_rx_closed: !has_event_triggered_calls,
            pending_event_triggers: Vec::new(),
            completed_event_ranges: HashSet::new(),
            emitted_complete_ranges: HashSet::new(),
        }
    }
}
