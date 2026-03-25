use crate::raw_data::historical::receipts::EventTriggerData;

/// Mutable local state for the eth_calls collector event loop.
pub(super) struct CollectorState {
    pub(super) block_rx_closed: bool,
    pub(super) event_trigger_rx_closed: bool,
    pub(super) pending_event_triggers: Vec<EventTriggerData>,
}

impl CollectorState {
    pub(super) fn new(has_event_triggered_calls: bool) -> Self {
        Self {
            block_rx_closed: false,
            event_trigger_rx_closed: !has_event_triggered_calls,
            pending_event_triggers: Vec::new(),
        }
    }
}
