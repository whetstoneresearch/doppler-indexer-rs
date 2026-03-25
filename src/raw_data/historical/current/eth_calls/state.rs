//! Collector-local mutable state for the eth_calls event loop.

use crate::raw_data::historical::receipts::EventTriggerData;

/// Mutable state local to the `collect_eth_calls` event loop.
///
/// The bulk of state lives in `EthCallCatchupState` (shared with the catchup
/// path). This struct holds the additional bookkeeping that only the
/// current-phase collector needs.
pub(super) struct CollectorState {
    pub(super) pending_event_triggers: Vec<EventTriggerData>,
    pub(super) block_rx_closed: bool,
    pub(super) event_trigger_rx_closed: bool,
}

impl CollectorState {
    pub(super) fn new(event_trigger_rx_closed: bool) -> Self {
        Self {
            pending_event_triggers: Vec::new(),
            block_rx_closed: false,
            event_trigger_rx_closed,
        }
    }
}
