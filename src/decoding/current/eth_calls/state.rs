//! State for the live eth_calls decoder.

use crate::live::LiveStorage;

/// Accumulated state for the live eth_calls decoder loop.
pub(super) struct DecoderState {
    pub(super) live_storage: LiveStorage,
}

impl DecoderState {
    pub(super) fn new(chain_name: &str) -> Self {
        Self {
            live_storage: LiveStorage::new(chain_name),
        }
    }
}
