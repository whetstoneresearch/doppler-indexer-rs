//! Live mode module for real-time block processing via WebSocket.
//!
//! This module provides:
//! - WebSocket subscription to `eth_subscribe("newHeads")`
//! - Per-block storage using bincode for fast serialization
//! - Reorg detection and recovery
//! - Compaction to merge live blocks into parquet ranges
//! - Catchup for incomplete blocks on restart

pub(crate) mod bincode_io;
mod catchup;
mod collector;
mod compaction;
mod error;
mod eth_calls;
mod progress;
mod reorg;
mod storage;
mod types;

pub use bincode_io::StorageError;
pub use collector::{LiveCollector, LiveCollectorConfig};
pub use compaction::{CompactionService, TransformRetryRequest};
pub use eth_calls::LiveEthCallCollector;
pub use progress::{LiveProgressTracker, ProgressStatusStorage};
pub use storage::LiveStorage;
#[allow(unused_imports)]
pub use types::{
    LiveBlockStatus, LiveDbValue, LiveDecodedCall, LiveDecodedEventCall, LiveDecodedLog,
    LiveDecodedOnceCall, LiveEthCall, LiveMessage, LiveModeConfig, LivePipelineExpectations,
    LiveUpsertSnapshot,
};
