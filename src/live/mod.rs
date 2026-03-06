//! Live mode module for real-time block processing via WebSocket.
//!
//! This module provides:
//! - WebSocket subscription to `eth_subscribe("newHeads")`
//! - Per-block storage using bincode for fast serialization
//! - Reorg detection and recovery
//! - Compaction to merge live blocks into parquet ranges
//! - Catchup for incomplete blocks on restart

mod catchup;
mod collector;
mod compaction;
mod error;
mod eth_calls;
mod progress;
mod reorg;
mod storage;
mod types;

pub use catchup::{CatchupScanResult, LiveCatchupService};
pub use collector::LiveCollector;
pub use compaction::{CompactionService, TransformRetryRequest};
pub use error::{LiveError, LiveEthCallError, ProgressError};
pub use eth_calls::LiveEthCallCollector;
pub use progress::LiveProgressTracker;
pub use reorg::{ReorgDetector, ReorgEvent};
pub use storage::{LiveStorage, StorageError};
pub use types::{
    DecodedFileMetadata, LiveBlock, LiveBlockStatus, LiveDecodedCall, LiveDecodedEventCall,
    LiveDecodedLog, LiveDecodedOnceCall, LiveDecodedValue, LiveEthCall, LiveFactoryAddresses,
    LiveLog, LiveMessage, LiveModeConfig, LiveProgress, LiveReceipt, LiveDbValue, LiveUpsertSnapshot,
};
