//! Live mode module for real-time block processing via WebSocket.
//!
//! This module provides:
//! - WebSocket subscription to `eth_subscribe("newHeads")`
//! - Per-block storage using bincode for fast serialization
//! - Reorg detection and recovery
//! - Compaction to merge live blocks into parquet ranges

mod collector;
mod compaction;
mod progress;
mod reorg;
mod storage;
mod types;

pub use collector::LiveCollector;
pub use compaction::CompactionService;
pub use progress::LiveProgressTracker;
pub use reorg::{ReorgDetector, ReorgEvent};
pub use storage::{LiveStorage, StorageError};
pub use types::{
    DecodedFileMetadata, LiveBlock, LiveBlockStatus, LiveDecodedCall, LiveDecodedEventCall,
    LiveDecodedLog, LiveDecodedOnceCall, LiveDecodedValue, LiveDbValue, LiveEthCall, LiveLog,
    LiveMessage, LiveModeConfig, LiveProgress, LiveReceipt, LiveUpsertSnapshot,
};
