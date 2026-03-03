//! Types for live mode block processing.

use serde::{Deserialize, Serialize};

/// A block in live mode, stored as bincode for fast serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveBlock {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub timestamp: u64,
    /// Transaction hashes in this block.
    pub tx_hashes: Vec<[u8; 32]>,
}

impl LiveBlock {
    /// Create a new LiveBlock from raw data.
    pub fn new(
        number: u64,
        hash: [u8; 32],
        parent_hash: [u8; 32],
        timestamp: u64,
        tx_hashes: Vec<[u8; 32]>,
    ) -> Self {
        Self {
            number,
            hash,
            parent_hash,
            timestamp,
            tx_hashes,
        }
    }
}

/// Status tracking for a live block's processing pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveBlockStatus {
    /// Block header collected from WebSocket.
    pub collected: bool,
    /// Full block data with transactions fetched.
    pub block_fetched: bool,
    /// Receipts collected for all transactions.
    pub receipts_collected: bool,
    /// Logs extracted from receipts.
    pub logs_collected: bool,
    /// Logs and calls decoded.
    pub decoded: bool,
    /// All transformation handlers complete.
    pub transformed: bool,
}

impl LiveBlockStatus {
    /// Create a new status indicating only initial collection.
    pub fn collected() -> Self {
        Self {
            collected: true,
            ..Default::default()
        }
    }

    /// Check if the block is fully processed and ready for compaction.
    pub fn is_complete(&self) -> bool {
        self.collected
            && self.block_fetched
            && self.receipts_collected
            && self.logs_collected
            && self.decoded
            && self.transformed
    }
}

/// Messages for live mode processing.
#[derive(Debug)]
pub enum LiveMessage {
    /// A new block was received.
    Block(LiveBlock),
    /// A reorg was detected.
    Reorg {
        /// The block number of the common ancestor (last valid block).
        common_ancestor: u64,
        /// Block numbers that were orphaned.
        orphaned: Vec<u64>,
    },
    /// All live processing should stop (shutdown signal).
    Shutdown,
}

/// Receipt data for a live block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveReceipt {
    pub transaction_hash: [u8; 32],
    pub transaction_index: u32,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub from: [u8; 20],
    pub to: Option<[u8; 20]>,
    pub logs: Vec<LiveLog>,
}

/// Log data for live mode storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveLog {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
    pub log_index: u32,
    pub transaction_index: u32,
}

/// Eth call result for live mode storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveEthCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub function_name: String,
    pub result: Vec<u8>,
}

/// Configuration for live mode.
#[derive(Debug, Clone)]
pub struct LiveModeConfig {
    /// Number of recent blocks to track for reorg detection.
    pub reorg_depth: u64,
    /// Interval in seconds between compaction checks.
    pub compaction_interval_secs: u64,
    /// Block range size for compaction (should match historical range).
    pub range_size: u64,
}

impl Default for LiveModeConfig {
    fn default() -> Self {
        Self {
            reorg_depth: 128,
            compaction_interval_secs: 10,
            range_size: 1000,
        }
    }
}

/// Progress entry for live mode per-block tracking.
#[derive(Debug, Clone)]
pub struct LiveProgress {
    pub chain_id: i64,
    pub handler_key: String,
    pub block_number: i64,
}

// =========================================================================
// Decoded data types for live mode storage
// =========================================================================

/// A decoded event value stored as bincode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiveDecodedValue {
    Address([u8; 20]),
    Uint256(String), // Stored as string to avoid serialization issues with U256
    Int256(String),  // Stored as string to avoid serialization issues with I256
    Uint64(u64),
    Int64(i64),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    /// Named tuple of (field_name, field_value) pairs
    NamedTuple(Vec<(String, LiveDecodedValue)>),
    /// Unnamed tuple of values
    UnnamedTuple(Vec<LiveDecodedValue>),
    /// Array of values
    Array(Vec<LiveDecodedValue>),
}

/// A decoded log record stored as bincode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDecodedLog {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub contract_address: [u8; 20],
    /// Decoded parameter values in flattened order
    pub decoded_values: Vec<LiveDecodedValue>,
}

/// A decoded eth_call result stored as bincode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDecodedCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub decoded_value: LiveDecodedValue,
}

/// A decoded event-triggered eth_call result stored as bincode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDecodedEventCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub decoded_value: LiveDecodedValue,
}

/// A decoded "once" call result stored as bincode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDecodedOnceCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    /// function_name -> decoded value
    pub decoded_values: Vec<(String, LiveDecodedValue)>,
}

/// Metadata for a decoded data file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedFileMetadata {
    /// Schema version for the decoded data
    pub schema_version: u32,
    /// Number of records in this file
    pub record_count: usize,
}
