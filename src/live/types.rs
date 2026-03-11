//! Types for live mode block processing.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::types::config::defaults::raw_data as defaults;

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
    #[allow(dead_code)]
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
    /// Factory addresses extracted from logs (or no factories configured).
    pub factories_extracted: bool,
    /// Eth calls collected for this block (or no eth_calls configured).
    pub eth_calls_collected: bool,
    /// Logs decoded (or no events configured).
    pub logs_decoded: bool,
    /// Eth calls decoded (or no eth_calls configured).
    pub eth_calls_decoded: bool,
    /// All transformation handlers complete (or no handlers configured).
    pub transformed: bool,
    /// Handler keys that have completed processing for this block.
    /// Used for catchup on restart to determine which handlers still need to run.
    #[serde(default)]
    pub completed_handlers: HashSet<String>,
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
            && self.factories_extracted
            && self.eth_calls_collected
            && self.logs_decoded
            && self.eth_calls_decoded
            && self.transformed
    }
}

/// Messages for live mode processing.
#[derive(Debug)]
#[allow(dead_code)]
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
    pub transaction_hash: [u8; 32],
}

/// Eth call result for live mode storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveEthCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_name: String,
    pub contract_address: [u8; 20],
    pub function_name: String,
    pub result: Vec<u8>,
}

/// Factory addresses discovered in a live block.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveFactoryAddresses {
    /// collection_name -> list of (timestamp, address) discovered
    pub addresses_by_collection: HashMap<String, Vec<(u64, [u8; 20])>>,
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
            reorg_depth: defaults::REORG_DEPTH,
            compaction_interval_secs: defaults::COMPACTION_INTERVAL_SECS,
            range_size: 1000,
        }
    }
}

/// Progress entry for live mode per-block tracking.
#[derive(Debug, Clone)]
#[allow(dead_code)]
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
#[allow(dead_code)]
pub struct DecodedFileMetadata {
    /// Schema version for the decoded data
    pub schema_version: u32,
    /// Number of records in this file
    pub record_count: usize,
}

// =========================================================================
// Upsert snapshot types for reorg rollback
// =========================================================================

/// Serializable database value for snapshot storage.
/// Mirrors DbValue but designed for bincode serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiveDbValue {
    Null,
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Int2(u8),
    Uint64(u64),
    Text(String),
    VarChar(String),
    Bytes(Vec<u8>),
    Address([u8; 20]),
    Bytes32([u8; 32]),
    Numeric(String),
    Timestamp(i64),
    Json(String),  // JSON stored as string for bincode
    JsonB(String), // JSONB stored as string for bincode
}

/// Snapshot of a row before modification, for reorg rollback.
/// Stored per-block and read during reorg to restore previous state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveUpsertSnapshot {
    /// Table name
    pub table: String,
    /// Handler source name (for scoped restoration)
    pub source: String,
    /// Handler source version
    pub source_version: u32,
    /// Key columns with their values (used for DELETE or identifying the row)
    pub key_columns: Vec<(String, LiveDbValue)>,
    /// Previous row state, if row existed before the upsert.
    /// None means the row didn't exist (INSERT case), so reorg should DELETE.
    /// Some means the row existed (UPDATE case), so reorg should restore.
    pub previous_row: Option<Vec<(String, LiveDbValue)>>,
}

impl LiveDbValue {
    /// Convert from crate::db::DbValue
    pub fn from_db_value(value: &crate::db::DbValue) -> Self {
        use crate::db::DbValue;
        match value {
            DbValue::Null => LiveDbValue::Null,
            DbValue::Bool(v) => LiveDbValue::Bool(*v),
            DbValue::Int64(v) => LiveDbValue::Int64(*v),
            DbValue::Int32(v) => LiveDbValue::Int32(*v),
            DbValue::Int2(v) => LiveDbValue::Int2(*v),
            DbValue::Uint64(v) => LiveDbValue::Uint64(*v),
            DbValue::Text(v) => LiveDbValue::Text(v.clone()),
            DbValue::VarChar(v) => LiveDbValue::VarChar(v.clone()),
            DbValue::Bytes(v) => LiveDbValue::Bytes(v.clone()),
            DbValue::Address(v) => LiveDbValue::Address(*v),
            DbValue::Bytes32(v) => LiveDbValue::Bytes32(*v),
            DbValue::Numeric(v) => LiveDbValue::Numeric(v.clone()),
            DbValue::Timestamp(v) => LiveDbValue::Timestamp(*v),
            DbValue::Json(v) => LiveDbValue::Json(v.to_string()),
            DbValue::JsonB(v) => LiveDbValue::JsonB(v.to_string()),
        }
    }

    /// Convert to crate::db::DbValue
    pub fn to_db_value(&self) -> crate::db::DbValue {
        use crate::db::DbValue;
        match self {
            LiveDbValue::Null => DbValue::Null,
            LiveDbValue::Bool(v) => DbValue::Bool(*v),
            LiveDbValue::Int64(v) => DbValue::Int64(*v),
            LiveDbValue::Int32(v) => DbValue::Int32(*v),
            LiveDbValue::Int2(v) => DbValue::Int2(*v),
            LiveDbValue::Uint64(v) => DbValue::Uint64(*v),
            LiveDbValue::Text(v) => DbValue::Text(v.clone()),
            LiveDbValue::VarChar(v) => DbValue::VarChar(v.clone()),
            LiveDbValue::Bytes(v) => DbValue::Bytes(v.clone()),
            LiveDbValue::Address(v) => DbValue::Address(*v),
            LiveDbValue::Bytes32(v) => DbValue::Bytes32(*v),
            LiveDbValue::Numeric(v) => DbValue::Numeric(v.clone()),
            LiveDbValue::Timestamp(v) => DbValue::Timestamp(*v),
            LiveDbValue::Json(v) => {
                DbValue::Json(serde_json::from_str(v).unwrap_or(serde_json::Value::Null))
            }
            LiveDbValue::JsonB(v) => {
                DbValue::JsonB(serde_json::from_str(v).unwrap_or(serde_json::Value::Null))
            }
        }
    }
}
