//! Types for eth_call decoding: error enum, config structs, record structs.

use std::collections::HashMap;

use thiserror::Error;

use crate::types::config::eth_call::EvmType;
use crate::types::decoded::DecodedValue;

#[derive(Debug, Error)]
pub enum EthCallDecodingError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet read error: {0}")]
    ParquetRead(#[from] crate::storage::parquet_readers::ParquetReadError),

    #[error("Decoding error: {0}")]
    Decode(String),

    #[error("Task join error: {0}")]
    JoinError(String),
}

/// Configuration for a call to decode
#[derive(Debug, Clone)]
pub struct CallDecodeConfig {
    pub contract_name: String,
    pub function_name: String,
    pub output_type: EvmType,
    pub _is_once: bool,
    /// Start block for this contract (calls before this block are skipped)
    pub start_block: Option<u64>,
}

/// Configuration for an event-triggered call to decode
#[derive(Debug, Clone)]
pub struct EventCallDecodeConfig {
    pub contract_name: String,
    pub function_name: String,
    pub output_type: EvmType,
    /// Start block for this contract (calls before this block are skipped)
    pub start_block: Option<u64>,
}

/// Decoded call result
#[derive(Debug)]
pub struct DecodedCallRecord {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub decoded_value: DecodedValue,
}

/// Decoded event-triggered call result
#[derive(Debug)]
pub struct DecodedEventCallRecord {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub decoded_value: DecodedValue,
}

/// Decoded "once" call result
#[derive(Debug)]
pub struct DecodedOnceRecord {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    /// function_name -> decoded value
    pub decoded_values: HashMap<String, DecodedValue>,
}

/// Grouped decode configs for eth_call decoding (regular, once, event-triggered).
pub struct EthCallDecodeConfigs<'a> {
    pub regular: &'a [CallDecodeConfig],
    pub once: &'a [CallDecodeConfig],
    pub event: &'a [EventCallDecodeConfig],
}

/// Result of processing once calls, used to batch update column indexes after all tasks complete.
pub struct OnceCallsResult {
    pub contract_name: String,
    pub file_name: String,
    pub columns: Vec<String>,
}

/// Config lookup for parquet columns: column_name -> (config, optional tuple field info).
pub type ColumnConfigLookup<'a> =
    HashMap<String, (&'a CallDecodeConfig, Option<(usize, &'a EvmType)>)>;
