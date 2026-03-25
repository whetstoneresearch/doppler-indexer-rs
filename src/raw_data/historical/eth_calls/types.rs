//! Types for eth_call collection: error types, config structs, result structs, and state.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use alloy::primitives::Address;
use thiserror::Error;

use crate::raw_data::historical::factories::FactoryAddressData;
use crate::rpc::RpcError;
use crate::storage::S3Manifest;
use crate::types::config::eth_call::{EthCallConfig, EvmType, Frequency, ParamConfig, ParamError};
use crate::types::config::tokens::PoolType;
use alloy::primitives::Bytes;

#[derive(Debug, Error)]
pub enum EthCallCollectionError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parameter error: {0}")]
    Param(#[from] ParamError),

    #[error("Task join error: {0}")]
    JoinError(String),

    #[error("Event parameter extraction error: {0}")]
    EventParamExtraction(String),
}

pub use crate::storage::BlockRange;

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub block_number: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct CallConfig {
    pub contract_name: String,
    pub address: Address,
    pub function_name: String,
    pub encoded_calldata: Bytes,
    pub param_values: Vec<Vec<u8>>,
    pub frequency: Frequency,
    /// Start block for this contract (calls before this block are skipped)
    pub start_block: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct OnceCallConfig {
    pub function_name: String,
    pub function_selector: [u8; 4],
    /// Pre-encoded calldata if no self-address params
    pub preencoded_calldata: Option<Bytes>,
    /// Param configs needed when preencoded_calldata is None
    pub params: Vec<ParamConfig>,
    /// Optional target address override (resolved from CallTarget)
    pub target_addresses: Option<Vec<Address>>,
    /// Start block for this contract (calls before this block are skipped)
    pub start_block: Option<u64>,
}

#[derive(Debug)]
pub struct OnceCallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub function_results: HashMap<String, Vec<u8>>,
}

pub struct FrequencyState {
    pub last_call_times: HashMap<(String, String), u64>,
}

/// Configuration for an event-triggered call
#[derive(Debug, Clone)]
pub struct EventTriggeredCallConfig {
    /// Contract name or factory collection name
    pub contract_name: String,
    /// Target address for the call (None for factory collections - use event emitter)
    pub target_address: Option<Address>,
    /// Function name (without params)
    pub function_name: String,
    /// Function selector (first 4 bytes of keccak256 of signature)
    pub function_selector: [u8; 4],
    /// Parameter configurations
    pub params: Vec<ParamConfig>,
    /// Whether this is for a factory collection
    pub is_factory: bool,
    /// Start block for this contract (calls before this block are skipped)
    pub start_block: Option<u64>,
}

/// Result from an event-triggered call
#[derive(Debug)]
pub struct EventCallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub value_bytes: Vec<u8>,
    pub param_values: Vec<Vec<u8>>,
}

/// Key for grouping event-triggered calls: (source_name, event_signature_hash)
pub type EventCallKey = (String, [u8; 32]);

#[derive(Debug)]
pub struct CallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub value_bytes: Vec<u8>,
    pub param_values: Vec<Vec<u8>>,
}

/// State computed during catchup and passed to the current/streaming phase
pub struct EthCallCatchupState {
    // Immutable configs
    pub base_output_dir: PathBuf,
    pub range_size: u64,
    pub rpc_batch_size: usize,
    pub multicall3_address: Option<Address>,
    pub call_configs: Vec<CallConfig>,
    pub factory_call_configs: HashMap<String, Vec<EthCallConfig>>,
    pub event_call_configs: HashMap<EventCallKey, Vec<EventTriggeredCallConfig>>,
    pub token_call_configs: Vec<TokenCallConfig>,
    pub once_configs: HashMap<String, Vec<OnceCallConfig>>,
    pub factory_once_configs: HashMap<String, Vec<OnceCallConfig>>,
    // Feature flags
    pub has_regular_calls: bool,
    pub has_once_calls: bool,
    pub has_factory_calls: bool,
    pub has_factory_once_calls: bool,
    pub has_event_triggered_calls: bool,
    pub has_token_calls: bool,
    // Derived constants
    pub max_params: usize,
    pub factory_max_params: usize,
    pub existing_files: HashSet<String>,
    // S3 manifest for checking remote files
    pub s3_manifest: Option<S3Manifest>,
    // Mutable state carried from catchup
    pub factory_addresses: HashMap<String, HashSet<Address>>,
    pub frequency_state: FrequencyState,
    pub range_data: HashMap<u64, Vec<BlockInfo>>,
    pub range_factory_data: HashMap<u64, FactoryAddressData>,
    pub range_regular_done: HashSet<u64>,
    pub range_factory_done: HashSet<u64>,
}

/// Configuration for a token pool call
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TokenCallConfig {
    /// Token name (used for output directory naming as {token_name}_pool)
    pub token_name: String,
    /// Pool type (v2, v3, v4)
    pub pool_type: PoolType,
    /// Target address for the call (pool address for v2/v3, StateView for v4)
    pub target_address: Address,
    /// Function name (e.g., "slot0")
    pub function_name: String,
    /// Encoded calldata including selector and any params
    pub encoded_calldata: Bytes,
    /// Call frequency
    pub frequency: Frequency,
    /// Output type for decoding
    pub output_type: EvmType,
}
