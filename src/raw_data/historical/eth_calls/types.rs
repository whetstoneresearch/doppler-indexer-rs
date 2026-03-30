//! Types for eth_call collection: error types, config structs, result structs, and state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::primitives::Address;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::eth_calls::event_triggers::SkippedFactoryTrigger;
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::storage::{S3Manifest, StorageManager};
use crate::types::config::eth_call::{
    EthCallConfig, EvmType, Frequency, ParamConfig, ParamError, ParamValue,
};
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

    #[error("Parquet read error: {0}")]
    ParquetRead(#[from] crate::storage::parquet_readers::ParquetReadError),

    #[error("Parameter error: {0}")]
    Param(#[from] ParamError),

    #[error("Task join error: {0}")]
    JoinError(String),

    #[error("Event parameter extraction error: {0}")]
    EventParamExtraction(String),
}

/// A collection of spawned write-task handles that aborts all remaining
/// tasks when dropped. Prevents silent detached writes on error paths.
pub(crate) struct AbortOnDropHandles {
    handles: VecDeque<JoinHandle<Result<(), EthCallCollectionError>>>,
}

impl AbortOnDropHandles {
    pub fn new() -> Self {
        Self {
            handles: VecDeque::new(),
        }
    }

    pub fn push(&mut self, handle: JoinHandle<Result<(), EthCallCollectionError>>) {
        self.handles.push_back(handle);
    }

    /// Await all handles front-to-back. On error, remaining handles stay in
    /// `self` and are aborted by `Drop`.
    pub async fn drain_all(&mut self) -> Result<(), EthCallCollectionError> {
        while let Some(handle) = self.handles.pop_front() {
            handle
                .await
                .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;
        }
        Ok(())
    }
}

impl Drop for AbortOnDropHandles {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub use crate::storage::BlockRange;

/// Shared context for eth_call processing functions.
///
/// Bundles the RPC client, output paths, and chain info that are threaded
/// through every `process_*` function in the execution and event_triggers
/// modules.
#[allow(dead_code)]
pub struct EthCallContext<'a> {
    pub client: &'a UnifiedRpcClient,
    pub output_dir: &'a Path,
    pub existing_files: &'a HashSet<String>,
    pub rpc_batch_size: usize,
    pub decoder_tx: &'a Option<Sender<DecoderMessage>>,
    pub chain_name: &'a str,
    pub storage_manager: Option<&'a Arc<StorageManager>>,
    pub s3_manifest: &'a Option<S3Manifest>,
}

/// Results indexed by address: maps `Address -> (block_number, timestamp, function_name -> raw_bytes)`.
pub type AddressResults = HashMap<Address, (u64, u64, HashMap<String, Vec<u8>>)>;

/// Results indexed by collection name, then by address.
pub type CollectionResults = HashMap<String, AddressResults>;

/// Info needed to process a single contract's once-calls through the multicall
/// pipeline. Replaces the six-element tuple that was previously threaded through
/// `process_once_calls_multicall`.
pub struct ContractProcessingInfo {
    pub name: String,
    pub all_fn_names: Vec<String>,
    pub missing_fn_names: Vec<String>,
    pub patch_fn_names: Vec<String>,
    pub output_path: PathBuf,
    pub has_existing_file: bool,
}

/// Info needed to process a factory collection's once-calls through the
/// multicall pipeline. Extends `ContractProcessingInfo` with the call configs
/// needed for the backfill phase.
pub struct FactoryContractProcessingInfo {
    pub name: String,
    pub all_fn_names: Vec<String>,
    pub missing_fn_names: Vec<String>,
    pub patch_fn_names: Vec<String>,
    pub output_path: PathBuf,
    pub has_existing_file: bool,
    pub once_configs: Vec<OnceCallConfig>,
}

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
    pub is_reverted: bool,
    pub revert_reason: Option<String>,
}

/// Key for grouping event-triggered calls: (source_name, event_signature_hash)
pub type EventCallKey = (String, [u8; 32]);

/// A single encoded parameter: (evm_type, param_value, abi-encoded bytes).
pub type EncodedParam = (EvmType, ParamValue, Vec<u8>);

/// All parameter combinations for an eth_call configuration.
pub type ParamCombinations = Vec<Vec<EncodedParam>>;

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
    pub once_configs: HashMap<String, Vec<OnceCallConfig>>,
    pub factory_once_configs: HashMap<String, Vec<OnceCallConfig>>,
    // Feature flags
    pub has_regular_calls: bool,
    pub has_once_calls: bool,
    pub has_factory_calls: bool,
    pub has_factory_once_calls: bool,
    pub has_event_triggered_calls: bool,
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
    /// Buffered triggers skipped because factory addresses weren't known yet.
    /// Each entry: (skipped_triggers, range_start, range_end_inclusive)
    pub factory_skipped_triggers: Vec<(Vec<SkippedFactoryTrigger>, u64, u64)>,
}

