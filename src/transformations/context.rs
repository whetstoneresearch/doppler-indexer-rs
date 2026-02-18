//! Transformation context and decoded data types.
//!
//! The TransformationContext provides handlers with all the data and utilities
//! they need: decoded events/calls, chain info, historical queries, and RPC access.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, Bytes, I256, U256};

use super::error::TransformationError;
use super::historical::HistoricalDataReader;
use crate::rpc::UnifiedRpcClient;

/// A decoded value from an event parameter or eth_call result.
#[derive(Debug, Clone)]
pub enum DecodedValue {
    Address([u8; 20]),
    Uint256(U256),
    Int256(I256),
    Uint128(u128),
    Int128(i128),
    Uint64(u64),
    Int64(i64),
    Uint32(u32),
    Int32(i32),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    /// Named tuple of (field_name, field_value) pairs
    NamedTuple(Vec<(String, DecodedValue)>),
    /// Array of values
    Array(Vec<DecodedValue>),
}

impl DecodedValue {
    /// Try to get as an address.
    pub fn as_address(&self) -> Option<[u8; 20]> {
        match self {
            DecodedValue::Address(a) => Some(*a),
            _ => None,
        }
    }

    /// Try to get as bytes32.
    pub fn as_bytes32(&self) -> Option<[u8; 32]> {
        match self {
            DecodedValue::Bytes32(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as U256.
    pub fn as_uint256(&self) -> Option<U256> {
        match self {
            DecodedValue::Uint256(v) => Some(*v),
            DecodedValue::Uint128(v) => Some(U256::from(*v)),
            DecodedValue::Uint64(v) => Some(U256::from(*v)),
            DecodedValue::Uint32(v) => Some(U256::from(*v)),
            DecodedValue::Uint8(v) => Some(U256::from(*v)),
            DecodedValue::String(s) => U256::from_str(s.trim()).ok(),
            _ => None,
        }
    }

    /// Try to get as I256.
    pub fn as_int256(&self) -> Option<I256> {
        match self {
            DecodedValue::Int256(v) => Some(*v),
            DecodedValue::Int128(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::Int64(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::Int32(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::Int8(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::String(s) => I256::from_str(s.trim()).ok(),
            _ => None,
        }
    }

    /// Try to get as u64.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            DecodedValue::Uint64(v) => Some(*v),
            DecodedValue::Uint32(v) => Some(*v as u64),
            DecodedValue::Uint8(v) => Some(*v as u64),
            DecodedValue::Uint256(v) => v.try_into().ok(),
            DecodedValue::Uint128(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            DecodedValue::Int64(v) => Some(*v),
            DecodedValue::Int32(v) => Some(*v as i64),
            DecodedValue::Int8(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Try to get as i32 (for tick values).
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            DecodedValue::Int32(v) => Some(*v),
            DecodedValue::Int8(v) => Some(*v as i32),
            DecodedValue::Int64(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as u32 (for fee values).
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            DecodedValue::Uint32(v) => Some(*v),
            DecodedValue::Uint8(v) => Some(*v as u32),
            DecodedValue::Uint64(v) => (*v).try_into().ok(),
            DecodedValue::Uint256(v) => v.try_into().ok(),
            DecodedValue::Uint128(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as u8.
    pub fn as_u8(&self) -> Option<u8> {
        match self {
            DecodedValue::Uint8(v) => Some(*v),
            DecodedValue::Uint64(v) => (*v).try_into().ok(),
            DecodedValue::Uint32(v) => (*v).try_into().ok(),
            DecodedValue::Uint256(v) => v.try_into().ok(),
            DecodedValue::Uint128(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            DecodedValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as string.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            DecodedValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            DecodedValue::Bytes(b) => Some(b),
            DecodedValue::Bytes32(b) => Some(b),
            DecodedValue::Address(a) => Some(a),
            _ => None,
        }
    }

    /// Get a field from a named tuple.
    pub fn get_field(&self, name: &str) -> Option<&DecodedValue> {
        match self {
            DecodedValue::NamedTuple(fields) => {
                fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
            }
            _ => None,
        }
    }

    /// Convert to a numeric string (for database storage).
    pub fn to_numeric_string(&self) -> Option<String> {
        match self {
            DecodedValue::Uint256(v) => Some(v.to_string()),
            DecodedValue::Int256(v) => Some(v.to_string()),
            DecodedValue::Uint128(v) => Some(v.to_string()),
            DecodedValue::Int128(v) => Some(v.to_string()),
            DecodedValue::Uint64(v) => Some(v.to_string()),
            DecodedValue::Int64(v) => Some(v.to_string()),
            DecodedValue::Uint32(v) => Some(v.to_string()),
            DecodedValue::Int32(v) => Some(v.to_string()),
            DecodedValue::Uint8(v) => Some(v.to_string()),
            DecodedValue::Int8(v) => Some(v.to_string()),
            DecodedValue::String(s) => {
                if U256::from_str(s.trim()).is_ok() || I256::from_str(s.trim()).is_ok() {
                    Some(s.trim().to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// A decoded event ready for transformation.
#[derive(Debug, Clone)]
pub struct DecodedEvent {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub contract_address: [u8; 20],
    /// Contract or collection name from config
    pub source_name: String,
    /// Event name (e.g., "Swap", "Transfer")
    pub event_name: String,
    /// Full event signature
    pub event_signature: String,
    /// Decoded parameter values keyed by field name.
    /// Uses flattened field names like "key.currency0" for nested tuples.
    pub params: HashMap<String, DecodedValue>,
}

impl DecodedEvent {
    /// Get a parameter by name, returning an error if missing.
    pub fn get(&self, name: &str) -> Result<&DecodedValue, TransformationError> {
        self.params
            .get(name)
            .ok_or_else(|| TransformationError::MissingField(name.to_string()))
    }

    /// Try to get a parameter by name.
    pub fn try_get(&self, name: &str) -> Option<&DecodedValue> {
        self.params.get(name)
    }
}

/// A decoded eth_call result ready for transformation.
#[derive(Debug, Clone)]
pub struct DecodedCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    /// Contract or collection name from config
    pub source_name: String,
    /// Function name (e.g., "slot0", "balanceOf")
    pub function_name: String,
    /// For event-triggered calls, the log index of the triggering event
    pub trigger_log_index: Option<u32>,
    /// Decoded return value(s) keyed by field name.
    /// For single return values, the key is the function name or "result".
    /// For tuples, uses field names from the output type.
    pub result: HashMap<String, DecodedValue>,
}

impl DecodedCall {
    /// Get a result field by name, returning an error if missing.
    pub fn get(&self, name: &str) -> Result<&DecodedValue, TransformationError> {
        self.result
            .get(name)
            .ok_or_else(|| TransformationError::MissingField(name.to_string()))
    }

    /// Try to get a result field by name.
    pub fn try_get(&self, name: &str) -> Option<&DecodedValue> {
        self.result.get(name)
    }
}

/// Query for historical events from parquet files.
#[derive(Debug, Clone, Default)]
pub struct HistoricalEventQuery {
    /// Filter by source name (contract or collection)
    pub source: Option<String>,
    /// Filter by event name
    pub event_name: Option<String>,
    /// Filter by contract address
    pub contract_address: Option<[u8; 20]>,
    /// Start block (inclusive)
    pub from_block: u64,
    /// End block (exclusive) - cannot exceed current block_range_end
    pub to_block: u64,
    /// Maximum results to return
    pub limit: Option<usize>,
}

/// Query for historical eth_call results from parquet files.
#[derive(Debug, Clone, Default)]
pub struct HistoricalCallQuery {
    /// Filter by source name (contract or collection)
    pub source: Option<String>,
    /// Filter by function name
    pub function_name: Option<String>,
    /// Filter by contract address
    pub contract_address: Option<[u8; 20]>,
    /// Start block (inclusive)
    pub from_block: u64,
    /// End block (exclusive)
    pub to_block: u64,
    /// Maximum results to return
    pub limit: Option<usize>,
}

/// Request for an ad-hoc eth_call.
#[derive(Debug, Clone)]
pub struct EthCallRequest {
    pub contract_address: [u8; 20],
    /// Function signature with parameter types and return type.
    /// e.g., "balanceOf(address)(uint256)" or "slot0()((uint160,int24,uint16,uint16,uint16,uint8,bool))"
    pub function_signature: String,
    /// Input parameters
    pub params: Vec<DynSolValue>,
    /// Block number to query at
    pub block_number: u64,
}

/// Context provided to transformation handlers.
///
/// Contains all decoded data for the current block range plus utilities
/// for querying historical data and making additional RPC calls.
pub struct TransformationContext<'a> {
    // ===== Chain Information =====
    pub chain_name: &'a str,
    pub chain_id: u64,

    // ===== Block Range =====
    pub block_range_start: u64,
    pub block_range_end: u64,

    // ===== Decoded Data for Current Block Range =====
    /// All decoded events in this range
    pub events: &'a [DecodedEvent],
    /// All decoded eth_call results in this range
    pub calls: &'a [DecodedCall],

    // ===== Services =====
    /// Historical data reader for querying parquet files
    pub(crate) historical: Arc<HistoricalDataReader>,
    /// RPC client for ad-hoc eth_calls
    pub(crate) rpc: Arc<UnifiedRpcClient>,
}

impl<'a> TransformationContext<'a> {
    /// Create a new transformation context.
    pub fn new(
        chain_name: &'a str,
        chain_id: u64,
        block_range_start: u64,
        block_range_end: u64,
        events: &'a [DecodedEvent],
        calls: &'a [DecodedCall],
        historical: Arc<HistoricalDataReader>,
        rpc: Arc<UnifiedRpcClient>,
    ) -> Self {
        Self {
            chain_name,
            chain_id,
            block_range_start,
            block_range_end,
            events,
            calls,
            historical,
            rpc,
        }
    }

    // ===== Filtering Helpers =====

    /// Get events of a specific type from the current block range.
    pub fn events_of_type(
        &self,
        source: &str,
        event_name: &str,
    ) -> impl Iterator<Item = &DecodedEvent> {
        let source = source.to_string();
        let event_name = event_name.to_string();
        self.events
            .iter()
            .filter(move |e| e.source_name == source && e.event_name == event_name)
    }

    /// Get all events for a specific contract address in the current range.
    pub fn events_for_address(&self, address: [u8; 20]) -> impl Iterator<Item = &DecodedEvent> {
        self.events
            .iter()
            .filter(move |e| e.contract_address == address)
    }

    /// Get calls of a specific type from the current block range.
    pub fn calls_of_type(
        &self,
        source: &str,
        function_name: &str,
    ) -> impl Iterator<Item = &DecodedCall> {
        let source = source.to_string();
        let function_name = function_name.to_string();
        self.calls
            .iter()
            .filter(move |c| c.source_name == source && c.function_name == function_name)
    }

    /// Get all calls for a specific contract address in the current range.
    pub fn calls_for_address(&self, address: [u8; 20]) -> impl Iterator<Item = &DecodedCall> {
        self.calls
            .iter()
            .filter(move |c| c.contract_address == address)
    }

    // ===== Historical Data Access =====

    /// Query decoded events from current or past blocks.
    /// The query's to_block cannot exceed the current block_range_end.
    pub async fn query_events(
        &self,
        mut query: HistoricalEventQuery,
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        // Enforce: cannot query future blocks
        if query.to_block > self.block_range_end {
            return Err(TransformationError::FutureBlockAccess {
                requested: query.to_block,
                current_max: self.block_range_end,
            });
        }

        // Clamp to_block to current range
        query.to_block = query.to_block.min(self.block_range_end);

        self.historical.query_events(query).await
    }

    /// Query decoded eth_call results from current or past blocks.
    pub async fn query_calls(
        &self,
        mut query: HistoricalCallQuery,
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        // Enforce: cannot query future blocks
        if query.to_block > self.block_range_end {
            return Err(TransformationError::FutureBlockAccess {
                requested: query.to_block,
                current_max: self.block_range_end,
            });
        }

        query.to_block = query.to_block.min(self.block_range_end);

        self.historical.query_calls(query).await
    }

    // ===== Ad-hoc RPC Calls =====

    /// Make an eth_call to any contract at a specific block.
    ///
    /// The function signature should include parameter types and return type:
    /// - "balanceOf(address)(uint256)"
    /// - "slot0()((uint160 sqrtPriceX96,int24 tick,...))"
    pub async fn eth_call(
        &self,
        contract_address: [u8; 20],
        function_signature: &str,
        params: Vec<DynSolValue>,
        block_number: u64,
    ) -> Result<DynSolValue, TransformationError> {
        // Validate block number doesn't exceed current range
        if block_number > self.block_range_end {
            return Err(TransformationError::FutureBlockAccess {
                requested: block_number,
                current_max: self.block_range_end,
            });
        }

        // Parse function signature to get selector and output type
        let (selector, output_type) = parse_function_signature(function_signature)?;

        // Encode calldata
        let calldata = encode_calldata(&selector, &params)?;

        // Build call
        let result = self
            .rpc
            .eth_call(
                Address::from(contract_address),
                Bytes::from(calldata),
                block_number,
            )
            .await
            .map_err(|e| TransformationError::RpcError(e.to_string()))?;

        // Decode result
        let decoded = output_type
            .abi_decode(&result)
            .map_err(|e| TransformationError::DecodeError(e.to_string()))?;

        Ok(decoded)
    }

    /// Make multiple eth_calls in a batch (more efficient).
    pub async fn eth_call_batch(
        &self,
        requests: Vec<EthCallRequest>,
    ) -> Result<Vec<Result<DynSolValue, TransformationError>>, TransformationError> {
        // Validate all block numbers
        for req in &requests {
            if req.block_number > self.block_range_end {
                return Err(TransformationError::FutureBlockAccess {
                    requested: req.block_number,
                    current_max: self.block_range_end,
                });
            }
        }

        let mut results = Vec::with_capacity(requests.len());

        // For now, execute sequentially. Could be optimized with batch RPC.
        for req in requests {
            let result = self
                .eth_call(
                    req.contract_address,
                    &req.function_signature,
                    req.params,
                    req.block_number,
                )
                .await;
            results.push(result);
        }

        Ok(results)
    }
}

/// Parse a function signature like "balanceOf(address)(uint256)" into selector and output type.
fn parse_function_signature(sig: &str) -> Result<([u8; 4], DynSolType), TransformationError> {
    // Find the input/output split - look for ")(" pattern
    let parts: Vec<&str> = sig.splitn(2, ")(").collect();

    let (input_sig, output_sig) = if parts.len() == 2 {
        // Has both input and output
        let input = format!("{})", parts[0]);
        let output = parts[1].trim_end_matches(')');
        (input, output)
    } else {
        // Try to find output type at the end
        // e.g., "slot0()((uint160,int24))" -> input is "slot0()", output is "(uint160,int24)"
        if let Some(idx) = sig.rfind(")(") {
            let input = sig[..=idx].to_string();
            let output = &sig[idx + 2..sig.len() - 1];
            (input, output)
        } else {
            return Err(TransformationError::DecodeError(format!(
                "Invalid function signature, missing output type: {}",
                sig
            )));
        }
    };

    // Compute selector from input signature (keccak256 of input sig, first 4 bytes)
    let selector_bytes = alloy::primitives::keccak256(input_sig.as_bytes());
    let selector: [u8; 4] = selector_bytes[..4].try_into().unwrap();

    // Parse output type
    let output_type = DynSolType::parse(&format!("({})", output_sig)).map_err(|e| {
        TransformationError::DecodeError(format!("Failed to parse output type '{}': {}", output_sig, e))
    })?;

    Ok((selector, output_type))
}

/// Encode calldata for an eth_call.
fn encode_calldata(selector: &[u8; 4], params: &[DynSolValue]) -> Result<Vec<u8>, TransformationError> {
    let mut calldata = selector.to_vec();

    if !params.is_empty() {
        // Create a tuple of params and encode
        let tuple = DynSolValue::Tuple(params.to_vec());
        let encoded = tuple.abi_encode_params();
        calldata.extend(encoded);
    }

    Ok(calldata)
}
