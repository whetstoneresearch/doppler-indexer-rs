//! Transformation context and decoded data types.
//!
//! The TransformationContext provides handlers with all the data and utilities
//! they need: decoded events/calls, chain info, historical queries, and RPC access.
use std::collections::HashMap;
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, Bytes, B256, I256, U256};

use super::error::TransformationError;
use super::historical::HistoricalDataReader;
use crate::rpc::UnifiedRpcClient;
use crate::types::chain::ChainAddress;
use crate::types::config::contract::{AddressOrAddresses, Contracts};

pub use crate::types::decoded::DecodedValue;

/// Trait for extracting typed fields from decoded events and calls.
///
/// This trait provides convenient methods for extracting fields with proper
/// type checking and descriptive error messages. It eliminates the boilerplate
/// pattern of `get("field")?.as_type().ok_or_else(|| ...)?`.
///
/// # Example
///
/// ```ignore
/// use doppler_indexer_rs::transformations::FieldExtractor;
///
/// // Before (verbose):
/// let asset = event.get("asset")?.as_address().ok_or_else(|| {
///     TransformationError::TypeConversion("asset is not an address".to_string())
/// })?;
///
/// // After (concise):
/// let asset = event.extract_address("asset")?;
/// ```
/// Generates a typed field extraction method on FieldExtractor.
///
/// Each invocation produces a method that calls `get_field(name)?.$as_method()`
/// and wraps the `None` case in a `TransformationError::TypeConversion` with
/// the provided `$type_name` in the message.
macro_rules! impl_field_extractor {
    ($method:ident, $as_method:ident, $ret:ty, $type_name:expr) => {
        fn $method(&self, name: &str) -> Result<$ret, TransformationError> {
            self.get_field(name)?.$as_method().ok_or_else(|| {
                TransformationError::TypeConversion(format!(
                    "'{}' is not {} in {}",
                    name,
                    $type_name,
                    self.context_info()
                ))
            })
        }
    };
}

#[allow(dead_code)]
pub trait FieldExtractor {
    /// Returns the underlying field values map.
    fn field_values(&self) -> &HashMap<String, DecodedValue>;

    /// Returns contextual information for error messages (e.g., event name, block number).
    fn context_info(&self) -> String;

    /// Get a field by name, returning an error with context if missing.
    fn get_field(&self, name: &str) -> Result<&DecodedValue, TransformationError> {
        self.field_values().get(name).ok_or_else(|| {
            TransformationError::MissingField(format!("{} in {}", name, self.context_info()))
        })
    }

    /// Try to get a field by name.
    fn try_get_field(&self, name: &str) -> Option<&DecodedValue> {
        self.field_values().get(name)
    }

    impl_field_extractor!(extract_address, as_address, [u8; 20], "an address");
    impl_field_extractor!(extract_uint256, as_uint256, U256, "a uint256");
    impl_field_extractor!(extract_int256, as_int256, I256, "an int256");
    impl_field_extractor!(extract_u64, as_u64, u64, "a u64");
    impl_field_extractor!(extract_i64, as_i64, i64, "an i64");
    impl_field_extractor!(extract_u32, as_u32, u32, "a u32");
    impl_field_extractor!(extract_i32, as_i32, i32, "an i32");
    impl_field_extractor!(extract_u8, as_u8, u8, "a u8");
    impl_field_extractor!(extract_bool, as_bool, bool, "a bool");
    impl_field_extractor!(extract_string, as_string, &str, "a string");
    impl_field_extractor!(extract_bytes32, as_bytes32, [u8; 32], "bytes32");
    impl_field_extractor!(extract_bytes, as_bytes, &[u8], "bytes");
    impl_field_extractor!(extract_pubkey, as_pubkey, [u8; 32], "a pubkey");

    fn extract_chain_address(&self, name: &str) -> Result<ChainAddress, TransformationError> {
        self.get_field(name)?
            .as_chain_address()
            .ok_or_else(|| {
                TransformationError::TypeConversion(format!(
                    "'{}' is not an address or pubkey in {}",
                    name,
                    self.context_info()
                ))
            })
    }

    /// Extract a u64 field with flexible parsing (handles i64, u64, or numeric strings).
    /// This is useful for timestamp fields that may come from different encoding formats.
    fn extract_u64_flexible(&self, name: &str) -> Result<u64, TransformationError> {
        let val = self.get_field(name)?;
        val.as_u64()
            .or_else(|| val.as_i64().and_then(|v| u64::try_from(v).ok()))
            .or_else(|| val.as_string().and_then(|s| s.parse().ok()))
            .ok_or_else(|| {
                TransformationError::TypeConversion(format!(
                    "'{}' is not a u64 or numeric string in {}",
                    name,
                    self.context_info()
                ))
            })
    }

    /// Extract an i32 field with flexible parsing (handles i32, u32, or numeric strings).
    /// This is useful for tick values that may come from different encoding formats.
    fn extract_i32_flexible(&self, name: &str) -> Result<i32, TransformationError> {
        let val = self.get_field(name)?;
        val.as_i32()
            .or_else(|| val.as_u32().and_then(|v| i32::try_from(v).ok()))
            .or_else(|| val.as_string().and_then(|s| s.parse().ok()))
            .ok_or_else(|| {
                TransformationError::TypeConversion(format!(
                    "'{}' is not an i32 or numeric string in {}",
                    name,
                    self.context_info()
                ))
            })
    }

    /// Extract a u32 field with flexible parsing (handles u32, i32, or numeric strings).
    /// This is useful for fee values that may come from different encoding formats.
    fn extract_u32_flexible(&self, name: &str) -> Result<u32, TransformationError> {
        let val = self.get_field(name)?;
        val.as_u32()
            .or_else(|| val.as_i32().and_then(|v| u32::try_from(v).ok()))
            .or_else(|| val.as_string().and_then(|s| s.parse().ok()))
            .ok_or_else(|| {
                TransformationError::TypeConversion(format!(
                    "'{}' is not a u32 or numeric string in {}",
                    name,
                    self.context_info()
                ))
            })
    }
}

/// A decoded event ready for transformation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
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

#[allow(dead_code)]
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

impl FieldExtractor for DecodedEvent {
    fn field_values(&self) -> &HashMap<String, DecodedValue> {
        &self.params
    }

    fn context_info(&self) -> String {
        format!(
            "event {}:{} at block {} log_index {}",
            self.source_name, self.event_name, self.block_number, self.log_index
        )
    }
}

/// A decoded eth_call result ready for transformation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
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
    /// Whether this call reverted during execution
    pub is_reverted: bool,
    /// If the call reverted, the reason string
    pub revert_reason: Option<String>,
}

#[allow(dead_code)]
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

impl FieldExtractor for DecodedCall {
    fn field_values(&self) -> &HashMap<String, DecodedValue> {
        &self.result
    }

    fn context_info(&self) -> String {
        format!(
            "call {}:{} at block {} address {:?}",
            self.source_name, self.function_name, self.block_number, self.contract_address
        )
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
    /// End block (exclusive) - cannot exceed current blockrange_end
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
#[allow(dead_code)]
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

/// Transaction from/to addresses from receipt data.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TransactionAddresses {
    pub from_address: [u8; 20],
    pub to_address: Option<[u8; 20]>,
}

/// Context provided to transformation handlers.
///
/// Contains all decoded data for the current block range plus utilities
/// for querying historical data and making additional RPC calls.
#[allow(dead_code)]
pub struct TransformationContext {
    // ===== Chain Information =====
    pub chain_name: String,
    pub chain_id: u64,

    // ===== Block Range =====
    pub blockrange_start: u64,
    pub blockrange_end: u64,

    // ===== Decoded Data for Current Block Range =====
    /// All decoded events in this range
    pub events: Arc<Vec<DecodedEvent>>,
    /// All decoded eth_call results in this range
    pub calls: Arc<Vec<DecodedCall>>,

    // ===== Transaction Address Data =====
    /// Transaction hash -> from/to addresses from receipt data
    tx_addresses: HashMap<[u8; 32], TransactionAddresses>,

    // ===== Services =====
    /// Historical data reader for querying parquet files
    pub(crate) historical: Arc<HistoricalDataReader>,
    /// RPC client for ad-hoc eth_calls
    pub(crate) rpc: Arc<UnifiedRpcClient>,

    // ===== Contract Configuration =====
    /// Contract configurations for the chain
    pub(crate) contracts: Arc<Contracts>,
}

#[allow(dead_code, clippy::too_many_arguments)]
impl TransformationContext {
    /// Create a new transformation context.
    pub fn new(
        chain_name: String,
        chain_id: u64,
        blockrange_start: u64,
        blockrange_end: u64,
        events: Arc<Vec<DecodedEvent>>,
        calls: Arc<Vec<DecodedCall>>,
        tx_addresses: HashMap<[u8; 32], TransactionAddresses>,
        historical: Arc<HistoricalDataReader>,
        rpc: Arc<UnifiedRpcClient>,
        contracts: Arc<Contracts>,
    ) -> Self {
        Self {
            chain_name,
            chain_id,
            blockrange_start,
            blockrange_end,
            events,
            calls,
            tx_addresses,
            historical,
            rpc,
            contracts,
        }
    }

    // ===== Filtering Helpers =====

    /// Get events of a specific type from the current block range.
    /// Filters out events before the contract's start_block if configured.
    pub fn events_of_type(
        &self,
        source: &str,
        event_name: &str,
    ) -> impl Iterator<Item = &DecodedEvent> {
        let start_block = self.get_contract_start_block(source);
        let source = source.to_string();
        let event_name = event_name.to_string();
        self.events.iter().filter(move |e| {
            e.source_name == source
                && e.event_name == event_name
                && start_block.is_none_or(|sb| e.block_number >= sb)
        })
    }

    /// Get all events for a specific contract address in the current range.
    /// Filters out events before the contract's start_block if configured.
    pub fn events_for_address(
        &self,
        address: [u8; 20],
    ) -> impl Iterator<Item = &DecodedEvent> + '_ {
        self.events.iter().filter(move |e| {
            if e.contract_address != address {
                return false;
            }
            // Look up start_block by source_name
            let start_block = self.get_contract_start_block(&e.source_name);
            start_block.is_none_or(|sb| e.block_number >= sb)
        })
    }

    /// Get calls of a specific type from the current block range.
    /// Filters out calls before the contract's start_block if configured.
    pub fn calls_of_type(
        &self,
        source: &str,
        function_name: &str,
    ) -> impl Iterator<Item = &DecodedCall> {
        let start_block = self.get_contract_start_block(source);
        let source = source.to_string();
        let function_name = function_name.to_string();
        self.calls.iter().filter(move |c| {
            c.source_name == source
                && c.function_name == function_name
                && start_block.is_none_or(|sb| c.block_number >= sb)
        })
    }

    /// Get all calls for a specific contract address in the current range.
    /// Filters out calls before the contract's start_block if configured.
    pub fn calls_for_address(&self, address: [u8; 20]) -> impl Iterator<Item = &DecodedCall> + '_ {
        self.calls.iter().filter(move |c| {
            if c.contract_address != address {
                return false;
            }
            // Look up start_block by source_name
            let start_block = self.get_contract_start_block(&c.source_name);
            start_block.is_none_or(|sb| c.block_number >= sb)
        })
    }

    /// Get the latest call for a source/function/address from the current range,
    /// falling back to cached historical parquet lookup when needed.
    pub async fn current_or_historical_call_for_address(
        &self,
        source: &str,
        function_name: &str,
        address: [u8; 20],
    ) -> Result<Option<DecodedCall>, TransformationError> {
        let start_block = self.get_contract_start_block(source);

        if let Some(current) = self
            .calls
            .iter()
            .rev()
            .find(|c| {
                c.source_name == source
                    && c.function_name == function_name
                    && c.contract_address == address
                    && start_block.is_none_or(|sb| c.block_number >= sb)
            })
            .cloned()
        {
            return Ok(Some(current));
        }

        let historical = self
            .historical
            .get_cached_call_for_address(source, function_name, address, self.blockrange_end)
            .await?;

        Ok(historical.filter(|call| start_block.is_none_or(|sb| call.block_number >= sb)))
    }

    /// Convenience wrapper for immutable `once` calls, which may be produced in
    /// an earlier historical range than the event currently being processed.
    pub async fn current_or_historical_once_call_for_address(
        &self,
        source: &str,
        address: [u8; 20],
    ) -> Result<Option<DecodedCall>, TransformationError> {
        self.current_or_historical_call_for_address(source, "once", address)
            .await
    }

    // ===== Contract Configuration Helpers =====

    /// Get the start_block for a contract or collection by name.
    pub fn get_contract_start_block(&self, name: &str) -> Option<u64> {
        self.contracts
            .get(name)
            .and_then(|c| c.start_block.map(|u| u.try_into().unwrap_or(u64::MAX)))
    }

    /// Look up a contract name by its address.
    /// Returns the first matching contract name if the address is configured.
    pub fn get_contract_name_by_address(&self, address: [u8; 20]) -> Option<&str> {
        let addr = Address::from(address);
        for (name, config) in self.contracts.iter() {
            let matches = match &config.address {
                AddressOrAddresses::Single(a) => *a == addr,
                AddressOrAddresses::Multiple(addrs) => addrs.contains(&addr),
            };
            if matches {
                return Some(name.as_str());
            }
        }
        None
    }

    /// Check if an address matches any of the specified contract names.
    /// Returns the first matching contract name from the provided list.
    pub fn match_contract_address(
        &self,
        address: [u8; 20],
        contract_names: &[&'static str],
    ) -> Option<&'static str> {
        let addr = Address::from(address);
        for contract_name in contract_names {
            if let Some(config) = self.contracts.get(*contract_name) {
                let matches = match &config.address {
                    AddressOrAddresses::Single(a) => *a == addr,
                    AddressOrAddresses::Multiple(addrs) => addrs.contains(&addr),
                };
                if matches {
                    return Some(*contract_name);
                }
            }
        }
        None
    }

    // ===== Transaction Address Lookup =====

    /// Look up the from_address for a transaction by its hash.
    pub fn tx_from(&self, tx_hash: &[u8; 32]) -> Option<&[u8; 20]> {
        self.tx_addresses.get(tx_hash).map(|a| &a.from_address)
    }

    /// Look up the to_address for a transaction by its hash.
    pub fn tx_to(&self, tx_hash: &[u8; 32]) -> Option<&[u8; 20]> {
        self.tx_addresses
            .get(tx_hash)
            .and_then(|a| a.to_address.as_ref())
    }

    // ===== Historical Data Access =====

    /// Query decoded events from current or past blocks.
    /// The query's to_block cannot exceed the current blockrange_end.
    pub async fn query_events(
        &self,
        mut query: HistoricalEventQuery,
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        // Enforce: cannot query future blocks
        if query.to_block > self.blockrange_end {
            return Err(TransformationError::FutureBlockAccess {
                requested: query.to_block,
                current_max: self.blockrange_end,
            });
        }

        // Clamp to_block to current range
        query.to_block = query.to_block.min(self.blockrange_end);

        self.historical.query_events(query).await
    }

    /// Query decoded eth_call results from current or past blocks.
    pub async fn query_calls(
        &self,
        mut query: HistoricalCallQuery,
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        // Enforce: cannot query future blocks
        if query.to_block > self.blockrange_end {
            return Err(TransformationError::FutureBlockAccess {
                requested: query.to_block,
                current_max: self.blockrange_end,
            });
        }

        query.to_block = query.to_block.min(self.blockrange_end);

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
        if block_number > self.blockrange_end {
            return Err(TransformationError::FutureBlockAccess {
                requested: block_number,
                current_max: self.blockrange_end,
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

    /// Find the address of a contract that emitted a log with a given topic0 in a transaction.
    ///
    /// Fetches the transaction receipt via RPC and searches for a matching log.
    /// Returns the emitting contract's address, or an error if not found.
    pub async fn find_log_emitter(
        &self,
        transaction_hash: [u8; 32],
        topic0: B256,
    ) -> Result<[u8; 20], TransformationError> {
        let tx_hash = B256::from(transaction_hash);
        let receipt = self
            .rpc
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| TransformationError::RpcError(e.to_string()))?
            .ok_or_else(|| {
                TransformationError::MissingData(format!("No receipt found for tx {}", tx_hash))
            })?;

        for log in receipt.inner.logs() {
            if log.topics().first() == Some(&topic0) {
                return Ok(log.address().0 .0);
            }
        }

        Err(TransformationError::MissingData(format!(
            "No log with topic0 {} found in tx {}",
            topic0, tx_hash
        )))
    }

    /// Make multiple eth_calls concurrently (more efficient than sequential).
    pub async fn eth_call_batch(
        &self,
        requests: Vec<EthCallRequest>,
    ) -> Result<Vec<Result<DynSolValue, TransformationError>>, TransformationError> {
        // Validate all block numbers
        for req in &requests {
            if req.block_number > self.blockrange_end {
                return Err(TransformationError::FutureBlockAccess {
                    requested: req.block_number,
                    current_max: self.blockrange_end,
                });
            }
        }

        let futures: Vec<_> = requests
            .into_iter()
            .map(|req| async move {
                self.eth_call(
                    req.contract_address,
                    &req.function_signature,
                    req.params,
                    req.block_number,
                )
                .await
            })
            .collect();

        Ok(futures::future::join_all(futures).await)
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
        TransformationError::DecodeError(format!(
            "Failed to parse output type '{}': {}",
            output_sig, e
        ))
    })?;

    // Unwrap single-element tuples so callers get the value directly
    let output_type = match output_type {
        DynSolType::Tuple(ref types) if types.len() == 1 => types[0].clone(),
        other => other,
    };

    Ok((selector, output_type))
}

/// Encode calldata for an eth_call.
fn encode_calldata(
    selector: &[u8; 4],
    params: &[DynSolValue],
) -> Result<Vec<u8>, TransformationError> {
    let mut calldata = selector.to_vec();

    if !params.is_empty() {
        // Create a tuple of params and encode
        let tuple = DynSolValue::Tuple(params.to_vec());
        let encoded = tuple.abi_encode_params();
        calldata.extend(encoded);
    }

    Ok(calldata)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_event(params: HashMap<String, DecodedValue>) -> DecodedEvent {
        DecodedEvent {
            block_number: 100,
            block_timestamp: 1234567890,
            transaction_hash: [0u8; 32],
            log_index: 0,
            contract_address: [1u8; 20],
            source_name: "TestContract".to_string(),
            event_name: "TestEvent".to_string(),
            event_signature: "TestEvent(address,uint256)".to_string(),
            params,
        }
    }

    fn make_test_call(result: HashMap<String, DecodedValue>) -> DecodedCall {
        DecodedCall {
            block_number: 100,
            block_timestamp: 1234567890,
            contract_address: [1u8; 20],
            source_name: "TestContract".to_string(),
            function_name: "testFunction".to_string(),
            trigger_log_index: None,
            result,
            is_reverted: false,
            revert_reason: None,
        }
    }

    #[test]
    fn test_field_extractor_extract_address() {
        let mut params = HashMap::new();
        params.insert("from".to_string(), DecodedValue::Address([42u8; 20]));
        let event = make_test_event(params);

        let addr = event.extract_address("from").unwrap();
        assert_eq!(addr, [42u8; 20]);
    }

    #[test]
    fn test_field_extractor_extract_address_wrong_type() {
        let mut params = HashMap::new();
        params.insert("from".to_string(), DecodedValue::Uint256(U256::from(100)));
        let event = make_test_event(params);

        let result = event.extract_address("from");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("'from' is not an address"));
        assert!(err_msg.contains("TestContract:TestEvent"));
    }

    #[test]
    fn test_field_extractor_extract_uint256() {
        let mut params = HashMap::new();
        params.insert("value".to_string(), DecodedValue::Uint256(U256::from(1000)));
        let event = make_test_event(params);

        let value = event.extract_uint256("value").unwrap();
        assert_eq!(value, U256::from(1000));
    }

    #[test]
    fn test_field_extractor_extract_bool() {
        let mut result = HashMap::new();
        result.insert("isToken0".to_string(), DecodedValue::Bool(true));
        let call = make_test_call(result);

        let is_token0 = call.extract_bool("isToken0").unwrap();
        assert!(is_token0);
    }

    #[test]
    fn test_field_extractor_missing_field() {
        let params = HashMap::new();
        let event = make_test_event(params);

        let result = event.extract_address("nonexistent");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("nonexistent"));
    }

    #[test]
    fn test_field_extractor_u64_flexible() {
        // Test with native u64
        let mut params = HashMap::new();
        params.insert("timestamp".to_string(), DecodedValue::Uint64(1234567890));
        let event = make_test_event(params);
        assert_eq!(event.extract_u64_flexible("timestamp").unwrap(), 1234567890);

        // Test with i64 (common in some encodings)
        let mut params = HashMap::new();
        params.insert("timestamp".to_string(), DecodedValue::Int64(1234567890));
        let event = make_test_event(params);
        assert_eq!(event.extract_u64_flexible("timestamp").unwrap(), 1234567890);

        // Test with string
        let mut params = HashMap::new();
        params.insert(
            "timestamp".to_string(),
            DecodedValue::String("1234567890".to_string()),
        );
        let event = make_test_event(params);
        assert_eq!(event.extract_u64_flexible("timestamp").unwrap(), 1234567890);
    }

    #[test]
    fn test_field_extractor_i32_flexible() {
        // Test with native i32
        let mut params = HashMap::new();
        params.insert("tick".to_string(), DecodedValue::Int32(-12345));
        let event = make_test_event(params);
        assert_eq!(event.extract_i32_flexible("tick").unwrap(), -12345);

        // Test with u32 (common for positive ticks)
        let mut params = HashMap::new();
        params.insert("tick".to_string(), DecodedValue::Uint32(12345));
        let event = make_test_event(params);
        assert_eq!(event.extract_i32_flexible("tick").unwrap(), 12345);

        // Test with string
        let mut params = HashMap::new();
        params.insert(
            "tick".to_string(),
            DecodedValue::String("-12345".to_string()),
        );
        let event = make_test_event(params);
        assert_eq!(event.extract_i32_flexible("tick").unwrap(), -12345);
    }

    #[test]
    fn test_field_extractor_u32_flexible() {
        // Test with native u32
        let mut params = HashMap::new();
        params.insert("fee".to_string(), DecodedValue::Uint32(3000));
        let event = make_test_event(params);
        assert_eq!(event.extract_u32_flexible("fee").unwrap(), 3000);

        // Test with i32 (common in some encodings)
        let mut params = HashMap::new();
        params.insert("fee".to_string(), DecodedValue::Int32(3000));
        let event = make_test_event(params);
        assert_eq!(event.extract_u32_flexible("fee").unwrap(), 3000);

        // Test with string
        let mut params = HashMap::new();
        params.insert("fee".to_string(), DecodedValue::String("3000".to_string()));
        let event = make_test_event(params);
        assert_eq!(event.extract_u32_flexible("fee").unwrap(), 3000);
    }

    #[test]
    fn test_field_extractor_context_info_event() {
        let params = HashMap::new();
        let event = make_test_event(params);

        let info = event.context_info();
        assert!(info.contains("TestContract:TestEvent"));
        assert!(info.contains("block 100"));
        assert!(info.contains("log_index 0"));
    }

    #[test]
    fn test_field_extractor_context_info_call() {
        let result = HashMap::new();
        let call = make_test_call(result);

        let info = call.context_info();
        assert!(info.contains("TestContract:testFunction"));
        assert!(info.contains("block 100"));
    }

    #[test]
    fn test_decoded_call_implements_field_extractor() {
        let mut result = HashMap::new();
        result.insert(
            "poolKey.currency0".to_string(),
            DecodedValue::Address([10u8; 20]),
        );
        result.insert("poolKey.fee".to_string(), DecodedValue::Uint32(3000));
        let call = make_test_call(result);

        // Use FieldExtractor methods
        let currency0 = call.extract_address("poolKey.currency0").unwrap();
        assert_eq!(currency0, [10u8; 20]);

        let fee = call.extract_u32("poolKey.fee").unwrap();
        assert_eq!(fee, 3000);
    }
}
