use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{I256, U256};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

use super::catchup;
use super::catchup::eth_calls::{read_decoded_column_index, read_decoded_parquet_function_names, write_decoded_column_index};
use super::current;
use super::types::{EthCallResult, OnceCallResult};
use crate::transformations::{
    DecodedCall as TransformDecodedCall, DecodedCallsMessage,
    DecodedValue as TransformDecodedValue,
};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::Contracts;
use crate::types::config::eth_call::EvmType;
use crate::types::config::raw_data::RawDataCollectionConfig;

#[derive(Debug, Error)]
pub enum EthCallDecodingError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Decoding error: {0}")]
    Decode(String),

    #[error("Task join error: {0}")]
    JoinError(String),
}

/// Configuration for a call to decode
#[derive(Debug, Clone)]
pub(crate) struct CallDecodeConfig {
    pub contract_name: String,
    pub function_name: String,
    pub output_type: EvmType,
    pub is_once: bool,
}

/// Decoded call result
#[derive(Debug)]
pub(crate) struct DecodedCallRecord {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub decoded_value: DecodedValue,
}

/// Decoded "once" call result
#[derive(Debug)]
pub(crate) struct DecodedOnceRecord {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    /// function_name -> decoded value
    pub decoded_values: HashMap<String, DecodedValue>,
}

/// A decoded value
#[derive(Debug, Clone)]
pub(crate) enum DecodedValue {
    Address([u8; 20]),
    Uint256(U256),
    Int256(I256),
    Uint64(u64),
    Int64(i64),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    /// Named tuple of (field_name, field_value) pairs
    NamedTuple(Vec<(std::string::String, DecodedValue)>),
}

/// Result of processing once calls, used to batch update column indexes after all tasks complete.
pub(crate) struct OnceCallsResult {
    pub contract_name: String,
    pub file_name: String,
    pub columns: Vec<String>,
}

pub async fn decode_eth_calls(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    decoder_rx: Receiver<super::types::DecoderMessage>,
    transform_tx: Option<Sender<DecodedCallsMessage>>,
    eth_calls_catchup_done_rx: Option<oneshot::Receiver<()>>,
    decode_catchup_done_tx: Option<oneshot::Sender<()>>,
) -> Result<(), EthCallDecodingError> {
    let output_base = PathBuf::from(format!("data/derived/{}/decoded/eth_calls", chain.name));
    std::fs::create_dir_all(&output_base)?;

    // Build decode configs from contract configurations
    let (regular_configs, once_configs) = build_decode_configs(&chain.contracts);

    if regular_configs.is_empty() && once_configs.is_empty() {
        tracing::info!(
            "No eth_calls configured for decoding on chain {}",
            chain.name
        );
        // Signal barrier before early return so the engine doesn't wait forever
        if let Some(tx) = decode_catchup_done_tx {
            let _ = tx.send(());
        }
        // Drain channel and return
        let mut decoder_rx = decoder_rx;
        while decoder_rx.recv().await.is_some() {}
        return Ok(());
    }

    tracing::info!(
        "Eth_call decoder starting for chain {} with {} regular configs and {} once configs",
        chain.name,
        regular_configs.len(),
        once_configs.len()
    );

    // =========================================================================
    // Wait for eth_call collection catchup before decoding
    // =========================================================================
    if let Some(rx) = eth_calls_catchup_done_rx {
        tracing::info!("Waiting for eth_call collection catchup to complete before decoding...");
        let _ = rx.await;
        tracing::info!("Eth_call collection catchup complete, proceeding with decoding");
    }

    // =========================================================================
    // Catchup phase: Process existing raw eth_call files
    // =========================================================================
    let raw_calls_dir = PathBuf::from(format!("data/raw/{}/eth_calls", chain.name));
    if raw_calls_dir.exists() {
        // Pass None for transform_tx during catchup to avoid deadlock:
        // the engine is blocked waiting for our barrier signal and won't read
        // from the channel, so sends could block forever. The engine will read
        // decoded parquet files during its own catchup instead.
        catchup::catchup_decode_eth_calls(
            &raw_calls_dir,
            &output_base,
            &regular_configs,
            &once_configs,
            raw_data_config,
            None,
        )
        .await?;
    }

    tracing::info!(
        "Eth_call decoding catchup complete for chain {}",
        chain.name
    );

    if let Some(tx) = decode_catchup_done_tx {
        let _ = tx.send(());
    }

    // =========================================================================
    // Live phase: Process new data as it arrives
    // =========================================================================
    current::decode_eth_calls_live(
        decoder_rx,
        &raw_calls_dir,
        &output_base,
        &regular_configs,
        &once_configs,
        raw_data_config,
        transform_tx.as_ref(),
    )
    .await?;

    tracing::info!("Eth_call decoding complete for chain {}", chain.name);
    Ok(())
}

/// Build decode configurations from contracts
pub(crate) fn build_decode_configs(
    contracts: &Contracts,
) -> (Vec<CallDecodeConfig>, Vec<CallDecodeConfig>) {
    let mut regular = Vec::new();
    let mut once = Vec::new();

    for (contract_name, contract) in contracts {
        if let Some(calls) = &contract.calls {
            for call in calls {
                let function_name = call
                    .function
                    .split('(')
                    .next()
                    .unwrap_or(&call.function)
                    .to_string();

                let config = CallDecodeConfig {
                    contract_name: contract_name.clone(),
                    function_name,
                    output_type: call.output_type.clone(),
                    is_once: call.frequency.is_once(),
                };

                if call.frequency.is_once() {
                    once.push(config);
                } else {
                    regular.push(config);
                }
            }
        }

        // Factory calls
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if let Some(calls) = &factory.calls {
                    for call in calls {
                        let function_name = call
                            .function
                            .split('(')
                            .next()
                            .unwrap_or(&call.function)
                            .to_string();

                        let config = CallDecodeConfig {
                            contract_name: factory.collection.clone(),
                            function_name,
                            output_type: call.output_type.clone(),
                            is_once: call.frequency.is_once(),
                        };

                        if call.frequency.is_once() {
                            once.push(config);
                        } else {
                            regular.push(config);
                        }
                    }
                }
            }
        }
    }

    (regular, once)
}

/// Process regular call results
pub(crate) async fn process_regular_calls(
    results: &[EthCallResult],
    range_start: u64,
    range_end: u64,
    config: &CallDecodeConfig,
    output_base: &Path,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
) -> Result<(), EthCallDecodingError> {
    let mut decoded_records = Vec::new();
    let mut decode_failures = 0u64;
    let mut non_empty_count = 0u64;

    for result in results {
        if result.value.is_empty() {
            continue;
        }
        non_empty_count += 1;

        match decode_value(&result.value, &config.output_type) {
            Ok(decoded) => {
                decoded_records.push(DecodedCallRecord {
                    block_number: result.block_number,
                    block_timestamp: result.block_timestamp,
                    contract_address: result.contract_address,
                    decoded_value: decoded,
                });
            }
            Err(e) => {
                decode_failures += 1;
                tracing::debug!(
                    "Failed to decode {}.{} at block {}: {}",
                    config.contract_name,
                    config.function_name,
                    result.block_number,
                    e
                );
            }
        }
    }

    if decode_failures > 0 && decode_failures == non_empty_count {
        tracing::warn!(
            "All {} decode attempts failed for {}.{} (output_type: {:?}) in blocks {}-{} — check output_type in config",
            decode_failures,
            config.contract_name,
            config.function_name,
            config.output_type,
            range_start,
            range_end - 1
        );
    } else if decode_failures > 0 {
        tracing::warn!(
            "{}/{} decode failures for {}.{} in blocks {}-{}",
            decode_failures,
            non_empty_count,
            config.contract_name,
            config.function_name,
            range_start,
            range_end - 1
        );
    }

    // Write decoded data
    let output_dir = output_base
        .join(&config.contract_name)
        .join(&config.function_name);
    std::fs::create_dir_all(&output_dir)?;

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let output_path = output_dir.join(&file_name);

    write_decoded_calls_to_parquet(&decoded_records, &config.output_type, &output_path)?;

    if decoded_records.is_empty() {
        tracing::debug!(
            "Wrote 0 decoded {}.{} results to {}",
            config.contract_name,
            config.function_name,
            output_path.display()
        );
    } else {
        tracing::info!(
            "Wrote {} decoded {}.{} results to {}",
            decoded_records.len(),
            config.contract_name,
            config.function_name,
            output_path.display()
        );
    }

    // Send to transformation channel if enabled
    if let Some(tx) = transform_tx {
        let transform_calls: Vec<TransformDecodedCall> = decoded_records
            .iter()
            .map(|r| convert_to_transform_call(r, &config.contract_name, &config.function_name, &config.output_type))
            .collect();

        if !transform_calls.is_empty() {
            let msg = DecodedCallsMessage {
                range_start,
                range_end,
                source_name: config.contract_name.clone(),
                function_name: config.function_name.clone(),
                calls: transform_calls,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!("Failed to send decoded calls to transformation channel: {}", e);
            }
        }
    }

    Ok(())
}

/// Process "once" call results.
/// Returns `OnceCallsResult` with column index info for batch updating (avoids race conditions).
/// When `return_index_info` is true, skips writing the column index (caller will batch update).
pub(crate) async fn process_once_calls(
    results: &[OnceCallResult],
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    configs: &[&CallDecodeConfig],
    output_base: &Path,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    return_index_info: bool,
) -> Result<Option<OnceCallsResult>, EthCallDecodingError> {
    let mut decoded_records = Vec::new();
    // Track decode failures per function: fn_name -> (successes, failures)
    let mut decode_stats: HashMap<String, (u64, u64)> = HashMap::new();

    for result in results {
        let mut decoded_values = HashMap::new();

        for config in configs {
            if let Some(raw_value) = result.results.get(&config.function_name) {
                if !raw_value.is_empty() {
                    let stats = decode_stats
                        .entry(config.function_name.clone())
                        .or_insert((0, 0));
                    match decode_value(raw_value, &config.output_type) {
                        Ok(decoded) => {
                            stats.0 += 1;
                            decoded_values.insert(config.function_name.clone(), decoded);
                        }
                        Err(e) => {
                            stats.1 += 1;
                            tracing::debug!(
                                "Failed to decode {}.{} at block {}: {}",
                                contract_name,
                                config.function_name,
                                result.block_number,
                                e
                            );
                        }
                    }
                }
            }
        }

        decoded_records.push(DecodedOnceRecord {
            block_number: result.block_number,
            block_timestamp: result.block_timestamp,
            contract_address: result.contract_address,
            decoded_values,
        });
    }

    // Log warnings for functions with decode failures
    for (fn_name, (successes, failures)) in &decode_stats {
        if *failures > 0 {
            let config = configs.iter().find(|c| &c.function_name == fn_name);
            let output_type_str = config
                .map(|c| format!("{:?}", c.output_type))
                .unwrap_or_default();
            if *successes == 0 {
                tracing::warn!(
                    "All {} decode attempts failed for {}/{} (output_type: {}) in blocks {}-{} — check output_type in config",
                    failures,
                    contract_name,
                    fn_name,
                    output_type_str,
                    range_start,
                    range_end - 1
                );
            } else {
                tracing::warn!(
                    "{}/{} decode failures for {}/{} in blocks {}-{}",
                    failures,
                    successes + failures,
                    contract_name,
                    fn_name,
                    range_start,
                    range_end - 1
                );
            }
        }
    }

    // Write decoded data
    let output_dir = output_base.join(contract_name).join("once");
    std::fs::create_dir_all(&output_dir)?;

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let output_path = output_dir.join(&file_name);

    // Check if we need to merge with existing decoded data
    if output_path.exists() {
        tracing::info!(
            "Merging new decoded columns into existing file {}",
            output_path.display()
        );
        merge_decoded_once_calls(
            &output_path,
            &decoded_records,
            configs,
        )?;
    } else {
        write_decoded_once_calls_to_parquet(&decoded_records, configs, &output_path)?;
    }

    // Read actual columns from written file
    let actual_cols: Vec<String> = read_decoded_parquet_function_names(&output_path)
        .into_iter()
        .collect();

    if decoded_records.is_empty() {
        tracing::debug!(
            "Wrote 0 decoded {}/once results to {} ({} columns: {:?})",
            contract_name,
            output_path.display(),
            actual_cols.len(),
            actual_cols
        );
    } else {
        tracing::info!(
            "Wrote {} decoded {}/once results to {} ({} columns: {:?})",
            decoded_records.len(),
            contract_name,
            output_path.display(),
            actual_cols.len(),
            actual_cols
        );
    }

    // If not returning index info, update column index directly (live mode)
    if !return_index_info {
        let mut index = read_decoded_column_index(&output_dir);
        index.insert(file_name.clone(), actual_cols.clone());
        write_decoded_column_index(&output_dir, &index)?;
    }

    // Send to transformation channel if enabled
    if let Some(tx) = transform_tx {
        // For "once" calls, we consolidate ALL functions per address into a single DecodedCall
        // with function_name = "once" and ALL results merged. This matches:
        // 1. How handlers specify call_dependencies (e.g., ("DERC20", "once"))
        // 2. How catchup reads from parquet (one row per address with all columns as result fields)
        // 3. How handlers filter (call.function_name == "once") and access (call.result.get("name"))
        let transform_calls: Vec<TransformDecodedCall> = decoded_records
            .iter()
            .map(|record| {
                // Merge all function results for this address into one result map
                let mut merged_result = HashMap::new();
                for config in configs {
                    if let Some(decoded_value) = record.decoded_values.get(&config.function_name) {
                        let partial_result = build_result_map(decoded_value, &config.output_type, &config.function_name);
                        merged_result.extend(partial_result);
                    }
                }
                TransformDecodedCall {
                    block_number: record.block_number,
                    block_timestamp: record.block_timestamp,
                    contract_address: record.contract_address,
                    source_name: contract_name.to_string(),
                    function_name: "once".to_string(),
                    trigger_log_index: None,
                    result: merged_result,
                }
            })
            .filter(|call| !call.result.is_empty())
            .collect();

        if !transform_calls.is_empty() {
            tracing::info!(
                "Sending DecodedCallsMessage: source_name={}, function_name=once, range=({}, {}), {} calls",
                contract_name, range_start, range_end, transform_calls.len()
            );
            let msg = DecodedCallsMessage {
                range_start,
                range_end,
                source_name: contract_name.to_string(),
                function_name: "once".to_string(),
                calls: transform_calls,
            };
            if let Err(e) = tx.send(msg).await {
                tracing::warn!("Failed to send decoded calls to transformation channel: {}", e);
            }
        } else {
            tracing::debug!(
                "No transform_calls to send for {}/once range ({}, {})",
                contract_name, range_start, range_end
            );
        }
    }

    // Return index info for batch updating (catchup mode)
    if return_index_info {
        Ok(Some(OnceCallsResult {
            contract_name: contract_name.to_string(),
            file_name,
            columns: actual_cols,
        }))
    } else {
        Ok(None)
    }
}

/// Convert an EvmType to a DynSolType for decoding
fn evm_type_to_dyn_sol_type(output_type: &EvmType) -> DynSolType {
    match output_type {
        EvmType::Int256 => DynSolType::Int(256),
        EvmType::Int128 => DynSolType::Int(128),
        EvmType::Int64 => DynSolType::Int(64),
        EvmType::Int32 => DynSolType::Int(32),
        EvmType::Int24 => DynSolType::Int(24),
        EvmType::Int16 => DynSolType::Int(16),
        EvmType::Int8 => DynSolType::Int(8),
        EvmType::Uint256 => DynSolType::Uint(256),
        EvmType::Uint160 => DynSolType::Uint(160),
        EvmType::Uint128 => DynSolType::Uint(128),
        EvmType::Uint96 => DynSolType::Uint(96),
        EvmType::Uint80 => DynSolType::Uint(80),
        EvmType::Uint64 => DynSolType::Uint(64),
        EvmType::Uint32 => DynSolType::Uint(32),
        EvmType::Uint24 => DynSolType::Uint(24),
        EvmType::Uint16 => DynSolType::Uint(16),
        EvmType::Uint8 => DynSolType::Uint(8),
        EvmType::Address => DynSolType::Address,
        EvmType::Bool => DynSolType::Bool,
        EvmType::Bytes32 => DynSolType::FixedBytes(32),
        EvmType::Bytes => DynSolType::Bytes,
        EvmType::String => DynSolType::String,
        EvmType::Named { inner, .. } => evm_type_to_dyn_sol_type(inner),
        EvmType::NamedTuple(fields) => {
            let field_types: Vec<DynSolType> = fields
                .iter()
                .map(|(_, ty)| evm_type_to_dyn_sol_type(ty))
                .collect();
            DynSolType::Tuple(field_types)
        }
    }
}

/// Decode a raw value using the specified type
fn decode_value(raw: &[u8], output_type: &EvmType) -> Result<DecodedValue, EthCallDecodingError> {
    let sol_type = evm_type_to_dyn_sol_type(output_type);

    let decoded = sol_type
        .abi_decode(raw)
        .map_err(|e| EthCallDecodingError::Decode(e.to_string()))?;

    convert_dyn_sol_value(&decoded, output_type)
}

/// Convert DynSolValue to DecodedValue
fn convert_dyn_sol_value(
    value: &DynSolValue,
    output_type: &EvmType,
) -> Result<DecodedValue, EthCallDecodingError> {
    // Handle Named types by delegating to inner type
    if let EvmType::Named { inner, .. } = output_type {
        return convert_dyn_sol_value(value, inner);
    }

    // Handle NamedTuple types
    if let EvmType::NamedTuple(fields) = output_type {
        if let DynSolValue::Tuple(values) = value {
            if values.len() != fields.len() {
                return Err(EthCallDecodingError::Decode(format!(
                    "Tuple length mismatch: expected {}, got {}",
                    fields.len(),
                    values.len()
                )));
            }
            let mut named_values = Vec::with_capacity(fields.len());
            for ((name, field_type), val) in fields.iter().zip(values.iter()) {
                let decoded = convert_dyn_sol_value(val, field_type)?;
                named_values.push((name.clone(), decoded));
            }
            return Ok(DecodedValue::NamedTuple(named_values));
        } else {
            return Err(EthCallDecodingError::Decode(format!(
                "Expected tuple value for NamedTuple type, got {:?}",
                value
            )));
        }
    }

    match value {
        DynSolValue::Address(addr) => Ok(DecodedValue::Address(addr.0 .0)),
        DynSolValue::Uint(val, _) => match output_type {
            EvmType::Uint8 => Ok(DecodedValue::Uint8((*val).try_into().unwrap_or(u8::MAX))),
            EvmType::Uint64 | EvmType::Uint32 | EvmType::Uint24 | EvmType::Uint16 => {
                Ok(DecodedValue::Uint64((*val).try_into().unwrap_or(u64::MAX)))
            }
            _ => Ok(DecodedValue::Uint256(*val)),
        },
        DynSolValue::Int(val, _) => match output_type {
            EvmType::Int8 => Ok(DecodedValue::Int8((*val).try_into().unwrap_or(i8::MAX))),
            EvmType::Int64 | EvmType::Int32 | EvmType::Int24 | EvmType::Int16 => {
                Ok(DecodedValue::Int64((*val).try_into().unwrap_or(i64::MAX)))
            }
            _ => Ok(DecodedValue::Int256(*val)),
        },
        DynSolValue::Bool(b) => Ok(DecodedValue::Bool(*b)),
        DynSolValue::FixedBytes(bytes, 32) => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes[..]);
            Ok(DecodedValue::Bytes32(arr))
        }
        DynSolValue::FixedBytes(bytes, _) => Ok(DecodedValue::Bytes(bytes.to_vec())),
        DynSolValue::Bytes(bytes) => Ok(DecodedValue::Bytes(bytes.clone())),
        DynSolValue::String(s) => Ok(DecodedValue::String(s.clone())),
        _ => Err(EthCallDecodingError::Decode(format!(
            "Unsupported value type: {:?}",
            value
        ))),
    }
}

/// Write decoded regular calls to parquet
fn write_decoded_calls_to_parquet(
    records: &[DecodedCallRecord],
    output_type: &EvmType,
    output_path: &Path,
) -> Result<(), EthCallDecodingError> {
    // Build schema based on output type
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("address", DataType::FixedSizeBinary(20), false),
    ];

    // Add value fields based on output type
    match output_type {
        EvmType::Named { name, inner } => {
            // Named single value: use the name as column name
            fields.push(Field::new(name, inner.to_arrow_type(), true));
        }
        EvmType::NamedTuple(tuple_fields) => {
            // Named tuple: create a column for each field
            for (field_name, field_type) in tuple_fields {
                fields.push(Field::new(field_name, field_type.to_arrow_type(), true));
            }
        }
        _ => {
            // Simple type: use "decoded_value"
            fields.push(Field::new("decoded_value", output_type.to_arrow_type(), true));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // contract_address
    if records.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    // Add value arrays based on output type
    match output_type {
        EvmType::Named { inner, .. } => {
            // Named single value: build array using inner type
            let value_array = build_value_array(records, inner)?;
            arrays.push(value_array);
        }
        EvmType::NamedTuple(tuple_fields) => {
            // Named tuple: build array for each field
            for (idx, (_, field_type)) in tuple_fields.iter().enumerate() {
                let arr = build_tuple_field_array(records, idx, field_type)?;
                arrays.push(arr);
            }
        }
        _ => {
            // Simple type
            let value_array = build_value_array(records, output_type)?;
            arrays.push(value_array);
        }
    }

    // Write to parquet
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Build an Arrow array for a specific field of a named tuple
fn build_tuple_field_array(
    records: &[DecodedCallRecord],
    field_idx: usize,
    field_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    // Extract the field value from each record's NamedTuple
    // For each record, find the tuple field at the given index
    match field_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::NamedTuple(fields) => {
                            if let Some((_, DecodedValue::Address(addr))) = fields.get(field_idx) {
                                addr.as_slice()
                            } else {
                                &[0u8; 20][..]
                            }
                        }
                        _ => &[0u8; 20][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| extract_tuple_uint8(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| extract_tuple_uint64(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| extract_tuple_uint32(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| extract_tuple_uint16(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256 | EvmType::Uint160 | EvmType::Uint128 | EvmType::Uint96 | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_tuple_uint256_string(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| extract_tuple_int8(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| extract_tuple_int64(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| extract_tuple_int32(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| extract_tuple_int16(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_tuple_int256_string(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| extract_tuple_bool(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::NamedTuple(fields) => {
                            if let Some((_, DecodedValue::Bytes32(b))) = fields.get(field_idx) {
                                b.as_slice()
                            } else {
                                &[0u8; 32][..]
                            }
                        }
                        _ => &[0u8; 32][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_tuple_string(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| extract_tuple_bytes(r, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => build_tuple_field_array(records, field_idx, inner),
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "Nested NamedTuple not supported".to_string(),
        )),
    }
}

// Helper functions for extracting tuple field values

fn extract_tuple_uint8(r: &DecodedCallRecord, idx: usize) -> Option<u8> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint8(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_uint64(r: &DecodedCallRecord, idx: usize) -> Option<u64> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => Some(*val),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_uint32(r: &DecodedCallRecord, idx: usize) -> Option<u32> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => (*val).try_into().ok(),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_uint16(r: &DecodedCallRecord, idx: usize) -> Option<u16> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => (*val).try_into().ok(),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_uint256_string(r: &DecodedCallRecord, idx: usize) -> Option<String> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint256(val) => Some(val.to_string()),
            DecodedValue::Uint64(val) => Some(val.to_string()),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_int8(r: &DecodedCallRecord, idx: usize) -> Option<i8> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int8(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_int64(r: &DecodedCallRecord, idx: usize) -> Option<i64> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => Some(*val),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_int32(r: &DecodedCallRecord, idx: usize) -> Option<i32> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => (*val).try_into().ok(),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_int16(r: &DecodedCallRecord, idx: usize) -> Option<i16> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => (*val).try_into().ok(),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_int256_string(r: &DecodedCallRecord, idx: usize) -> Option<String> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int256(val) => Some(val.to_string()),
            DecodedValue::Int64(val) => Some(val.to_string()),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_bool(r: &DecodedCallRecord, idx: usize) -> Option<bool> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Bool(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_string(r: &DecodedCallRecord, idx: usize) -> Option<&str> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::String(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_tuple_bytes(r: &DecodedCallRecord, idx: usize) -> Option<&[u8]> {
    match &r.decoded_value {
        DecodedValue::NamedTuple(fields) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Bytes(b) => Some(b.as_slice()),
            DecodedValue::Bytes32(b) => Some(b.as_slice()),
            _ => None,
        }),
        _ => None,
    }
}

/// Read existing decoded once parquet file and return record batches
fn read_existing_decoded_once_parquet(
    path: &Path,
) -> Result<Vec<RecordBatch>, EthCallDecodingError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

/// Merge new decoded columns into an existing decoded once parquet file
fn merge_decoded_once_calls(
    output_path: &Path,
    new_records: &[DecodedOnceRecord],
    new_configs: &[&CallDecodeConfig],
) -> Result<(), EthCallDecodingError> {
    // Read existing parquet
    let existing_batches = read_existing_decoded_once_parquet(output_path)?;
    if existing_batches.is_empty() {
        // No existing data, just write new
        return write_decoded_once_calls_to_parquet(new_records, new_configs, output_path);
    }

    let existing_batch = &existing_batches[0];
    let existing_schema = existing_batch.schema();

    // Build address lookup map from new_records
    let new_records_by_address: HashMap<[u8; 20], &DecodedOnceRecord> = new_records
        .iter()
        .map(|r| (r.contract_address, r))
        .collect();

    // Extract address column from existing batch
    let address_col_idx = existing_schema
        .index_of("address")
        .map_err(|e| EthCallDecodingError::Decode(format!("Missing address column: {}", e)))?;
    let address_arr = existing_batch
        .column(address_col_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            EthCallDecodingError::Decode("address column is not FixedSizeBinaryArray".to_string())
        })?;

    // Build new schema with existing fields + new fields
    let mut fields: Vec<Field> = existing_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    // Add new fields for each new config
    for config in new_configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                for (field_name, field_type) in tuple_fields {
                    let col_name = format!("{}.{}", config.function_name, field_name);
                    if !fields.iter().any(|f| f.name() == &col_name) {
                        fields.push(Field::new(&col_name, field_type.to_arrow_type(), true));
                    }
                }
            }
            _ => {
                let col_name = config.function_name.clone();
                if !fields.iter().any(|f| f.name() == &col_name) {
                    let value_type = config.output_type.to_arrow_type();
                    fields.push(Field::new(&col_name, value_type, true));
                }
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Build a lookup from column name -> (config, optional tuple field info)
    // This is used to fill nulls in existing columns from new decoded records.
    let mut col_config_lookup: HashMap<
        String,
        (&CallDecodeConfig, Option<(usize, &EvmType)>),
    > = HashMap::new();
    for config in new_configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                for (idx, (field_name, field_type)) in tuple_fields.iter().enumerate() {
                    let col_name = format!("{}.{}", config.function_name, field_name);
                    col_config_lookup.insert(col_name, (config, Some((idx, field_type))));
                }
            }
            _ => {
                col_config_lookup
                    .insert(config.function_name.clone(), (config, None));
            }
        }
    }

    // Copy existing columns, filling nulls where possible from new decoded records
    for i in 0..existing_batch.num_columns() {
        let col = existing_batch.column(i);
        let col_name = existing_schema.field(i).name().clone();

        if col.null_count() > 0 {
            if let Some((config, tuple_info)) = col_config_lookup.get(&col_name) {
                // This column has nulls and we have new decoded data — rebuild it
                let rebuilt = match tuple_info {
                    Some((field_idx, field_type)) => build_once_tuple_field_array_aligned(
                        address_arr,
                        &new_records_by_address,
                        &config.function_name,
                        *field_idx,
                        field_type,
                    )?,
                    None => build_once_value_array_aligned(
                        address_arr,
                        &new_records_by_address,
                        &config.function_name,
                        &config.output_type,
                    )?,
                };

                let old_nulls = col.null_count();
                let new_nulls = rebuilt.null_count();
                if new_nulls < old_nulls {
                    tracing::info!(
                        "Replacing column '{}' (had {} nulls, now {} nulls)",
                        col_name,
                        old_nulls,
                        new_nulls
                    );
                    // Update field type if the config's output type changed
                    if rebuilt.data_type() != col.data_type() {
                        tracing::info!(
                            "Column '{}' type changed from {:?} to {:?}",
                            col_name,
                            col.data_type(),
                            rebuilt.data_type()
                        );
                        fields[i] = Field::new(&col_name, rebuilt.data_type().clone(), true);
                    }
                    arrays.push(rebuilt);
                    continue;
                } else if old_nulls > 0 {
                    tracing::warn!(
                        "Column '{}' has {} nulls but rebuilt column still has {} nulls (all decodes failed — check output_type {:?})",
                        col_name,
                        old_nulls,
                        new_nulls,
                        config.output_type
                    );
                }
            }
        }
        arrays.push(col.clone());
    }

    // Build new columns aligned to existing addresses
    for config in new_configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                for (idx, (_, field_type)) in tuple_fields.iter().enumerate() {
                    let col_name = format!("{}.{}", config.function_name, tuple_fields[idx].0);
                    if existing_schema.index_of(&col_name).is_err() {
                        let arr = build_once_tuple_field_array_aligned(
                            address_arr,
                            &new_records_by_address,
                            &config.function_name,
                            idx,
                            field_type,
                        )?;
                        arrays.push(arr);
                    }
                }
            }
            _ => {
                let col_name = config.function_name.clone();
                if existing_schema.index_of(&col_name).is_err() {
                    let arr = build_once_value_array_aligned(
                        address_arr,
                        &new_records_by_address,
                        &config.function_name,
                        &config.output_type,
                    )?;
                    arrays.push(arr);
                }
            }
        }
    }

    // Build schema after column processing (fields may have been updated during null-filling)
    let new_schema = Arc::new(Schema::new(fields));

    // Write merged parquet
    let batch = RecordBatch::try_new(new_schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, new_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Write decoded "once" calls to parquet
fn write_decoded_once_calls_to_parquet(
    records: &[DecodedOnceRecord],
    configs: &[&CallDecodeConfig],
    output_path: &Path,
) -> Result<(), EthCallDecodingError> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("address", DataType::FixedSizeBinary(20), false),
    ];

    // Add a field for each function result
    for config in configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                // Named tuple: create a column for each field with {function}_decoded.{field_name}
                for (field_name, field_type) in tuple_fields {
                    fields.push(Field::new(
                        &format!("{}.{}", config.function_name, field_name),
                        field_type.to_arrow_type(),
                        true,
                    ));
                }
            }
            _ => {
                // Simple type or named single value
                let value_type = config.output_type.to_arrow_type();
                fields.push(Field::new(
                    &format!("{}", config.function_name),
                    value_type,
                    true,
                ));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // contract_address
    if records.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    // Function result columns
    for config in configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                // Named tuple: build an array for each field
                for (idx, (_, field_type)) in tuple_fields.iter().enumerate() {
                    let arr =
                        build_once_tuple_field_array(records, &config.function_name, idx, field_type)?;
                    arrays.push(arr);
                }
            }
            _ => {
                let arr =
                    build_once_value_array(records, &config.function_name, &config.output_type)?;
                arrays.push(arr);
            }
        }
    }

    // Write to parquet
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Build an Arrow array for decoded values
fn build_value_array(
    records: &[DecodedCallRecord],
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match output_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::Address(addr) => addr.as_slice(),
                        _ => &[0u8; 20][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => Some(*v),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => (*v).try_into().ok(),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => (*v).try_into().ok(),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256 | EvmType::Uint160 | EvmType::Uint128 | EvmType::Uint96 | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint256(v) => Some(v.to_string()),
                    DecodedValue::Uint64(v) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => Some(*v),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => (*v).try_into().ok(),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => (*v).try_into().ok(),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int256(v) => Some(v.to_string()),
                    DecodedValue::Int64(v) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Bool(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::Bytes32(b) => b.as_slice(),
                        _ => &[0u8; 32][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Bytes(b) => Some(b.as_slice()),
                    DecodedValue::Bytes32(b) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        // Named types delegate to their inner type
        EvmType::Named { inner, .. } => build_value_array(records, inner),
        // NamedTuple should not use this function directly - use build_named_tuple_arrays
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple should use build_named_tuple_arrays".to_string(),
        )),
    }
}

/// Build an Arrow array for "once" decoded values
fn build_once_value_array(
    records: &[DecodedOnceRecord],
    function_name: &str,
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match output_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match r.decoded_values.get(function_name) {
                        Some(DecodedValue::Address(addr)) => addr.as_slice(),
                        _ => &[0u8; 20][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint8(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint64(v)) => Some(*v),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256 | EvmType::Uint160 | EvmType::Uint128 | EvmType::Uint96 | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint256(v)) => Some(v.to_string()),
                    Some(DecodedValue::Uint64(v)) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int8(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int64(v)) => Some(*v),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int256(v)) => Some(v.to_string()),
                    Some(DecodedValue::Int64(v)) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Bool(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match r.decoded_values.get(function_name) {
                        Some(DecodedValue::Bytes32(b)) => b.as_slice(),
                        _ => &[0u8; 32][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::String(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Bytes(b)) => Some(b.as_slice()),
                    Some(DecodedValue::Bytes32(b)) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        // Named types delegate to their inner type
        EvmType::Named { inner, .. } => build_once_value_array(records, function_name, inner),
        // NamedTuple should use build_once_tuple_field_array instead
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple should use build_once_tuple_field_array".to_string(),
        )),
    }
}

/// Build an Arrow array for "once" decoded values, aligned to existing addresses.
/// For each address in `address_arr`, looks up the record in `records_by_addr` and extracts the value.
/// Returns NULL for addresses not found in the lookup map.
fn build_once_value_array_aligned(
    address_arr: &FixedSizeBinaryArray,
    records_by_addr: &HashMap<[u8; 20], &DecodedOnceRecord>,
    function_name: &str,
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    let num_rows = address_arr.len();

    // Helper to get address at index
    let get_addr = |i: usize| -> [u8; 20] {
        address_arr
            .value(i)
            .try_into()
            .unwrap_or([0u8; 20])
    };

    match output_type {
        EvmType::Address => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 20);
            for i in 0..num_rows {
                let addr = get_addr(i);
                match records_by_addr.get(&addr).and_then(|r| r.decoded_values.get(function_name)) {
                    Some(DecodedValue::Address(a)) => builder.append_value(a)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint8(val) => Some(*val),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint64(val) => Some(*val),
                            DecodedValue::Uint256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint64(val) => (*val).try_into().ok(),
                            DecodedValue::Uint256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint64(val) => (*val).try_into().ok(),
                            DecodedValue::Uint256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256 | EvmType::Uint160 | EvmType::Uint128 | EvmType::Uint96 | EvmType::Uint80 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint256(val) => Some(val.to_string()),
                            DecodedValue::Uint64(val) => Some(val.to_string()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int8(val) => Some(*val),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int64(val) => Some(*val),
                            DecodedValue::Int256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int64(val) => (*val).try_into().ok(),
                            DecodedValue::Int256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int64(val) => (*val).try_into().ok(),
                            DecodedValue::Int256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int256(val) => Some(val.to_string()),
                            DecodedValue::Int64(val) => Some(val.to_string()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Bool(val) => Some(*val),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 32);
            for i in 0..num_rows {
                let addr = get_addr(i);
                match records_by_addr.get(&addr).and_then(|r| r.decoded_values.get(function_name)) {
                    Some(DecodedValue::Bytes32(b)) => builder.append_value(b)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::String => {
            let arr: StringArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::String(s) => Some(s.as_str()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Bytes(b) => Some(b.as_slice()),
                            DecodedValue::Bytes32(b) => Some(b.as_slice()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => {
            build_once_value_array_aligned(address_arr, records_by_addr, function_name, inner)
        }
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple should use build_once_tuple_field_array_aligned".to_string(),
        )),
    }
}

/// Build an Arrow array for a specific field of a named tuple from "once" calls, aligned to existing addresses.
fn build_once_tuple_field_array_aligned(
    address_arr: &FixedSizeBinaryArray,
    records_by_addr: &HashMap<[u8; 20], &DecodedOnceRecord>,
    function_name: &str,
    field_idx: usize,
    field_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    let num_rows = address_arr.len();

    // Helper to get address at index
    let get_addr = |i: usize| -> [u8; 20] {
        address_arr
            .value(i)
            .try_into()
            .unwrap_or([0u8; 20])
    };

    // Helper to extract tuple field value
    let get_tuple_field = |i: usize| -> Option<&DecodedValue> {
        let addr = get_addr(i);
        records_by_addr
            .get(&addr)
            .and_then(|r| r.decoded_values.get(function_name))
            .and_then(|v| match v {
                DecodedValue::NamedTuple(fields) => fields.get(field_idx).map(|(_, val)| val),
                _ => None,
            })
    };

    match field_type {
        EvmType::Address => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 20);
            for i in 0..num_rows {
                match get_tuple_field(i) {
                    Some(DecodedValue::Address(a)) => builder.append_value(a)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint8(val)) => Some(*val),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint64(val)) => Some(*val),
                    Some(DecodedValue::Uint256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Uint256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Uint256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256 | EvmType::Uint160 | EvmType::Uint128 | EvmType::Uint96 | EvmType::Uint80 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint256(val)) => Some(val.to_string()),
                    Some(DecodedValue::Uint64(val)) => Some(val.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int8(val)) => Some(*val),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int64(val)) => Some(*val),
                    Some(DecodedValue::Int256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Int256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Int256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int256(val)) => Some(val.to_string()),
                    Some(DecodedValue::Int64(val)) => Some(val.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Bool(val)) => Some(*val),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 32);
            for i in 0..num_rows {
                match get_tuple_field(i) {
                    Some(DecodedValue::Bytes32(b)) => builder.append_value(b)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::String => {
            let arr: StringArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::String(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Bytes(b)) => Some(b.as_slice()),
                    Some(DecodedValue::Bytes32(b)) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => {
            build_once_tuple_field_array_aligned(address_arr, records_by_addr, function_name, field_idx, inner)
        }
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "Nested NamedTuple not supported".to_string(),
        )),
    }
}

/// Build an Arrow array for a specific field of a named tuple from "once" calls
fn build_once_tuple_field_array(
    records: &[DecodedOnceRecord],
    function_name: &str,
    field_idx: usize,
    field_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match field_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    extract_once_tuple_address(r, function_name, field_idx)
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| extract_once_tuple_uint8(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| extract_once_tuple_uint64(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| extract_once_tuple_uint32(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| extract_once_tuple_uint16(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256 | EvmType::Uint160 | EvmType::Uint128 | EvmType::Uint96 | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_once_tuple_uint256_string(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| extract_once_tuple_int8(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| extract_once_tuple_int64(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| extract_once_tuple_int32(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| extract_once_tuple_int16(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_once_tuple_int256_string(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| extract_once_tuple_bool(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    extract_once_tuple_bytes32(r, function_name, field_idx)
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_once_tuple_string(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| extract_once_tuple_bytes(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => {
            build_once_tuple_field_array(records, function_name, field_idx, inner)
        }
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "Nested NamedTuple not supported".to_string(),
        )),
    }
}

// Helper functions for extracting tuple field values from "once" records

fn extract_once_tuple_address<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> &'a [u8] {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => {
            if let Some((_, DecodedValue::Address(addr))) = fields.get(idx) {
                addr.as_slice()
            } else {
                &[0u8; 20][..]
            }
        }
        _ => &[0u8; 20][..],
    }
}

fn extract_once_tuple_uint8(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<u8> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint8(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_uint64(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<u64> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => Some(*val),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_uint32(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<u32> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => (*val).try_into().ok(),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_uint16(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<u16> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => (*val).try_into().ok(),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_uint256_string(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<String> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint256(val) => Some(val.to_string()),
            DecodedValue::Uint64(val) => Some(val.to_string()),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_int8(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<i8> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int8(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_int64(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<i64> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => Some(*val),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_int32(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<i32> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => (*val).try_into().ok(),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_int16(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<i16> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => (*val).try_into().ok(),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_int256_string(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<String> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int256(val) => Some(val.to_string()),
            DecodedValue::Int64(val) => Some(val.to_string()),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_bool(r: &DecodedOnceRecord, function_name: &str, idx: usize) -> Option<bool> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Bool(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_bytes32<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> &'a [u8] {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => {
            if let Some((_, DecodedValue::Bytes32(b))) = fields.get(idx) {
                b.as_slice()
            } else {
                &[0u8; 32][..]
            }
        }
        _ => &[0u8; 32][..],
    }
}

fn extract_once_tuple_string<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<&'a str> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::String(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    }
}

fn extract_once_tuple_bytes<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<&'a [u8]> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Bytes(b) => Some(b.as_slice()),
            DecodedValue::Bytes32(b) => Some(b.as_slice()),
            _ => None,
        }),
        _ => None,
    }
}

/// Convert internal DecodedValue to transformation DecodedValue
fn convert_decoded_value(value: &DecodedValue) -> TransformDecodedValue {
    match value {
        DecodedValue::Address(a) => TransformDecodedValue::Address(*a),
        DecodedValue::Uint256(v) => TransformDecodedValue::Uint256(*v),
        DecodedValue::Int256(v) => TransformDecodedValue::Int256(*v),
        DecodedValue::Uint64(v) => TransformDecodedValue::Uint64(*v),
        DecodedValue::Int64(v) => TransformDecodedValue::Int64(*v),
        DecodedValue::Uint8(v) => TransformDecodedValue::Uint8(*v),
        DecodedValue::Int8(v) => TransformDecodedValue::Int8(*v),
        DecodedValue::Bool(v) => TransformDecodedValue::Bool(*v),
        DecodedValue::Bytes32(b) => TransformDecodedValue::Bytes32(*b),
        DecodedValue::Bytes(b) => TransformDecodedValue::Bytes(b.clone()),
        DecodedValue::String(s) => TransformDecodedValue::String(s.clone()),
        DecodedValue::NamedTuple(fields) => {
            let converted: Vec<(String, TransformDecodedValue)> = fields
                .iter()
                .map(|(name, val)| (name.clone(), convert_decoded_value(val)))
                .collect();
            TransformDecodedValue::NamedTuple(converted)
        }
    }
}

/// Build result HashMap based on output type
fn build_result_map(
    value: &DecodedValue,
    output_type: &EvmType,
    function_name: &str,
) -> HashMap<String, TransformDecodedValue> {
    let mut result = HashMap::new();
    match output_type {
        EvmType::Named { name, .. } => {
            // Named single value
            result.insert(name.clone(), convert_decoded_value(value));
        }
        EvmType::NamedTuple(fields) => {
            // Named tuple: extract each field
            if let DecodedValue::NamedTuple(named_values) = value {
                for ((field_name, _), (_, val)) in fields.iter().zip(named_values.iter()) {
                    result.insert(field_name.clone(), convert_decoded_value(val));
                }
            }
        }
        _ => {
            // Simple type: use function name or "result"
            result.insert(function_name.to_string(), convert_decoded_value(value));
        }
    }
    result
}

/// Convert a DecodedCallRecord to a TransformDecodedCall
fn convert_to_transform_call(
    record: &DecodedCallRecord,
    source_name: &str,
    function_name: &str,
    output_type: &EvmType,
) -> TransformDecodedCall {
    TransformDecodedCall {
        block_number: record.block_number,
        block_timestamp: record.block_timestamp,
        contract_address: record.contract_address,
        source_name: source_name.to_string(),
        function_name: function_name.to_string(),
        trigger_log_index: None,
        result: build_result_map(&record.decoded_value, output_type, function_name),
    }
}

/// Convert a DecodedOnceRecord entry to a TransformDecodedCall
fn convert_once_to_transform_call(
    record: &DecodedOnceRecord,
    source_name: &str,
    function_name: &str,
    decoded_value: &DecodedValue,
    output_type: &EvmType,
) -> TransformDecodedCall {
    TransformDecodedCall {
        block_number: record.block_number,
        block_timestamp: record.block_timestamp,
        contract_address: record.contract_address,
        source_name: source_name.to_string(),
        function_name: function_name.to_string(),
        trigger_log_index: None,
        result: build_result_map(decoded_value, output_type, function_name),
    }
}
