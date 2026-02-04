use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{I256, U256};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Int64Array, Int8Array, StringArray, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;

use super::types::{DecoderMessage, EthCallResult, OnceCallResult};
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
struct CallDecodeConfig {
    contract_name: String,
    function_name: String,
    output_type: EvmType,
    is_once: bool,
}

/// Decoded call result
#[derive(Debug)]
struct DecodedCallRecord {
    block_number: u64,
    block_timestamp: u64,
    contract_address: [u8; 20],
    decoded_value: DecodedValue,
}

/// Decoded "once" call result
#[derive(Debug)]
struct DecodedOnceRecord {
    block_number: u64,
    block_timestamp: u64,
    contract_address: [u8; 20],
    /// function_name -> decoded value
    decoded_values: HashMap<String, DecodedValue>,
}

/// A decoded value
#[derive(Debug, Clone)]
enum DecodedValue {
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

pub async fn decode_eth_calls(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    mut decoder_rx: Receiver<DecoderMessage>,
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
        // Drain channel and return
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
    // Catchup phase: Process existing raw eth_call files
    // =========================================================================
    let raw_calls_dir = PathBuf::from(format!("data/raw/{}/eth_calls", chain.name));
    if raw_calls_dir.exists() {
        catchup_decode_eth_calls(
            &raw_calls_dir,
            &output_base,
            &regular_configs,
            &once_configs,
        )
        .await?;
    }

    tracing::info!(
        "Eth_call decoding catchup complete for chain {}",
        chain.name
    );

    // =========================================================================
    // Live phase: Process new data as it arrives
    // =========================================================================
    loop {
        match decoder_rx.recv().await {
            Some(DecoderMessage::EthCallsReady {
                range_start,
                range_end,
                contract_name,
                function_name,
                results,
            }) => {
                // Find the decode config for this call
                let config = regular_configs
                    .iter()
                    .find(|c| c.contract_name == contract_name && c.function_name == function_name);

                if let Some(config) = config {
                    process_regular_calls(
                        &results,
                        range_start,
                        range_end,
                        config,
                        &output_base,
                    )
                    .await?;
                }
            }
            Some(DecoderMessage::OnceCallsReady {
                range_start,
                range_end,
                contract_name,
                results,
            }) => {
                // Find the once configs for this contract
                let configs: Vec<&CallDecodeConfig> = once_configs
                    .iter()
                    .filter(|c| c.contract_name == contract_name)
                    .collect();

                if !configs.is_empty() {
                    process_once_calls(
                        &results,
                        range_start,
                        range_end,
                        &contract_name,
                        &configs,
                        &output_base,
                    )
                    .await?;
                }
            }
            Some(DecoderMessage::AllComplete) => {
                break;
            }
            None => {
                break;
            }
            _ => {}
        }
    }

    tracing::info!("Eth_call decoding complete for chain {}", chain.name);
    Ok(())
}

/// Build decode configurations from contracts
fn build_decode_configs(
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
                for call in &factory.calls {
                    let function_name = call
                        .function
                        .split('(')
                        .next()
                        .unwrap_or(&call.function)
                        .to_string();

                    let config = CallDecodeConfig {
                        contract_name: factory.collection_name.clone(),
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

    (regular, once)
}

/// Catchup phase: decode existing raw eth_call files
async fn catchup_decode_eth_calls(
    raw_calls_dir: &Path,
    output_base: &Path,
    regular_configs: &[CallDecodeConfig],
    once_configs: &[CallDecodeConfig],
) -> Result<(), EthCallDecodingError> {
    // Scan existing decoded files
    let existing_decoded = scan_existing_decoded_files(output_base);

    // Scan raw call directories
    if let Ok(contract_entries) = std::fs::read_dir(raw_calls_dir) {
        for contract_entry in contract_entries.flatten() {
            let contract_path = contract_entry.path();
            if !contract_path.is_dir() {
                continue;
            }
            let contract_name = match contract_entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => continue,
            };

            if let Ok(function_entries) = std::fs::read_dir(&contract_path) {
                for function_entry in function_entries.flatten() {
                    let function_path = function_entry.path();
                    if !function_path.is_dir() {
                        continue;
                    }
                    let function_name = match function_entry.file_name().to_str() {
                        Some(name) => name.to_string(),
                        None => continue,
                    };

                    let is_once = function_name == "once";

                    // Find matching config
                    if is_once {
                        let configs: Vec<&CallDecodeConfig> = once_configs
                            .iter()
                            .filter(|c| c.contract_name == contract_name)
                            .collect();

                        if configs.is_empty() {
                            continue;
                        }

                        // Process once call files
                        if let Ok(file_entries) = std::fs::read_dir(&function_path) {
                            for file_entry in file_entries.flatten() {
                                let file_path = file_entry.path();
                                if !file_path
                                    .extension()
                                    .map(|e| e == "parquet")
                                    .unwrap_or(false)
                                {
                                    continue;
                                }

                                let file_name =
                                    file_path.file_name().and_then(|s| s.to_str()).unwrap_or("");

                                // Check if already decoded
                                let rel_path =
                                    format!("{}/once/{}", contract_name, file_name);
                                if existing_decoded.contains(&rel_path) {
                                    continue;
                                }

                                // Parse range from filename
                                let range_str = file_name.strip_suffix(".parquet").unwrap_or("");
                                let parts: Vec<&str> = range_str.split('-').collect();
                                if parts.len() != 2 {
                                    continue;
                                }
                                let range_start: u64 = match parts[0].parse() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };
                                let range_end: u64 = match parts[1].parse::<u64>() {
                                    Ok(v) => v + 1,
                                    Err(_) => continue,
                                };

                                // Read and decode
                                let results = read_once_calls_from_parquet(&file_path, &configs)?;
                                process_once_calls(
                                    &results,
                                    range_start,
                                    range_end,
                                    &contract_name,
                                    &configs,
                                    output_base,
                                )
                                .await?;
                            }
                        }
                    } else {
                        let config = regular_configs
                            .iter()
                            .find(|c| {
                                c.contract_name == contract_name && c.function_name == function_name
                            });

                        if config.is_none() {
                            continue;
                        }
                        let config = config.unwrap();

                        // Process regular call files
                        if let Ok(file_entries) = std::fs::read_dir(&function_path) {
                            for file_entry in file_entries.flatten() {
                                let file_path = file_entry.path();
                                if !file_path
                                    .extension()
                                    .map(|e| e == "parquet")
                                    .unwrap_or(false)
                                {
                                    continue;
                                }

                                let file_name =
                                    file_path.file_name().and_then(|s| s.to_str()).unwrap_or("");

                                // Check if already decoded
                                let rel_path =
                                    format!("{}/{}/{}", contract_name, function_name, file_name);
                                if existing_decoded.contains(&rel_path) {
                                    continue;
                                }

                                // Parse range from filename
                                let range_str = file_name.strip_suffix(".parquet").unwrap_or("");
                                let parts: Vec<&str> = range_str.split('-').collect();
                                if parts.len() != 2 {
                                    continue;
                                }
                                let range_start: u64 = match parts[0].parse() {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };
                                let range_end: u64 = match parts[1].parse::<u64>() {
                                    Ok(v) => v + 1,
                                    Err(_) => continue,
                                };

                                // Read and decode
                                let results = read_regular_calls_from_parquet(&file_path)?;
                                process_regular_calls(
                                    &results,
                                    range_start,
                                    range_end,
                                    config,
                                    output_base,
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Scan existing decoded files
fn scan_existing_decoded_files(output_base: &Path) -> HashSet<String> {
    let mut files = HashSet::new();

    fn scan_recursive(dir: &Path, base: &Path, files: &mut HashSet<String>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan_recursive(&path, base, files);
                } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    if let Ok(rel) = path.strip_prefix(base) {
                        files.insert(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    scan_recursive(output_base, output_base, &mut files);
    files
}

/// Read regular call results from parquet
fn read_regular_calls_from_parquet(path: &Path) -> Result<Vec<EthCallResult>, EthCallDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let address_idx = schema.index_of("contract_address").ok();
        let value_idx = schema.index_of("value").ok();

        for row in 0..batch.num_rows() {
            let block_number = block_number_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let block_timestamp = block_timestamp_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let contract_address = address_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 20 {
                                let mut arr = [0u8; 20];
                                arr.copy_from_slice(bytes);
                                Some(arr)
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let value = value_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|a| a.value(row).to_vec())
                })
                .unwrap_or_default();

            results.push(EthCallResult {
                block_number,
                block_timestamp,
                contract_address,
                value,
            });
        }
    }

    Ok(results)
}

/// Read "once" call results from parquet
fn read_once_calls_from_parquet(
    path: &Path,
    configs: &[&CallDecodeConfig],
) -> Result<Vec<OnceCallResult>, EthCallDecodingError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let address_idx = schema.index_of("address").ok();

        // Find function result columns
        let function_indices: Vec<(String, Option<usize>)> = configs
            .iter()
            .map(|c| {
                let col_name = format!("{}_result", c.function_name);
                (c.function_name.clone(), schema.index_of(&col_name).ok())
            })
            .collect();

        for row in 0..batch.num_rows() {
            let block_number = block_number_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let block_timestamp = block_timestamp_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let contract_address = address_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 20 {
                                let mut arr = [0u8; 20];
                                arr.copy_from_slice(bytes);
                                Some(arr)
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let mut function_results = HashMap::new();
            for (function_name, idx_opt) in &function_indices {
                if let Some(idx) = idx_opt {
                    if let Some(arr) = batch.column(*idx).as_any().downcast_ref::<BinaryArray>() {
                        if !arr.is_null(row) {
                            function_results
                                .insert(function_name.clone(), arr.value(row).to_vec());
                        }
                    }
                }
            }

            results.push(OnceCallResult {
                block_number,
                block_timestamp,
                contract_address,
                results: function_results,
            });
        }
    }

    Ok(results)
}

/// Process regular call results
async fn process_regular_calls(
    results: &[EthCallResult],
    range_start: u64,
    range_end: u64,
    config: &CallDecodeConfig,
    output_base: &Path,
) -> Result<(), EthCallDecodingError> {
    let mut decoded_records = Vec::new();

    for result in results {
        if result.value.is_empty() {
            continue;
        }

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

    // Write decoded data
    let output_dir = output_base
        .join(&config.contract_name)
        .join(&config.function_name);
    std::fs::create_dir_all(&output_dir)?;

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let output_path = output_dir.join(&file_name);

    write_decoded_calls_to_parquet(&decoded_records, &config.output_type, &output_path)?;

    tracing::info!(
        "Wrote {} decoded {}.{} results to {}",
        decoded_records.len(),
        config.contract_name,
        config.function_name,
        output_path.display()
    );

    Ok(())
}

/// Process "once" call results
async fn process_once_calls(
    results: &[OnceCallResult],
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    configs: &[&CallDecodeConfig],
    output_base: &Path,
) -> Result<(), EthCallDecodingError> {
    let mut decoded_records = Vec::new();

    for result in results {
        let mut decoded_values = HashMap::new();

        for config in configs {
            if let Some(raw_value) = result.results.get(&config.function_name) {
                if !raw_value.is_empty() {
                    match decode_value(raw_value, &config.output_type) {
                        Ok(decoded) => {
                            decoded_values.insert(config.function_name.clone(), decoded);
                        }
                        Err(e) => {
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

    // Write decoded data
    let output_dir = output_base.join(contract_name).join("once");
    std::fs::create_dir_all(&output_dir)?;

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let output_path = output_dir.join(&file_name);

    write_decoded_once_calls_to_parquet(&decoded_records, configs, &output_path)?;

    tracing::info!(
        "Wrote {} decoded {}/once results to {}",
        decoded_records.len(),
        contract_name,
        output_path.display()
    );

    Ok(())
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
        Field::new("contract_address", DataType::FixedSizeBinary(20), false),
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
        EvmType::Uint64 | EvmType::Uint32 | EvmType::Uint24 | EvmType::Uint16 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| extract_tuple_uint64(r, field_idx))
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
        EvmType::Int64 | EvmType::Int32 | EvmType::Int24 | EvmType::Int16 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| extract_tuple_int64(r, field_idx))
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
        let value_type = config.output_type.to_arrow_type();
        fields.push(Field::new(
            &format!("{}_decoded", config.function_name),
            value_type,
            true,
        ));
    }

    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // address
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
        let arr = build_once_value_array(records, &config.function_name, &config.output_type)?;
        arrays.push(arr);
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
        EvmType::Uint64 | EvmType::Uint32 | EvmType::Uint24 | EvmType::Uint16 => {
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
        EvmType::Int64 | EvmType::Int32 | EvmType::Int24 | EvmType::Int16 => {
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
        EvmType::Uint64 | EvmType::Uint32 | EvmType::Uint24 | EvmType::Uint16 => {
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
        EvmType::Int64 | EvmType::Int32 | EvmType::Int24 | EvmType::Int16 => {
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
        // NamedTuple not yet supported for once calls
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple not yet supported for once calls".to_string(),
        )),
    }
}
