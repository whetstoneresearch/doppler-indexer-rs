use std::collections::{HashMap, HashSet};
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
use parquet::file::reader::{FileReader, SerializedFileReader};
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::types::{DecoderMessage, EthCallResult, OnceCallResult};
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
    transform_tx: Option<Sender<DecodedCallsMessage>>,
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
            raw_data_config,
            transform_tx.as_ref(),
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
                        transform_tx.as_ref(),
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
                        transform_tx.as_ref(),
                        false, // Live mode: update index directly (no concurrent tasks)
                    )
                    .await?;
                }
            }
            Some(DecoderMessage::OnceFileBackfilled {
                range_start,
                range_end,
                contract_name,
            }) => {
                // A once-call file was backfilled with new columns - decode missing columns
                let configs: Vec<&CallDecodeConfig> = once_configs
                    .iter()
                    .filter(|c| c.contract_name == contract_name)
                    .collect();

                if !configs.is_empty() {
                    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
                    let raw_path = raw_calls_dir
                        .join(&contract_name)
                        .join("once")
                        .join(&file_name);

                    if raw_path.exists() {
                        // Read raw columns from parquet
                        let raw_cols: HashSet<String> = read_raw_parquet_function_names(&raw_path);

                        // Read decoded columns from index or parquet
                        let decoded_dir = output_base.join(&contract_name).join("once");
                        let decoded_index = load_or_build_decoded_column_index(&decoded_dir);
                        let decoded_cols: HashSet<String> = decoded_index
                            .get(&file_name)
                            .map(|cols| cols.iter().cloned().collect())
                            .unwrap_or_default();

                        // Find configs that need decoding
                        let missing_configs: Vec<CallDecodeConfig> = configs
                            .iter()
                            .filter(|c| {
                                raw_cols.contains(&c.function_name)
                                    && !decoded_cols.contains(&c.function_name)
                            })
                            .cloned()
                            .cloned()
                            .collect();

                        if !missing_configs.is_empty() {
                            tracing::info!(
                                "OnceFileBackfilled: decoding {} new columns for {}/{}",
                                missing_configs.len(),
                                contract_name,
                                file_name
                            );

                            // Read raw parquet and decode the missing columns
                            let configs_ref: Vec<&CallDecodeConfig> =
                                missing_configs.iter().collect();
                            let results = read_once_calls_from_parquet(&raw_path, &configs_ref)?;
                            process_once_calls(
                                &results,
                                range_start,
                                range_end,
                                &contract_name,
                                &configs_ref,
                                &output_base,
                                transform_tx.as_ref(),
                                false,
                            )
                            .await?;
                        }
                    }
                }
            }
            Some(DecoderMessage::AllComplete) => {
                // Re-run catchup to decode any new columns added by raw backfill.
                // Initial catchup may have decoded partial data (e.g., name, symbol)
                // while raw backfill was still adding new columns (e.g., getAssetData).
                // This second pass picks up the newly added columns with fresh indexes.
                if raw_calls_dir.exists() {
                    tracing::info!(
                        "Raw collection complete, re-running decode catchup for chain {}",
                        chain.name
                    );
                    catchup_decode_eth_calls(
                        &raw_calls_dir,
                        &output_base,
                        &regular_configs,
                        &once_configs,
                        raw_data_config,
                        transform_tx.as_ref(),
                    )
                    .await?;
                }
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

/// Work item for concurrent catchup processing
enum CatchupWorkItem {
    Regular {
        file_path: PathBuf,
        range_start: u64,
        range_end: u64,
        config: CallDecodeConfig,
    },
    Once {
        file_path: PathBuf,
        range_start: u64,
        range_end: u64,
        contract_name: String,
        configs: Vec<CallDecodeConfig>,
    },
}

/// Result of processing once calls, used to batch update column indexes after all tasks complete.
/// This avoids race conditions when multiple concurrent tasks try to read/modify/write the same index.
struct OnceCallsResult {
    contract_name: String,
    file_name: String,
    columns: Vec<String>,
}

/// Catchup phase: decode existing raw eth_call files
async fn catchup_decode_eth_calls(
    raw_calls_dir: &Path,
    output_base: &Path,
    regular_configs: &[CallDecodeConfig],
    once_configs: &[CallDecodeConfig],
    raw_data_config: &RawDataCollectionConfig,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
) -> Result<(), EthCallDecodingError> {
    let concurrency = raw_data_config.decoding_concurrency.unwrap_or(4);

    // Scan existing decoded files
    let existing_decoded = scan_existing_decoded_files(output_base);

    // Build set of unique contract names with once configs
    let once_contract_names: HashSet<String> = once_configs
        .iter()
        .map(|c| c.contract_name.clone())
        .collect();

    // Pre-load column indexes for all once directories
    let mut raw_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
    let mut decoded_column_indexes: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

    for contract_name in &once_contract_names {
        let raw_once_dir = raw_calls_dir.join(contract_name).join("once");
        let raw_index = if raw_once_dir.exists() {
            let mut idx = read_raw_column_index(&raw_once_dir);
            // If index is empty, try to build from parquet schemas
            if idx.is_empty() {
                if let Ok(entries) = std::fs::read_dir(&raw_once_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
                            let cols: Vec<String> =
                                read_raw_parquet_function_names(&path).into_iter().collect();
                            if !cols.is_empty() {
                                idx.insert(file_name, cols);
                            }
                        }
                    }
                }
            }
            idx
        } else {
            HashMap::new()
        };
        raw_column_indexes.insert(contract_name.clone(), raw_index);

        let decoded_once_dir = output_base.join(contract_name).join("once");
        let decoded_index = load_or_build_decoded_column_index(&decoded_once_dir);
        decoded_column_indexes.insert(contract_name.clone(), decoded_index);
    }

    // Collect all work items
    let mut work_items = Vec::new();

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

                    if is_once {
                        let configs: Vec<CallDecodeConfig> = once_configs
                            .iter()
                            .filter(|c| c.contract_name == contract_name)
                            .cloned()
                            .collect();

                        if configs.is_empty() {
                            continue;
                        }

                        // Get pre-loaded column indexes
                        let raw_index = raw_column_indexes.get(&contract_name);
                        let decoded_index = decoded_column_indexes.get(&contract_name);

                        // Collect once call files
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

                                // Get raw columns from index (source of truth for what's been collected)
                                let raw_cols: HashSet<String> = raw_index
                                    .and_then(|idx| idx.get(file_name))
                                    .map(|cols| cols.iter().cloned().collect())
                                    .unwrap_or_else(|| read_raw_parquet_function_names(&file_path));

                                // Get decoded columns for this file
                                let decoded_cols: HashSet<String> = decoded_index
                                    .and_then(|idx| idx.get(file_name))
                                    .map(|cols| cols.iter().cloned().collect())
                                    .unwrap_or_default();

                                // Find functions that exist in raw but need decoding
                                let missing_configs: Vec<CallDecodeConfig> = configs
                                    .iter()
                                    .filter(|c| {
                                        raw_cols.contains(&c.function_name)
                                            && !decoded_cols.contains(&c.function_name)
                                    })
                                    .cloned()
                                    .collect();

                                if missing_configs.is_empty() {
                                    continue; // All columns already decoded
                                }

                                tracing::debug!(
                                    "File {}/{} missing decoded columns: {:?}",
                                    contract_name,
                                    file_name,
                                    missing_configs
                                        .iter()
                                        .map(|c| &c.function_name)
                                        .collect::<Vec<_>>()
                                );

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

                                work_items.push(CatchupWorkItem::Once {
                                    file_path,
                                    range_start,
                                    range_end,
                                    contract_name: contract_name.clone(),
                                    configs: missing_configs,
                                });
                            }
                        }
                    } else {
                        let config = regular_configs.iter().find(|c| {
                            c.contract_name == contract_name && c.function_name == function_name
                        });

                        if config.is_none() {
                            continue;
                        }
                        let config = config.unwrap().clone();

                        // Collect regular call files
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

                                work_items.push(CatchupWorkItem::Regular {
                                    file_path,
                                    range_start,
                                    range_end,
                                    config: config.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    if work_items.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Eth_call decoding catchup: processing {} files with concurrency {}",
        work_items.len(),
        concurrency
    );

    // Process work items concurrently
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let output_base = Arc::new(output_base.to_path_buf());
    let transform_tx = transform_tx.cloned();
    let mut join_set = JoinSet::new();

    for item in work_items {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let output_base = output_base.clone();
        let transform_tx = transform_tx.clone();

        join_set.spawn(async move {
            let _permit = permit; // Hold permit until task completes

            match item {
                CatchupWorkItem::Regular {
                    file_path,
                    range_start,
                    range_end,
                    config,
                } => {
                    let results = read_regular_calls_from_parquet(&file_path)?;
                    process_regular_calls(
                        &results,
                        range_start,
                        range_end,
                        &config,
                        &output_base,
                        transform_tx.as_ref(),
                    )
                    .await?;
                    // Regular calls don't need index updates
                    Ok(None)
                }
                CatchupWorkItem::Once {
                    file_path,
                    range_start,
                    range_end,
                    contract_name,
                    configs,
                } => {
                    let config_refs: Vec<&CallDecodeConfig> = configs.iter().collect();
                    let results = read_once_calls_from_parquet(&file_path, &config_refs)?;
                    // Return index info for batch update (avoids race condition)
                    process_once_calls(
                        &results,
                        range_start,
                        range_end,
                        &contract_name,
                        &config_refs,
                        &output_base,
                        transform_tx.as_ref(),
                        true, // return_index_info: batch update after all tasks complete
                    )
                    .await
                }
            }
        });
    }

    // Collect results and column index updates
    // Using a HashMap to group updates by contract_name for batch updating
    let mut index_updates: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(Some(once_result))) => {
                // Collect index updates for batch processing
                index_updates
                    .entry(once_result.contract_name)
                    .or_default()
                    .push((once_result.file_name, once_result.columns));
            }
            Ok(Ok(None)) => {} // Regular call, no index update needed
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(EthCallDecodingError::JoinError(e.to_string())),
        }
    }

    // Batch update all column indexes (no race condition - sequential after all tasks complete)
    for (contract_name, updates) in index_updates {
        let output_dir = output_base.join(&contract_name).join("once");
        let mut index = read_decoded_column_index(&output_dir);
        for (file_name, columns) in updates {
            index.insert(file_name, columns);
        }
        write_decoded_column_index(&output_dir, &index)?;
        tracing::info!(
            "Updated decoded column index for {}: {} files tracked",
            contract_name,
            index.len()
        );
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
        let address_idx = schema.index_of("contract_address").ok();

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
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
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
async fn process_once_calls(
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
        // For "once" calls, we send each function as a separate message
        for config in configs {
            let transform_calls: Vec<TransformDecodedCall> = decoded_records
                .iter()
                .filter_map(|r| {
                    r.decoded_values.get(&config.function_name).map(|decoded| {
                        convert_once_to_transform_call(
                            r,
                            contract_name,
                            &config.function_name,
                            decoded,
                            &config.output_type,
                        )
                    })
                })
                .collect();

            if !transform_calls.is_empty() {
                let msg = DecodedCallsMessage {
                    range_start,
                    range_end,
                    source_name: contract_name.to_string(),
                    function_name: config.function_name.clone(),
                    calls: transform_calls,
                };
                if let Err(e) = tx.send(msg).await {
                    tracing::warn!("Failed to send decoded calls to transformation channel: {}", e);
                }
            }
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
        .index_of("contract_address")
        .map_err(|e| EthCallDecodingError::Decode(format!("Missing contract_address column: {}", e)))?;
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

    let new_schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Copy existing columns
    for i in 0..existing_batch.num_columns() {
        arrays.push(existing_batch.column(i).clone());
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
        Field::new("contract_address", DataType::FixedSizeBinary(20), false),
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
        EvmType::Named { name, inner } => {
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

// ============================================================================
// Column Index Functions for Decoded Data
// ============================================================================

/// Read the column index sidecar file from a decoded `once/` directory.
/// Returns a map of filename -> list of decoded function names.
fn read_decoded_column_index(decoded_once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = decoded_once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> =
                serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read decoded column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No decoded column index at {} ({}), starting fresh",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Write the column index sidecar file to a decoded `once/` directory.
fn write_decoded_column_index(
    decoded_once_dir: &Path,
    index: &HashMap<String, Vec<String>>,
) -> Result<(), EthCallDecodingError> {
    let index_path = decoded_once_dir.join("column_index.json");
    tracing::debug!(
        "Writing decoded column index to {}: {} files tracked",
        index_path.display(),
        index.len()
    );
    let content = serde_json::to_string_pretty(index).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("JSON serialize error: {}", e),
        )
    })?;
    std::fs::write(&index_path, content)?;
    Ok(())
}

/// Load or build the column index for a decoded once/ directory.
/// If the index file exists, load it. Otherwise, scan all parquet files to build it.
fn load_or_build_decoded_column_index(decoded_once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = decoded_once_dir.join("column_index.json");

    // Try to load existing index
    if index_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&index_path) {
            if let Ok(index) = serde_json::from_str::<HashMap<String, Vec<String>>>(&content) {
                tracing::debug!(
                    "Loaded decoded column index from {}: {} files tracked",
                    index_path.display(),
                    index.len()
                );
                return index;
            }
        }
    }

    // Index doesn't exist or couldn't be loaded - build from parquet files
    if !decoded_once_dir.exists() {
        return HashMap::new();
    }

    let mut index = HashMap::new();
    let parquet_files: Vec<_> = match std::fs::read_dir(decoded_once_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .collect(),
        Err(_) => return HashMap::new(),
    };

    if parquet_files.is_empty() {
        return HashMap::new();
    }

    tracing::info!(
        "Building decoded column index for {} from {} parquet files",
        decoded_once_dir.display(),
        parquet_files.len()
    );

    for entry in parquet_files {
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let cols: Vec<String> = read_decoded_parquet_function_names(&path)
            .into_iter()
            .collect();
        if !cols.is_empty() {
            index.insert(file_name, cols);
        }
    }

    // Write the newly built index
    if !index.is_empty() {
        if let Err(e) = write_decoded_column_index(decoded_once_dir, &index) {
            tracing::warn!(
                "Failed to write decoded column index to {}: {}",
                decoded_once_dir.display(),
                e
            );
        } else {
            tracing::info!(
                "Built and saved decoded column index for {}: {} files tracked",
                decoded_once_dir.display(),
                index.len()
            );
        }
    }

    index
}

/// Read function names from a decoded parquet file's schema.
/// Decoded columns are named like:
/// - `name` (simple type) -> returns `name`
/// - `getAssetData.numeraire` (tuple field) -> returns `getAssetData`
/// Excludes standard columns like block_number, block_timestamp, address.
fn read_decoded_parquet_function_names(path: &Path) -> HashSet<String> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    // Standard columns to skip
    let skip_columns: HashSet<&str> =
        ["block_number", "block_timestamp", "address", "contract_address"]
            .into_iter()
            .collect();

    for field in schema.columns() {
        let name = field.name();

        // Skip standard columns
        if skip_columns.contains(name) {
            continue;
        }

        // Extract base function name (part before '.' for tuple fields)
        let fn_name = name.split('.').next().unwrap_or(name);
        fn_names.insert(fn_name.to_string());
    }

    fn_names
}

/// Read the raw column index from a raw `once/` directory.
/// Returns a map of filename -> list of function names whose `{name}_result` columns exist.
fn read_raw_column_index(raw_once_dir: &Path) -> HashMap<String, Vec<String>> {
    let index_path = raw_once_dir.join("column_index.json");
    match std::fs::read_to_string(&index_path) {
        Ok(content) => {
            let index: HashMap<String, Vec<String>> =
                serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read raw column index from {}: {} files tracked",
                index_path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No raw column index at {} ({}), will scan parquet schemas",
                index_path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Read function names from a raw parquet file's schema.
/// Raw columns are named like `{function_name}_result` -> returns `function_name`.
fn read_raw_parquet_function_names(path: &Path) -> HashSet<String> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return HashSet::new(),
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };

    let schema = reader.metadata().file_metadata().schema_descr();
    let mut fn_names = HashSet::new();

    for field in schema.columns() {
        let name = field.name();
        // Extract function name from column name (e.g., "getAssetData_result" -> "getAssetData")
        if let Some(fn_name) = name.strip_suffix("_result") {
            fn_names.insert(fn_name.to_string());
        }
    }

    fn_names
}
