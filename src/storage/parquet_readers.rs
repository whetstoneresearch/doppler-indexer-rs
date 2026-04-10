//! Shared parquet reader functions for reading logs, factory addresses, and eth_call results.
//!
//! These functions consolidate duplicated parquet-reading logic that was previously
//! scattered across `decoding/catchup/logs.rs`, `raw_data/historical/eth_calls/factory.rs`,
//! and `decoding/catchup/eth_calls.rs`.

use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

use alloy::primitives::Address;
use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, ListArray, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use thiserror::Error;

use crate::decoding::{EthCallResult, EventCallResult};
use crate::raw_data::historical::receipts::LogData;

/// Errors that can occur when reading parquet files.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ParquetReadError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Missing required columns in {path}: {details}")]
    MissingColumns { path: String, details: String },
}

/// Read raw log data from a parquet file.
///
/// Reads all rows from the parquet file and returns them as `LogData` structs.
/// This consolidates the two identical implementations that previously existed in
/// `decoding/catchup/logs.rs` and `raw_data/historical/eth_calls/factory.rs`.
pub fn read_raw_logs_from_parquet(path: &Path) -> Result<Vec<LogData>, ParquetReadError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut logs = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let tx_hash_idx = schema.index_of("transaction_hash").ok();
        let log_index_idx = schema.index_of("log_index").ok();
        let address_idx = schema.index_of("address").ok();
        let topics_idx = schema.index_of("topics").ok();
        let data_idx = schema.index_of("data").ok();

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

            let transaction_hash = tx_hash_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .and_then(|a| {
                            let bytes = a.value(row);
                            if bytes.len() == 32 {
                                let mut arr = [0u8; 32];
                                arr.copy_from_slice(bytes);
                                Some(alloy::primitives::B256::from(arr))
                            } else {
                                None
                            }
                        })
                })
                .unwrap_or_default();

            let log_index = log_index_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let address = address_idx
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

            let topics = topics_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .and_then(|list| {
                            let values = list.value(row);
                            values.as_any().downcast_ref::<FixedSizeBinaryArray>().map(
                                |topics_arr| {
                                    (0..topics_arr.len())
                                        .filter_map(|j| {
                                            let bytes = topics_arr.value(j);
                                            if bytes.len() == 32 {
                                                let mut arr = [0u8; 32];
                                                arr.copy_from_slice(bytes);
                                                Some(arr)
                                            } else {
                                                None
                                            }
                                        })
                                        .collect::<Vec<[u8; 32]>>()
                                },
                            )
                        })
                })
                .unwrap_or_default();

            let data = data_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|a| a.value(row).to_vec())
                })
                .unwrap_or_default();

            logs.push(LogData {
                block_number,
                block_timestamp,
                transaction_hash,
                log_index,
                address,
                topics,
                data,
            });
        }
    }

    Ok(logs)
}

/// Read projected raw log batches for event-triggered eth_call audit.
///
/// Only reads the columns needed to identify and reconstruct triggering events:
/// `block_number`, `block_timestamp`, `log_index`, `address`, `topics`, and `data`.
pub fn read_event_trigger_log_batches_from_parquet(
    path: &Path,
) -> Result<Vec<RecordBatch>, ParquetReadError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();

    let needed = [
        "block_number",
        "block_timestamp",
        "log_index",
        "address",
        "topics",
        "data",
    ];
    let root_indices: Vec<usize> = needed
        .iter()
        .filter_map(|name| arrow_schema.index_of(name).ok())
        .collect();

    let mask = ProjectionMask::roots(builder.parquet_schema(), root_indices);
    let reader = builder.with_projection(mask).build()?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }

    Ok(batches)
}

/// Read factory addresses from a parquet file, returning a set of raw 20-byte addresses.
///
/// Reads the `factory_address` column from a factory parquet file.
pub fn read_factory_addresses_from_parquet(
    path: &Path,
) -> Result<HashSet<[u8; 20]>, ParquetReadError> {
    let file = File::open(path)?;
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(builder) => match builder.build() {
            Ok(r) => r,
            Err(e) => {
                return Err(ParquetReadError::Parquet(e));
            }
        },
        Err(e) => {
            return Err(ParquetReadError::Parquet(e));
        }
    };

    let mut addresses = HashSet::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                continue;
            }
        };

        if let Ok(col_idx) = batch.schema().index_of("factory_address") {
            let col = batch.column(col_idx);
            if let Some(addr_array) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                for i in 0..addr_array.len() {
                    if !addr_array.is_null(i) {
                        let bytes = addr_array.value(i);
                        if bytes.len() == 20 {
                            let mut addr = [0u8; 20];
                            addr.copy_from_slice(bytes);
                            addresses.insert(addr);
                        }
                    }
                }
            }
        }
    }

    Ok(addresses)
}

/// Read factory addresses with block number and timestamp from a parquet file.
///
/// Reads the `factory_address`, `block_number`, and `block_timestamp` columns.
/// Parquet read errors are logged as warnings and result in an empty vec (not a hard error),
/// matching the original behavior.
pub fn read_factory_addresses_with_blocks(
    path: &Path,
) -> Result<Vec<(Address, u64, u64)>, ParquetReadError> {
    let file = File::open(path)?;
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(builder) => match builder.build() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "Failed to build parquet reader for {}: {}",
                    path.display(),
                    e
                );
                return Ok(Vec::new());
            }
        },
        Err(e) => {
            tracing::warn!(
                "Failed to create parquet reader for {}: {}",
                path.display(),
                e
            );
            return Ok(Vec::new());
        }
    };

    let mut addresses = Vec::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                continue;
            }
        };

        let addr_col_idx = batch.schema().index_of("factory_address").ok();
        let block_col_idx = batch.schema().index_of("block_number").ok();
        let ts_col_idx = batch.schema().index_of("block_timestamp").ok();

        if addr_col_idx.is_none() || block_col_idx.is_none() {
            continue;
        }

        let addr_col = batch.column(addr_col_idx.unwrap());
        let block_col = batch.column(block_col_idx.unwrap());
        let ts_col = ts_col_idx.map(|i| batch.column(i));

        let addr_array = match addr_col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            Some(a) => a,
            None => continue,
        };
        let block_array = match block_col.as_any().downcast_ref::<UInt64Array>() {
            Some(a) => a,
            None => continue,
        };
        let ts_array = ts_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        for i in 0..addr_array.len() {
            if addr_array.is_null(i) || block_array.is_null(i) {
                continue;
            }
            let bytes = addr_array.value(i);
            if bytes.len() != 20 {
                continue;
            }
            let mut addr_bytes = [0u8; 20];
            addr_bytes.copy_from_slice(bytes);
            let block_num = block_array.value(i);
            let timestamp = ts_array.map(|a| a.value(i)).unwrap_or(0);
            addresses.push((Address::from(addr_bytes), block_num, timestamp));
        }
    }

    Ok(addresses)
}

/// Read regular eth_call results from a parquet file.
///
/// Reads `block_number`, `block_timestamp`, `contract_address`, and `value` columns.
pub fn read_regular_calls_from_parquet(
    path: &Path,
) -> Result<Vec<EthCallResult>, ParquetReadError> {
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

/// Read event-triggered eth_call results from a parquet file.
///
/// Reads `block_number`, `block_timestamp`, `log_index`, `target_address`, and `value` columns.
pub fn read_event_calls_from_parquet(
    path: &Path,
) -> Result<Vec<EventCallResult>, ParquetReadError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut results = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let schema = batch.schema();

        let block_number_idx = schema.index_of("block_number").ok();
        let block_timestamp_idx = schema.index_of("block_timestamp").ok();
        let log_index_idx = schema.index_of("log_index").ok();
        let address_idx = schema.index_of("target_address").ok();
        let value_idx = schema.index_of("value").ok();
        let is_reverted_idx = schema.index_of("is_reverted").ok();
        let revert_reason_idx = schema.index_of("revert_reason").ok();

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

            let log_index = log_index_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(0);

            let target_address = address_idx
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

            let is_reverted = is_reverted_idx
                .and_then(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .map(|a| a.value(row))
                })
                .unwrap_or(false);

            let revert_reason = revert_reason_idx.and_then(|i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| {
                        if a.is_null(row) {
                            None
                        } else {
                            Some(a.value(row).to_string())
                        }
                    })
            });

            results.push(EventCallResult {
                block_number,
                block_timestamp,
                log_index,
                target_address,
                value,
                is_reverted,
                revert_reason,
            });
        }
    }

    Ok(results)
}
