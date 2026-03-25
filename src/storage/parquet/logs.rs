//! Parquet readers for log data.

use std::fs::File;
use std::path::Path;

use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use super::ParquetReadError;
use crate::raw_data::historical::receipts::LogData;

/// Read logs from a raw parquet file.
///
/// Parses every row into a `LogData` struct, reading all standard log columns:
/// block_number, block_timestamp, transaction_hash, log_index, address, topics, data.
pub fn read_logs_from_parquet(path: &Path) -> Result<Vec<LogData>, ParquetReadError> {
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
                    use arrow::array::ListArray;
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

/// Read log batches from a parquet file with column projection.
///
/// Only reads block_number, block_timestamp, address, topics, data -- skips
/// transaction_hash and log_index for faster reads when those fields are not needed
/// (e.g., factory address extraction).
pub fn read_log_batches_projected(file_path: &Path) -> Result<Vec<RecordBatch>, ParquetReadError> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();

    let needed = [
        "block_number",
        "block_timestamp",
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
