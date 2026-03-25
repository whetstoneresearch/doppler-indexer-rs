//! Parquet readers for factory address data.

use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

use alloy::primitives::Address;
use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::ParquetReadError;

/// Read factory addresses from a parquet file, returning just the 20-byte addresses.
///
/// Gracefully handles parquet read errors by logging warnings and returning an empty
/// set rather than propagating (consistent with original behaviour in callers).
pub fn read_factory_addresses_from_parquet(
    path: &Path,
) -> Result<HashSet<[u8; 20]>, ParquetReadError> {
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
                return Ok(HashSet::new());
            }
        },
        Err(e) => {
            tracing::warn!(
                "Failed to create parquet reader for {}: {}",
                path.display(),
                e
            );
            return Ok(HashSet::new());
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
/// Returns `(Address, block_number, block_timestamp)` tuples. Gracefully handles
/// parquet read errors with warnings instead of propagation.
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
