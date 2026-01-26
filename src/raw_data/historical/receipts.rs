use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "bench")]
use std::time::Instant;

use alloy::primitives::B256;
use alloy::rpc::types::Log;
use arrow::array::{ArrayRef, FixedSizeBinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::{RawDataCollectionConfig, ReceiptField};

#[derive(Debug, Error)]
pub enum ReceiptCollectionError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Receipt not found for tx: {0}")]
    ReceiptNotFound(B256),

    #[error("Channel send error")]
    ChannelSend,
}

#[derive(Debug, Clone)]
struct BlockRange {
    start: u64,
    end: u64,
}

impl BlockRange {
    fn file_name(&self) -> String {
        format!("receipts_{}-{}.parquet", self.start, self.end - 1)
    }
}

#[derive(Debug)]
struct FullReceiptRecord {
    block_number: u64,
    block_timestamp: u64,
    transaction_hash: [u8; 32],
    transaction_index: u32,
    from_address: [u8; 20],
    to_address: Option<[u8; 20]>,
    cumulative_gas_used: u64,
    gas_used: u64,
    contract_address: Option<[u8; 20]>,
    status: bool,
    log_count: u32,
}

#[derive(Debug)]
struct MinimalReceiptRecord {
    block_number: u64,
    block_timestamp: u64,
    transaction_hash: [u8; 32],
    from_address: [u8; 20],
    to_address: Option<[u8; 20]>,
}

/// Log data to be sent to the logs collector
#[derive(Debug, Clone)]
pub struct LogData {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: B256,
    pub log_index: u32,
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

/// Block info received from blocks collector
#[derive(Debug)]
struct BlockInfo {
    block_number: u64,
    timestamp: u64,
    tx_hashes: Vec<B256>,
}

pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    mut block_rx: Receiver<(u64, u64, Vec<B256>)>,
    log_tx: Option<Sender<Vec<LogData>>>,
) -> Result<(), ReceiptCollectionError> {
    let output_dir = PathBuf::from(format!("data/raw/{}/receipts", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;
    let receipt_fields = &raw_data_config.fields.receipt_fields;
    let schema = build_receipt_schema(receipt_fields);

    // Track which ranges we've already written
    let existing_files = scan_existing_parquet_files(&output_dir);

    // Accumulate block info by range
    let mut range_data: HashMap<u64, Vec<BlockInfo>> = HashMap::new();

    tracing::info!("Starting receipt collection for chain {}", chain.name);

    while let Some((block_number, timestamp, tx_hashes)) = block_rx.recv().await {
        let range_start = (block_number / range_size) * range_size;

        range_data
            .entry(range_start)
            .or_default()
            .push(BlockInfo {
                block_number,
                timestamp,
                tx_hashes,
            });

        // Check if we have a complete range (all blocks from range_start to range_start + range_size - 1)
        if let Some(blocks) = range_data.get(&range_start) {
            let expected_blocks: HashSet<u64> =
                (range_start..range_start + range_size).collect();
            let received_blocks: HashSet<u64> =
                blocks.iter().map(|b| b.block_number).collect();

            if expected_blocks.is_subset(&received_blocks) {
                let range = BlockRange {
                    start: range_start,
                    end: range_start + range_size,
                };

                // Skip if already exists
                if existing_files.contains(&range.file_name()) {
                    tracing::info!(
                        "Skipping receipts for blocks {}-{} (already exists)",
                        range.start,
                        range.end - 1
                    );
                    range_data.remove(&range_start);
                    continue;
                }

                let blocks = range_data.remove(&range_start).unwrap();
                process_range(
                    &range,
                    blocks,
                    client,
                    receipt_fields,
                    &schema,
                    &output_dir,
                    &log_tx,
                    rpc_batch_size,
                )
                .await?;
            }
        }
    }

    // Process any remaining partial ranges
    for (range_start, blocks) in range_data {
        if blocks.is_empty() {
            continue;
        }

        let max_block = blocks.iter().map(|b| b.block_number).max().unwrap_or(range_start);
        let range = BlockRange {
            start: range_start,
            end: max_block + 1,
        };

        if existing_files.contains(&range.file_name()) {
            tracing::info!(
                "Skipping receipts for blocks {}-{} (already exists)",
                range.start,
                range.end - 1
            );
            continue;
        }

        process_range(
            &range,
            blocks,
            client,
            receipt_fields,
            &schema,
            &output_dir,
            &log_tx,
            rpc_batch_size,
        )
        .await?;
    }

    tracing::info!("Receipt collection complete for chain {}", chain.name);
    Ok(())
}

async fn process_range(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    client: &UnifiedRpcClient,
    receipt_fields: &Option<Vec<ReceiptField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    log_tx: &Option<Sender<Vec<LogData>>>,
    rpc_batch_size: usize,
) -> Result<(), ReceiptCollectionError> {
    tracing::info!("Fetching receipts for blocks {}-{}", range.start, range.end - 1);

    // Collect all tx hashes with their block info
    let mut tx_block_info: Vec<(B256, u64, u64)> = Vec::new();
    for block in &blocks {
        for tx_hash in &block.tx_hashes {
            tx_block_info.push((*tx_hash, block.block_number, block.timestamp));
        }
    }

    if tx_block_info.is_empty() {
        tracing::info!(
            "No transactions in blocks {}-{}, skipping",
            range.start,
            range.end - 1
        );
        return Ok(());
    }

    // Fetch receipts in smaller RPC batches, sending logs after each batch
    // This allows overlapping log collection with receipt fetching
    let mut all_minimal_records: Vec<MinimalReceiptRecord> = Vec::new();
    let mut all_full_records: Vec<FullReceiptRecord> = Vec::new();

    #[cfg(feature = "bench")]
    let mut rpc_time = std::time::Duration::ZERO;
    #[cfg(feature = "bench")]
    let mut process_time = std::time::Duration::ZERO;

    for chunk in tx_block_info.chunks(rpc_batch_size) {
        let tx_hashes: Vec<B256> = chunk.iter().map(|(h, _, _)| *h).collect();

        #[cfg(feature = "bench")]
        let rpc_start = Instant::now();
        let receipts = client.get_transaction_receipts_batch(tx_hashes).await?;
        #[cfg(feature = "bench")]
        {
            rpc_time += rpc_start.elapsed();
        }

        #[cfg(feature = "bench")]
        let process_start = Instant::now();
        let mut batch_logs: Vec<LogData> = Vec::new();

        match receipt_fields {
            Some(_) => {
                let records = process_receipts_minimal(&receipts, chunk, &mut batch_logs)?;
                all_minimal_records.extend(records);
            }
            None => {
                let records = process_receipts_full(&receipts, chunk, &mut batch_logs)?;
                all_full_records.extend(records);
            }
        }

        // Send logs to logs collector immediately after each RPC batch
        if let Some(sender) = log_tx {
            if !batch_logs.is_empty() {
                sender
                    .send(batch_logs)
                    .await
                    .map_err(|_| ReceiptCollectionError::ChannelSend)?;
            }
        }
        #[cfg(feature = "bench")]
        {
            process_time += process_start.elapsed();
        }
    }

    // Write all accumulated records to parquet
    #[cfg(feature = "bench")]
    let write_start = Instant::now();
    match receipt_fields {
        Some(fields) => {
            write_minimal_receipts_to_parquet(&all_minimal_records, schema, fields, &output_dir.join(range.file_name()))?;
        }
        None => {
            write_full_receipts_to_parquet(&all_full_records, schema, &output_dir.join(range.file_name()))?;
        }
    }

    let total_receipts = if receipt_fields.is_some() {
        all_minimal_records.len()
    } else {
        all_full_records.len()
    };

    #[cfg(feature = "bench")]
    {
        let write_time = write_start.elapsed();
        crate::bench::record("receipts", range.start, range.end, total_receipts, rpc_time, process_time, write_time);
    }

    tracing::info!(
        "Wrote {} receipts to {}",
        total_receipts,
        output_dir.join(range.file_name()).display()
    );

    Ok(())
}

fn process_receipts_minimal(
    receipts: &[Option<alloy::rpc::types::TransactionReceipt>],
    tx_block_info: &[(B256, u64, u64)],
    all_logs: &mut Vec<LogData>,
) -> Result<Vec<MinimalReceiptRecord>, ReceiptCollectionError> {
    let mut records = Vec::with_capacity(receipts.len());

    for (i, receipt_opt) in receipts.iter().enumerate() {
        let (tx_hash, block_number, timestamp) = tx_block_info[i];
        let receipt = receipt_opt
            .as_ref()
            .ok_or(ReceiptCollectionError::ReceiptNotFound(tx_hash))?;

        // Extract logs
        extract_logs(&receipt.inner.logs(), block_number, timestamp, tx_hash, all_logs);

        records.push(MinimalReceiptRecord {
            block_number,
            block_timestamp: timestamp,
            transaction_hash: tx_hash.0,
            from_address: receipt.from.0 .0,
            to_address: receipt.to.map(|a| a.0 .0),
        });
    }

    Ok(records)
}

fn process_receipts_full(
    receipts: &[Option<alloy::rpc::types::TransactionReceipt>],
    tx_block_info: &[(B256, u64, u64)],
    all_logs: &mut Vec<LogData>,
) -> Result<Vec<FullReceiptRecord>, ReceiptCollectionError> {
    let mut records = Vec::with_capacity(receipts.len());

    for (i, receipt_opt) in receipts.iter().enumerate() {
        let (tx_hash, block_number, timestamp) = tx_block_info[i];
        let receipt = receipt_opt
            .as_ref()
            .ok_or(ReceiptCollectionError::ReceiptNotFound(tx_hash))?;

        let inner = &receipt.inner;
        let logs = inner.logs();

        // Extract logs
        extract_logs(&logs, block_number, timestamp, tx_hash, all_logs);

        records.push(FullReceiptRecord {
            block_number,
            block_timestamp: timestamp,
            transaction_hash: tx_hash.0,
            transaction_index: receipt.transaction_index.unwrap_or(0) as u32,
            from_address: receipt.from.0 .0,
            to_address: receipt.to.map(|a| a.0 .0),
            cumulative_gas_used: inner.cumulative_gas_used(),
            gas_used: receipt.gas_used as u64,
            contract_address: receipt.contract_address.map(|a| a.0 .0),
            status: inner.status(),
            log_count: logs.len() as u32,
        });
    }

    Ok(records)
}

fn extract_logs(
    logs: &[Log],
    block_number: u64,
    block_timestamp: u64,
    tx_hash: B256,
    all_logs: &mut Vec<LogData>,
) {
    for (log_index, log) in logs.iter().enumerate() {
        let topics: Vec<[u8; 32]> = log.topics().iter().map(|t| t.0).collect();

        all_logs.push(LogData {
            block_number,
            block_timestamp,
            transaction_hash: tx_hash,
            log_index: log_index as u32,
            address: log.address().0 .0,
            topics,
            data: log.data().data.to_vec(),
        });
    }
}

fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("receipts_") && name.ends_with(".parquet") {
                    files.insert(name.to_string());
                }
            }
        }
    }
    files
}

fn build_receipt_schema(fields: &Option<Vec<ReceiptField>>) -> Arc<Schema> {
    match fields {
        Some(receipt_fields) => {
            let mut arrow_fields = Vec::new();

            for field in receipt_fields {
                match field {
                    ReceiptField::BlockNumber => {
                        arrow_fields.push(Field::new("block_number", DataType::UInt64, false));
                    }
                    ReceiptField::BlockTimestamp => {
                        arrow_fields.push(Field::new("block_timestamp", DataType::UInt64, false));
                    }
                    ReceiptField::TransactionHash => {
                        arrow_fields.push(Field::new(
                            "transaction_hash",
                            DataType::FixedSizeBinary(32),
                            false,
                        ));
                    }
                    ReceiptField::From => {
                        arrow_fields.push(Field::new(
                            "from_address",
                            DataType::FixedSizeBinary(20),
                            false,
                        ));
                    }
                    ReceiptField::To => {
                        arrow_fields.push(Field::new(
                            "to_address",
                            DataType::FixedSizeBinary(20),
                            true,
                        ));
                    }
                    ReceiptField::Logs => {
                        arrow_fields.push(Field::new("log_count", DataType::UInt32, false));
                    }
                }
            }

            Arc::new(Schema::new(arrow_fields))
        }
        None => Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("transaction_hash", DataType::FixedSizeBinary(32), false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("from_address", DataType::FixedSizeBinary(20), false),
            Field::new("to_address", DataType::FixedSizeBinary(20), true),
            Field::new("cumulative_gas_used", DataType::UInt64, false),
            Field::new("gas_used", DataType::UInt64, false),
            Field::new("contract_address", DataType::FixedSizeBinary(20), true),
            Field::new("status", DataType::Boolean, false),
            Field::new("log_count", DataType::UInt32, false),
        ])),
    }
}

fn write_minimal_receipts_to_parquet(
    records: &[MinimalReceiptRecord],
    schema: &Arc<Schema>,
    fields: &[ReceiptField],
    output_path: &Path,
) -> Result<(), ReceiptCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for field in fields {
        match field {
            ReceiptField::BlockNumber => {
                let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
                arrays.push(Arc::new(arr));
            }
            ReceiptField::BlockTimestamp => {
                let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
                arrays.push(Arc::new(arr));
            }
            ReceiptField::TransactionHash => {
                let arr = FixedSizeBinaryArray::try_from_iter(
                    records.iter().map(|r| r.transaction_hash.as_slice()),
                )?;
                arrays.push(Arc::new(arr));
            }
            ReceiptField::From => {
                let arr = FixedSizeBinaryArray::try_from_iter(
                    records.iter().map(|r| r.from_address.as_slice()),
                )?;
                arrays.push(Arc::new(arr));
            }
            ReceiptField::To => {
                let values: Vec<Option<&[u8]>> = records
                    .iter()
                    .map(|r| r.to_address.as_ref().map(|a| a.as_slice()))
                    .collect();
                let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 20)?;
                arrays.push(Arc::new(arr));
            }
            ReceiptField::Logs => {
                // For minimal records, we don't have log_count - use 0 as placeholder
                let arr: UInt32Array = records.iter().map(|_| Some(0u32)).collect();
                arrays.push(Arc::new(arr));
            }
        }
    }

    write_parquet(arrays, schema, output_path)
}

fn write_full_receipts_to_parquet(
    records: &[FullReceiptRecord],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), ReceiptCollectionError> {
    use arrow::array::BooleanArray;

    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.transaction_hash.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    let arr: UInt32Array = records.iter().map(|r| Some(r.transaction_index)).collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.from_address.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    let values: Vec<Option<&[u8]>> = records
        .iter()
        .map(|r| r.to_address.as_ref().map(|a| a.as_slice()))
        .collect();
    let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 20)?;
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.cumulative_gas_used)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.gas_used)).collect();
    arrays.push(Arc::new(arr));

    let values: Vec<Option<&[u8]>> = records
        .iter()
        .map(|r| r.contract_address.as_ref().map(|a| a.as_slice()))
        .collect();
    let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 20)?;
    arrays.push(Arc::new(arr));

    let arr: BooleanArray = records.iter().map(|r| Some(r.status)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt32Array = records.iter().map(|r| Some(r.log_count)).collect();
    arrays.push(Arc::new(arr));

    write_parquet(arrays, schema, output_path)
}

fn write_parquet(
    arrays: Vec<ArrayRef>,
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), ReceiptCollectionError> {
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
