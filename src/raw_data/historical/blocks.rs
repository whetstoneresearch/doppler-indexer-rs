use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "bench")]
use std::time::Instant;

use alloy::primitives::B256;
use alloy::rpc::types::{Block, BlockNumberOrTag};
use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, ListBuilder, StringBuilder, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{self, Sender};

use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::{BlockField, RawDataCollectionConfig};

#[derive(Debug, Error)]
pub enum BlockCollectionError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Block not found: {0}")]
    BlockNotFound(u64),

    #[error("Channel send error: {0}")]
    ChannelSend(String),

    #[error("Task join error: {0}")]
    JoinError(String),
}

#[derive(Debug, Clone)]
struct BlockRange {
    start: u64,
    end: u64,
}

impl BlockRange {
    fn file_name(&self) -> String {
        format!("blocks_{}-{}.parquet", self.start, self.end - 1)
    }
}

#[derive(Debug)]
struct FullBlockRecord {
    number: u64,
    hash: [u8; 32],
    parent_hash: [u8; 32],
    nonce: [u8; 8],
    ommers_hash: [u8; 32],
    logs_bloom: Vec<u8>,
    transactions_root: [u8; 32],
    state_root: [u8; 32],
    receipts_root: [u8; 32],
    miner: [u8; 20],
    difficulty: String,
    total_difficulty: Option<String>,
    extra_data: Vec<u8>,
    gas_limit: u64,
    gas_used: u64,
    timestamp: u64,
    mix_hash: [u8; 32],
    base_fee_per_gas: Option<u64>,
    withdrawals_root: Option<[u8; 32]>,
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
    parent_beacon_block_root: Option<[u8; 32]>,
    transaction_count: u32,
    transaction_hashes: Vec<String>,
    uncle_count: u32,
    size: Option<u64>,
}

#[derive(Debug)]
struct MinimalBlockRecord {
    number: u64,
    timestamp: u64,
    transaction_count: u32,
    transaction_hashes: Vec<String>,
    uncle_count: u32,
}

pub async fn collect_blocks(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    tx_sender: Option<Sender<(u64, u64, Vec<B256>)>>,
    eth_call_sender: Option<Sender<(u64, u64)>>,
) -> Result<(), BlockCollectionError> {
    let output_dir = PathBuf::from(format!("data/raw/{}/blocks", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let start_block = chain.start_block.map(|u| u.to::<u64>()).unwrap_or(0);

    let chain_head = client.get_block_number().await?;

    tracing::info!(
        "Chain {} head at block {}, starting collection from block {}",
        chain.name,
        chain_head,
        start_block
    );

    let ranges = compute_ranges_to_fetch(start_block, chain_head, range_size, &output_dir);

    if ranges.is_empty() {
        tracing::info!(
            "All block ranges already collected for chain {}",
            chain.name
        );
        return Ok(());
    }

    tracing::info!(
        "Collecting {} block ranges for chain {} (blocks {}-{})",
        ranges.len(),
        chain.name,
        ranges.first().map(|r| r.start).unwrap_or(0),
        ranges.last().map(|r| r.end - 1).unwrap_or(0)
    );

    let block_fields = &raw_data_config.fields.block_fields;
    let schema = build_block_schema(block_fields);

    for range in ranges {
        tracing::info!("Fetching blocks {}-{}", range.start, range.end - 1);

        let record_count = collect_blocks_streaming(
            client,
            &range,
            block_fields,
            &schema,
            &output_dir,
            &tx_sender,
            &eth_call_sender,
        )
        .await?;

        tracing::info!(
            "Wrote {} blocks to {}",
            record_count,
            output_dir.join(range.file_name()).display()
        );
    }

    Ok(())
}

/// Streaming block collection: fetches blocks concurrently and forwards to downstream
/// collectors immediately as each block arrives. Buffers records for ordered parquet writing.
async fn collect_blocks_streaming(
    client: &UnifiedRpcClient,
    range: &BlockRange,
    block_fields: &Option<Vec<BlockField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    tx_sender: &Option<Sender<(u64, u64, Vec<B256>)>>,
    eth_call_sender: &Option<Sender<(u64, u64)>>,
) -> Result<usize, BlockCollectionError> {
    let block_numbers: Vec<BlockNumberOrTag> = (range.start..range.end)
        .map(BlockNumberOrTag::Number)
        .collect();
    let expected_count = block_numbers.len();

    // Channel for streaming block results
    let (result_tx, mut result_rx) = mpsc::channel(256);

    // Start streaming fetch
    let fetch_handle = client.get_blocks_streaming(block_numbers, false, result_tx);

    // Process blocks as they arrive, using BTreeMap for ordered parquet output
    let mut received_count = 0usize;
    let mut errors: Vec<(u64, RpcError)> = Vec::new();

    match block_fields {
        Some(fields) => {
            let mut records_map: BTreeMap<u64, MinimalBlockRecord> = BTreeMap::new();

            while let Some((block_num_tag, result)) = result_rx.recv().await {
                let block_number = match block_num_tag {
                    BlockNumberOrTag::Number(n) => n,
                    _ => continue,
                };

                received_count += 1;

                match result {
                    Ok(Some(block)) => {
                        let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
                        let timestamp = block.header.timestamp;

                        // Immediately forward to downstream collectors
                        if let Some(sender) = tx_sender {
                            if sender.send((block_number, timestamp, tx_hashes.clone())).await.is_err() {
                                tracing::warn!("Receipts channel closed, continuing block collection");
                            }
                        }

                        if let Some(sender) = eth_call_sender {
                            if sender.send((block_number, timestamp)).await.is_err() {
                                tracing::warn!("Eth_call channel closed, continuing block collection");
                            }
                        }

                        // Buffer record for parquet (ordered by block number)
                        records_map.insert(block_number, MinimalBlockRecord {
                            number: block_number,
                            timestamp,
                            transaction_count: tx_hashes.len() as u32,
                            transaction_hashes: tx_hashes.iter().map(|h| format!("{:?}", h)).collect(),
                            uncle_count: block.uncles.len() as u32,
                        });
                    }
                    Ok(None) => {
                        return Err(BlockCollectionError::BlockNotFound(block_number));
                    }
                    Err(e) => {
                        errors.push((block_number, e));
                    }
                }
            }

            // Wait for fetch task to complete
            fetch_handle.await.map_err(|e| BlockCollectionError::JoinError(e.to_string()))?;

            // Check for errors
            if !errors.is_empty() {
                let (block_num, err) = errors.remove(0);
                tracing::error!("Block {} fetch failed: {}", block_num, err);
                return Err(BlockCollectionError::Rpc(err));
            }

            // Convert to ordered vec for parquet
            let all_records: Vec<MinimalBlockRecord> = records_map.into_values().collect();
            let count = all_records.len();

            if count != expected_count {
                tracing::warn!(
                    "Expected {} blocks but received {}, some may be missing",
                    expected_count,
                    count
                );
            }

            // Write to parquet
            let schema_clone = schema.clone();
            let fields_vec = fields.to_vec();
            let output_path = output_dir.join(range.file_name());
            tokio::task::spawn_blocking(move || {
                write_minimal_blocks_to_parquet(&all_records, &schema_clone, &fields_vec, &output_path)
            })
            .await
            .map_err(|e| BlockCollectionError::JoinError(e.to_string()))??;

            Ok(count)
        }
        None => {
            let mut records_map: BTreeMap<u64, FullBlockRecord> = BTreeMap::new();

            while let Some((block_num_tag, result)) = result_rx.recv().await {
                let block_number = match block_num_tag {
                    BlockNumberOrTag::Number(n) => n,
                    _ => continue,
                };

                received_count += 1;

                match result {
                    Ok(Some(block)) => {
                        let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
                        let timestamp = block.header.timestamp;

                        // Immediately forward to downstream collectors
                        if let Some(sender) = tx_sender {
                            if sender.send((block_number, timestamp, tx_hashes.clone())).await.is_err() {
                                tracing::warn!("Receipts channel closed, continuing block collection");
                            }
                        }

                        if let Some(sender) = eth_call_sender {
                            if sender.send((block_number, timestamp)).await.is_err() {
                                tracing::warn!("Eth_call channel closed, continuing block collection");
                            }
                        }

                        // Buffer full record for parquet
                        records_map.insert(block_number, process_single_block_full(&block, block_number)?);
                    }
                    Ok(None) => {
                        return Err(BlockCollectionError::BlockNotFound(block_number));
                    }
                    Err(e) => {
                        errors.push((block_number, e));
                    }
                }
            }

            // Wait for fetch task to complete
            fetch_handle.await.map_err(|e| BlockCollectionError::JoinError(e.to_string()))?;

            // Check for errors
            if !errors.is_empty() {
                let (block_num, err) = errors.remove(0);
                tracing::error!("Block {} fetch failed: {}", block_num, err);
                return Err(BlockCollectionError::Rpc(err));
            }

            // Convert to ordered vec for parquet
            let all_records: Vec<FullBlockRecord> = records_map.into_values().collect();
            let count = all_records.len();

            // Write to parquet
            let schema_clone = schema.clone();
            let output_path = output_dir.join(range.file_name());
            tokio::task::spawn_blocking(move || {
                write_full_blocks_to_parquet(&all_records, &schema_clone, &output_path)
            })
            .await
            .map_err(|e| BlockCollectionError::JoinError(e.to_string()))??;

            Ok(count)
        }
    }
}

/// Process a single block into a FullBlockRecord
fn process_single_block_full(block: &Block, block_number: u64) -> Result<FullBlockRecord, BlockCollectionError> {
    let header = &block.header;
    let inner = &header.inner;
    let tx_hashes: Vec<String> = block
        .transactions
        .hashes()
        .map(|h| format!("{h:?}"))
        .collect();

    Ok(FullBlockRecord {
        number: block_number,
        hash: header.hash.0,
        parent_hash: inner.parent_hash.0,
        nonce: inner.nonce.0,
        ommers_hash: inner.ommers_hash.0,
        logs_bloom: inner.logs_bloom.0.to_vec(),
        transactions_root: inner.transactions_root.0,
        state_root: inner.state_root.0,
        receipts_root: inner.receipts_root.0,
        miner: inner.beneficiary.0 .0,
        difficulty: inner.difficulty.to_string(),
        total_difficulty: header.total_difficulty.map(|d| d.to_string()),
        extra_data: inner.extra_data.to_vec(),
        gas_limit: inner.gas_limit,
        gas_used: inner.gas_used,
        timestamp: inner.timestamp,
        mix_hash: inner.mix_hash.0,
        base_fee_per_gas: inner.base_fee_per_gas,
        withdrawals_root: inner.withdrawals_root.map(|h| h.0),
        blob_gas_used: inner.blob_gas_used,
        excess_blob_gas: inner.excess_blob_gas,
        parent_beacon_block_root: inner.parent_beacon_block_root.map(|h| h.0),
        transaction_count: tx_hashes.len() as u32,
        transaction_hashes: tx_hashes,
        uncle_count: block.uncles.len() as u32,
        size: header.size.map(|s| s.to::<u64>()),
    })
}

fn process_blocks_minimal(
    blocks: &[Option<Block>],
    start_block: u64,
    _fields: Vec<BlockField>,
) -> Result<(Vec<MinimalBlockRecord>, Vec<(u64, u64, Vec<B256>)>), BlockCollectionError> {
    let mut records = Vec::with_capacity(blocks.len());
    let mut tx_data = Vec::with_capacity(blocks.len());

    for (i, block_opt) in blocks.iter().enumerate() {
        let block_number = start_block + i as u64;
        let block = block_opt
            .as_ref()
            .ok_or(BlockCollectionError::BlockNotFound(block_number))?;

        let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
        let timestamp = block.header.timestamp;

        tx_data.push((block_number, timestamp, tx_hashes.clone()));

        records.push(MinimalBlockRecord {
            number: block_number,
            timestamp,
            transaction_count: tx_hashes.len() as u32,
            transaction_hashes: tx_hashes.iter().map(|h| format!("{h:?}")).collect(),
            uncle_count: block.uncles.len() as u32,
        });
    }

    Ok((records, tx_data))
}

fn process_blocks_full(
    blocks: &[Option<Block>],
    start_block: u64,
) -> Result<(Vec<FullBlockRecord>, Vec<(u64, u64, Vec<B256>)>), BlockCollectionError> {
    let mut records = Vec::with_capacity(blocks.len());
    let mut tx_data = Vec::with_capacity(blocks.len());

    for (i, block_opt) in blocks.iter().enumerate() {
        let block_number = start_block + i as u64;
        let block = block_opt
            .as_ref()
            .ok_or(BlockCollectionError::BlockNotFound(block_number))?;

        let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
        let timestamp = block.header.timestamp;

        tx_data.push((block_number, timestamp, tx_hashes.clone()));

        let header = &block.header;
        let inner = &header.inner;
        records.push(FullBlockRecord {
            number: block_number,
            hash: header.hash.0,
            parent_hash: inner.parent_hash.0,
            nonce: inner.nonce.0,
            ommers_hash: inner.ommers_hash.0,
            logs_bloom: inner.logs_bloom.0.to_vec(),
            transactions_root: inner.transactions_root.0,
            state_root: inner.state_root.0,
            receipts_root: inner.receipts_root.0,
            miner: inner.beneficiary.0 .0,
            difficulty: inner.difficulty.to_string(),
            total_difficulty: header.total_difficulty.map(|d| d.to_string()),
            extra_data: inner.extra_data.to_vec(),
            gas_limit: inner.gas_limit,
            gas_used: inner.gas_used,
            timestamp,
            mix_hash: inner.mix_hash.0,
            base_fee_per_gas: inner.base_fee_per_gas,
            withdrawals_root: inner.withdrawals_root.map(|h| h.0),
            blob_gas_used: inner.blob_gas_used,
            excess_blob_gas: inner.excess_blob_gas,
            parent_beacon_block_root: inner.parent_beacon_block_root.map(|h| h.0),
            transaction_count: tx_hashes.len() as u32,
            transaction_hashes: tx_hashes.iter().map(|h| format!("{h:?}")).collect(),
            uncle_count: block.uncles.len() as u32,
            size: header.size.map(|s| s.to::<u64>()),
        });
    }

    Ok((records, tx_data))
}

fn compute_ranges_to_fetch(
    start_block: u64,
    chain_head: u64,
    range_size: u64,
    output_dir: &Path,
) -> Vec<BlockRange> {
    let aligned_start = (start_block / range_size) * range_size;

    let mut all_ranges = Vec::new();
    let mut current = aligned_start;
    while current <= chain_head {
        let range_end = std::cmp::min(current + range_size, chain_head + 1);
        all_ranges.push(BlockRange {
            start: current,
            end: range_end,
        });
        current += range_size;
    }

    let existing_files = scan_existing_parquet_files(output_dir);

    all_ranges
        .into_iter()
        .filter(|range| !existing_files.contains(&range.file_name()))
        .collect()
}

fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("blocks_") && name.ends_with(".parquet") {
                    files.insert(name.to_string());
                }
            }
        }
    }
    files
}

fn build_block_schema(fields: &Option<Vec<BlockField>>) -> Arc<Schema> {
    match fields {
        Some(block_fields) => {
            let mut arrow_fields = Vec::new();

            for field in block_fields {
                match field {
                    BlockField::Number => {
                        arrow_fields.push(Field::new("number", DataType::UInt64, false));
                    }
                    BlockField::Timestamp => {
                        arrow_fields.push(Field::new("timestamp", DataType::UInt64, false));
                    }
                    BlockField::Transactions => {
                        arrow_fields
                            .push(Field::new("transaction_count", DataType::UInt32, false));
                        arrow_fields.push(Field::new(
                            "transaction_hashes",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            false,
                        ));
                    }
                    BlockField::Uncles => {
                        arrow_fields.push(Field::new("uncle_count", DataType::UInt32, false));
                    }
                }
            }

            Arc::new(Schema::new(arrow_fields))
        }
        None => {
            Arc::new(Schema::new(vec![
                Field::new("number", DataType::UInt64, false),
                Field::new("hash", DataType::FixedSizeBinary(32), false),
                Field::new("parent_hash", DataType::FixedSizeBinary(32), false),
                Field::new("nonce", DataType::FixedSizeBinary(8), false),
                Field::new("ommers_hash", DataType::FixedSizeBinary(32), false),
                Field::new("logs_bloom", DataType::Binary, false),
                Field::new("transactions_root", DataType::FixedSizeBinary(32), false),
                Field::new("state_root", DataType::FixedSizeBinary(32), false),
                Field::new("receipts_root", DataType::FixedSizeBinary(32), false),
                Field::new("miner", DataType::FixedSizeBinary(20), false),
                Field::new("difficulty", DataType::Utf8, false),
                Field::new("total_difficulty", DataType::Utf8, true),
                Field::new("extra_data", DataType::Binary, false),
                Field::new("gas_limit", DataType::UInt64, false),
                Field::new("gas_used", DataType::UInt64, false),
                Field::new("timestamp", DataType::UInt64, false),
                Field::new("mix_hash", DataType::FixedSizeBinary(32), false),
                Field::new("base_fee_per_gas", DataType::UInt64, true),
                Field::new("withdrawals_root", DataType::FixedSizeBinary(32), true),
                Field::new("blob_gas_used", DataType::UInt64, true),
                Field::new("excess_blob_gas", DataType::UInt64, true),
                Field::new("parent_beacon_block_root", DataType::FixedSizeBinary(32), true),
                Field::new("transaction_count", DataType::UInt32, false),
                Field::new(
                    "transaction_hashes",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    false,
                ),
                Field::new("uncle_count", DataType::UInt32, false),
                Field::new("size", DataType::UInt64, true),
            ]))
        }
    }
}

fn write_minimal_blocks_to_parquet(
    records: &[MinimalBlockRecord],
    schema: &Arc<Schema>,
    fields: &[BlockField],
    output_path: &Path,
) -> Result<(), BlockCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for field in fields {
        match field {
            BlockField::Number => {
                let arr: UInt64Array = records.iter().map(|r| Some(r.number)).collect();
                arrays.push(Arc::new(arr));
            }
            BlockField::Timestamp => {
                let arr: UInt64Array = records.iter().map(|r| Some(r.timestamp)).collect();
                arrays.push(Arc::new(arr));
            }
            BlockField::Transactions => {
                let count_arr: UInt32Array =
                    records.iter().map(|r| Some(r.transaction_count)).collect();
                arrays.push(Arc::new(count_arr));

                let mut list_builder = ListBuilder::new(StringBuilder::new());
                for record in records {
                    for hash in &record.transaction_hashes {
                        list_builder.values().append_value(hash);
                    }
                    list_builder.append(true);
                }
                arrays.push(Arc::new(list_builder.finish()));
            }
            BlockField::Uncles => {
                let arr: UInt32Array = records.iter().map(|r| Some(r.uncle_count)).collect();
                arrays.push(Arc::new(arr));
            }
        }
    }

    write_parquet(arrays, schema, output_path)
}

fn write_full_blocks_to_parquet(
    records: &[FullBlockRecord],
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), BlockCollectionError> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    let arr: UInt64Array = records.iter().map(|r| Some(r.number)).collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.hash.as_slice()))?;
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.parent_hash.as_slice()))?;
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.nonce.as_slice()))?;
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.ommers_hash.as_slice()))?;
    arrays.push(Arc::new(arr));

    let arr: BinaryArray = records
        .iter()
        .map(|r| Some(r.logs_bloom.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.transactions_root.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.state_root.as_slice()))?;
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.receipts_root.as_slice()),
    )?;
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.miner.as_slice()))?;
    arrays.push(Arc::new(arr));

    let mut builder = StringBuilder::new();
    for record in records {
        builder.append_value(&record.difficulty);
    }
    arrays.push(Arc::new(builder.finish()));

    let mut builder = StringBuilder::new();
    for record in records {
        match &record.total_difficulty {
            Some(td) => builder.append_value(td),
            None => builder.append_null(),
        }
    }
    arrays.push(Arc::new(builder.finish()));

    let arr: BinaryArray = records
        .iter()
        .map(|r| Some(r.extra_data.as_slice()))
        .collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.gas_limit)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.gas_used)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| Some(r.timestamp)).collect();
    arrays.push(Arc::new(arr));

    let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.mix_hash.as_slice()))?;
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| r.base_fee_per_gas).collect();
    arrays.push(Arc::new(arr));

    let values: Vec<Option<&[u8]>> = records
        .iter()
        .map(|r| r.withdrawals_root.as_ref().map(|h| h.as_slice()))
        .collect();
    let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 32)?;
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| r.blob_gas_used).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| r.excess_blob_gas).collect();
    arrays.push(Arc::new(arr));

    let values: Vec<Option<&[u8]>> = records
        .iter()
        .map(|r| r.parent_beacon_block_root.as_ref().map(|h| h.as_slice()))
        .collect();
    let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 32)?;
    arrays.push(Arc::new(arr));

    let arr: UInt32Array = records.iter().map(|r| Some(r.transaction_count)).collect();
    arrays.push(Arc::new(arr));

    let mut list_builder = ListBuilder::new(StringBuilder::new());
    for record in records {
        for hash in &record.transaction_hashes {
            list_builder.values().append_value(hash);
        }
        list_builder.append(true);
    }
    arrays.push(Arc::new(list_builder.finish()));

    let arr: UInt32Array = records.iter().map(|r| Some(r.uncle_count)).collect();
    arrays.push(Arc::new(arr));

    let arr: UInt64Array = records.iter().map(|r| r.size).collect();
    arrays.push(Arc::new(arr));

    write_parquet(arrays, schema, output_path)
}

fn write_parquet(
    arrays: Vec<ArrayRef>,
    schema: &Arc<Schema>,
    output_path: &Path,
) -> Result<(), BlockCollectionError> {
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

#[derive(Debug, Clone)]
pub struct BlockInfoForDownstream {
    pub block_number: u64,
    pub timestamp: u64,
    pub tx_hashes: Vec<B256>,
}

#[derive(Debug, Clone)]
pub struct ExistingBlockRange {
    pub start: u64,
    pub end: u64,
    pub file_path: PathBuf,
}

pub fn get_existing_block_ranges(chain_name: &str) -> Vec<ExistingBlockRange> {
    let blocks_dir = PathBuf::from(format!("data/raw/{}/blocks", chain_name));
    let mut ranges = Vec::new();

    let entries = match std::fs::read_dir(&blocks_dir) {
        Ok(entries) => entries,
        Err(_) => return ranges,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.extension().map(|e| e == "parquet").unwrap_or(false) {
            continue;
        }

        let file_name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(name) => name,
            None => continue,
        };

        // Parse "blocks_START-END" format
        if !file_name.starts_with("blocks_") {
            continue;
        }

        let range_part = &file_name[7..]; // Skip "blocks_"
        let range_parts: Vec<&str> = range_part.split('-').collect();
        if range_parts.len() != 2 {
            continue;
        }

        let start: u64 = match range_parts[0].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let end: u64 = match range_parts[1].parse::<u64>() {
            Ok(v) => v + 1, // Convert inclusive end to exclusive
            Err(_) => continue,
        };

        ranges.push(ExistingBlockRange {
            start,
            end,
            file_path: path,
        });
    }

    ranges.sort_by_key(|r| r.start);
    ranges
}

pub fn read_block_info_from_parquet(
    file_path: &Path,
) -> Result<Vec<BlockInfoForDownstream>, BlockCollectionError> {
    use arrow::array::{Array, ListArray, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .build()
        .map_err(|e| BlockCollectionError::Parquet(e))?;

    let mut blocks = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(BlockCollectionError::Arrow)?;

        let block_numbers = batch
            .column_by_name("number")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let timestamps = batch
            .column_by_name("timestamp")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let tx_hashes_col = batch.column_by_name("transaction_hashes");

        if let (Some(numbers), Some(times)) = (block_numbers, timestamps) {
            for i in 0..batch.num_rows() {
                let block_number = numbers.value(i);
                let timestamp = times.value(i);

                // Extract transaction hashes from the list column
                let tx_hashes: Vec<B256> = if let Some(col) = tx_hashes_col {
                    if let Some(list_array) = col.as_any().downcast_ref::<ListArray>() {
                        let values = list_array.value(i);
                        if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
                            (0..string_array.len())
                                .filter_map(|j| {
                                    let hash_str = string_array.value(j);
                                    // Parse "0x..." format
                                    let hash_str = hash_str.strip_prefix("0x").unwrap_or(hash_str);
                                    let bytes = hex::decode(hash_str).ok()?;
                                    if bytes.len() == 32 {
                                        Some(B256::from_slice(&bytes))
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                blocks.push(BlockInfoForDownstream {
                    block_number,
                    timestamp,
                    tx_hashes,
                });
            }
        }
    }

    blocks.sort_by_key(|b| b.block_number);
    Ok(blocks)
}
