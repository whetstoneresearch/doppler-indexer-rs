use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::primitives::B256;
use alloy::rpc::types::BlockNumberOrTag;
use arrow::array::{ArrayRef, ListBuilder, StringBuilder, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

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

    #[error("Missing environment variable: {0}")]
    EnvVar(String),

    #[error("Block not found: {0}")]
    BlockNotFound(u64),

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
        format!("blocks_{}-{}.parquet", self.start, self.end - 1)
    }
}

#[derive(Debug)]
struct BlockRecord {
    number: u64,
    timestamp: u64,
    transaction_count: u32,
    transaction_hashes: Vec<String>,
    uncle_count: u32,
    raw_json: Option<String>,
}

pub async fn collect_blocks(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    tx_sender: Option<Sender<(u64, u64, Vec<B256>)>>,
) -> Result<(), BlockCollectionError> {

    let output_dir = PathBuf::from(format!("data/raw/{}/blocks", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let start_block = chain
        .start_block
        .map(|u| u.to::<u64>())
        .unwrap_or(0);

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

    let schema = build_block_schema(&raw_data_config.fields.block_fields);

    for range in ranges {
        tracing::info!("Fetching blocks {}-{}", range.start, range.end - 1);

        let records = fetch_range(client, &range, &tx_sender).await?;

        let output_path = output_dir.join(range.file_name());
        write_blocks_to_parquet(
            &records,
            &schema,
            &raw_data_config.fields.block_fields,
            &output_path,
        )?;

        tracing::info!(
            "Wrote {} blocks to {}",
            records.len(),
            output_path.display()
        );
    }

    Ok(())
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
        .filter(|range| {
            let file_name = range.file_name();
            !existing_files.contains(&file_name)
        })
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
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
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
                Field::new("raw_json", DataType::Utf8, false),
            ]))
        }
    }
}

async fn fetch_range(
    client: &UnifiedRpcClient,
    range: &BlockRange,
    tx_sender: &Option<Sender<(u64, u64, Vec<B256>)>>,
) -> Result<Vec<BlockRecord>, BlockCollectionError> {
    let block_numbers: Vec<BlockNumberOrTag> = (range.start..range.end)
        .map(BlockNumberOrTag::Number)
        .collect();

    let blocks = client.get_blocks_batch(block_numbers, false).await?;

    let mut records = Vec::with_capacity(blocks.len());

    for (i, block_opt) in blocks.into_iter().enumerate() {
        let block_number = range.start + i as u64;

        let block = block_opt.ok_or(BlockCollectionError::BlockNotFound(block_number))?;

        let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
        let timestamp = block.header.timestamp;

        if let Some(sender) = tx_sender {
            sender
                .send((block_number, timestamp, tx_hashes.clone()))
                .await
                .map_err(|_| BlockCollectionError::ChannelSend)?;
        }

        records.push(BlockRecord {
            number: block_number,
            timestamp,
            transaction_count: tx_hashes.len() as u32,
            transaction_hashes: tx_hashes.iter().map(|h| format!("{:?}", h)).collect(),
            uncle_count: block.uncles.len() as u32,
            raw_json: None,
        });
    }

    Ok(records)
}

fn write_blocks_to_parquet(
    records: &[BlockRecord],
    schema: &Arc<Schema>,
    fields: &Option<Vec<BlockField>>,
    output_path: &Path,
) -> Result<(), BlockCollectionError> {
    let arrays = build_arrow_arrays(records, fields)?;

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

fn build_arrow_arrays(
    records: &[BlockRecord],
    fields: &Option<Vec<BlockField>>,
) -> Result<Vec<ArrayRef>, BlockCollectionError> {
    match fields {
        Some(block_fields) => {
            let mut arrays: Vec<ArrayRef> = Vec::new();

            for field in block_fields {
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
                        let arr: UInt32Array =
                            records.iter().map(|r| Some(r.uncle_count)).collect();
                        arrays.push(Arc::new(arr));
                    }
                }
            }

            Ok(arrays)
        }
        None => {
            let numbers: UInt64Array = records.iter().map(|r| Some(r.number)).collect();
            let mut json_builder = StringBuilder::new();
            for record in records {
                json_builder.append_value(record.raw_json.as_deref().unwrap_or("{}"));
            }

            Ok(vec![Arc::new(numbers), Arc::new(json_builder.finish())])
        }
    }
}
