use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::primitives::B256;
use alloy::rpc::types::BlockNumberOrTag;
use arrow::datatypes::Schema;
use tokio::sync::mpsc::{self, Sender};

use crate::raw_data::historical::blocks::{
    build_block_schema, process_single_block_full, write_full_blocks_to_parquet,
    write_minimal_blocks_to_parquet, BlockCollectionError, FullBlockRecord, MinimalBlockRecord,
};
use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::{BlockField, RawDataCollectionConfig};

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
