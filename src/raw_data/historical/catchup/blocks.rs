use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use alloy::primitives::B256;
use alloy::rpc::types::BlockNumberOrTag;
use arrow::datatypes::Schema;
use metrics::{counter, histogram};
use tokio::sync::mpsc::{self, Sender};

use crate::raw_data::historical::blocks::{
    build_block_schema, process_single_block_full, write_full_blocks_to_parquet_async,
    write_minimal_blocks_to_parquet_async, BlockCollectionError, FullBlockRecord,
    MinimalBlockRecord,
};
use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::storage::paths::{parse_range_from_filename, raw_blocks_dir, scan_parquet_filenames};
use crate::storage::{upload_parquet_to_s3, BlockRange, S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::{BlockField, RawDataCollectionConfig};

/// Holds owned data for a deferred parquet write, allowing the next range's
/// RPC fetch to proceed while the previous range's I/O completes in the background.
enum PendingBlockWrite {
    Minimal {
        records: Vec<MinimalBlockRecord>,
        schema: Arc<Schema>,
        fields: Vec<BlockField>,
        output_path: PathBuf,
        chain_name: String,
    },
    Full {
        records: Vec<FullBlockRecord>,
        schema: Arc<Schema>,
        output_path: PathBuf,
        chain_name: String,
    },
}

#[allow(clippy::too_many_arguments)]
pub async fn collect_blocks(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    tx_sender: Option<Sender<(u64, u64, Vec<B256>)>>,
    eth_call_sender: Option<Sender<(u64, u64)>>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
    catch_up_only: bool,
) -> Result<(), BlockCollectionError> {
    let output_dir = raw_blocks_dir(&chain.name);
    tokio::fs::create_dir_all(&output_dir).await?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let start_block = chain.start_block.map(|u| u.to::<u64>()).unwrap_or(0);

    let chain_head = client.get_block_number().await?;

    tracing::info!(
        "Chain {} head at block {}, starting collection from block {}",
        chain.name,
        chain_head,
        start_block
    );

    // In catch-up-only mode, only fill gaps within existing data — don't extend
    // to chain head. The upper bound is the highest block in existing files.
    let effective_head = if catch_up_only {
        let dir = output_dir.clone();
        let existing = tokio::task::spawn_blocking(move || scan_existing_parquet_files(&dir))
            .await
            .map_err(|e| BlockCollectionError::JoinError(e.to_string()))?;
        let max_existing = highest_block_from_files(&existing, s3_manifest);
        match max_existing {
            Some(max) => {
                tracing::info!(
                    "Catch-up-only mode: limiting block collection to existing frontier (block {})",
                    max,
                );
                max
            }
            None => {
                tracing::info!(
                    "Catch-up-only mode: no existing block data found, nothing to catch up"
                );
                return Ok(());
            }
        }
    } else {
        chain_head
    };

    let dir = output_dir.clone();
    let s3_manifest_owned = s3_manifest.cloned();
    let ranges = tokio::task::spawn_blocking(move || {
        compute_ranges_to_fetch(
            start_block,
            effective_head,
            range_size,
            &dir,
            s3_manifest_owned.as_ref(),
        )
    })
    .await
    .map_err(|e| BlockCollectionError::JoinError(e.to_string()))?;

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

    // Pipeline: overlap parquet writes with the next range's RPC fetch.
    // While range N+1 is being fetched, range N's parquet write runs in the background.
    let mut pending_write_handle: Option<
        tokio::task::JoinHandle<Result<(), BlockCollectionError>>,
    > = None;
    let mut pending_s3_info: Option<(PathBuf, u64, u64)> = None;

    for range in ranges {
        // Drain the previous range's write before starting a new one.
        if let Some(handle) = pending_write_handle.take() {
            handle
                .await
                .map_err(|e| BlockCollectionError::JoinError(e.to_string()))??;

            // Upload the now-written file to S3 if configured.
            if let (Some(ref sm), Some((ref path, s3_start, s3_end))) =
                (&storage_manager, &pending_s3_info)
            {
                upload_parquet_to_s3(sm, path, &chain.name, "raw/blocks", *s3_start, *s3_end)
                    .await
                    .map_err(|e| BlockCollectionError::Io(std::io::Error::other(e.to_string())))?;
            }
        }

        tracing::info!("Fetching blocks {}-{}", range.start, range.end - 1);

        let (record_count, pending_write) = collect_blocks_streaming(
            client,
            &range,
            block_fields,
            &schema,
            &output_dir,
            &tx_sender,
            &eth_call_sender,
            &chain.name,
        )
        .await?;

        let output_path = output_dir.join(range.file_name("blocks"));
        tracing::info!("Wrote {} blocks to {}", record_count, output_path.display());

        // Spawn background write and record S3 metadata for the next drain.
        pending_s3_info = Some((output_path, range.start, range.end - 1));
        pending_write_handle = Some(tokio::spawn(execute_block_write(pending_write)));
    }

    // Drain the final pending write.
    if let Some(handle) = pending_write_handle.take() {
        handle
            .await
            .map_err(|e| BlockCollectionError::JoinError(e.to_string()))??;

        if let (Some(ref sm), Some((ref path, s3_start, s3_end))) =
            (&storage_manager, &pending_s3_info)
        {
            upload_parquet_to_s3(sm, path, &chain.name, "raw/blocks", *s3_start, *s3_end)
                .await
                .map_err(|e| BlockCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }
    }

    Ok(())
}

/// Streaming block collection: fetches blocks concurrently and forwards to downstream
/// collectors immediately as each block arrives. Buffers records and returns a
/// `PendingBlockWrite` so the caller can overlap the parquet I/O with the next fetch.
async fn collect_blocks_streaming(
    client: &UnifiedRpcClient,
    range: &BlockRange,
    block_fields: &Option<Vec<BlockField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    tx_sender: &Option<Sender<(u64, u64, Vec<B256>)>>,
    eth_call_sender: &Option<Sender<(u64, u64)>>,
    chain_name: &str,
) -> Result<(usize, PendingBlockWrite), BlockCollectionError> {
    let range_start_time = Instant::now();
    let block_numbers: Vec<BlockNumberOrTag> = (range.start..range.end)
        .map(BlockNumberOrTag::Number)
        .collect();
    let expected_count = block_numbers.len();

    // Channel for streaming block results
    let (result_tx, mut result_rx) = mpsc::channel(256);

    // Start streaming fetch
    let fetch_handle = client.get_blocks_streaming(block_numbers, false, result_tx);

    // Process blocks as they arrive, using BTreeMap for ordered parquet output
    let mut _received_count = 0usize;
    let mut errors: Vec<(u64, RpcError)> = Vec::new();

    match block_fields {
        Some(fields) => {
            let mut records_map: BTreeMap<u64, MinimalBlockRecord> = BTreeMap::new();

            while let Some((block_num_tag, result)) = result_rx.recv().await {
                let block_number = match block_num_tag {
                    BlockNumberOrTag::Number(n) => n,
                    _ => continue,
                };

                _received_count += 1;

                match result {
                    Ok(Some(block)) => {
                        let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
                        let timestamp = block.header.timestamp;

                        // Immediately forward to downstream collectors
                        if let Some(sender) = tx_sender {
                            if sender
                                .send((block_number, timestamp, tx_hashes.clone()))
                                .await
                                .is_err()
                            {
                                tracing::warn!(
                                    "Receipts channel closed, continuing block collection"
                                );
                            }
                        }

                        if let Some(sender) = eth_call_sender {
                            if sender.send((block_number, timestamp)).await.is_err() {
                                tracing::warn!(
                                    "Eth_call channel closed, continuing block collection"
                                );
                            }
                        }

                        // Buffer record for parquet (ordered by block number)
                        records_map.insert(
                            block_number,
                            MinimalBlockRecord {
                                number: block_number,
                                timestamp,
                                transaction_count: tx_hashes.len() as u32,
                                transaction_hashes: tx_hashes
                                    .iter()
                                    .map(|h| format!("{:?}", h))
                                    .collect(),
                                uncle_count: block.uncles.len() as u32,
                            },
                        );
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
            fetch_handle
                .await
                .map_err(|e| BlockCollectionError::JoinError(e.to_string()))?;

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

            // Record collection metrics
            counter!(
                "collection_blocks_processed_total",
                "chain" => chain_name.to_string(),
                "mode" => "historical"
            )
            .increment(count as u64);

            for record in &all_records {
                histogram!(
                    "collection_block_transactions",
                    "chain" => chain_name.to_string()
                )
                .record(record.transaction_count as f64);
            }

            histogram!(
                "collection_block_processing_duration_seconds",
                "chain" => chain_name.to_string(),
                "mode" => "historical"
            )
            .record(range_start_time.elapsed().as_secs_f64());

            // Return data for deferred parquet write
            let pending = PendingBlockWrite::Minimal {
                records: all_records,
                schema: schema.clone(),
                fields: fields.to_vec(),
                output_path: output_dir.join(range.file_name("blocks")),
                chain_name: chain_name.to_string(),
            };

            Ok((count, pending))
        }
        None => {
            let mut records_map: BTreeMap<u64, FullBlockRecord> = BTreeMap::new();

            while let Some((block_num_tag, result)) = result_rx.recv().await {
                let block_number = match block_num_tag {
                    BlockNumberOrTag::Number(n) => n,
                    _ => continue,
                };

                _received_count += 1;

                match result {
                    Ok(Some(block)) => {
                        let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
                        let timestamp = block.header.timestamp;

                        // Immediately forward to downstream collectors
                        if let Some(sender) = tx_sender {
                            if sender
                                .send((block_number, timestamp, tx_hashes.clone()))
                                .await
                                .is_err()
                            {
                                tracing::warn!(
                                    "Receipts channel closed, continuing block collection"
                                );
                            }
                        }

                        if let Some(sender) = eth_call_sender {
                            if sender.send((block_number, timestamp)).await.is_err() {
                                tracing::warn!(
                                    "Eth_call channel closed, continuing block collection"
                                );
                            }
                        }

                        // Buffer full record for parquet
                        records_map.insert(
                            block_number,
                            process_single_block_full(&block, block_number)?,
                        );
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
            fetch_handle
                .await
                .map_err(|e| BlockCollectionError::JoinError(e.to_string()))?;

            // Check for errors
            if !errors.is_empty() {
                let (block_num, err) = errors.remove(0);
                tracing::error!("Block {} fetch failed: {}", block_num, err);
                return Err(BlockCollectionError::Rpc(err));
            }

            // Convert to ordered vec for parquet
            let all_records: Vec<FullBlockRecord> = records_map.into_values().collect();
            let count = all_records.len();

            // Record collection metrics
            counter!(
                "collection_blocks_processed_total",
                "chain" => chain_name.to_string(),
                "mode" => "historical"
            )
            .increment(count as u64);

            for record in &all_records {
                histogram!(
                    "collection_block_transactions",
                    "chain" => chain_name.to_string()
                )
                .record(record.transaction_count as f64);
            }

            histogram!(
                "collection_block_processing_duration_seconds",
                "chain" => chain_name.to_string(),
                "mode" => "historical"
            )
            .record(range_start_time.elapsed().as_secs_f64());

            // Return data for deferred parquet write
            let pending = PendingBlockWrite::Full {
                records: all_records,
                schema: schema.clone(),
                output_path: output_dir.join(range.file_name("blocks")),
                chain_name: chain_name.to_string(),
            };

            Ok((count, pending))
        }
    }
}

/// Execute a deferred parquet write using async wrappers (which internally
/// use `spawn_blocking`).
async fn execute_block_write(write: PendingBlockWrite) -> Result<(), BlockCollectionError> {
    let write_start = Instant::now();
    match write {
        PendingBlockWrite::Minimal {
            records,
            schema,
            fields,
            output_path,
            chain_name,
        } => {
            write_minimal_blocks_to_parquet_async(records, schema, fields, output_path.clone())
                .await?;
            crate::metrics::record_parquet_write(
                &chain_name,
                "blocks",
                write_start.elapsed().as_secs_f64(),
                &output_path,
            );
        }
        PendingBlockWrite::Full {
            records,
            schema,
            output_path,
            chain_name,
        } => {
            write_full_blocks_to_parquet_async(records, schema, output_path.clone()).await?;
            crate::metrics::record_parquet_write(
                &chain_name,
                "blocks",
                write_start.elapsed().as_secs_f64(),
                &output_path,
            );
        }
    }
    Ok(())
}

fn compute_ranges_to_fetch(
    start_block: u64,
    chain_head: u64,
    range_size: u64,
    output_dir: &Path,
    s3_manifest: Option<&S3Manifest>,
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
            // Skip if file exists locally
            if existing_files.contains(&range.file_name("blocks")) {
                return false;
            }
            // Skip if range exists in S3 manifest
            if let Some(manifest) = s3_manifest {
                if manifest.raw_blocks.has(range.start, range.end - 1) {
                    tracing::debug!(
                        "Skipping blocks range {}-{}: exists in S3 manifest",
                        range.start,
                        range.end - 1
                    );
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Returns the highest block number covered by existing parquet files (local + S3).
fn highest_block_from_files(
    local_files: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
) -> Option<u64> {
    let mut max_block: Option<u64> = None;

    // Parse "blocks_{start}-{end}.parquet" file names
    for name in local_files {
        if let Some((_start, end)) = parse_range_from_filename(Path::new(name)) {
            max_block = Some(max_block.map_or(end, |m: u64| m.max(end)));
        }
    }

    // Also check S3 manifest for block ranges
    if let Some(manifest) = s3_manifest {
        if let Some(s3_max) = manifest.max_raw_blocks_block() {
            max_block = Some(max_block.map_or(s3_max, |m: u64| m.max(s3_max)));
        }
    }

    max_block
}

fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    scan_parquet_filenames(dir, "blocks_")
}
