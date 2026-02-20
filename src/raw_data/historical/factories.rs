use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::Address;
use alloy::primitives::B256;
use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListArray, StringBuilder, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::raw_data::decoding::DecoderMessage;
use crate::raw_data::historical::receipts::{LogData, LogMessage};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{
    resolve_factory_config, AddressOrAddresses, Contracts, FactoryCollections,
    FactoryParameterLocation,
};
use crate::types::config::raw_data::RawDataCollectionConfig;

#[derive(Debug, Error)]
pub enum FactoryCollectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Channel send error: {0}")]
    ChannelSend(String),

    #[error("ABI decode error: {0}")]
    AbiDecode(String),

    #[error("Task join error: {0}")]
    JoinError(String),
}

#[derive(Debug, Clone)]
pub struct FactoryAddressData {
    pub range_start: u64,
    pub range_end: u64,
    pub addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
}

/// Message type for factory -> eth_calls communication
/// Supports incremental address forwarding for early RPC fetching
#[derive(Debug, Clone)]
pub enum FactoryMessage {
    /// Incremental batch of factory addresses discovered (sent per rpc_batch_size logs)
    IncrementalAddresses(FactoryAddressData),
    /// A block range is complete - parquet files written
    RangeComplete { range_start: u64, range_end: u64 },
    /// All processing is complete
    AllComplete,
}

/// State for batch-based factory processing within a range
/// Enables early address forwarding before full range is complete
#[derive(Debug)]
struct FactoryBatchState {
    range_start: u64,
    range_end: u64,
    /// Logs received but not yet processed
    logs_buffered: Vec<LogData>,
    /// Addresses already sent to downstream (avoid duplicates)
    addresses_sent: HashSet<[u8; 20]>,
    /// All factory records for parquet write
    all_records: Vec<FactoryRecord>,
    /// All addresses by block for final range complete message
    addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
}

#[derive(Debug)]
struct FactoryRecord {
    block_number: u64,
    block_timestamp: u64,
    factory_address: [u8; 20],
    collection_name: String,
}

struct FactoryMatcher {
    factory_contract_address: [u8; 20],
    event_topic0: [u8; 32],
    param_location: FactoryParameterLocation,
    data_types: Vec<DynSolType>,
    collection_name: String,
}

pub async fn collect_factories(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    mut log_rx: Receiver<LogMessage>,
    logs_factory_tx: Option<Sender<FactoryAddressData>>,
    eth_calls_factory_tx: Option<Sender<FactoryMessage>>,
    log_decoder_tx: Option<Sender<DecoderMessage>>,
    call_decoder_tx: Option<Sender<DecoderMessage>>,
    recollect_tx: Option<Sender<RecollectRequest>>,
) -> Result<(), FactoryCollectionError> {
    let output_dir = PathBuf::from(format!("data/derived/{}/factories", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;

    let existing_factory_data = load_factory_addresses_from_parquet(&output_dir)?;
    if !existing_factory_data.is_empty() {
        tracing::info!(
            "Loaded {} existing factory ranges from parquet for chain {}",
            existing_factory_data.len(),
            chain.name
        );

        for factory_data in existing_factory_data {
            if let Some(ref tx) = logs_factory_tx {
                if tx.send(factory_data.clone()).await.is_err() {
                    tracing::error!(
                        "Failed to send existing factory data for range {}-{} to logs_factory_tx - receiver dropped",
                        factory_data.range_start,
                        factory_data.range_end
                    );
                    return Err(FactoryCollectionError::ChannelSend(format!(
                        "logs_factory_tx (existing data {}-{}) - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    )));
                }
            }

            if let Some(ref tx) = eth_calls_factory_tx {
                let _ = tx.send(FactoryMessage::IncrementalAddresses(factory_data.clone())).await;
            }

            // Send to decoders
            let addresses: HashMap<String, Vec<Address>> = factory_data
                .addresses_by_block
                .values()
                .flatten()
                .fold(HashMap::new(), |mut acc, (_, addr, collection)| {
                    acc.entry(collection.clone()).or_default().push(*addr);
                    acc
                });

            if let Some(ref tx) = log_decoder_tx {
                let _ = tx.send(DecoderMessage::FactoryAddresses {
                    range_start: factory_data.range_start,
                    range_end: factory_data.range_end,
                    addresses: addresses.clone(),
                }).await;
            }

            if let Some(ref tx) = call_decoder_tx {
                let _ = tx.send(DecoderMessage::FactoryAddresses {
                    range_start: factory_data.range_start,
                    range_end: factory_data.range_end,
                    addresses,
                }).await;
            }
        }
    }

    let matchers = build_factory_matchers(&chain.contracts);

    if matchers.is_empty() {
        tracing::info!("No factory matchers configured for chain {}, forwarding empty ranges", chain.name);

        while let Some(message) = log_rx.recv().await {
            match message {
                LogMessage::Logs(_) => {}
                LogMessage::RangeComplete {
                    range_start,
                    range_end,
                } => {
                    let empty_data = FactoryAddressData {
                        range_start,
                        range_end,
                        addresses_by_block: HashMap::new(),
                    };

                    if let Some(ref tx) = logs_factory_tx {
                        if tx.send(empty_data.clone()).await.is_err() {
                            tracing::error!(
                                "Failed to send empty factory data for range {}-{} to logs_factory_tx - receiver dropped",
                                range_start,
                                range_end
                            );
                            return Err(FactoryCollectionError::ChannelSend(format!(
                                "logs_factory_tx (empty data {}-{}) - receiver dropped",
                                range_start, range_end
                            )));
                        }
                    }

                    if let Some(ref tx) = eth_calls_factory_tx {
                        let _ = tx.send(FactoryMessage::RangeComplete { range_start, range_end }).await;
                    }

                    // Send empty addresses to decoders
                    if let Some(ref tx) = log_decoder_tx {
                        let _ = tx.send(DecoderMessage::FactoryAddresses {
                            range_start,
                            range_end,
                            addresses: HashMap::new(),
                        }).await;
                    }
                    if let Some(ref tx) = call_decoder_tx {
                        let _ = tx.send(DecoderMessage::FactoryAddresses {
                            range_start,
                            range_end,
                            addresses: HashMap::new(),
                        }).await;
                    }
                }
                LogMessage::AllRangesComplete => {
                    // Signal all complete to eth_calls
                    if let Some(ref tx) = eth_calls_factory_tx {
                        let _ = tx.send(FactoryMessage::AllComplete).await;
                    }
                    break;
                }
            }
        }

        return Ok(());
    }

    let existing_files = scan_existing_parquet_files(&output_dir);

    // Get the factory collection names from matchers
    let factory_collection_names: HashSet<String> = matchers
        .iter()
        .map(|m| m.collection_name.clone())
        .collect();

    // =========================================================================
    // Catchup phase: Process existing logs files where factory files are missing
    // This avoids re-fetching receipts when logs already exist
    // =========================================================================
    let log_ranges = get_existing_log_ranges(&chain.name);
    let mut catchup_count = 0;

    let factory_concurrency = raw_data_config.factory_concurrency.unwrap_or(4);
    let matchers = Arc::new(matchers);
    let existing_files = Arc::new(existing_files);
    let output_dir = Arc::new(output_dir);

    {
        let semaphore = Arc::new(Semaphore::new(factory_concurrency));
        let mut join_set: JoinSet<
            Result<Option<FactoryAddressData>, FactoryCollectionError>,
        > = JoinSet::new();

        for log_range in &log_ranges {
            let all_factory_files_exist = factory_collection_names.iter().all(|collection| {
                let rel_path = format!(
                    "{}/{}-{}.parquet",
                    collection,
                    log_range.start,
                    log_range.end - 1
                );
                existing_files.contains(&rel_path)
            });

            if all_factory_files_exist {
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let matchers = matchers.clone();
            let existing_files = existing_files.clone();
            let output_dir = output_dir.clone();
            let file_path = log_range.file_path.clone();
            let start = log_range.start;
            let end = log_range.end;

            let recollect_tx = recollect_tx.clone();
            join_set.spawn(async move {
                let _permit = permit;

                let file_path_for_read = file_path.clone();
                let file_path_display = file_path.display().to_string();
                let batches = match tokio::task::spawn_blocking(move || {
                    read_log_batches_from_parquet(&file_path_for_read)
                })
                .await
                {
                    Ok(Ok(b)) => b,
                    Ok(Err(e)) => {
                        tracing::warn!(
                            "Corrupted log file {}: {} - deleting and requesting recollection for range {}-{}",
                            file_path_display,
                            e,
                            start,
                            end - 1
                        );

                        // Delete the corrupted file
                        if let Err(del_err) = std::fs::remove_file(&file_path) {
                            tracing::error!(
                                "Failed to delete corrupted log file {}: {}",
                                file_path.display(),
                                del_err
                            );
                        } else {
                            tracing::info!("Deleted corrupted log file: {}", file_path.display());
                        }

                        // Send recollect request
                        if let Some(tx) = recollect_tx {
                            let request = RecollectRequest {
                                range_start: start,
                                range_end: end,
                                file_path: file_path.clone(),
                            };
                            if let Err(send_err) = tx.send(request).await {
                                tracing::error!(
                                    "Failed to send recollect request for range {}-{}: {}",
                                    start,
                                    end - 1,
                                    send_err
                                );
                            }
                        }

                        return Ok(None);
                    }
                    Err(e) => {
                        return Err(FactoryCollectionError::JoinError(e.to_string()));
                    }
                };

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                tracing::info!(
                    "Catchup: processing factories for blocks {}-{} from existing logs file ({} rows)",
                    start,
                    end - 1,
                    total_rows
                );

                process_range_batches(start, end, batches, &matchers, &output_dir, &existing_files)
                    .await
                    .map(Some)
            });
        }

        let mut catchup_results: Vec<FactoryAddressData> = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(Some(data))) => catchup_results.push(data),
                Ok(Ok(None)) => {} // Skipped (corrupt/unreadable file)
                Ok(Err(e)) => {
                    tracing::error!("Factory catchup task failed: {:?}", e);
                    return Err(e);
                }
                Err(e) => return Err(FactoryCollectionError::JoinError(e.to_string())),
            }
        }

        catchup_results.sort_by_key(|d| d.range_start);

        for factory_data in catchup_results {
            if let Some(ref tx) = logs_factory_tx {
                if tx.send(factory_data.clone()).await.is_err() {
                    tracing::error!(
                        "Failed to send catchup factory data for range {}-{} to logs_factory_tx - receiver dropped",
                        factory_data.range_start,
                        factory_data.range_end
                    );
                    return Err(FactoryCollectionError::ChannelSend(format!(
                        "logs_factory_tx (catchup {}-{}) - receiver dropped",
                        factory_data.range_start, factory_data.range_end
                    )));
                }
            }

            if let Some(ref tx) = eth_calls_factory_tx {
                // Send incremental addresses during catchup, then range complete
                let _ = tx.send(FactoryMessage::IncrementalAddresses(factory_data.clone())).await;
                let _ = tx.send(FactoryMessage::RangeComplete {
                    range_start: factory_data.range_start,
                    range_end: factory_data.range_end,
                }).await;
            }

            let addresses: HashMap<String, Vec<Address>> = factory_data
                .addresses_by_block
                .values()
                .flatten()
                .fold(HashMap::new(), |mut acc, (_, addr, collection)| {
                    acc.entry(collection.clone()).or_default().push(*addr);
                    acc
                });

            if let Some(ref tx) = log_decoder_tx {
                let _ = tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses: addresses.clone(),
                    })
                    .await;
            }

            if let Some(ref tx) = call_decoder_tx {
                let _ = tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses,
                    })
                    .await;
            }

            catchup_count += 1;
        }
    }

    if catchup_count > 0 {
        tracing::info!(
            "Factory catchup complete: processed {} ranges from logs files for chain {}",
            catchup_count,
            chain.name
        );
    }

    // =========================================================================
    // Normal phase: Process new logs from channel
    // =========================================================================
    let mut range_data: HashMap<u64, Vec<LogData>> = HashMap::new();

    tracing::info!(
        "Starting factory collection for chain {} with {} matchers",
        chain.name,
        matchers.len()
    );

    loop {
        let message = match log_rx.recv().await {
            Some(msg) => msg,
            None => break,
        };

        match message {
            LogMessage::Logs(logs) => {
                for log in logs {
                    let range_start = (log.block_number / range_size) * range_size;
                    range_data.entry(range_start).or_default().push(log);
                }
            }
            LogMessage::RangeComplete {
                range_start,
                range_end,
            } => {
                let logs = range_data.remove(&range_start).unwrap_or_default();

                let factory_data = match process_range(
                    range_start,
                    range_end,
                    logs,
                    &matchers,
                    &output_dir,
                    &existing_files,
                )
                .await
                {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::error!("Factory processing failed for range {}-{}: {:?}", range_start, range_end, e);
                        return Err(e);
                    }
                };

                if let Some(ref tx) = logs_factory_tx {
                    if tx.send(factory_data.clone()).await.is_err() {
                        tracing::error!(
                            "Failed to send factory data for range {}-{} to logs_factory_tx - receiver dropped",
                            factory_data.range_start,
                            factory_data.range_end
                        );
                        return Err(FactoryCollectionError::ChannelSend(format!(
                            "logs_factory_tx ({}-{}) - receiver dropped",
                            factory_data.range_start, factory_data.range_end
                        )));
                    }
                }

                if let Some(ref tx) = eth_calls_factory_tx {
                    // Send incremental addresses then range complete
                    let _ = tx.send(FactoryMessage::IncrementalAddresses(factory_data.clone())).await;
                    let _ = tx.send(FactoryMessage::RangeComplete {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                    }).await;
                }

                // Send to decoders
                let addresses: HashMap<String, Vec<Address>> = factory_data
                    .addresses_by_block
                    .values()
                    .flatten()
                    .fold(HashMap::new(), |mut acc, (_, addr, collection)| {
                        acc.entry(collection.clone()).or_default().push(*addr);
                        acc
                    });

                if let Some(ref tx) = log_decoder_tx {
                    let _ = tx.send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses: addresses.clone(),
                    }).await;
                }

                if let Some(ref tx) = call_decoder_tx {
                    let _ = tx.send(DecoderMessage::FactoryAddresses {
                        range_start: factory_data.range_start,
                        range_end: factory_data.range_end,
                        addresses,
                    }).await;
                }
            }
            LogMessage::AllRangesComplete => {
                // Signal all complete to eth_calls
                if let Some(ref tx) = eth_calls_factory_tx {
                    let _ = tx.send(FactoryMessage::AllComplete).await;
                }
                break;
            }
        }
    }

    tracing::info!("Factory collection complete for chain {}", chain.name);
    Ok(())
}

fn build_factory_matchers(contracts: &Contracts) -> Vec<FactoryMatcher> {
    let mut matchers = Vec::new();

    for (contract_name, contract) in contracts {
        let contract_addresses: Vec<[u8; 20]> = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![addr.0 .0],
            AddressOrAddresses::Multiple(addrs) => addrs.iter().map(|a| a.0 .0).collect(),
        };

        if let Some(factories) = &contract.factories {
            for factory in factories {
                let events = factory.factory_events.clone().into_vec();

                for event in events {
                    let event_topic0 = event.compute_event_signature();
                    let param_location = event.parse_factory_parameter();

                    let data_types = event
                        .data_signature
                        .as_ref()
                        .map(|sig| parse_data_signature(sig))
                        .unwrap_or_default();

                    for addr in &contract_addresses {
                        tracing::debug!(
                            "Created factory matcher for {} -> {} (topic0: 0x{})",
                            contract_name,
                            factory.collection,
                            hex::encode(&event_topic0[..8])
                        );

                        matchers.push(FactoryMatcher {
                            factory_contract_address: *addr,
                            event_topic0,
                            param_location: param_location.clone(),
                            data_types: data_types.clone(),
                            collection_name: factory.collection.clone(),
                        });
                    }
                }
            }
        }
    }

    matchers
}

fn parse_data_signature(sig: &str) -> Vec<DynSolType> {
    if sig.is_empty() {
        return Vec::new();
    }

    let mut parts = Vec::new();
    let mut depth = 0;
    let mut current = String::new();

    for ch in sig.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    parts.push(trimmed);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        parts.push(trimmed);
    }

    parts
        .iter()
        .filter_map(|s| s.parse::<DynSolType>().ok())
        .collect()
}

async fn process_range(
    range_start: u64,
    range_end: u64,
    logs: Vec<LogData>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
) -> Result<FactoryAddressData, FactoryCollectionError> {
    let mut records: Vec<FactoryRecord> = Vec::new();
    let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> = HashMap::new();

    for log in &logs {
        for matcher in matchers {            
            if log.address != matcher.factory_contract_address {
                continue;
            }

            if log.topics.is_empty() || log.topics[0] != matcher.event_topic0 {
                continue;
            }
            
            let extracted_address = match &matcher.param_location {
                FactoryParameterLocation::Topic(idx) => {
                    if *idx < log.topics.len() {
                        let topic = &log.topics[*idx];
                        let mut addr = [0u8; 20];
                        addr.copy_from_slice(&topic[12..32]);
                        Some(addr)
                    } else {
                        None
                    }
                }
                FactoryParameterLocation::Data(indices) => {
                    match decode_address_from_data(&log.data, &matcher.data_types, indices) {
                        Ok(addr) => addr,
                        Err(e) => {
                            tracing::warn!(
                                block_number = log.block_number,
                                contract = %Address::from(log.address),
                                topic0 = %hex::encode(&log.topics.first().map(|t| t.as_slice()).unwrap_or(&[])),
                                collection = %matcher.collection_name,
                                data_len = log.data.len(),
                                "Failed to decode factory event data: {}",
                                e
                            );
                            None
                        }
                    }
                }
            };

            if let Some(addr) = extracted_address {
                records.push(FactoryRecord {
                    block_number: log.block_number,
                    block_timestamp: log.block_timestamp,
                    factory_address: addr,
                    collection_name: matcher.collection_name.clone(),
                });

                addresses_by_block
                    .entry(log.block_number)
                    .or_default()
                    .push((
                        log.block_timestamp,
                        Address::from(addr),
                        matcher.collection_name.clone(),
                    ));
            }
        }
    }

    write_factory_parquet_files(range_start, range_end, records, matchers, output_dir, existing_files).await?;

    Ok(FactoryAddressData {
        range_start,
        range_end,
        addresses_by_block,
    })
}

/// Read log batches from a parquet file with column projection.
/// Only reads block_number, block_timestamp, address, topics, data — skips transaction_hash and log_index.
fn read_log_batches_from_parquet(file_path: &Path) -> Result<Vec<RecordBatch>, FactoryCollectionError> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();

    let needed = ["block_number", "block_timestamp", "address", "topics", "data"];
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

/// Process log record batches using Arrow-native column access.
/// Works directly on Arrow arrays instead of materializing LogData structs.
async fn process_range_batches(
    range_start: u64,
    range_end: u64,
    batches: Vec<RecordBatch>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
) -> Result<FactoryAddressData, FactoryCollectionError> {
    let mut records: Vec<FactoryRecord> = Vec::new();
    let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> = HashMap::new();

    for batch in &batches {
        let block_nums = match batch
            .column_by_name("block_number")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        {
            Some(arr) => arr,
            None => continue,
        };
        let timestamps = match batch
            .column_by_name("block_timestamp")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        {
            Some(arr) => arr,
            None => continue,
        };
        let addrs = match batch
            .column_by_name("address")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        {
            Some(arr) => arr,
            None => continue,
        };

        let topics_list = batch
            .column_by_name("topics")
            .and_then(|c| c.as_any().downcast_ref::<ListArray>());

        // Pre-extract inner topics array and offsets for zero-copy access
        let topics_inner = topics_list
            .and_then(|l| l.values().as_any().downcast_ref::<FixedSizeBinaryArray>());
        let topics_offsets = topics_list.map(|l| l.offsets());

        let data_col = batch
            .column_by_name("data")
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());

        for i in 0..batch.num_rows() {
            let addr_bytes = addrs.value(i);
            if addr_bytes.len() != 20 {
                continue;
            }

            for matcher in matchers {
                if addr_bytes != matcher.factory_contract_address.as_slice() {
                    continue;
                }

                // Check topic0 match using pre-extracted inner array and offsets
                let topic0_match = match (topics_offsets, topics_inner) {
                    (Some(offsets), Some(inner)) => {
                        let start = offsets[i] as usize;
                        let end = offsets[i + 1] as usize;
                        end > start && inner.value(start) == matcher.event_topic0.as_slice()
                    }
                    _ => false,
                };

                if !topic0_match {
                    continue;
                }

                let block_number = block_nums.value(i);
                let block_timestamp = timestamps.value(i);

                let extracted_address = match &matcher.param_location {
                    FactoryParameterLocation::Topic(idx) => {
                        match (topics_offsets, topics_inner) {
                            (Some(offsets), Some(inner)) => {
                                let start = offsets[i] as usize;
                                let end = offsets[i + 1] as usize;
                                let num_topics = end - start;
                                if *idx < num_topics {
                                    let topic = inner.value(start + *idx);
                                    if topic.len() == 32 {
                                        let mut addr = [0u8; 20];
                                        addr.copy_from_slice(&topic[12..32]);
                                        Some(addr)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    }
                    FactoryParameterLocation::Data(indices) => {
                        let data = data_col.map(|arr| arr.value(i)).unwrap_or(&[]);
                        match decode_address_from_data(data, &matcher.data_types, indices) {
                            Ok(addr) => addr,
                            Err(e) => {
                                tracing::warn!(
                                    block_number,
                                    contract = %Address::from_slice(addr_bytes),
                                    collection = %matcher.collection_name,
                                    "Failed to decode factory event data: {}",
                                    e
                                );
                                None
                            }
                        }
                    }
                };

                if let Some(addr) = extracted_address {
                    records.push(FactoryRecord {
                        block_number,
                        block_timestamp,
                        factory_address: addr,
                        collection_name: matcher.collection_name.clone(),
                    });

                    addresses_by_block
                        .entry(block_number)
                        .or_default()
                        .push((
                            block_timestamp,
                            Address::from(addr),
                            matcher.collection_name.clone(),
                        ));
                }
            }
        }
    }

    write_factory_parquet_files(range_start, range_end, records, matchers, output_dir, existing_files).await?;

    Ok(FactoryAddressData {
        range_start,
        range_end,
        addresses_by_block,
    })
}

/// Write factory records to parquet files, grouped by collection.
/// Writes empty parquet files for collections with no matching events.
async fn write_factory_parquet_files(
    range_start: u64,
    range_end: u64,
    records: Vec<FactoryRecord>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
) -> Result<(), FactoryCollectionError> {
    let mut by_collection: HashMap<String, Vec<FactoryRecord>> = HashMap::new();
    for record in records {
        by_collection
            .entry(record.collection_name.clone())
            .or_default()
            .push(record);
    }

    let collections_with_events: HashSet<String> = by_collection.keys().cloned().collect();

    for (collection_name, collection_records) in by_collection {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let rel_path = format!("{}/{}", collection_name, file_name);

        if existing_files.contains(&rel_path) {
            tracing::debug!(
                "Skipping factory parquet for {} blocks {}-{} (already exists)",
                collection_name,
                range_start,
                range_end - 1
            );
            continue;
        }

        let record_count = collection_records.len();
        let sub_dir = output_dir.join(&collection_name);
        std::fs::create_dir_all(&sub_dir)?;
        let output_path = sub_dir.join(&file_name);
        tokio::task::spawn_blocking(move || {
            write_factory_records_to_parquet(&collection_records, &output_path)
        })
        .await
        .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))??;

        tracing::info!(
            "Wrote {} factory addresses for {} to {}",
            record_count,
            collection_name,
            sub_dir.join(&file_name).display()
        );
    }

    for matcher in matchers {
        let collection_name = &matcher.collection_name;
        if !collections_with_events.contains(collection_name) {
            let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
            let rel_path = format!("{}/{}", collection_name, file_name);

            if !existing_files.contains(&rel_path) {
                let sub_dir = output_dir.join(collection_name);
                std::fs::create_dir_all(&sub_dir)?;
                let output_path = sub_dir.join(&file_name);
                tokio::task::spawn_blocking(move || write_empty_factory_parquet(&output_path))
                    .await
                    .map_err(|e| FactoryCollectionError::JoinError(e.to_string()))??;

                tracing::debug!(
                    "Wrote empty factory file for {} range {}-{}",
                    collection_name,
                    range_start,
                    range_end - 1
                );
            }
        }
    }

    Ok(())
}

fn extract_address_recursive(values: &[DynSolValue], indices: &[usize]) -> Option<[u8; 20]> {
    if indices.is_empty() {
        return None;
    }

    let first_idx = indices[0];
    if first_idx >= values.len() {
        return None;
    }

    let value = &values[first_idx];

    if indices.len() == 1 {
        // Last index - extract address
        if let DynSolValue::Address(addr) = value {
            return Some(addr.0 .0);
        }
        None
    } else {
        // More indices to traverse - must be a tuple
        if let DynSolValue::Tuple(inner_values) = value {
            extract_address_recursive(inner_values, &indices[1..])
        } else {
            None
        }
    }
}

fn decode_address_from_data(data: &[u8], types: &[DynSolType], indices: &[usize]) -> Result<Option<[u8; 20]>, alloy::dyn_abi::Error> {
    if types.is_empty() || indices.is_empty() {
        return Ok(None);
    }

    let tuple_type = DynSolType::Tuple(types.to_vec());

    match tuple_type.abi_decode_params(data) {
        Ok(DynSolValue::Tuple(values)) => Ok(extract_address_recursive(&values, indices)),
        Ok(_) => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_factory_records_to_parquet(
    records: &[FactoryRecord],
    output_path: &Path,
) -> Result<(), FactoryCollectionError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("factory_address", DataType::FixedSizeBinary(20), false),
        Field::new("collection_name", DataType::Utf8, false),
    ]));

    let block_numbers: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    let timestamps: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    let addresses =
        FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| r.factory_address.as_slice()))?;

    let mut names_builder = StringBuilder::new();
    for record in records {
        names_builder.append_value(&record.collection_name);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(block_numbers),
        Arc::new(timestamps),
        Arc::new(addresses),
        Arc::new(names_builder.finish()),
    ];

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

fn scan_existing_parquet_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();

    // Scan nested directories: dir/collection/*.parquet
    if let Ok(collection_entries) = std::fs::read_dir(dir) {
        for collection_entry in collection_entries.flatten() {
            let collection_path = collection_entry.path();
            if !collection_path.is_dir() {
                continue;
            }
            let collection_name = match collection_entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => continue,
            };

            if let Ok(file_entries) = std::fs::read_dir(&collection_path) {
                for file_entry in file_entries.flatten() {
                    if let Some(name) = file_entry.file_name().to_str() {
                        if name.ends_with(".parquet") {
                            // Store as collection/filename
                            files.insert(format!("{}/{}", collection_name, name));
                        }
                    }
                }
            }
        }
    }
    files
}

fn load_factory_addresses_from_parquet(
    dir: &Path,
) -> Result<Vec<FactoryAddressData>, FactoryCollectionError> {
    let mut results = Vec::new();

    // Scan nested directories: dir/collection/*.parquet
    let collection_entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(results),
    };

    for collection_entry in collection_entries.flatten() {
        let collection_path = collection_entry.path();
        if !collection_path.is_dir() {
            continue;
        }

        let file_entries = match std::fs::read_dir(&collection_path) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for file_entry in file_entries.flatten() {
            let path = file_entry.path();
            if !path.extension().map(|e| e == "parquet").unwrap_or(false) {
                continue;
            }

            // Filename is now just "start-end.parquet"
            let file_name = match path.file_stem().and_then(|s| s.to_str()) {
                Some(name) => name,
                None => continue,
            };

            let range_parts: Vec<&str> = file_name.split('-').collect();
            if range_parts.len() != 2 {
                continue;
            }

            let range_start: u64 = match range_parts[0].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let range_end: u64 = match range_parts[1].parse::<u64>() {
                Ok(v) => v + 1,
                Err(_) => continue,
            };

            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!("Failed to open factory parquet {}: {}", path.display(), e);
                    continue;
                }
            };

            let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
                Ok(builder) => match builder.build() {
                    Ok(reader) => reader,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to build parquet reader for {}: {}",
                            path.display(),
                            e
                        );
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to read factory parquet {}: {}", path.display(), e);
                    continue;
                }
            };

            let mut addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>> = HashMap::new();

            for batch_result in reader {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!("Failed to read batch from {}: {}", path.display(), e);
                        continue;
                    }
                };

                let block_numbers = batch
                    .column_by_name("block_number")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

                let timestamps = batch
                    .column_by_name("block_timestamp")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

                let addresses = batch
                    .column_by_name("factory_address")
                    .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

                let collection_names = batch.column_by_name("collection_name").and_then(|c| {
                    c.as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                });

                if let (Some(blocks), Some(times), Some(addrs), Some(names)) =
                    (block_numbers, timestamps, addresses, collection_names)
                {
                    for i in 0..batch.num_rows() {
                        let block = blocks.value(i);
                        let timestamp = times.value(i);
                        let addr_bytes = addrs.value(i);
                        let collection = names.value(i);

                        if addr_bytes.len() == 20 {
                            let mut addr = [0u8; 20];
                            addr.copy_from_slice(addr_bytes);
                            addresses_by_block.entry(block).or_default().push((
                                timestamp,
                                Address::from(addr),
                                collection.to_string(),
                            ));
                        }
                    }
                }
            }

            if !addresses_by_block.is_empty() {
                results.push(FactoryAddressData {
                    range_start,
                    range_end,
                    addresses_by_block,
                });
            }
        }
    }

    results.sort_by_key(|r| r.range_start);

    Ok(results)
}

pub fn get_factory_call_configs(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> HashMap<String, Vec<crate::types::config::eth_call::EthCallConfig>> {
    let mut configs: HashMap<String, Vec<crate::types::config::eth_call::EthCallConfig>> =
        HashMap::new();

    for (_, contract) in contracts {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                let resolved = resolve_factory_config(factory, factory_collections);
                if !resolved.calls.is_empty() {
                    // Use entry().or_default().extend() to merge calls from multiple contracts
                    let entry = configs.entry(resolved.collection_name).or_default();
                    for call in resolved.calls {
                        // Deduplicate by function signature
                        if !entry.iter().any(|c| c.function == call.function) {
                            entry.push(call);
                        }
                    }
                }
            }
        }
    }

    configs
}

pub fn get_factory_collection_names(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> Vec<String> {
    let mut names = Vec::new();

    // Include collection names from contracts
    for (_, contract) in contracts {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if !names.contains(&factory.collection) {
                    names.push(factory.collection.clone());
                }
            }
        }
    }

    // Include collection names from factory_collections that aren't already present
    for name in factory_collections.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }

    names
}

fn write_empty_factory_parquet(output_path: &Path) -> Result<(), FactoryCollectionError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("factory_address", DataType::FixedSizeBinary(20), false),
        Field::new("collection_name", DataType::Utf8, false),
    ]));

    let block_numbers: UInt64Array = std::iter::empty::<Option<u64>>().collect();
    let timestamps: UInt64Array = std::iter::empty::<Option<u64>>().collect();
    // Use builder to create empty FixedSizeBinaryArray (try_from_iter fails with empty iterators)
    let addresses = FixedSizeBinaryBuilder::new(20).finish();
    let names: StringArray = std::iter::empty::<Option<&str>>().collect();

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(block_numbers),
        Arc::new(timestamps),
        Arc::new(addresses),
        Arc::new(names),
    ];

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

#[derive(Debug, Clone)]
pub struct ExistingLogRange {
    pub start: u64,
    pub end: u64,
    pub file_path: PathBuf,
}

/// Request to recollect a corrupted log file range
#[derive(Debug, Clone)]
pub struct RecollectRequest {
    pub range_start: u64,
    pub range_end: u64,
    pub file_path: PathBuf,
}

/// Scan existing logs parquet files and return their ranges
fn get_existing_log_ranges(chain_name: &str) -> Vec<ExistingLogRange> {
    let logs_dir = PathBuf::from(format!("data/raw/{}/logs", chain_name));
    let mut ranges = Vec::new();

    let entries = match std::fs::read_dir(&logs_dir) {
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

        // Parse "logs_START-END" format
        if !file_name.starts_with("logs_") {
            continue;
        }

        let range_part = &file_name[5..]; // Skip "logs_"
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

        ranges.push(ExistingLogRange {
            start,
            end,
            file_path: path,
        });
    }

    ranges.sort_by_key(|r| r.start);
    ranges
}

/// Read logs from a parquet file
fn read_logs_from_parquet(file_path: &Path) -> Result<Vec<LogData>, FactoryCollectionError> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut logs = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(FactoryCollectionError::Arrow)?;

        let block_numbers = batch
            .column_by_name("block_number")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let timestamps = batch
            .column_by_name("block_timestamp")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let tx_hashes = batch
            .column_by_name("transaction_hash")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        let log_indices = batch
            .column_by_name("log_index")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>());

        let addresses = batch
            .column_by_name("address")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        let topics_col = batch.column_by_name("topics");

        let data_col = batch
            .column_by_name("data")
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());

        if let (Some(block_nums), Some(times), Some(addrs)) = (block_numbers, timestamps, addresses)
        {
            for i in 0..batch.num_rows() {
                let block_number = block_nums.value(i);
                let block_timestamp = times.value(i);

                let transaction_hash = tx_hashes
                    .and_then(|arr| {
                        let bytes = arr.value(i);
                        if bytes.len() == 32 {
                            Some(B256::from_slice(bytes))
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();

                let log_index = log_indices.map(|arr| arr.value(i)).unwrap_or(0);

                let addr_bytes = addrs.value(i);
                let mut address = [0u8; 20];
                if addr_bytes.len() == 20 {
                    address.copy_from_slice(addr_bytes);
                }

                // Extract topics from list column
                let topics: Vec<[u8; 32]> = if let Some(col) = topics_col {
                    if let Some(list_array) = col.as_any().downcast_ref::<ListArray>() {
                        let values = list_array.value(i);
                        if let Some(fsb_array) =
                            values.as_any().downcast_ref::<FixedSizeBinaryArray>()
                        {
                            (0..fsb_array.len())
                                .filter_map(|j| {
                                    let bytes = fsb_array.value(j);
                                    if bytes.len() == 32 {
                                        let mut arr = [0u8; 32];
                                        arr.copy_from_slice(bytes);
                                        Some(arr)
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

                let data = data_col
                    .map(|arr| arr.value(i).to_vec())
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
    }

    Ok(logs)
}
