use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use alloy::primitives::{keccak256, B256};
use alloy::rpc::types::Log;
use arrow::array::{ArrayRef, FixedSizeBinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::raw_data::historical::blocks::{get_existing_block_ranges, read_block_info_from_parquet};
use crate::rpc::{RpcError, UnifiedRpcClient};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::eth_call::Frequency;
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

#[derive(Debug, Clone)]
pub enum LogMessage {
    Logs(Vec<LogData>),
    RangeComplete { range_start: u64, range_end: u64 },
    AllRangesComplete,
}

/// Data from an event that triggered an eth_call
#[derive(Debug, Clone)]
pub struct EventTriggerData {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    /// Address that emitted the event
    pub emitter_address: [u8; 20],
    /// Matched source name (contract or factory collection)
    pub source_name: String,
    /// Event signature hash (topic[0])
    pub event_signature: [u8; 32],
    /// All topics including topic0
    pub topics: Vec<[u8; 32]>,
    /// ABI-encoded event data
    pub data: Vec<u8>,
}

/// Message for event-triggered eth_calls channel
#[derive(Debug, Clone)]
pub enum EventTriggerMessage {
    /// Batch of event triggers from a range
    Triggers(Vec<EventTriggerData>),
    /// A block range is complete
    RangeComplete { range_start: u64, range_end: u64 },
    /// All ranges are complete
    AllComplete,
}

/// Matcher for detecting events that should trigger eth_calls
#[derive(Debug, Clone)]
pub struct EventTriggerMatcher {
    /// Source name (contract name or factory collection name)
    pub source_name: String,
    /// Addresses to match (empty for factory collections - matched dynamically)
    pub addresses: HashSet<[u8; 20]>,
    /// Whether this is a factory collection (addresses discovered dynamically)
    pub is_factory: bool,
    /// Event signature hash (keccak256 of signature)
    pub event_topic0: [u8; 32],
}

/// Build event trigger matchers from contract configurations
pub fn build_event_trigger_matchers(contracts: &Contracts) -> Vec<EventTriggerMatcher> {
    let mut matchers = Vec::new();
    let mut seen: HashSet<(String, [u8; 32])> = HashSet::new();

    for (contract_name, contract) in contracts {
        // Check contract-level calls for on_events
        if let Some(calls) = &contract.calls {
            for call in calls {
                if let Frequency::OnEvents(config) = &call.frequency {
                    let key = (config.source.clone(), compute_event_signature_hash(&config.event));
                    if !seen.contains(&key) {
                        if let Some(matcher) = build_matcher_for_source(
                            &config.source,
                            &config.event,
                            contracts,
                        ) {
                            matchers.push(matcher);
                            seen.insert(key);
                        }
                    }
                }
            }
        }

        // Check factory calls for on_events
        if let Some(factories) = &contract.factories {
            for factory in factories {
                for call in &factory.calls {
                    if let Frequency::OnEvents(config) = &call.frequency {
                        let key = (config.source.clone(), compute_event_signature_hash(&config.event));
                        if !seen.contains(&key) {
                            if let Some(matcher) = build_matcher_for_source(
                                &config.source,
                                &config.event,
                                contracts,
                            ) {
                                matchers.push(matcher);
                                seen.insert(key);
                            }
                        }
                    }
                }
            }
        }
    }

    matchers
}

fn build_matcher_for_source(
    source: &str,
    event_signature: &str,
    contracts: &Contracts,
) -> Option<EventTriggerMatcher> {
    let event_topic0 = compute_event_signature_hash(event_signature);

    // Check if source is a contract name
    if let Some(contract) = contracts.get(source) {
        let addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => {
                let mut set = HashSet::new();
                set.insert(addr.0 .0);
                set
            }
            AddressOrAddresses::Multiple(addrs) => {
                addrs.iter().map(|a| a.0 .0).collect()
            }
        };
        return Some(EventTriggerMatcher {
            source_name: source.to_string(),
            addresses,
            is_factory: false,
            event_topic0,
        });
    }

    // Check if source is a factory collection name
    for (_, contract) in contracts {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if factory.collection_name == source {
                    return Some(EventTriggerMatcher {
                        source_name: source.to_string(),
                        addresses: HashSet::new(), // Factory addresses discovered dynamically
                        is_factory: true,
                        event_topic0,
                    });
                }
            }
        }
    }

    tracing::warn!("Unknown event trigger source: {}", source);
    None
}

/// Compute the keccak256 hash of an event signature
fn compute_event_signature_hash(signature: &str) -> [u8; 32] {
    keccak256(signature.as_bytes()).0
}

/// Extract event triggers from a batch of logs
pub fn extract_event_triggers(
    logs: &[LogData],
    matchers: &[EventTriggerMatcher],
) -> Vec<EventTriggerData> {
    let mut triggers = Vec::new();
    // Track which (block_number, log_index, source_name) tuples we've already added
    // to avoid duplicates when multiple matchers have the same event signature
    let mut seen: HashSet<(u64, u32, String)> = HashSet::new();

    for log in logs {
        if log.topics.is_empty() {
            continue;
        }

        for matcher in matchers {
            // Check topic0 match
            if log.topics[0] != matcher.event_topic0 {
                continue;
            }

            // Check address match (for non-factory matchers)
            // For factory matchers, we send all matching events - eth_calls will filter
            if !matcher.is_factory && !matcher.addresses.contains(&log.address) {
                continue;
            }

            // Avoid duplicate triggers for the same log and source
            let key = (log.block_number, log.log_index, matcher.source_name.clone());
            if seen.contains(&key) {
                continue;
            }
            seen.insert(key);

            triggers.push(EventTriggerData {
                block_number: log.block_number,
                block_timestamp: log.block_timestamp,
                log_index: log.log_index,
                emitter_address: log.address,
                source_name: matcher.source_name.clone(),
                event_signature: log.topics[0],
                topics: log.topics.clone(),
                data: log.data.clone(),
            });
        }
    }

    triggers
}

#[derive(Debug)]
struct BlockInfo {
    block_number: u64,
    timestamp: u64,
    tx_hashes: Vec<B256>,
}

/// Tracks channel backpressure metrics for monitoring
#[derive(Debug, Default)]
struct ChannelMetrics {
    /// Total time spent blocked on sends
    total_send_wait_time: std::time::Duration,
    /// Number of sends performed
    send_count: u64,
    /// Number of sends where channel was >50% full
    high_pressure_sends: u64,
    /// Number of sends where channel was >90% full
    critical_pressure_sends: u64,
    /// Maximum time spent on a single send
    max_send_time: std::time::Duration,
    /// Total logs sent
    total_logs_sent: u64,
}

impl ChannelMetrics {
    fn record_send(&mut self, send_time: std::time::Duration, capacity_before: usize, max_capacity: usize) {
        self.send_count += 1;
        self.total_send_wait_time += send_time;

        if send_time > self.max_send_time {
            self.max_send_time = send_time;
        }

        let fill_ratio = 1.0 - (capacity_before as f64 / max_capacity as f64);
        if fill_ratio > 0.9 {
            self.critical_pressure_sends += 1;
        } else if fill_ratio > 0.5 {
            self.high_pressure_sends += 1;
        }
    }

    fn log_summary(&self, channel_name: &str) {
        if self.send_count == 0 {
            return;
        }

        let avg_send_time = self.total_send_wait_time.as_micros() as f64 / self.send_count as f64;
        tracing::info!(
            "Channel '{}' backpressure summary: sends={}, avg_send_time={:.1}µs, max_send_time={:.1}ms, high_pressure={} ({:.1}%), critical_pressure={} ({:.1}%), total_logs={}",
            channel_name,
            self.send_count,
            avg_send_time,
            self.max_send_time.as_secs_f64() * 1000.0,
            self.high_pressure_sends,
            (self.high_pressure_sends as f64 / self.send_count as f64) * 100.0,
            self.critical_pressure_sends,
            (self.critical_pressure_sends as f64 / self.send_count as f64) * 100.0,
            self.total_logs_sent,
        );
    }
}

pub async fn collect_receipts(
    chain: &ChainConfig,
    client: &UnifiedRpcClient,
    raw_data_config: &RawDataCollectionConfig,
    mut block_rx: Receiver<(u64, u64, Vec<B256>)>,
    log_tx: Option<Sender<LogMessage>>,
    factory_log_tx: Option<Sender<LogMessage>>,
    event_trigger_tx: Option<Sender<EventTriggerMessage>>,
    event_matchers: Vec<EventTriggerMatcher>,
) -> Result<(), ReceiptCollectionError> {
    let output_dir = PathBuf::from(format!("data/raw/{}/receipts", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let rpc_batch_size = raw_data_config.rpc_batch_size.unwrap_or(100) as usize;
    let block_receipt_concurrency = raw_data_config.block_receipt_concurrency.unwrap_or(10);
    let receipt_fields = &raw_data_config.fields.receipt_fields;
    let schema = build_receipt_schema(receipt_fields);

    let existing_files = scan_existing_parquet_files(&output_dir);

    let mut range_data: HashMap<u64, Vec<BlockInfo>> = HashMap::new();

    // Check if factories need to wait for us before processing
    let has_factories = factory_log_tx.is_some();

    // Get channel capacities for backpressure monitoring
    let log_tx_capacity = log_tx.as_ref().map(|s| s.max_capacity()).unwrap_or(0);
    let factory_log_tx_capacity = factory_log_tx.as_ref().map(|s| s.max_capacity()).unwrap_or(0);

    // Initialize channel metrics for backpressure tracking
    let mut log_tx_metrics = ChannelMetrics::default();
    let mut factory_log_tx_metrics = ChannelMetrics::default();

    tracing::info!(
        "Starting receipt collection for chain {} (log_tx: {}, factory_log_tx: {}, log_tx_capacity: {}, factory_log_tx_capacity: {})",
        chain.name,
        log_tx.is_some(),
        factory_log_tx.is_some(),
        log_tx_capacity,
        factory_log_tx_capacity
    );

    // =========================================================================
    // Catchup phase: Process any ranges where blocks exist but receipts don't
    // Also check for missing logs files - if receipts exist but logs don't, we
    // need to re-process to regenerate the logs data
    // Also check for missing factory files - if factories are configured but
    // factory files don't exist, we need to re-process
    // =========================================================================
    let block_ranges = get_existing_block_ranges(&chain.name);
    let mut catchup_count = 0;

    // Check existing logs files
    let logs_dir = PathBuf::from(format!("data/raw/{}/logs", chain.name));
    let existing_logs_files = scan_existing_logs_files(&logs_dir);

    // Note: Factory catchup is handled by the factories module reading from logs files directly.
    // Receipts catchup only needs to check for receipts and logs.

    for block_range in &block_ranges {
        let range = BlockRange {
            start: block_range.start,
            end: block_range.end,
        };

        let receipts_exist = existing_files.contains(&range.file_name());
        let logs_file_name = format!("logs_{}-{}.parquet", range.start, range.end - 1);
        let logs_exist = existing_logs_files.contains(&logs_file_name);

        // Skip if receipts and logs both exist (or logs aren't needed)
        // Factory catchup is handled separately by factories.rs reading from logs files
        if receipts_exist && (logs_exist || log_tx.is_none()) {
            // Still need to signal range complete for downstream collectors
            // when catching up, so they know this range is done
            if has_factories || event_trigger_tx.is_some() {
                send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
            }
            continue;
        }

        // Read block info from the existing parquet file
        let block_infos = match read_block_info_from_parquet(&block_range.file_path) {
            Ok(infos) => infos,
            Err(e) => {
                tracing::warn!(
                    "Failed to read block info from {}: {}",
                    block_range.file_path.display(),
                    e
                );
                continue;
            }
        };

        if block_infos.is_empty() {
            continue;
        }

        tracing::info!(
            "Catchup: processing receipts for blocks {}-{} from existing block file",
            range.start,
            range.end - 1
        );

        let blocks: Vec<BlockInfo> = block_infos
            .into_iter()
            .map(|info| BlockInfo {
                block_number: info.block_number,
                timestamp: info.timestamp,
                tx_hashes: info.tx_hashes,
            })
            .collect();

        process_range(
            &range,
            blocks,
            client,
            receipt_fields,
            &schema,
            &output_dir,
            &log_tx,
            &factory_log_tx,
            &event_trigger_tx,
            &event_matchers,
            rpc_batch_size,
            &mut log_tx_metrics,
            &mut factory_log_tx_metrics,
            log_tx_capacity,
            factory_log_tx_capacity,
            chain.block_receipts_method.as_deref(),
            block_receipt_concurrency,
        )
        .await?;

        send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
        catchup_count += 1;
    }

    if catchup_count > 0 {
        tracing::info!(
            "Catchup complete: processed {} receipt ranges for chain {}",
            catchup_count,
            chain.name
        );
    }

    // =========================================================================
    // Normal phase: Process new blocks from the channel
    // =========================================================================
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

                if existing_files.contains(&range.file_name()) {
                    tracing::info!(
                        "Skipping receipts for blocks {}-{} (already exists)",
                        range.start,
                        range.end - 1
                    );
                    range_data.remove(&range_start);

                    // Still need to signal range complete for skipped ranges
                    send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
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
                    &factory_log_tx,
                    &event_trigger_tx,
                    &event_matchers,
                    rpc_batch_size,
                    &mut log_tx_metrics,
                    &mut factory_log_tx_metrics,
                    log_tx_capacity,
                    factory_log_tx_capacity,
                    chain.block_receipts_method.as_deref(),
                    block_receipt_concurrency,
                )
                .await?;

                send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
            }
        }
    }

    for (range_start, blocks) in range_data.drain() {
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
            send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
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
            &factory_log_tx,
            &event_trigger_tx,
            &event_matchers,
            rpc_batch_size,
            &mut log_tx_metrics,
            &mut factory_log_tx_metrics,
            log_tx_capacity,
            factory_log_tx_capacity,
            chain.block_receipts_method.as_deref(),
            block_receipt_concurrency,
        )
        .await?;

        send_range_complete(&factory_log_tx, &log_tx, &event_trigger_tx, range.start, range.end).await?;
    }

    if let Some(sender) = &factory_log_tx {
        if sender.send(LogMessage::AllRangesComplete).await.is_err() {
            tracing::error!("Failed to send AllRangesComplete to factory_log_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "factory_log_tx (AllRangesComplete) - receiver dropped".to_string(),
            ));
        }
    }
    if let Some(sender) = &log_tx {
        if sender.send(LogMessage::AllRangesComplete).await.is_err() {
            tracing::error!("Failed to send AllRangesComplete to log_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "log_tx (AllRangesComplete) - receiver dropped".to_string(),
            ));
        }
    }
    if let Some(sender) = &event_trigger_tx {
        if sender.send(EventTriggerMessage::AllComplete).await.is_err() {
            tracing::error!("Failed to send AllComplete to event_trigger_tx - receiver dropped");
            return Err(ReceiptCollectionError::ChannelSend(
                "event_trigger_tx (AllComplete) - receiver dropped".to_string(),
            ));
        }
    }

    // Log channel backpressure summaries
    if log_tx.is_some() {
        log_tx_metrics.log_summary("log_tx");
    }
    if factory_log_tx.is_some() {
        factory_log_tx_metrics.log_summary("factory_log_tx");
    }

    tracing::info!("Receipt collection complete for chain {}", chain.name);
    Ok(())
}

async fn send_range_complete(
    factory_log_tx: &Option<Sender<LogMessage>>,
    log_tx: &Option<Sender<LogMessage>>,
    event_trigger_tx: &Option<Sender<EventTriggerMessage>>,
    range_start: u64,
    range_end: u64,
) -> Result<(), ReceiptCollectionError> {
    let message = LogMessage::RangeComplete {
        range_start,
        range_end,
    };

    if let Some(sender) = factory_log_tx {
        if sender.send(message.clone()).await.is_err() {
            tracing::error!(
                "Failed to send RangeComplete({}-{}) to factory_log_tx - receiver dropped",
                range_start,
                range_end
            );
            return Err(ReceiptCollectionError::ChannelSend(format!(
                "factory_log_tx (RangeComplete {}-{}) - receiver dropped",
                range_start, range_end
            )));
        }
    }
    if let Some(sender) = log_tx {
        if sender.send(message).await.is_err() {
            tracing::error!(
                "Failed to send RangeComplete({}-{}) to log_tx - receiver dropped",
                range_start,
                range_end
            );
            return Err(ReceiptCollectionError::ChannelSend(format!(
                "log_tx (RangeComplete {}-{}) - receiver dropped",
                range_start, range_end
            )));
        }
    }
    if let Some(sender) = event_trigger_tx {
        if sender.send(EventTriggerMessage::RangeComplete { range_start, range_end }).await.is_err() {
            tracing::error!(
                "Failed to send RangeComplete({}-{}) to event_trigger_tx - receiver dropped",
                range_start,
                range_end
            );
            return Err(ReceiptCollectionError::ChannelSend(format!(
                "event_trigger_tx (RangeComplete {}-{}) - receiver dropped",
                range_start, range_end
            )));
        }
    }

    Ok(())
}

async fn send_logs_to_channels(
    batch_logs: Vec<LogData>,
    log_tx: &Option<Sender<LogMessage>>,
    factory_log_tx: &Option<Sender<LogMessage>>,
    log_tx_metrics: &mut ChannelMetrics,
    factory_log_tx_metrics: &mut ChannelMetrics,
    log_tx_capacity: usize,
    factory_log_tx_capacity: usize,
    total_channel_send_time: &mut std::time::Duration,
) -> Result<(), ReceiptCollectionError> {
    let log_count = batch_logs.len();

    if let Some(sender) = factory_log_tx {
        let capacity_before = sender.capacity();
        let fill_pct = 100.0 * (1.0 - capacity_before as f64 / factory_log_tx_capacity as f64);

        if fill_pct > 90.0 {
            tracing::warn!(
                "factory_log_tx channel at {:.1}% capacity - downstream may be slow",
                fill_pct
            );
        }

        let send_start = Instant::now();
        if sender.send(LogMessage::Logs(batch_logs.clone())).await.is_err() {
            tracing::error!(
                "Failed to send {} logs to factory_log_tx - receiver dropped",
                log_count
            );
            return Err(ReceiptCollectionError::ChannelSend(format!(
                "factory_log_tx (Logs batch of {}) - receiver dropped",
                log_count
            )));
        }
        let send_time = send_start.elapsed();
        *total_channel_send_time += send_time;

        factory_log_tx_metrics.record_send(send_time, capacity_before, factory_log_tx_capacity);
        factory_log_tx_metrics.total_logs_sent += log_count as u64;
    }

    if let Some(sender) = log_tx {
        let capacity_before = sender.capacity();
        let fill_pct = 100.0 * (1.0 - capacity_before as f64 / log_tx_capacity as f64);

        if fill_pct > 90.0 {
            tracing::warn!(
                "log_tx channel at {:.1}% capacity - downstream may be slow",
                fill_pct
            );
        }

        let send_start = Instant::now();
        if sender.send(LogMessage::Logs(batch_logs)).await.is_err() {
            tracing::error!(
                "Failed to send {} logs to log_tx - receiver dropped",
                log_count
            );
            return Err(ReceiptCollectionError::ChannelSend(format!(
                "log_tx (Logs batch of {}) - receiver dropped",
                log_count
            )));
        }
        let send_time = send_start.elapsed();
        *total_channel_send_time += send_time;

        log_tx_metrics.record_send(send_time, capacity_before, log_tx_capacity);
        log_tx_metrics.total_logs_sent += log_count as u64;
    }

    Ok(())
}

async fn process_range(
    range: &BlockRange,
    blocks: Vec<BlockInfo>,
    client: &UnifiedRpcClient,
    receipt_fields: &Option<Vec<ReceiptField>>,
    schema: &Arc<Schema>,
    output_dir: &Path,
    log_tx: &Option<Sender<LogMessage>>,
    factory_log_tx: &Option<Sender<LogMessage>>,
    event_trigger_tx: &Option<Sender<EventTriggerMessage>>,
    event_matchers: &[EventTriggerMatcher],
    rpc_batch_size: usize,
    log_tx_metrics: &mut ChannelMetrics,
    factory_log_tx_metrics: &mut ChannelMetrics,
    log_tx_capacity: usize,
    factory_log_tx_capacity: usize,
    block_receipts_method: Option<&str>,
    block_receipt_concurrency: usize,
) -> Result<(), ReceiptCollectionError> {
    let range_start_time = Instant::now();

    let mut all_minimal_records: Vec<MinimalReceiptRecord> = Vec::new();
    let mut all_full_records: Vec<FullReceiptRecord> = Vec::new();

    // Timing metrics (always tracked)
    let mut total_rpc_time = std::time::Duration::ZERO;
    let mut total_process_time = std::time::Duration::ZERO;
    let mut total_channel_send_time = std::time::Duration::ZERO;

    #[cfg(feature = "bench")]
    let mut rpc_time = std::time::Duration::ZERO;
    #[cfg(feature = "bench")]
    let mut process_time = std::time::Duration::ZERO;

    if let Some(method) = block_receipts_method {
        let blocks_with_txs: Vec<&BlockInfo> = blocks
            .iter()
            .filter(|b| !b.tx_hashes.is_empty())
            .collect();

        let total_blocks = blocks_with_txs.len();
        tracing::info!(
            "Fetching receipts for blocks {}-{}: {} blocks with txs using {} (concurrency: {})",
            range.start, range.end - 1, total_blocks, method, block_receipt_concurrency
        );

        for (batch_idx, batch) in blocks_with_txs.chunks(block_receipt_concurrency).enumerate() {
            let batch_start = batch_idx * block_receipt_concurrency;
            let batch_end = std::cmp::min(batch_start + batch.len(), total_blocks);

            tracing::debug!(
                "receipts: fetching batch {}/{} (blocks {}-{} of {})",
                batch_idx + 1,
                (total_blocks + block_receipt_concurrency - 1) / block_receipt_concurrency,
                batch_start + 1,
                batch_end,
                total_blocks
            );

            let block_numbers: Vec<alloy::rpc::types::BlockNumberOrTag> = batch
                .iter()
                .map(|b| alloy::rpc::types::BlockNumberOrTag::Number(b.block_number))
                .collect();

            let rpc_start = Instant::now();
            let all_receipts = match client
                .get_block_receipts_concurrent(method, block_numbers, block_receipt_concurrency)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(
                        "receipts: RPC error fetching block receipts for batch starting at block {}: {:?}",
                        batch[0].block_number,
                        e
                    );
                    return Err(e.into());
                }
            };
            let rpc_elapsed = rpc_start.elapsed();
            total_rpc_time += rpc_elapsed;
            #[cfg(feature = "bench")]
            {
                rpc_time += rpc_elapsed;
            }

            let process_start = Instant::now();
            let mut batch_logs: Vec<LogData> = Vec::new();

            for (block, receipts) in batch.iter().zip(all_receipts.into_iter()) {
                let tx_block_info: Vec<(B256, u64, u64)> = receipts
                    .iter()
                    .map(|r| {
                        let tx_hash = r.as_ref().map_or(B256::ZERO, |r| r.transaction_hash);
                        (tx_hash, block.block_number, block.timestamp)
                    })
                    .collect();

                match receipt_fields {
                    Some(_) => {
                        let records = match process_receipts_minimal(&receipts, &tx_block_info, &mut batch_logs) {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("receipts: error processing minimal receipts: {:?}", e);
                                return Err(e);
                            }
                        };
                        all_minimal_records.extend(records);
                    }
                    None => {
                        let records = match process_receipts_full(&receipts, &tx_block_info, &mut batch_logs) {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("receipts: error processing full receipts: {:?}", e);
                                return Err(e);
                            }
                        };
                        all_full_records.extend(records);
                    }
                }
            }
            
            let process_elapsed = process_start.elapsed();
            total_process_time += process_elapsed;

            if !batch_logs.is_empty() {
                // Extract event triggers before sending logs (logs are consumed by send)
                let triggers = if !event_matchers.is_empty() {
                    extract_event_triggers(&batch_logs, event_matchers)
                } else {
                    Vec::new()
                };

                send_logs_to_channels(
                    batch_logs,
                    log_tx,
                    factory_log_tx,
                    log_tx_metrics,
                    factory_log_tx_metrics,
                    log_tx_capacity,
                    factory_log_tx_capacity,
                    &mut total_channel_send_time,
                )
                .await?;

                // Send event triggers if any
                if !triggers.is_empty() {
                    if let Some(tx) = event_trigger_tx {
                        tx.send(EventTriggerMessage::Triggers(triggers))
                            .await
                            .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
                    }
                }
            }

            #[cfg(feature = "bench")]
            {
                process_time += process_elapsed;
            }
        }
    } else {
        let mut tx_block_info: Vec<(B256, u64, u64)> = Vec::new();
        for block in &blocks {
            for tx_hash in &block.tx_hashes {
                tx_block_info.push((*tx_hash, block.block_number, block.timestamp));
            }
        }

        let total_txs = tx_block_info.len();
        let total_batches = (total_txs + rpc_batch_size - 1) / rpc_batch_size;
        tracing::info!(
            "Fetching receipts for blocks {}-{}: {} transactions in {} batches",
            range.start, range.end - 1, total_txs, total_batches
        );

        if tx_block_info.is_empty() {
            tracing::info!(
                "No transactions in blocks {}-{}, writing empty receipts file",
                range.start,
                range.end - 1
            );
        }

        for (chunk_idx, chunk) in tx_block_info.chunks(rpc_batch_size).enumerate() {
            let tx_hashes: Vec<B256> = chunk.iter().map(|(h, _, _)| *h).collect();

            tracing::debug!(
                "receipts: fetching batch {}/{}, {} transactions",
                chunk_idx + 1,
                total_batches,
                tx_hashes.len()
            );

            let rpc_start = Instant::now();
            let receipts = match client.get_transaction_receipts_batch(tx_hashes).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("receipts: RPC error fetching receipts: {:?}", e);
                    return Err(e.into());
                }
            };
            let rpc_elapsed = rpc_start.elapsed();
            total_rpc_time += rpc_elapsed;
            #[cfg(feature = "bench")]
            {
                rpc_time += rpc_elapsed;
            }

            let process_start = Instant::now();
            let mut batch_logs: Vec<LogData> = Vec::new();

            match receipt_fields {
                Some(_) => {
                    let records = match process_receipts_minimal(&receipts, chunk, &mut batch_logs) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!("receipts: error processing minimal receipts: {:?}", e);
                            return Err(e);
                        }
                    };
                    all_minimal_records.extend(records);
                }
                None => {
                    let records = match process_receipts_full(&receipts, chunk, &mut batch_logs) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!("receipts: error processing full receipts: {:?}", e);
                            return Err(e);
                        }
                    };
                    all_full_records.extend(records);
                }
            }

            let process_elapsed = process_start.elapsed();
            total_process_time += process_elapsed;

            if !batch_logs.is_empty() {
                // Extract event triggers before sending logs (logs are consumed by send)
                let triggers = if !event_matchers.is_empty() {
                    extract_event_triggers(&batch_logs, event_matchers)
                } else {
                    Vec::new()
                };

                send_logs_to_channels(
                    batch_logs,
                    log_tx,
                    factory_log_tx,
                    log_tx_metrics,
                    factory_log_tx_metrics,
                    log_tx_capacity,
                    factory_log_tx_capacity,
                    &mut total_channel_send_time,
                )
                .await?;

                // Send event triggers if any
                if !triggers.is_empty() {
                    if let Some(tx) = event_trigger_tx {
                        tx.send(EventTriggerMessage::Triggers(triggers))
                            .await
                            .map_err(|e| ReceiptCollectionError::ChannelSend(e.to_string()))?;
                    }
                }
            }
            #[cfg(feature = "bench")]
            {
                process_time += process_elapsed;
            }
        }
    }

    let write_start = Instant::now();
    let total_receipts = match receipt_fields {
        Some(fields) => {
            let count = all_minimal_records.len();
            let schema_clone = schema.clone();
            let fields_vec = fields.to_vec();
            let output_path = output_dir.join(range.file_name());
            tokio::task::spawn_blocking(move || {
                write_minimal_receipts_to_parquet(&all_minimal_records, &schema_clone, &fields_vec, &output_path)
            })
            .await
            .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))??;
            count
        }
        None => {
            let count = all_full_records.len();
            let schema_clone = schema.clone();
            let output_path = output_dir.join(range.file_name());
            tokio::task::spawn_blocking(move || {
                write_full_receipts_to_parquet(&all_full_records, &schema_clone, &output_path)
            })
            .await
            .map_err(|e| ReceiptCollectionError::JoinError(e.to_string()))??;
            count
        }
    };
    let total_write_time = write_start.elapsed();

    #[cfg(feature = "bench")]
    {
        crate::bench::record("receipts", range.start, range.end, total_receipts, rpc_time, process_time, total_write_time);
    }

    let total_time = range_start_time.elapsed();
    let rpc_pct = (total_rpc_time.as_secs_f64() / total_time.as_secs_f64()) * 100.0;
    let process_pct = (total_process_time.as_secs_f64() / total_time.as_secs_f64()) * 100.0;
    let channel_pct = (total_channel_send_time.as_secs_f64() / total_time.as_secs_f64()) * 100.0;
    let write_pct = (total_write_time.as_secs_f64() / total_time.as_secs_f64()) * 100.0;

    tracing::info!(
        "Receipts {}-{}: {} receipts in {:.1}s | RPC: {:.1}s ({:.0}%) | Process: {:.1}s ({:.0}%) | Channel: {:.1}s ({:.0}%) | Write: {:.1}s ({:.0}%)",
        range.start,
        range.end - 1,
        total_receipts,
        total_time.as_secs_f64(),
        total_rpc_time.as_secs_f64(),
        rpc_pct,
        total_process_time.as_secs_f64(),
        process_pct,
        total_channel_send_time.as_secs_f64(),
        channel_pct,
        total_write_time.as_secs_f64(),
        write_pct
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

        let Some(receipt) = receipt_opt.as_ref() else {
            continue;
        };

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

        let Some(receipt) = receipt_opt.as_ref() else {
            continue;
        };

        let inner = &receipt.inner;
        let logs = inner.logs();

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

fn scan_existing_logs_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("logs_") && name.ends_with(".parquet") {
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
