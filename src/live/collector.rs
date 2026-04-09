//! Live block collector that processes blocks from WebSocket subscription.
//!
//! The collector:
//! 1. Receives block headers from WebSocket subscription
//! 2. Detects reorgs using parent hash verification
//! 3. Fetches full block data and receipts via HTTP
//! 4. Stores data in live storage format
//! 5. Forwards to decoder channels

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, B256};
use alloy::rpc::types::BlockNumberOrTag;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_postgres::types::ToSql;

use super::catchup::{CollectionResumeRequest, CollectionResumeStage, LiveCatchupService};
use super::eth_calls::{CollectedEthCallBatch, LiveEthCallCollector};
use super::progress::LiveProgressTracker;
use super::reorg::{ReorgDetector, ReorgEvent};
use super::storage::{LiveStorage, StorageError};
use super::types::{
    LiveBlock, LiveBlockStatus, LiveFactoryAddresses, LiveLog, LiveMessage, LiveModeConfig,
    LivePipelineExpectations, LiveReceipt,
};
use super::TransformRetryRequest;
use crate::db::DbError;
use crate::db::DbPool;
use crate::decoding::DecoderMessage;
use crate::raw_data::historical::factories::FactoryMatcher;
use crate::raw_data::historical::receipts::LogData;
use crate::rpc::{RpcError, UnifiedRpcClient, WsEvent};
use crate::transformations::ReorgMessage;
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::FactoryParameterLocation;

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Database error: {0}")]
    Database(#[from] DbError),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Block not found: {0}")]
    BlockNotFound(u64),
    #[error("Reorg cleanup failed for blocks: {0:?}")]
    ReorgCleanupFailed(Vec<u64>),
}

/// Collects live blocks from WebSocket and processes them.
pub struct LiveCollector {
    chain: Arc<ChainConfig>,
    http_client: Arc<UnifiedRpcClient>,
    storage: LiveStorage,
    reorg_detector: ReorgDetector,
    config: LiveModeConfig,
    /// Buffer for blocks that arrive faster than we can process.
    buffer: VecDeque<LiveBlock>,
    /// Progress tracker for coordination with compaction service.
    progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
    /// Expected starting block from historical processing or previous live session.
    expected_start_block: Option<u64>,
    /// Factory matchers for extracting new contract addresses from factory events.
    factory_matchers: Arc<Vec<FactoryMatcher>>,
    /// Pre-built index mapping topic0 -> indices into `factory_matchers` for O(n) extraction.
    factory_topic_index: HashMap<[u8; 32], Vec<usize>>,
    /// Optional eth_call collector for live mode.
    eth_call_collector: Option<LiveEthCallCollector>,
    /// Database pool for status file reconstruction during catchup.
    db_pool: Option<Arc<DbPool>>,
    /// Runtime expectations for optional pipeline stages.
    expectations: LivePipelineExpectations,
    /// Channel for direct transformation retries.
    transform_retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
}

impl LiveCollector {
    /// Create a new LiveCollector.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain: Arc<ChainConfig>,
        http_client: Arc<UnifiedRpcClient>,
        config: LiveModeConfig,
        progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
        factory_matchers: Arc<Vec<FactoryMatcher>>,
        eth_call_collector: Option<LiveEthCallCollector>,
        db_pool: Option<Arc<DbPool>>,
        expectations: LivePipelineExpectations,
        transform_retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
    ) -> Self {
        let storage = LiveStorage::new(&chain.name);
        let mut reorg_detector = ReorgDetector::new(config.reorg_depth);

        // Get expected start block from storage (max block + 1)
        let expected_start_block = match storage.max_block_number() {
            Ok(Some(max)) => {
                tracing::info!(
                    "Live storage has blocks up to {}, expecting next block to be {}",
                    max,
                    max + 1
                );
                Some(max)
            }
            Ok(None) => {
                tracing::info!("Live storage is empty, no gap detection on first block");
                None
            }
            Err(e) => {
                tracing::warn!("Failed to query max block number: {}", e);
                None
            }
        };

        // Seed reorg detector from existing storage on restart
        match storage.get_recent_blocks_for_reorg(config.reorg_depth) {
            Ok(recent) if !recent.is_empty() => {
                tracing::info!(
                    "Seeding reorg detector with {} blocks from storage",
                    recent.len()
                );
                reorg_detector.seed(recent);
            }
            Err(e) => {
                tracing::warn!("Failed to seed reorg detector from storage: {}", e);
            }
            _ => {}
        }

        if !factory_matchers.is_empty() {
            tracing::info!(
                "Live collector initialized with {} factory matchers",
                factory_matchers.len()
            );
        }

        if eth_call_collector.is_some() {
            tracing::info!("Live collector initialized with eth_call collector");
        }

        // Pre-build topic0 -> matcher index map for O(n) factory extraction
        let mut factory_topic_index: HashMap<[u8; 32], Vec<usize>> = HashMap::new();
        for (idx, matcher) in factory_matchers.iter().enumerate() {
            factory_topic_index
                .entry(matcher.event_topic0)
                .or_default()
                .push(idx);
        }

        Self {
            chain,
            http_client,
            storage,
            reorg_detector,
            config,
            buffer: VecDeque::new(),
            progress_tracker,
            expected_start_block,
            factory_matchers,
            factory_topic_index,
            eth_call_collector,
            db_pool,
            expectations,
            transform_retry_tx,
        }
    }

    /// Run the live collector.
    ///
    /// Listens for WebSocket events and processes blocks.
    /// The optional `transform_reorg_tx` sends reorg notifications to the transformation engine
    /// so it can clean up pending events for orphaned blocks.
    pub async fn run(
        mut self,
        mut ws_rx: mpsc::UnboundedReceiver<WsEvent>,
        live_tx: mpsc::Sender<LiveMessage>,
        log_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        // Ensure storage directories exist
        self.storage.ensure_dirs()?;

        tracing::info!(
            "Live collector started for chain {} with reorg_depth={}",
            self.chain.name,
            self.config.reorg_depth
        );

        // Run catchup phase for incomplete blocks from previous session
        self.run_catchup_phase(&log_decoder_tx, &eth_call_decoder_tx)
            .await?;

        // Backfill gaps from incomplete previous backfills
        self.backfill_storage_gaps(
            &live_tx,
            &log_decoder_tx,
            &eth_call_decoder_tx,
            &transform_reorg_tx,
        )
        .await?;

        // Gap detection and backfill happen on first block
        let mut first_block_seen = false;
        let expected_start_block = self.expected_start_block;

        while let Some(event) = ws_rx.recv().await {
            match event {
                WsEvent::NewBlock {
                    number,
                    hash,
                    parent_hash,
                    timestamp,
                } => {
                    tracing::debug!(
                        "WebSocket: new block {} (hash={:.8}..)",
                        number,
                        hex::encode(&hash.0[..4])
                    );

                    // Check for gap on first block
                    if !first_block_seen {
                        first_block_seen = true;
                        tracing::info!("First live block received: {}", number);
                        if let Some(last_processed) = expected_start_block {
                            // Calculate the expected next block after the last processed one.
                            // If we receive a block higher than expected_next, there's a gap.
                            let expected_next = last_processed + 1;
                            if number > expected_next {
                                // Gap detected: blocks [expected_next, number - 1] are missing.
                                // For example: last_processed=100, number=103
                                //   -> expected_next=101, gap_start=101, gap_end=102
                                //   -> backfill 2 blocks (101 and 102)
                                let gap_start = expected_next;
                                let gap_end = number - 1;
                                let gap_size = gap_end - gap_start + 1;
                                tracing::warn!(
                                    "Gap detected: expected block {}, received {}. Backfilling {} blocks ({}-{}).",
                                    expected_next,
                                    number,
                                    gap_size,
                                    gap_start,
                                    gap_end,
                                );
                                // Backfill the gap before processing this block
                                if let Err(e) = self
                                    .backfill_blocks(
                                        gap_start,
                                        gap_end,
                                        &live_tx,
                                        &log_decoder_tx,
                                        &eth_call_decoder_tx,
                                        &transform_reorg_tx,
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error backfilling gap blocks {}-{}: {}",
                                        gap_start,
                                        gap_end,
                                        e
                                    );
                                }
                            }
                        }
                    }

                    let block = LiveBlock {
                        number,
                        hash: hash.0,
                        parent_hash: parent_hash.0,
                        timestamp,
                        tx_hashes: vec![], // Will be filled when we fetch the full block
                    };

                    if let Err(e) = self
                        .process_block(
                            block,
                            &live_tx,
                            &log_decoder_tx,
                            &eth_call_decoder_tx,
                            &transform_reorg_tx,
                        )
                        .await
                    {
                        tracing::error!("Error processing block {}: {}", number, e);
                    }
                }

                WsEvent::Disconnected { last_block } => {
                    tracing::warn!(
                        "WebSocket disconnected, last processed block: {:?}",
                        last_block
                    );
                }

                WsEvent::Reconnected {
                    missed_from,
                    missed_to,
                } => {
                    tracing::info!(
                        "WebSocket reconnected, backfilling blocks {} to {}",
                        missed_from,
                        missed_to
                    );

                    if let Err(e) = self
                        .backfill_blocks(
                            missed_from,
                            missed_to,
                            &live_tx,
                            &log_decoder_tx,
                            &eth_call_decoder_tx,
                            &transform_reorg_tx,
                        )
                        .await
                    {
                        tracing::error!(
                            "Error backfilling blocks {}-{}: {}",
                            missed_from,
                            missed_to,
                            e
                        );
                    }
                }
            }
        }

        tracing::info!("Live collector shutting down");
        Ok(())
    }

    /// Process a single block.
    async fn process_block(
        &mut self,
        block: LiveBlock,
        live_tx: &mpsc::Sender<LiveMessage>,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        let block_number = block.number;

        // Check for reorg
        if let Some(reorg_event) = self.reorg_detector.process_block(&block) {
            self.handle_reorg(
                &reorg_event,
                live_tx,
                log_decoder_tx,
                eth_call_decoder_tx,
                transform_reorg_tx,
            )
            .await?;
        }

        // Store the block header
        self.storage.write_block(&block)?;
        self.storage
            .write_status(block_number, &LiveBlockStatus::collected())?;

        // Fetch full block with transactions
        let full_block = self.fetch_full_block(block_number).await?;

        // Use the fetched block with all its transaction hashes
        let updated_block = full_block;

        self.storage.write_block(&updated_block)?;

        // Update status
        self.storage.update_status_atomic(block_number, |status| {
            status.block_fetched = true;
        })?;

        tracing::info!(
            "Block {} collected: {} txs",
            block_number,
            updated_block.tx_hashes.len()
        );

        // Fetch receipts and logs
        let (receipts, logs) = self.fetch_receipts_and_logs(block_number).await?;

        self.storage.write_receipts(block_number, &receipts)?;
        self.storage.write_logs(block_number, &logs)?;

        tracing::info!(
            "Block {} receipts collected: {} logs",
            block_number,
            logs.len()
        );

        // Extract factory addresses if we have matchers configured
        let factory_addresses = self.extract_factory_addresses(&logs, updated_block.timestamp);
        let has_factory_addresses = !factory_addresses.addresses_by_collection.is_empty();

        if has_factory_addresses {
            self.storage
                .write_factories(block_number, &factory_addresses)?;
            tracing::debug!(
                "Extracted {} factory addresses from block {}",
                factory_addresses
                    .addresses_by_collection
                    .values()
                    .map(|v| v.len())
                    .sum::<usize>(),
                block_number
            );
        }

        // Update status
        self.storage.update_status_atomic(block_number, |status| {
            status.receipts_collected = true;
            status.logs_collected = true;
            // Factory extraction is always done at this point (even if no factories found)
            status.factories_extracted = true;
        })?;

        // Send to live message channel
        live_tx
            .send(LiveMessage::Block(updated_block.clone()))
            .await
            .map_err(|_| CollectorError::ChannelClosed)?;

        // Send logs to decoder if configured
        if let Some(decoder_tx) = log_decoder_tx {
            // Send factory addresses first so decoder can use them when processing logs
            if has_factory_addresses {
                let factory_addrs: HashMap<String, Vec<Address>> = factory_addresses
                    .addresses_by_collection
                    .iter()
                    .map(|(name, addrs)| {
                        (
                            name.clone(),
                            addrs.iter().map(|(_, addr)| Address::from(*addr)).collect(),
                        )
                    })
                    .collect();

                if let Err(e) = decoder_tx
                    .send(DecoderMessage::FactoryAddresses {
                        range_start: block_number,
                        range_end: block_number + 1,
                        addresses: factory_addrs,
                    })
                    .await
                {
                    tracing::warn!(
                        "Failed to send factory addresses for block {} to decoder: {}",
                        block_number,
                        e
                    );
                }
            }

            // Send logs (even if empty, for consistency)
            let log_data: Vec<LogData> = logs
                .iter()
                .map(|log| LogData {
                    block_number,
                    block_timestamp: updated_block.timestamp,
                    transaction_hash: B256::from(log.transaction_hash),
                    log_index: log.log_index,
                    address: log.address,
                    topics: log.topics.clone(),
                    data: log.data.clone(),
                })
                .collect();

            if let Err(e) = decoder_tx
                .send(DecoderMessage::LogsReady {
                    range_start: block_number,
                    range_end: block_number + 1, // Exclusive end for single block
                    logs: log_data,
                    live_mode: true, // Live mode: write to bincode
                    has_factory_matchers: !self.factory_matchers.is_empty(),
                })
                .await
            {
                tracing::warn!(
                    "Failed to send logs for block {} to decoder: {}",
                    block_number,
                    e
                );
            }
        }

        // If no log decoder is configured, mark logs as decoded
        if log_decoder_tx.is_none() {
            self.storage.update_status_atomic(block_number, |status| {
                status.logs_decoded = true;
            })?;
        }

        // Collect eth_calls if configured
        if self.eth_call_collector.is_some() {
            // Update factory addresses in collector
            if let Some(ref mut eth_collector) = self.eth_call_collector {
                eth_collector.update_factory_addresses(&factory_addresses);
            }

            // Collect eth_calls for this block
            let batch_result = {
                let eth_collector = self
                    .eth_call_collector
                    .as_ref()
                    .expect("checked eth_call_collector above");
                eth_collector
                    .collect_for_block(
                        block_number,
                        updated_block.timestamp,
                        &logs,
                        &factory_addresses,
                        false,
                    )
                    .await
            };

            match batch_result {
                Ok(batch) => {
                    let count = batch.calls.len();
                    self.commit_eth_call_batch(
                        block_number,
                        batch,
                        eth_call_decoder_tx,
                        false,
                        false,
                    )
                    .await?;
                    if count != 0 {
                        tracing::info!("Block {} eth_calls collected: {}", block_number, count);
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to collect eth_calls for block {}: {}",
                        block_number,
                        e
                    );
                }
            }
        } else {
            // No eth_call collector configured - mark as collected and decoded
            self.storage.update_status_atomic(block_number, |status| {
                status.eth_calls_collected = true;
                status.eth_calls_decoded = true;
            })?;
        };

        // If no handlers are registered, mark transformed=true
        // (decoders will still set logs_decoded/eth_calls_decoded as they finish)
        if let Some(ref tracker) = self.progress_tracker {
            tracker
                .lock()
                .await
                .mark_transformed_if_no_handlers(block_number);
        }

        Ok(())
    }

    /// Handle a reorg event.
    async fn handle_reorg(
        &mut self,
        event: &ReorgEvent,
        live_tx: &mpsc::Sender<LiveMessage>,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        tracing::warn!(
            "Handling reorg: common_ancestor={}, orphaned={:?}, depth={}",
            event.common_ancestor,
            event.orphaned,
            event.depth
        );

        if event.is_deep {
            tracing::error!(
                "DEEP REORG detected at block {}: reorg goes beyond retention window. \
                 Orphaning all {} tracked blocks as best-effort cleanup. \
                 Manual verification may be needed.",
                event._new_block_number,
                event.orphaned.len()
            );
        }

        // Delete orphaned blocks from storage (including decoded data)
        // Only clear progress for blocks that were successfully deleted
        let mut failed_deletions: Vec<u64> = Vec::new();
        for &orphaned_number in &event.orphaned {
            match self.storage.delete_all(orphaned_number) {
                Ok(()) => {
                    // Only clear progress if deletion succeeded
                    if let Some(ref tracker) = self.progress_tracker {
                        tracker.lock().await.clear_block(orphaned_number);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to delete orphaned block {}: {}", orphaned_number, e);
                    failed_deletions.push(orphaned_number);
                }
            }
        }

        // Compute which blocks were actually cleaned up
        let successfully_deleted: Vec<u64> = event
            .orphaned
            .iter()
            .filter(|b| !failed_deletions.contains(b))
            .copied()
            .collect();

        // If ALL deletions failed, don't broadcast stale reorg data — fail immediately
        if successfully_deleted.is_empty() && !failed_deletions.is_empty() {
            return Err(CollectorError::ReorgCleanupFailed(failed_deletions));
        }

        // Broadcast reorg to decoder channels (only for successfully deleted blocks)
        broadcast_reorg_to_decoders(
            event.common_ancestor,
            &successfully_deleted,
            log_decoder_tx,
            eth_call_decoder_tx,
        )
        .await;

        // Notify transformation engine of reorg so it can clean up pending events
        if let Some(transform_tx) = transform_reorg_tx {
            if let Err(e) = transform_tx
                .send(ReorgMessage {
                    common_ancestor: event.common_ancestor,
                    orphaned: successfully_deleted.clone(),
                })
                .await
            {
                tracing::warn!(
                    "Failed to send reorg notification to transform engine: {}",
                    e
                );
            }
        }

        // Notify downstream of reorg
        live_tx
            .send(LiveMessage::Reorg {
                common_ancestor: event.common_ancestor,
                orphaned: successfully_deleted.clone(),
            })
            .await
            .map_err(|_| CollectorError::ChannelClosed)?;

        // Return error if any deletions failed (partial failure)
        if !failed_deletions.is_empty() {
            return Err(CollectorError::ReorgCleanupFailed(failed_deletions));
        }

        Ok(())
    }

    /// Backfill blocks that were missed during disconnect.
    async fn backfill_blocks(
        &mut self,
        from: u64,
        to: u64,
        live_tx: &mpsc::Sender<LiveMessage>,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        tracing::info!("Backfilling {} blocks ({} to {})", to - from + 1, from, to);

        for block_number in from..=to {
            let block = self.fetch_full_block(block_number).await?;

            // Process as if it came from WebSocket
            self.process_block(
                block,
                live_tx,
                log_decoder_tx,
                eth_call_decoder_tx,
                transform_reorg_tx,
            )
            .await?;
        }

        Ok(())
    }

    /// Backfill gaps in storage from incomplete previous backfills.
    async fn backfill_storage_gaps(
        &mut self,
        live_tx: &mpsc::Sender<LiveMessage>,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        let gaps = self.storage.find_gaps()?;
        if gaps.is_empty() {
            return Ok(());
        }

        let total_missing: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
        tracing::info!(
            "Found {} gaps ({} missing blocks) in storage, backfilling",
            gaps.len(),
            total_missing
        );

        for (start, end) in gaps {
            self.backfill_blocks(
                start,
                end,
                live_tx,
                log_decoder_tx,
                eth_call_decoder_tx,
                transform_reorg_tx,
            )
            .await?;
        }

        Ok(())
    }

    /// Fetch full block data via HTTP.
    async fn fetch_full_block(&self, block_number: u64) -> Result<LiveBlock, CollectorError> {
        // Use full_transactions=false to only fetch tx hashes, not full transaction objects.
        // This avoids deserialization errors for L2-specific transaction types (e.g., OP deposit
        // transactions with type 0x7e) that alloy's default types don't support.
        let block = self
            .http_client
            .get_block_by_number(BlockNumberOrTag::Number(block_number), false)
            .await?
            .ok_or(CollectorError::BlockNotFound(block_number))?;

        let tx_hashes: Vec<[u8; 32]> = block.transactions.hashes().map(|h| h.0).collect();

        Ok(LiveBlock {
            number: block_number,
            hash: block.header.hash.0,
            parent_hash: block.header.parent_hash.0,
            timestamp: block.header.timestamp,
            tx_hashes,
        })
    }

    /// Fetch receipts and extract logs for a block.
    async fn fetch_receipts_and_logs(
        &self,
        block_number: u64,
    ) -> Result<(Vec<LiveReceipt>, Vec<LiveLog>), CollectorError> {
        let method_name = self
            .chain
            .block_receipts_method
            .as_ref()
            .map(|m| m.as_str())
            .unwrap_or("eth_getBlockReceipts");

        let receipts = self
            .http_client
            .get_block_receipts(method_name, BlockNumberOrTag::Number(block_number))
            .await?;

        let block = self.storage.read_block(block_number)?;

        let mut live_receipts = Vec::new();
        let mut all_logs = Vec::new();

        for (idx, receipt_opt) in receipts.into_iter().enumerate() {
            if let Some(receipt) = receipt_opt {
                let live_receipt = LiveReceipt {
                    transaction_hash: receipt.transaction_hash.0,
                    transaction_index: idx as u32,
                    block_number,
                    block_timestamp: block.timestamp,
                    from: receipt.from.0 .0,
                    to: receipt.to.map(|a| a.0 .0),
                    logs: receipt
                        .inner
                        .logs()
                        .iter()
                        .enumerate()
                        .map(|(log_idx, log)| LiveLog {
                            address: log.address().0 .0,
                            topics: log.topics().iter().map(|t| t.0).collect(),
                            data: log.data().data.to_vec(),
                            log_index: log_idx as u32,
                            transaction_index: idx as u32,
                            transaction_hash: receipt.transaction_hash.0,
                        })
                        .collect(),
                };

                all_logs.extend(live_receipt.logs.clone());
                live_receipts.push(live_receipt);
            }
        }

        Ok((live_receipts, all_logs))
    }

    /// Extract factory addresses from logs using configured matchers.
    ///
    /// Uses a pre-built topic0 index for O(n) lookup instead of O(n*m) nested iteration.
    fn extract_factory_addresses(
        &self,
        logs: &[LiveLog],
        block_timestamp: u64,
    ) -> LiveFactoryAddresses {
        let mut addresses_by_collection: HashMap<String, Vec<(u64, [u8; 20])>> = HashMap::new();

        if self.factory_matchers.is_empty() {
            return LiveFactoryAddresses::default();
        }

        for log in logs {
            // Skip logs with no topics
            if log.topics.is_empty() {
                continue;
            }

            // Look up matchers by topic0
            let Some(matcher_indices) = self.factory_topic_index.get(&log.topics[0]) else {
                continue;
            };

            for &idx in matcher_indices {
                let matcher = &self.factory_matchers[idx];

                // Check if log address matches factory contract
                if log.address != matcher.factory_contract_address {
                    continue;
                }

                // Extract address based on parameter location
                let extracted_address = match &matcher.param_location {
                    FactoryParameterLocation::Topic(topic_idx) => {
                        if *topic_idx < log.topics.len() {
                            let topic = &log.topics[*topic_idx];
                            let mut addr = [0u8; 20];
                            addr.copy_from_slice(&topic[12..32]);
                            Some(addr)
                        } else {
                            None
                        }
                    }
                    FactoryParameterLocation::Data(indices) => {
                        decode_address_from_data(&log.data, &matcher.data_types, indices)
                    }
                };

                if let Some(addr) = extracted_address {
                    addresses_by_collection
                        .entry(matcher.collection_name.clone())
                        .or_default()
                        .push((block_timestamp, addr));
                }
            }
        }

        LiveFactoryAddresses {
            addresses_by_collection,
        }
    }

    async fn resume_collection_blocks(
        &mut self,
        requests: &[CollectionResumeRequest],
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<usize, CollectorError> {
        let mut resumed = 0;

        for request in requests {
            self.resume_collection_block(request, log_decoder_tx, eth_call_decoder_tx)
                .await?;
            resumed += 1;
        }

        Ok(resumed)
    }

    async fn resume_collection_block(
        &mut self,
        request: &CollectionResumeRequest,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<(), CollectorError> {
        tracing::info!(
            "Resuming collection for block {} from stage {:?}",
            request.block_number,
            request.stage
        );

        match request.stage {
            CollectionResumeStage::FetchBlock => {
                // Reset is deferred into resume_from_block_fetch, after the
                // RPC fetch succeeds, so a transient failure won't destroy
                // existing local state.
                self.resume_from_block_fetch(
                    request.block_number,
                    log_decoder_tx,
                    eth_call_decoder_tx,
                )
                .await
            }
            CollectionResumeStage::FetchReceiptsAndLogs => {
                self.resume_from_receipts_and_logs(
                    request.block_number,
                    log_decoder_tx,
                    eth_call_decoder_tx,
                )
                .await
            }
            CollectionResumeStage::CollectEthCalls => {
                self.resume_eth_call_collection(
                    request.block_number,
                    eth_call_decoder_tx,
                    request.retry_transform_after_decode,
                )
                .await
            }
        }
    }

    async fn reset_downstream_from_block_fetch(
        &mut self,
        block_number: u64,
    ) -> Result<(), CollectorError> {
        self.storage.delete_receipts(block_number)?;
        self.storage.delete_logs(block_number)?;
        self.storage.delete_factories(block_number)?;
        self.storage.delete_eth_calls(block_number)?;
        self.storage.delete_all_decoded_logs(block_number)?;
        self.storage.delete_all_decoded_calls(block_number)?;
        self.storage.delete_snapshots(block_number)?;
        self.clear_persisted_progress(block_number).await?;

        let expectations = &self.expectations;
        self.storage.update_status_atomic(block_number, |status| {
            status.block_fetched = false;
            status.receipts_collected = false;
            status.logs_collected = false;
            status.factories_extracted = false;
            status.eth_calls_collected = false;
            status.logs_decoded = false;
            status.eth_calls_decoded = false;
            status.transformed = false;
            status.completed_handlers.clear();
            status.failed_handlers.clear();
            status.apply_expectations(expectations);
        })?;

        Ok(())
    }

    async fn reset_downstream_from_logs(
        &mut self,
        block_number: u64,
    ) -> Result<(), CollectorError> {
        self.storage.delete_factories(block_number)?;
        self.storage.delete_eth_calls(block_number)?;
        self.storage.delete_all_decoded_logs(block_number)?;
        self.storage.delete_all_decoded_calls(block_number)?;
        self.storage.delete_snapshots(block_number)?;
        self.clear_persisted_progress(block_number).await?;

        let expectations = &self.expectations;
        self.storage.update_status_atomic(block_number, |status| {
            status.receipts_collected = false;
            status.logs_collected = false;
            status.factories_extracted = false;
            status.eth_calls_collected = false;
            status.logs_decoded = false;
            status.eth_calls_decoded = false;
            status.transformed = false;
            status.completed_handlers.clear();
            status.failed_handlers.clear();
            status.apply_expectations(expectations);
        })?;

        Ok(())
    }

    async fn reset_eth_call_state(&mut self, block_number: u64) -> Result<(), CollectorError> {
        self.storage.delete_eth_calls(block_number)?;
        self.storage.delete_all_decoded_calls(block_number)?;
        self.storage.delete_snapshots(block_number)?;

        let expectations = &self.expectations;
        self.storage.update_status_atomic(block_number, |status| {
            status.eth_calls_collected = false;
            status.eth_calls_decoded = false;
            status.transformed = false;
            status.failed_handlers.clear();
            status.apply_expectations(expectations);
        })?;

        Ok(())
    }

    async fn clear_persisted_progress(&mut self, block_number: u64) -> Result<(), CollectorError> {
        if let Some(ref tracker) = self.progress_tracker {
            tracker.lock().await.clear_block(block_number);
        }

        if let Some(ref db_pool) = self.db_pool {
            let chain_id = self.chain.chain_id as i64;
            let block_num = block_number as i64;
            db_pool
                .query(
                    "DELETE FROM _live_progress WHERE chain_id = $1 AND block_number = $2",
                    &[
                        &chain_id as &(dyn ToSql + Sync),
                        &block_num as &(dyn ToSql + Sync),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    async fn resume_from_block_fetch(
        &mut self,
        block_number: u64,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<(), CollectorError> {
        let full_block = self.fetch_full_block(block_number).await?;

        // Fetch succeeded — now safe to reset downstream state
        self.reset_downstream_from_block_fetch(block_number).await?;

        // Update reorg detector so the first live head after restart is
        // compared against the canonical hash, not the stale pre-catchup one.
        self.reorg_detector
            .update_block_hash(block_number, full_block.hash);

        self.storage.write_block(&full_block)?;

        let expectations = &self.expectations;
        self.storage.update_status_atomic(block_number, |status| {
            status.collected = true;
            status.block_fetched = true;
            status.apply_expectations(expectations);
        })?;

        self.resume_from_receipts_and_logs(block_number, log_decoder_tx, eth_call_decoder_tx)
            .await
    }

    async fn resume_from_receipts_and_logs(
        &mut self,
        block_number: u64,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<(), CollectorError> {
        let block = self.storage.read_block(block_number)?;
        let (receipts, logs) = self.fetch_receipts_and_logs(block_number).await?;

        // Fetch succeeded — now safe to reset downstream state.
        // Harmless if already reset by reset_downstream_from_block_fetch.
        self.reset_downstream_from_logs(block_number).await?;

        self.storage.write_receipts(block_number, &receipts)?;
        self.storage.write_logs(block_number, &logs)?;

        let factory_addresses = self.extract_factory_addresses(&logs, block.timestamp);
        if !factory_addresses.addresses_by_collection.is_empty() {
            self.storage
                .write_factories(block_number, &factory_addresses)?;
        }

        let expectations = &self.expectations;
        self.storage.update_status_atomic(block_number, |status| {
            status.receipts_collected = true;
            status.logs_collected = true;
            status.factories_extracted = true;
            status.apply_expectations(expectations);
        })?;

        self.dispatch_logs_for_block(
            block_number,
            block.timestamp,
            &logs,
            &factory_addresses,
            log_decoder_tx,
        )
        .await?;

        if log_decoder_tx.is_none() {
            let expectations = &self.expectations;
            self.storage.update_status_atomic(block_number, |status| {
                status.logs_decoded = true;
                status.apply_expectations(expectations);
            })?;
        }

        // retry_transform_after_decode=false: logs were re-dispatched through the
        // decoder above, so the normal pipeline (decode -> transform) handles it.
        self.resume_eth_call_collection(block_number, eth_call_decoder_tx, false)
            .await
    }

    async fn dispatch_logs_for_block(
        &self,
        block_number: u64,
        block_timestamp: u64,
        logs: &[LiveLog],
        factory_addresses: &LiveFactoryAddresses,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<(), CollectorError> {
        let Some(decoder_tx) = log_decoder_tx else {
            return Ok(());
        };

        if !factory_addresses.addresses_by_collection.is_empty() {
            let factory_addrs: HashMap<String, Vec<Address>> = factory_addresses
                .addresses_by_collection
                .iter()
                .map(|(name, addrs)| {
                    (
                        name.clone(),
                        addrs.iter().map(|(_, addr)| Address::from(*addr)).collect(),
                    )
                })
                .collect();

            if let Err(e) = decoder_tx
                .send(DecoderMessage::FactoryAddresses {
                    range_start: block_number,
                    range_end: block_number + 1,
                    addresses: factory_addrs,
                })
                .await
            {
                tracing::warn!(
                    "Failed to send factory addresses for block {} during resume: {}",
                    block_number,
                    e
                );
            }
        }

        let log_data: Vec<LogData> = logs
            .iter()
            .map(|log| LogData {
                block_number,
                block_timestamp,
                transaction_hash: B256::from(log.transaction_hash),
                log_index: log.log_index,
                address: log.address,
                topics: log.topics.clone(),
                data: log.data.clone(),
            })
            .collect();

        decoder_tx
            .send(DecoderMessage::LogsReady {
                range_start: block_number,
                range_end: block_number + 1,
                logs: log_data,
                live_mode: true,
                has_factory_matchers: !self.factory_matchers.is_empty(),
            })
            .await
            .map_err(|_| CollectorError::ChannelClosed)
    }

    async fn commit_eth_call_batch(
        &mut self,
        block_number: u64,
        batch: CollectedEthCallBatch,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        reset_existing_state: bool,
        retry_transform_after_decode: bool,
    ) -> Result<(), CollectorError> {
        if reset_existing_state {
            self.reset_eth_call_state(block_number).await?;
        }

        // Write data before updating status to preserve invariant:
        // data exists before status says it's collected.
        let empty_batch = batch.calls.is_empty();
        if !empty_batch {
            self.storage.write_eth_calls(block_number, &batch.calls)?;
        }

        let no_decoder = eth_call_decoder_tx.is_none();
        let expectations = &self.expectations;
        self.storage.update_status_atomic(block_number, |status| {
            status.eth_calls_collected = true;

            if empty_batch {
                status.eth_calls_decoded = true;
            }

            if no_decoder {
                status.eth_calls_decoded = true;
            }

            status.apply_expectations(expectations);
        })?;

        if let Some(ref mut eth_collector) = self.eth_call_collector {
            eth_collector.apply_collected_batch(&batch);
        }

        self.dispatch_eth_call_messages(block_number, batch.decoder_messages, eth_call_decoder_tx)
            .await;

        if retry_transform_after_decode && eth_call_decoder_tx.is_none() {
            self.queue_transform_retry(block_number, None).await;
        }

        Ok(())
    }

    async fn dispatch_eth_call_messages(
        &self,
        block_number: u64,
        messages: Vec<DecoderMessage>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) {
        let Some(decoder_tx) = eth_call_decoder_tx else {
            return;
        };

        for message in messages {
            if let Err(e) = decoder_tx.send(message).await {
                tracing::warn!(
                    "Failed to send staged eth_call decoder message for block {}: {}",
                    block_number,
                    e
                );
                break;
            }
        }
    }

    async fn resume_eth_call_collection(
        &mut self,
        block_number: u64,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        retry_transform_after_decode: bool,
    ) -> Result<(), CollectorError> {
        let block = self.storage.read_block(block_number)?;
        let logs = self.storage.read_logs(block_number)?;
        let factory_addresses = self
            .storage
            .read_factories(block_number)
            .unwrap_or_default();

        if self.eth_call_collector.is_some() {
            if let Some(ref mut eth_collector) = self.eth_call_collector {
                eth_collector.update_factory_addresses(&factory_addresses);
            }

            let batch_result = {
                let eth_collector = self
                    .eth_call_collector
                    .as_ref()
                    .expect("checked eth_call_collector above");
                eth_collector
                    .collect_for_block(
                        block_number,
                        block.timestamp,
                        &logs,
                        &factory_addresses,
                        retry_transform_after_decode,
                    )
                    .await
            };

            match batch_result {
                Ok(batch) => {
                    self.commit_eth_call_batch(
                        block_number,
                        batch,
                        eth_call_decoder_tx,
                        true,
                        retry_transform_after_decode,
                    )
                    .await?;
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to resume eth_calls for block {}: {}",
                        block_number,
                        e
                    );
                }
            }
        } else {
            let expectations = &self.expectations;
            self.storage.update_status_atomic(block_number, |status| {
                status.eth_calls_collected = true;
                status.eth_calls_decoded = true;
                status.apply_expectations(expectations);
            })?;

            if retry_transform_after_decode {
                self.queue_transform_retry(block_number, None).await;
            }
        }

        if let Some(ref tracker) = self.progress_tracker {
            tracker
                .lock()
                .await
                .mark_transformed_if_no_handlers(block_number);
        }

        Ok(())
    }

    async fn queue_transform_retry(
        &self,
        block_number: u64,
        missing_handlers: Option<HashSet<String>>,
    ) {
        let Some(retry_tx) = &self.transform_retry_tx else {
            tracing::warn!(
                "Block {} needs transformation retry but no retry channel is configured",
                block_number
            );
            return;
        };

        if let Err(e) = retry_tx
            .send(TransformRetryRequest {
                block_number,
                missing_handlers,
            })
            .await
        {
            tracing::warn!(
                "Failed to queue transform retry for block {}: {}",
                block_number,
                e
            );
        }
    }

    /// Run catchup phase for incomplete blocks from previous session.
    ///
    /// Scans storage for blocks that were partially processed and replays
    /// them through the appropriate pipeline stages before starting live mode.
    async fn run_catchup_phase(
        &mut self,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    ) -> Result<(), CollectorError> {
        // Get registered handler keys from progress tracker
        let registered_handlers = if let Some(ref tracker) = self.progress_tracker {
            let tracker = tracker.lock().await;
            tracker.handler_keys().clone()
        } else {
            std::collections::HashSet::new()
        };

        // Create catchup service with database access if available
        let catchup_service = if let Some(ref db_pool) = self.db_pool {
            LiveCatchupService::with_db(
                &self.chain.name,
                self.chain.chain_id as i64,
                registered_handlers.clone(),
                self.expectations,
                db_pool.clone(),
            )
        } else {
            LiveCatchupService::new(
                &self.chain.name,
                registered_handlers.clone(),
                self.expectations,
            )
        };

        // Reconstruct missing status files before scanning for incomplete blocks
        match catchup_service.reconstruct_missing_status_files().await {
            Ok(count) if count > 0 => {
                tracing::info!("Reconstructed {} missing status files", count);
            }
            Err(e) => {
                tracing::warn!("Failed to reconstruct missing status files: {}", e);
            }
            _ => {}
        }

        // Load progress from storage files for accurate catchup detection
        if let Some(ref tracker) = self.progress_tracker {
            let mut tracker = tracker.lock().await;
            if let Err(e) = tracker.load_from_storage(&self.storage) {
                tracing::warn!("Failed to load progress from storage: {}", e);
            }
        }

        // Scan for incomplete blocks
        let scan_result = match catchup_service.scan_incomplete_blocks() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to scan for incomplete blocks: {}", e);
                return Ok(()); // Continue with live mode even if catchup scan fails
            }
        };

        if scan_result.is_empty() {
            tracing::info!("No incomplete blocks found, skipping catchup phase");
            return Ok(());
        }

        tracing::info!(
            "Catchup phase: {} blocks need processing ({} collection resume, {} log decode, {} call decode, {} transform)",
            scan_result.total_blocks(),
            scan_result.blocks_needing_collection_resume.len(),
            scan_result.blocks_needing_log_decode.len(),
            scan_result.blocks_needing_call_decode.len(),
            scan_result.blocks_needing_transform.len()
        );

        if !scan_result.blocks_needing_collection_resume.is_empty() {
            match self
                .resume_collection_blocks(
                    &scan_result.blocks_needing_collection_resume,
                    log_decoder_tx,
                    eth_call_decoder_tx,
                )
                .await
            {
                Ok(count) => {
                    tracing::info!("Catchup: resumed collection for {} blocks", count);
                }
                Err(e) => {
                    tracing::error!("Failed to resume collection during catchup: {}", e);
                }
            }
        }

        // Replay logs for decoding
        if !scan_result.blocks_needing_log_decode.is_empty() {
            if let Some(decoder_tx) = log_decoder_tx {
                match catchup_service
                    .replay_logs_for_decode(&scan_result.blocks_needing_log_decode, decoder_tx)
                    .await
                {
                    Ok(count) => {
                        tracing::info!("Catchup: replayed logs for {} blocks to decoder", count);
                    }
                    Err(e) => {
                        tracing::error!("Failed to replay logs for catchup: {}", e);
                    }
                }
            } else {
                tracing::warn!("Blocks need log decoding but no log decoder configured, skipping");
            }
        }

        // Replay eth_calls for decoding
        if !scan_result.blocks_needing_call_decode.is_empty() {
            if let Some(decoder_tx) = eth_call_decoder_tx {
                match catchup_service
                    .replay_calls_for_decode(&scan_result.blocks_needing_call_decode, decoder_tx)
                    .await
                {
                    Ok(count) => {
                        tracing::info!(
                            "Catchup: replayed eth_calls for {} blocks to decoder",
                            count
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to replay eth_calls for catchup: {}", e);
                    }
                }
            } else {
                tracing::warn!(
                    "Blocks need eth_call decoding but no eth_call decoder configured, skipping"
                );
            }
        }

        // Replay blocks needing transformation retry
        if !scan_result.blocks_needing_transform.is_empty() {
            if self.transform_retry_tx.is_some() {
                for (block_number, missing_handlers) in &scan_result.blocks_needing_transform {
                    self.queue_transform_retry(*block_number, Some(missing_handlers.clone()))
                        .await;
                }

                tracing::info!(
                    "Catchup: queued {} blocks for transformation retry",
                    scan_result.blocks_needing_transform.len()
                );
            } else {
                tracing::warn!(
                    "Blocks need transformation but no transform retry channel is configured, skipping"
                );
            }
        }

        tracing::info!("Catchup phase complete");
        Ok(())
    }
}

/// Decode an address from ABI-encoded log data.
fn decode_address_from_data(
    data: &[u8],
    types: &[DynSolType],
    indices: &[usize],
) -> Option<[u8; 20]> {
    if types.is_empty() || indices.is_empty() || data.is_empty() {
        return None;
    }

    let tuple_type = DynSolType::Tuple(types.to_vec());

    match tuple_type.abi_decode_params(data) {
        Ok(DynSolValue::Tuple(values)) => extract_address_recursive(&values, indices),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!("Failed to decode factory event data: {}", e);
            None
        }
    }
}

/// Recursively extract an address from nested tuple values.
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

/// Broadcast a reorg notification to decoder channels.
///
/// Sends `DecoderMessage::Reorg` to multiple optional decoder channels,
/// logging warnings for any failures but not propagating errors.
async fn broadcast_reorg_to_decoders(
    common_ancestor: u64,
    orphaned: &[u64],
    log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
    eth_call_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
) {
    if let Some(decoder_tx) = log_decoder_tx {
        let msg = DecoderMessage::Reorg {
            _common_ancestor: common_ancestor,
            orphaned: orphaned.to_vec(),
        };
        if let Err(e) = decoder_tx.send(msg).await {
            tracing::warn!("Failed to send reorg notification to log decoder: {}", e);
        }
    }

    if let Some(decoder_tx) = eth_call_decoder_tx {
        let msg = DecoderMessage::Reorg {
            _common_ancestor: common_ancestor,
            orphaned: orphaned.to_vec(),
        };
        if let Err(e) = decoder_tx.send(msg).await {
            tracing::warn!(
                "Failed to send reorg notification to eth_call decoder: {}",
                e
            );
        }
    }
}

impl std::fmt::Debug for LiveCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveCollector")
            .field("chain", &self.chain.name)
            .field("buffer_size", &self.buffer.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;

    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use super::*;
    use crate::live::{LiveDecodedCall, LiveEthCall};
    use crate::rpc::UnifiedRpcClient;
    use crate::types::config::chain::ChainConfig;
    use crate::types::decoded::DecodedValue;
    use alloy::primitives::U256;

    /// Computes the gap range for backfill given the last processed block and received block.
    /// Returns Some((gap_start, gap_end)) if there is a gap, None otherwise.
    fn compute_gap_range(last_processed: u64, received: u64) -> Option<(u64, u64)> {
        let expected_next = last_processed + 1;
        if received > expected_next {
            let gap_start = expected_next;
            let gap_end = received - 1;
            Some((gap_start, gap_end))
        } else {
            None
        }
    }

    fn test_chain(name: &str) -> ChainConfig {
        ChainConfig {
            name: name.to_string(),
            chain_id: 1,
            chain_type: crate::types::chain::ChainType::Evm,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            contracts: HashMap::new(),
            block_receipts_method: None,
            factory_collections: HashMap::new(),
            rpc: Default::default(),
        }
    }

    fn dummy_client() -> Arc<UnifiedRpcClient> {
        Arc::new(UnifiedRpcClient::from_url("http://127.0.0.1:8545").unwrap())
    }

    fn test_collector(storage: LiveStorage) -> LiveCollector {
        LiveCollector {
            chain: Arc::new(test_chain("collector-test")),
            http_client: dummy_client(),
            storage,
            reorg_detector: ReorgDetector::new(8),
            config: LiveModeConfig::default(),
            buffer: VecDeque::new(),
            progress_tracker: None,
            expected_start_block: None,
            factory_matchers: Arc::new(vec![]),
            factory_topic_index: HashMap::new(),
            eth_call_collector: None,
            db_pool: None,
            expectations: LivePipelineExpectations {
                expect_eth_call_collection: true,
                expect_eth_call_decode: true,
                ..Default::default()
            },
            transform_retry_tx: None,
        }
    }

    #[test]
    fn test_gap_detection_range() {
        // Last processed: 100, received: 103
        // Should backfill blocks 101, 102 (2 blocks)
        let (gap_start, gap_end) = compute_gap_range(100, 103).unwrap();
        assert_eq!(gap_start, 101);
        assert_eq!(gap_end, 102);
        assert_eq!(gap_end - gap_start + 1, 2);
    }

    #[test]
    fn test_gap_detection_single_block_gap() {
        // Last processed: 100, received: 102
        // Should backfill block 101 (1 block)
        let (gap_start, gap_end) = compute_gap_range(100, 102).unwrap();
        assert_eq!(gap_start, 101);
        assert_eq!(gap_end, 101);
        assert_eq!(gap_end - gap_start + 1, 1);
    }

    #[test]
    fn test_gap_detection_no_gap() {
        // Last processed: 100, received: 101
        // No gap - this is the expected next block
        let result = compute_gap_range(100, 101);
        assert!(result.is_none());
    }

    #[test]
    fn test_gap_detection_large_gap() {
        // Last processed: 1000, received: 1010
        // Should backfill blocks 1001-1009 (9 blocks)
        let (gap_start, gap_end) = compute_gap_range(1000, 1010).unwrap();
        assert_eq!(gap_start, 1001);
        assert_eq!(gap_end, 1009);
        assert_eq!(gap_end - gap_start + 1, 9);
    }

    #[tokio::test]
    async fn commit_eth_call_batch_clears_old_decoded_state_before_messages_are_observable() {
        let tmp = TempDir::new().unwrap();
        let storage = LiveStorage::with_base_dir(tmp.path().join("live"));
        storage.ensure_dirs().unwrap();

        let block_number = 77;
        storage
            .write_status(block_number, &LiveBlockStatus::collected())
            .unwrap();
        storage
            .write_decoded_calls(
                block_number,
                "contract",
                "foo",
                &[LiveDecodedCall {
                    block_number,
                    block_timestamp: 1_000,
                    contract_address: [9u8; 20],
                    decoded_value: DecodedValue::Uint256(U256::from(1)),
                }],
            )
            .unwrap();

        let mut batch = CollectedEthCallBatch::default();
        batch.calls.push(LiveEthCall {
            block_number,
            block_timestamp: 1_000,
            contract_name: "contract".to_string(),
            contract_address: [7u8; 20],
            function_name: "foo".to_string(),
            result: vec![0u8; 32],
        });
        batch.decoder_messages.push(DecoderMessage::EthCallsReady {
            range_start: block_number,
            range_end: block_number + 1,
            contract_name: "contract".to_string(),
            function_name: "foo".to_string(),
            results: vec![crate::decoding::EthCallResult {
                block_number,
                block_timestamp: 1_000,
                contract_address: [7u8; 20],
                value: vec![0u8; 32],
            }],
            live_mode: true,
            retry_transform_after_decode: false,
        });
        batch
            .decoder_messages
            .push(DecoderMessage::EthCallsBlockComplete {
                range_start: block_number,
                range_end: block_number + 1,
                retry_transform_after_decode: false,
            });

        let (decoder_tx, mut decoder_rx) = mpsc::channel(1);
        let mut collector = test_collector(storage.clone());
        let tx_opt = Some(decoder_tx);

        let commit_task = tokio::spawn(async move {
            collector
                .commit_eth_call_batch(block_number, batch, &tx_opt, true, false)
                .await
                .unwrap();
        });

        let first_msg = decoder_rx.recv().await.unwrap();
        assert!(matches!(first_msg, DecoderMessage::EthCallsReady { .. }));
        assert!(matches!(
            storage.read_decoded_calls(block_number, "contract", "foo"),
            Err(StorageError::NotFound(_))
        ));
        assert_eq!(storage.read_eth_calls(block_number).unwrap().len(), 1);

        let status = storage.read_status(block_number).unwrap();
        assert!(status.eth_calls_collected);
        assert!(!status.eth_calls_decoded);

        let second_msg = decoder_rx.recv().await.unwrap();
        assert!(matches!(
            second_msg,
            DecoderMessage::EthCallsBlockComplete { .. }
        ));

        commit_task.await.unwrap();
    }
}
