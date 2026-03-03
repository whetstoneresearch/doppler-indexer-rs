//! Live block collector that processes blocks from WebSocket subscription.
//!
//! The collector:
//! 1. Receives block headers from WebSocket subscription
//! 2. Detects reorgs using parent hash verification
//! 3. Fetches full block data and receipts via HTTP
//! 4. Stores data in live storage format
//! 5. Forwards to decoder channels

use std::collections::VecDeque;
use std::sync::Arc;

use alloy::primitives::B256;
use alloy::rpc::types::BlockNumberOrTag;
use thiserror::Error;
use tokio::sync::mpsc;

use super::reorg::{ReorgDetector, ReorgEvent};
use super::storage::{LiveStorage, StorageError};
use super::types::{LiveBlock, LiveBlockStatus, LiveLog, LiveMessage, LiveModeConfig, LiveReceipt};
use crate::decoding::DecoderMessage;
use crate::raw_data::historical::receipts::LogData;
use crate::rpc::{RpcError, UnifiedRpcClient, WsEvent};
use crate::transformations::ReorgMessage;
use crate::types::config::chain::ChainConfig;

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Block not found: {0}")]
    BlockNotFound(u64),
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
}

impl LiveCollector {
    /// Create a new LiveCollector.
    pub fn new(
        chain: Arc<ChainConfig>,
        http_client: Arc<UnifiedRpcClient>,
        config: LiveModeConfig,
    ) -> Self {
        let storage = LiveStorage::new(&chain.name);
        let reorg_detector = ReorgDetector::new(config.reorg_depth);

        Self {
            chain,
            http_client,
            storage,
            reorg_detector,
            config,
            buffer: VecDeque::new(),
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
        transform_reorg_tx: Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        // Ensure storage directories exist
        self.storage.ensure_dirs()?;

        tracing::info!(
            "Live collector started for chain {} with reorg_depth={}",
            self.chain.name,
            self.config.reorg_depth
        );

        while let Some(event) = ws_rx.recv().await {
            match event {
                WsEvent::NewBlock {
                    number,
                    hash,
                    parent_hash,
                    timestamp,
                } => {
                    let block = LiveBlock {
                        number,
                        hash: hash.0,
                        parent_hash: parent_hash.0,
                        timestamp,
                        tx_hashes: vec![], // Will be filled when we fetch the full block
                    };

                    if let Err(e) = self
                        .process_block(block, &live_tx, &log_decoder_tx, &transform_reorg_tx)
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
                        .backfill_blocks(missed_from, missed_to, &live_tx, &log_decoder_tx, &transform_reorg_tx)
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
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        let block_number = block.number;

        // Check for reorg
        if let Some(reorg_event) = self.reorg_detector.process_block(&block) {
            self.handle_reorg(&reorg_event, live_tx, log_decoder_tx, transform_reorg_tx).await?;
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
        let mut status = self.storage.read_status(block_number)?;
        status.block_fetched = true;
        self.storage.write_status(block_number, &status)?;

        // Fetch receipts and logs
        let (receipts, logs) = self.fetch_receipts_and_logs(block_number).await?;

        self.storage.write_receipts(block_number, &receipts)?;
        self.storage.write_logs(block_number, &logs)?;

        // Update status
        let mut status = self.storage.read_status(block_number)?;
        status.receipts_collected = true;
        status.logs_collected = true;
        self.storage.write_status(block_number, &status)?;

        // Send to live message channel
        live_tx
            .send(LiveMessage::Block(updated_block.clone()))
            .await
            .map_err(|_| CollectorError::ChannelClosed)?;

        // Send logs to decoder if configured
        if let Some(decoder_tx) = log_decoder_tx {
            if !logs.is_empty() {
                let log_data: Vec<LogData> = logs
                    .iter()
                    .map(|log| LogData {
                        block_number,
                        block_timestamp: updated_block.timestamp,
                        transaction_hash: B256::ZERO, // TODO: Get from receipt
                        log_index: log.log_index,
                        address: log.address,
                        topics: log.topics.clone(),
                        data: log.data.clone(),
                    })
                    .collect();

                let _ = decoder_tx
                    .send(DecoderMessage::LogsReady {
                        range_start: block_number,
                        range_end: block_number + 1, // Exclusive end for single block
                        logs: log_data,
                        live_mode: true, // Live mode: write to bincode
                    })
                    .await;
            }
        }

        tracing::debug!(
            "Processed live block {} with {} receipts and {} logs",
            block_number,
            receipts.len(),
            logs.len()
        );

        Ok(())
    }

    /// Handle a reorg event.
    async fn handle_reorg(
        &mut self,
        event: &ReorgEvent,
        live_tx: &mpsc::Sender<LiveMessage>,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        tracing::warn!(
            "Handling reorg: common_ancestor={}, orphaned={:?}, depth={}",
            event.common_ancestor,
            event.orphaned,
            event.depth
        );

        // Delete orphaned blocks from storage (including decoded data)
        for &orphaned_number in &event.orphaned {
            if let Err(e) = self.storage.delete_all(orphaned_number) {
                tracing::warn!("Failed to delete orphaned block {}: {}", orphaned_number, e);
            }
        }

        // Notify decoder of reorg so it can clean up any orphaned decoded data
        if let Some(decoder_tx) = log_decoder_tx {
            let _ = decoder_tx
                .send(DecoderMessage::Reorg {
                    common_ancestor: event.common_ancestor,
                    orphaned: event.orphaned.clone(),
                })
                .await;
        }

        // Notify transformation engine of reorg so it can clean up pending events
        if let Some(transform_tx) = transform_reorg_tx {
            let _ = transform_tx
                .send(ReorgMessage {
                    common_ancestor: event.common_ancestor,
                    orphaned: event.orphaned.clone(),
                })
                .await;
        }

        // Notify downstream of reorg
        live_tx
            .send(LiveMessage::Reorg {
                common_ancestor: event.common_ancestor,
                orphaned: event.orphaned.clone(),
            })
            .await
            .map_err(|_| CollectorError::ChannelClosed)?;

        Ok(())
    }

    /// Backfill blocks that were missed during disconnect.
    async fn backfill_blocks(
        &mut self,
        from: u64,
        to: u64,
        live_tx: &mpsc::Sender<LiveMessage>,
        log_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), CollectorError> {
        tracing::info!("Backfilling {} blocks ({} to {})", to - from + 1, from, to);

        for block_number in from..=to {
            let block = self.fetch_full_block(block_number).await?;

            // Process as if it came from WebSocket
            self.process_block(block, live_tx, log_decoder_tx, transform_reorg_tx).await?;
        }

        Ok(())
    }

    /// Fetch full block data via HTTP.
    async fn fetch_full_block(&self, block_number: u64) -> Result<LiveBlock, CollectorError> {
        let block = self
            .http_client
            .get_block_by_number(BlockNumberOrTag::Number(block_number), true)
            .await?
            .ok_or(CollectorError::BlockNotFound(block_number))?;

        let tx_hashes: Vec<[u8; 32]> = block
            .transactions
            .hashes()
            .map(|h| h.0)
            .collect();

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
            .as_deref()
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
                        })
                        .collect(),
                };

                all_logs.extend(live_receipt.logs.clone());
                live_receipts.push(live_receipt);
            }
        }

        Ok((live_receipts, all_logs))
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
