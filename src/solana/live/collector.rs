//! Solana live slot collector that processes slots from WebSocket subscription.
//!
//! The collector:
//! 1. Receives slot notifications from WebSocket subscription
//! 2. Detects reorgs using parent-slot chain verification
//! 3. Fetches full block data via RPC
//! 4. Extracts events and instructions from transactions
//! 5. Stores data in live storage format
//! 6. Forwards to decoder channels

use std::collections::HashSet;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{mpsc, Mutex};

use solana_transaction_status::{EncodedTransaction, UiConfirmedBlock};

use crate::decoding::DecoderMessage;
use crate::live::bincode_io::StorageError;
use crate::live::{LiveModeConfig, LiveProgressTracker};
use crate::solana::raw_data::extraction::extract_events_and_instructions;
use crate::solana::rpc::{SolanaRpcClient, SolanaRpcError};
use crate::solana::ws::SolanaWsEvent;
use crate::transformations::engine::ReorgMessage;

use super::reorg::{SolanaReorgDetector, SolanaReorgEvent};
use super::storage::SolanaLiveStorage;
use super::types::{
    AccountReadTrigger, LiveSlot, LiveSlotStatus, LiveTransaction, SolanaLiveMessage,
    SolanaLivePipelineExpectations,
};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum SolanaCollectorError {
    #[error("RPC error: {0}")]
    Rpc(#[from] SolanaRpcError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Channel send error: {0}")]
    ChannelSend(String),
    #[error("WebSocket closed")]
    WsClosed,
}

// ---------------------------------------------------------------------------
// Collector
// ---------------------------------------------------------------------------

/// Collects live Solana slots from WebSocket and processes them.
///
/// Parallels the EVM `LiveCollector` but operates on Solana slots instead
/// of EVM blocks, using parent-slot linkage for reorg detection and
/// extracting events/instructions from on-chain transactions.
pub struct SolanaLiveCollector {
    chain_name: String,
    rpc_client: Arc<SolanaRpcClient>,
    storage: SolanaLiveStorage,
    reorg_detector: SolanaReorgDetector,
    config: LiveModeConfig,
    configured_programs: HashSet<[u8; 32]>,
    progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
    expected_start_slot: Option<u64>,
    expectations: SolanaLivePipelineExpectations,
    /// Internal gaps discovered during catchup cleanup that must be
    /// backfilled before the main WS loop begins.
    startup_backfill_gaps: Vec<(u64, u64)>,
}

impl SolanaLiveCollector {
    /// Create a new SolanaLiveCollector.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_name: String,
        rpc_client: Arc<SolanaRpcClient>,
        storage: SolanaLiveStorage,
        reorg_detector: SolanaReorgDetector,
        config: LiveModeConfig,
        configured_programs: HashSet<[u8; 32]>,
        progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
        expected_start_slot: Option<u64>,
        expectations: SolanaLivePipelineExpectations,
    ) -> Self {
        Self {
            chain_name,
            rpc_client,
            storage,
            reorg_detector,
            config,
            configured_programs,
            progress_tracker,
            expected_start_slot,
            expectations,
            startup_backfill_gaps: Vec::new(),
        }
    }

    /// Set internal gaps from catchup cleanup that need backfilling on startup.
    pub fn with_startup_backfill_gaps(mut self, gaps: Vec<(u64, u64)>) -> Self {
        self.startup_backfill_gaps = gaps;
        self
    }

    /// Run the live collector.
    ///
    /// Listens for WebSocket slot events and processes each slot through the
    /// full pipeline: fetch -> extract -> store -> forward to decoders.
    pub async fn run(
        mut self,
        mut ws_rx: mpsc::UnboundedReceiver<SolanaWsEvent>,
        live_tx: mpsc::Sender<SolanaLiveMessage>,
        event_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
        instr_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: Option<mpsc::Sender<ReorgMessage>>,
        account_trigger_tx: Option<mpsc::Sender<AccountReadTrigger>>,
    ) -> Result<(), SolanaCollectorError> {
        // Ensure storage directories exist
        self.storage.ensure_dirs()?;

        // Seed reorg detector from existing storage on restart
        self.seed_reorg_detector()?;

        // Backfill internal gaps from catchup cleanup before entering the WS loop.
        // These are holes created when incomplete slots were deleted during startup.
        if !self.startup_backfill_gaps.is_empty() {
            let gap_count: u64 = self
                .startup_backfill_gaps
                .iter()
                .map(|(s, e)| e - s + 1)
                .sum();
            tracing::info!(
                chain = %self.chain_name,
                gaps = self.startup_backfill_gaps.len(),
                total_slots = gap_count,
                "Backfilling internal gaps from catchup cleanup",
            );
            let gaps = std::mem::take(&mut self.startup_backfill_gaps);
            for (from, to) in gaps {
                self.backfill_slots(
                    from,
                    to,
                    &live_tx,
                    &event_decoder_tx,
                    &instr_decoder_tx,
                    &transform_reorg_tx,
                    &account_trigger_tx,
                )
                .await?;
            }
        }

        tracing::info!(
            chain = %self.chain_name,
            reorg_depth = self.config.reorg_depth,
            programs = self.configured_programs.len(),
            "Solana live collector started",
        );

        while let Some(event) = ws_rx.recv().await {
            match event {
                SolanaWsEvent::NewSlot { slot, parent, .. } => {
                    // Check for gap on first slot
                    if let Some(expected) = self.expected_start_slot.take() {
                        if slot > expected + 1 {
                            tracing::warn!(
                                chain = %self.chain_name,
                                expected_next = expected + 1,
                                received = slot,
                                gap_size = slot - expected - 1,
                                "Gap detected on first slot, backfilling",
                            );
                            self.backfill_slots(
                                expected + 1,
                                slot - 1,
                                &live_tx,
                                &event_decoder_tx,
                                &instr_decoder_tx,
                                &transform_reorg_tx,
                                &account_trigger_tx,
                            )
                            .await?;
                        }
                    }

                    if let Err(e) = self
                        .process_slot(
                            slot,
                            parent,
                            &live_tx,
                            &event_decoder_tx,
                            &instr_decoder_tx,
                            &transform_reorg_tx,
                            &account_trigger_tx,
                        )
                        .await
                    {
                        tracing::error!(
                            chain = %self.chain_name,
                            slot,
                            error = %e,
                            "Error processing slot",
                        );
                    }
                }

                SolanaWsEvent::Disconnected { .. } => {
                    tracing::warn!(
                        chain = %self.chain_name,
                        "WebSocket disconnected",
                    );
                }

                SolanaWsEvent::Reconnected {
                    missed_from,
                    missed_to,
                } => {
                    tracing::info!(
                        chain = %self.chain_name,
                        missed_from,
                        missed_to,
                        count = missed_to - missed_from + 1,
                        "WebSocket reconnected, backfilling missed slots",
                    );
                    self.backfill_slots(
                        missed_from,
                        missed_to,
                        &live_tx,
                        &event_decoder_tx,
                        &instr_decoder_tx,
                        &transform_reorg_tx,
                        &account_trigger_tx,
                    )
                    .await?;
                }
            }
        }

        tracing::info!(
            chain = %self.chain_name,
            "Solana live collector shutting down",
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Slot processing
    // -----------------------------------------------------------------------

    /// Process a single slot through the full pipeline.
    async fn process_slot(
        &mut self,
        slot: u64,
        _parent_slot: u64,
        live_tx: &mpsc::Sender<SolanaLiveMessage>,
        event_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        instr_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
        account_trigger_tx: &Option<mpsc::Sender<AccountReadTrigger>>,
    ) -> Result<(), SolanaCollectorError> {
        // 1. Fetch block
        let block = self.rpc_client.get_block(slot).await?;

        // 2. Skipped slot — mark as complete and return
        let block = match block {
            Some(b) => b,
            None => {
                tracing::debug!(
                    chain = %self.chain_name,
                    slot,
                    "Slot was skipped, no block produced",
                );
                let mut status = LiveSlotStatus::collected();
                status.block_fetched = true;
                status.events_extracted = true;
                status.instructions_extracted = true;
                status.events_decoded = true;
                status.instructions_decoded = true;
                status.accounts_read = true;
                status.accounts_decoded = true;
                status.transformed = true;
                self.storage.write_status(slot, &status)?;
                return Ok(());
            }
        };

        // 3. Parse blockhash to [u8; 32]
        let blockhash_bytes = parse_blockhash(&block.blockhash);
        let previous_blockhash_bytes = parse_blockhash(&block.previous_blockhash);

        // 4. Check for reorg
        if let Some(reorg_event) =
            self.reorg_detector
                .process_slot(slot, block.parent_slot, blockhash_bytes)
        {
            // 5. Handle reorg
            self.handle_reorg(&reorg_event, live_tx, transform_reorg_tx)
                .await?;
        }

        // 6. Build LiveSlot
        let tx_count = block
            .transactions
            .as_ref()
            .map(|txs| txs.len() as u32)
            .unwrap_or(0);

        let live_slot = LiveSlot {
            slot,
            block_time: block.block_time,
            block_height: block.block_height,
            parent_slot: block.parent_slot,
            blockhash: blockhash_bytes,
            previous_blockhash: previous_blockhash_bytes,
            transaction_count: tx_count,
        };

        // 7. Write slot to storage and initial status
        self.storage.write_slot(slot, &live_slot)?;
        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        self.storage.write_status(slot, &status)?;

        tracing::info!(
            chain = %self.chain_name,
            slot,
            txs = tx_count,
            "Slot collected",
        );

        // 8. Extract events and instructions
        let (events, instructions) =
            extract_events_and_instructions(&block, slot, &self.configured_programs);

        // 9. Write events and instructions to storage
        self.storage.write_events(slot, &events)?;
        self.storage.write_instructions(slot, &instructions)?;

        // 9b. Write transaction records so compaction can populate
        //     transaction_signatures in the slots parquet.
        let live_txs = extract_live_transactions(&block, slot);
        self.storage.write_transactions(slot, &live_txs)?;

        // 10. Update status
        self.storage.update_status_atomic(slot, |s| {
            s.events_extracted = true;
            s.instructions_extracted = true;
            s.apply_expectations(&self.expectations);
        })?;

        tracing::debug!(
            chain = %self.chain_name,
            slot,
            events = events.len(),
            instructions = instructions.len(),
            "Events and instructions extracted",
        );

        // 11. Send slot to live message channel
        let slot_block_time = live_slot.block_time;
        if let Err(e) = live_tx.send(SolanaLiveMessage::Slot(live_slot)).await {
            // Expected steady state when no live consumer is wired in
            // (the receiver is intentionally dropped at pipeline setup to
            // prevent the collector from blocking). Keep at debug to avoid
            // log flooding in healthy runs.
            tracing::debug!(
                chain = %self.chain_name,
                slot,
                error = %e,
                "Slot live channel has no consumer (receiver dropped)",
            );
        }

        // 12. Forward events to event decoder
        if let Some(decoder_tx) = event_decoder_tx {
            if let Err(e) = decoder_tx
                .send(DecoderMessage::SolanaEventsReady {
                    range_start: slot,
                    range_end: slot + 1,
                    events,
                    live_mode: true,
                })
                .await
            {
                tracing::warn!(
                    chain = %self.chain_name,
                    slot,
                    error = %e,
                    "Failed to send events to decoder (receiver dropped)",
                );
            }
        }

        // 13. Forward instructions to instruction decoder
        if let Some(decoder_tx) = instr_decoder_tx {
            if let Err(e) = decoder_tx
                .send(DecoderMessage::SolanaInstructionsReady {
                    range_start: slot,
                    range_end: slot + 1,
                    instructions,
                    live_mode: true,
                })
                .await
            {
                tracing::warn!(
                    chain = %self.chain_name,
                    slot,
                    error = %e,
                    "Failed to send instructions to decoder (receiver dropped)",
                );
            }
        }

        // 14. If no decoders configured, mark decoded flags true immediately
        if event_decoder_tx.is_none() {
            self.storage.update_status_atomic(slot, |s| {
                s.events_decoded = true;
            })?;
        }
        if instr_decoder_tx.is_none() {
            self.storage.update_status_atomic(slot, |s| {
                s.instructions_decoded = true;
            })?;
        }

        // 15. Trigger account reads for this slot. The account reader will
        //     read all known addresses and mark accounts_read/accounts_decoded.
        //     If there's no account reader, mark account flags directly so the
        //     slot doesn't stay permanently incomplete.
        if let Some(ref tx) = account_trigger_tx {
            // Send an empty trigger — the reader reads per-slot known accounts
            // from the shared registry, applying cadence filtering.
            let _ = tx
                .send(AccountReadTrigger {
                    slot,
                    block_time: slot_block_time,
                    source_name: String::new(),
                    addresses: Vec::new(),
                    await_completion_barrier: false,
                    mark_complete_only: false,
                })
                .await;
        } else if self.expectations.expect_account_reads {
            // No account reader running; mark accounts complete directly
            self.storage.update_status_atomic(slot, |s| {
                s.accounts_read = true;
                s.accounts_decoded = true;
            })?;
        }

        // 16. If no handlers are registered, mark transformed=true
        if let Some(ref tracker) = self.progress_tracker {
            let guard = tracker.lock().await;
            guard.mark_transformed_if_no_handlers(slot);
            drop(guard);
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Reorg handling
    // -----------------------------------------------------------------------

    /// Handle a detected reorg by cleaning up orphaned data and notifying
    /// downstream consumers.
    async fn handle_reorg(
        &mut self,
        event: &SolanaReorgEvent,
        live_tx: &mpsc::Sender<SolanaLiveMessage>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
    ) -> Result<(), SolanaCollectorError> {
        tracing::warn!(
            chain = %self.chain_name,
            common_ancestor = event.common_ancestor,
            orphaned = ?event.orphaned,
            depth = event.depth,
            is_deep = event.is_deep,
            "Handling Solana reorg",
        );

        if event.is_deep {
            tracing::error!(
                chain = %self.chain_name,
                new_slot = event.new_slot,
                orphaned_count = event.orphaned.len(),
                "DEEP REORG detected: reorg extends beyond retention window. \
                 Orphaning all tracked slots as best-effort cleanup.",
            );
        }

        // Delete storage for each orphaned slot
        for &orphaned_slot in &event.orphaned {
            if let Err(e) = self.storage.delete_all(orphaned_slot) {
                tracing::warn!(
                    chain = %self.chain_name,
                    slot = orphaned_slot,
                    error = %e,
                    "Failed to delete orphaned slot data",
                );
            }
        }

        // Clear progress tracker for orphaned slots
        if let Some(ref tracker) = self.progress_tracker {
            let mut guard: tokio::sync::MutexGuard<'_, LiveProgressTracker> = tracker.lock().await;
            for &orphaned_slot in &event.orphaned {
                guard.clear_block(orphaned_slot);
            }
        }

        // Notify live channel of reorg
        if let Err(e) = live_tx
            .send(SolanaLiveMessage::Reorg {
                common_ancestor: event.common_ancestor,
                orphaned: event.orphaned.clone(),
            })
            .await
        {
            // Same story as above: the live-channel receiver is
            // intentionally dropped when no live consumer is wired.
            tracing::debug!(
                chain = %self.chain_name,
                error = %e,
                "Reorg live channel has no consumer (receiver dropped)",
            );
        }

        // Notify transformation engine of reorg
        if let Some(transform_tx) = transform_reorg_tx {
            if let Err(e) = transform_tx
                .send(ReorgMessage {
                    common_ancestor: event.common_ancestor,
                    orphaned: event.orphaned.clone(),
                })
                .await
            {
                tracing::warn!(
                    chain = %self.chain_name,
                    error = %e,
                    "Failed to send reorg to transform engine (receiver dropped)",
                );
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Backfill
    // -----------------------------------------------------------------------

    /// Backfill a range of slots that were missed during a disconnect.
    async fn backfill_slots(
        &mut self,
        from: u64,
        to: u64,
        live_tx: &mpsc::Sender<SolanaLiveMessage>,
        event_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        instr_decoder_tx: &Option<mpsc::Sender<DecoderMessage>>,
        transform_reorg_tx: &Option<mpsc::Sender<ReorgMessage>>,
        account_trigger_tx: &Option<mpsc::Sender<AccountReadTrigger>>,
    ) -> Result<(), SolanaCollectorError> {
        let count = to.saturating_sub(from) + 1;
        tracing::info!(
            chain = %self.chain_name,
            from,
            to,
            count,
            "Backfilling missed slots",
        );

        for slot in from..=to {
            self.process_slot(
                slot,
                0,
                live_tx,
                event_decoder_tx,
                instr_decoder_tx,
                transform_reorg_tx,
                account_trigger_tx,
            )
            .await?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Startup
    // -----------------------------------------------------------------------

    /// Seed the reorg detector from recent slots in storage.
    ///
    /// Called on startup before the main loop so that reorg detection works
    /// immediately for the first incoming slot, even if it forks from a slot
    /// that was processed in a previous session.
    fn seed_reorg_detector(&mut self) -> Result<(), SolanaCollectorError> {
        match self
            .storage
            .get_recent_slots_for_reorg(self.config.reorg_depth)
        {
            Ok(recent) if !recent.is_empty() => {
                tracing::info!(
                    chain = %self.chain_name,
                    count = recent.len(),
                    "Seeding reorg detector from storage",
                );
                self.reorg_detector.seed(
                    recent
                        .into_iter()
                        .map(|s| (s.slot, s.parent_slot, s.blockhash)),
                );
            }
            Ok(_) => {
                tracing::debug!(
                    chain = %self.chain_name,
                    "No recent slots in storage for reorg seeding",
                );
            }
            Err(e) => {
                tracing::warn!(
                    chain = %self.chain_name,
                    error = %e,
                    "Failed to seed reorg detector from storage",
                );
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract `LiveTransaction` records from a block for compaction-time signature lookup.
fn extract_live_transactions(block: &UiConfirmedBlock, slot: u64) -> Vec<LiveTransaction> {
    let transactions = match &block.transactions {
        Some(txs) => txs,
        None => return Vec::new(),
    };
    let block_time = block.block_time;
    let mut result = Vec::with_capacity(transactions.len());

    for encoded_tx in transactions {
        let signature = match extract_signature(&encoded_tx.transaction) {
            Some(s) => s,
            None => continue,
        };
        let (is_err, err_msg, fee, compute_units_consumed, log_messages) =
            match &encoded_tx.meta {
                Some(meta) => {
                    let is_err = meta.err.is_some();
                    let err_msg = meta.err.as_ref().map(|e| e.to_string());
                    let fee = meta.fee;
                    let cu: Option<u64> = meta.compute_units_consumed.clone().into();
                    let logs: Option<&Vec<String>> = meta.log_messages.as_ref().into();
                    (is_err, err_msg, fee, cu, logs.cloned().unwrap_or_default())
                }
                None => (false, None, 0, None, Vec::new()),
            };
        result.push(LiveTransaction {
            slot,
            block_time,
            signature,
            is_err,
            err_msg,
            fee,
            compute_units_consumed,
            log_messages,
            account_keys: Vec::new(),
        });
    }

    result
}

fn extract_signature(encoded_tx: &EncodedTransaction) -> Option<[u8; 64]> {
    if let Some(versioned_tx) = encoded_tx.decode() {
        let sig = versioned_tx.signatures.first()?;
        let mut out = [0u8; 64];
        out.copy_from_slice(sig.as_ref());
        return Some(out);
    }
    if let EncodedTransaction::Json(ui_tx) = encoded_tx {
        let sig_str = ui_tx.signatures.first()?;
        let sig: solana_sdk::signature::Signature = sig_str.parse().ok()?;
        let mut out = [0u8; 64];
        out.copy_from_slice(sig.as_ref());
        return Some(out);
    }
    None
}

/// Parse a base58-encoded blockhash string into a 32-byte array.
///
/// If decoding fails or produces fewer than 32 bytes, returns zeroed bytes
/// for the remainder (defensive — should not happen with valid blockhashes).
fn parse_blockhash(hash_str: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    match bs58::decode(hash_str).into_vec() {
        Ok(bytes) => {
            let len = bytes.len().min(32);
            out[..len].copy_from_slice(&bytes[..len]);
        }
        Err(e) => {
            tracing::warn!(
                blockhash = hash_str,
                error = %e,
                "Failed to decode blockhash from base58",
            );
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_blockhash_valid() {
        // A known 32-byte value encoded as base58
        let bytes = [0xAA; 32];
        let encoded = bs58::encode(&bytes).into_string();
        let decoded = parse_blockhash(&encoded);
        assert_eq!(decoded, bytes);
    }

    #[test]
    fn test_parse_blockhash_invalid() {
        // Invalid base58 should return zeroes
        let decoded = parse_blockhash("!!!invalid!!!");
        assert_eq!(decoded, [0u8; 32]);
    }

    #[test]
    fn test_parse_blockhash_short() {
        // Shorter than 32 bytes should zero-pad
        let bytes = [0xBB; 16];
        let encoded = bs58::encode(&bytes).into_string();
        let decoded = parse_blockhash(&encoded);
        assert_eq!(&decoded[..16], &bytes[..]);
        assert_eq!(&decoded[16..], &[0u8; 16]);
    }

    #[test]
    fn test_error_display() {
        let err = SolanaCollectorError::WsClosed;
        assert_eq!(err.to_string(), "WebSocket closed");

        let err = SolanaCollectorError::ChannelSend("test".into());
        assert!(err.to_string().contains("test"));
    }
}
