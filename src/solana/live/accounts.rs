//! Live account reader — event-triggered account state fetches.
//!
//! Reads Solana account state when triggered by decoded events or address
//! discovery. This is the Solana analog of the EVM `LiveEthCallCollector`:
//! instead of calling contract functions, it reads on-chain account data
//! via `getMultipleAccounts` and decodes it through program-specific decoders.
//!
//! Live-only — no historical equivalent.

use std::collections::HashMap;
use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::live::bincode_io::StorageError;
use crate::solana::decoding::accounts::SolanaAccountDecoder;
use crate::solana::discovery::{DiscoveryAddresses, SharedKnownAccounts};
use crate::solana::pipeline::AccountReadCadence;
use crate::solana::rpc::{SolanaRpcClient, SolanaRpcError};
use crate::transformations::engine::{
    DecodedAccountStatesMessage, RangeCompleteKind, RangeCompleteMessage,
};

use super::storage::SolanaLiveStorage;
use super::types::{AccountReadTrigger, LiveAccountRead};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum AccountReaderError {
    #[error("RPC error: {0}")]
    Rpc(#[from] SolanaRpcError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Channel send error: {0}")]
    ChannelSend(String),
}

// ---------------------------------------------------------------------------
// Account reader
// ---------------------------------------------------------------------------

/// Reads Solana account state when triggered by discovery or the collector.
///
/// Batches addresses into chunks of up to `batch_size` (max 100 for Solana's
/// `getMultipleAccounts`), fetches data via RPC, writes raw reads to live
/// storage, decodes through the `SolanaAccountDecoder`, and forwards decoded
/// states to the transformation engine.
pub struct SolanaLiveAccountReader {
    rpc_client: Arc<SolanaRpcClient>,
    account_decoder: Arc<SolanaAccountDecoder>,
    storage: SolanaLiveStorage,
    /// program_id bytes -> human-readable program/source name
    program_names: Arc<HashMap<[u8; 32], String>>,
    /// Max pubkeys per `getMultipleAccounts` call (Solana limit: 100).
    batch_size: usize,
    /// Shared registry of known account addresses from discovery.
    /// When present, empty triggers read all known addresses instead of skipping.
    known_accounts: Option<SharedKnownAccounts>,
    /// Cadence configuration per (program, account_type). Absent entries
    /// default to EverySlot.
    cadence_map: HashMap<(String, String), AccountReadCadence>,
    /// Whether slot-level account completion is deferred through the discovery
    /// loop. When true, per-slot and discovery-triggered reads wait for the
    /// `mark_complete_only` barrier sent after all discovery-managed reads for
    /// that slot have been enqueued.
    defer_account_completion: bool,
}

impl SolanaLiveAccountReader {
    /// Create a new account reader.
    ///
    /// `batch_size` defaults to 100 (the Solana RPC limit for
    /// `getMultipleAccounts`).
    pub fn new(
        rpc_client: Arc<SolanaRpcClient>,
        account_decoder: Arc<SolanaAccountDecoder>,
        storage: SolanaLiveStorage,
        program_names: Arc<HashMap<[u8; 32], String>>,
        known_accounts: Option<SharedKnownAccounts>,
        cadence_map: HashMap<(String, String), AccountReadCadence>,
        defer_account_completion: bool,
    ) -> Self {
        Self {
            rpc_client,
            account_decoder,
            storage,
            program_names,
            batch_size: 100,
            known_accounts,
            cadence_map,
            defer_account_completion,
        }
    }

    fn should_defer_explicit_trigger_completion(&self, trigger: &AccountReadTrigger) -> bool {
        self.defer_account_completion && trigger.await_completion_barrier
    }

    /// Main event loop.
    ///
    /// Listens on two channels:
    /// - `discovery_rx`: newly discovered addresses from the `DiscoveryManager`
    /// - `trigger_rx`: explicit read triggers from the collector after a slot
    ///   is fetched
    ///
    /// On either signal, fetches account data, decodes it, and sends decoded
    /// account states downstream. Sends a `RangeCompleteMessage` with
    /// `AccountStates` kind after processing each trigger or discovery batch.
    pub async fn run(
        self,
        mut discovery_rx: mpsc::Receiver<DiscoveryAddresses>,
        mut trigger_rx: mpsc::Receiver<AccountReadTrigger>,
        account_states_tx: mpsc::Sender<DecodedAccountStatesMessage>,
        account_complete_tx: mpsc::Sender<RangeCompleteMessage>,
    ) -> Result<(), AccountReaderError> {
        tracing::info!("Solana live account reader started");

        // Cadence tracking: last slot and last block_time when each
        // (program, account_type) group was actually read.
        let mut last_read_slot: HashMap<(String, String), u64> = HashMap::new();
        let mut last_read_time: HashMap<(String, String), i64> = HashMap::new();

        // Slots where a read trigger failed. mark_complete_only will not
        // mark these complete so catchup can retry on next restart.
        let mut failed_slots: std::collections::HashSet<u64> = std::collections::HashSet::new();

        loop {
            tokio::select! {
                Some(discovery) = discovery_rx.recv() => {
                    let addresses: Vec<[u8; 32]> = discovery
                        .addresses
                        .iter()
                        .map(|pk| pk.to_bytes())
                        .collect();

                    if addresses.is_empty() {
                        continue;
                    }

                    let source_name = &discovery.program_name;

                    tracing::debug!(
                        source = %source_name,
                        account_type = %discovery.account_type,
                        count = addresses.len(),
                        "Processing discovery addresses for account reads",
                    );

                    // Discovery addresses don't have a specific slot context;
                    // use slot 0 to indicate "latest" — the RPC reads current state.
                    if let Err(e) = self
                        .read_and_decode_accounts(
                            0,
                            None,
                            source_name,
                            &addresses,
                            &account_states_tx,
                        )
                        .await
                    {
                        tracing::error!(
                            source = %source_name,
                            error = %e,
                            "Failed to read discovery accounts",
                        );
                    }
                }

                Some(trigger) = trigger_rx.recv() => {
                    // mark_complete_only: sent by the discovery loop after all
                    // decoded events for a slot have been processed. At this
                    // point, per-slot reads and any discovery-triggered reads
                    // for the slot have already been enqueued and processed.
                    if trigger.mark_complete_only {
                        if failed_slots.remove(&trigger.slot) {
                            tracing::warn!(
                                slot = trigger.slot,
                                "Not marking account-complete: prior read failed for slot",
                            );
                            continue;
                        }
                        if let Err(e) = self.storage.update_status_atomic(trigger.slot, |s| {
                            s.accounts_read = true;
                            s.accounts_decoded = true;
                        }) {
                            tracing::warn!(
                                slot = trigger.slot,
                                error = %e,
                                "Failed to update account status (slot may be skipped or deleted)",
                            );
                        }
                        let complete_msg = RangeCompleteMessage {
                            range_start: trigger.slot,
                            range_end: trigger.slot + 1,
                            kind: RangeCompleteKind::AccountStates,
                        };
                        if let Err(e) = account_complete_tx.send(complete_msg).await {
                            // Expected when no transformation engine is wired:
                            // the pipeline stubs this sender with a dropped
                            // receiver to prevent the reader from blocking.
                            tracing::debug!(
                                slot = trigger.slot,
                                error = %e,
                                "Account-complete channel has no consumer (receiver dropped)",
                            );
                        }
                        continue;
                    }

                    let mut had_error = false;

                    if trigger.addresses.is_empty() {
                        // Per-slot trigger from the collector: read known accounts
                        // from the shared registry, applying cadence filtering.
                        let known = self.known_accounts.as_ref()
                            .map(|reg| reg.read().unwrap().clone())
                            .unwrap_or_default();

                        for (program_name, type_groups) in &known {
                            for (acct_type, addresses) in type_groups {
                                if addresses.is_empty() {
                                    continue;
                                }

                                // Apply cadence filtering
                                let key = (program_name.clone(), acct_type.clone());
                                if let Some(cadence) = self.cadence_map.get(&key) {
                                    let should_read = match cadence {
                                        AccountReadCadence::EverySlot => true,
                                        AccountReadCadence::EveryNSlots(n) => {
                                            let last = last_read_slot
                                                .get(&key)
                                                .copied()
                                                .unwrap_or(0);
                                            trigger.slot >= last + n
                                        }
                                        AccountReadCadence::Duration { seconds } => {
                                            if let Some(block_time) = trigger.block_time {
                                                let last = last_read_time
                                                    .get(&key)
                                                    .copied()
                                                    .unwrap_or(0);
                                                block_time >= last + (*seconds as i64)
                                            } else {
                                                true
                                            }
                                        }
                                    };
                                    if !should_read {
                                        continue;
                                    }
                                }

                                tracing::debug!(
                                    slot = trigger.slot,
                                    source = %program_name,
                                    account_type = %acct_type,
                                    count = addresses.len(),
                                    "Reading known accounts for slot",
                                );

                                if let Err(e) = self
                                    .read_and_decode_accounts(
                                        trigger.slot,
                                        trigger.block_time,
                                        program_name,
                                        addresses,
                                        &account_states_tx,
                                    )
                                    .await
                                {
                                    had_error = true;
                                    tracing::error!(
                                        slot = trigger.slot,
                                        source = %program_name,
                                        error = %e,
                                        "Failed to read known accounts",
                                    );
                                } else {
                                    last_read_slot.insert(key.clone(), trigger.slot);
                                    if let Some(bt) = trigger.block_time {
                                        last_read_time.insert(key, bt);
                                    }
                                }
                            }
                        }

                        // When discovery can enqueue post-decode reads for a
                        // live slot, defer account-complete to the
                        // mark_complete_only trigger sent after the discovery
                        // loop has processed that slot.
                        if self.defer_account_completion {
                            if had_error {
                                failed_slots.insert(trigger.slot);
                            }
                            continue;
                        }
                    } else {
                        // Explicit trigger: discovery (Once), OnEvents, or
                        // event-driven reads with specific addresses.
                        tracing::debug!(
                            slot = trigger.slot,
                            source = %trigger.source_name,
                            count = trigger.addresses.len(),
                            "Processing account read trigger",
                        );

                        if let Err(e) = self
                            .read_and_decode_accounts(
                                trigger.slot,
                                trigger.block_time,
                                &trigger.source_name,
                                &trigger.addresses,
                                &account_states_tx,
                            )
                            .await
                        {
                            had_error = true;
                            tracing::error!(
                                slot = trigger.slot,
                                source = %trigger.source_name,
                                error = %e,
                                "Failed to read triggered accounts",
                            );
                        }

                        // Discovery-managed explicit reads participate in the
                        // same deferred completion barrier. Startup Once reads
                        // do not, even though they share the explicit-trigger
                        // path.
                        if self.should_defer_explicit_trigger_completion(&trigger) {
                            if had_error {
                                failed_slots.insert(trigger.slot);
                            }
                            continue;
                        }
                    }

                    if had_error {
                        tracing::warn!(
                            slot = trigger.slot,
                            "Skipping account completion for slot due to read errors",
                        );
                        continue;
                    }

                    // Mark slot status and signal completion.
                    if let Err(e) = self.storage.update_status_atomic(trigger.slot, |s| {
                        s.accounts_read = true;
                        s.accounts_decoded = true;
                    }) {
                        tracing::warn!(
                            slot = trigger.slot,
                            error = %e,
                            "Failed to update account status (slot may be skipped or deleted)",
                        );
                    }

                    let complete_msg = RangeCompleteMessage {
                        range_start: trigger.slot,
                        range_end: trigger.slot + 1,
                        kind: RangeCompleteKind::AccountStates,
                    };
                    if let Err(e) = account_complete_tx.send(complete_msg).await {
                        // Same as above — dropped receiver is the expected
                        // steady state when no transformation engine runs.
                        tracing::debug!(
                            slot = trigger.slot,
                            error = %e,
                            "Account-complete channel has no consumer (receiver dropped)",
                        );
                    }
                }

                else => {
                    tracing::info!("All account reader channels closed, shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Fetch, store, decode, and forward account states for a batch of
    /// addresses.
    ///
    /// 1. Chunks `addresses` into batches of `self.batch_size` (100).
    /// 2. Calls `rpc_client.get_multiple_accounts` for each chunk.
    /// 3. Builds `LiveAccountRead` records and writes them to storage.
    /// 4. Decodes each account via `account_decoder.decode_account`.
    /// 5. Groups decoded states by `account_type`.
    /// 6. Sends a `DecodedAccountStatesMessage` per group.
    async fn read_and_decode_accounts(
        &self,
        slot: u64,
        block_time: Option<i64>,
        source_name: &str,
        addresses: &[[u8; 32]],
        account_states_tx: &mpsc::Sender<DecodedAccountStatesMessage>,
    ) -> Result<(), AccountReaderError> {
        let mut all_reads: Vec<LiveAccountRead> = Vec::new();
        let mut decoded_by_source_and_type: HashMap<
            (String, String),
            Vec<crate::transformations::context::DecodedAccountState>,
        > = HashMap::new();

        for chunk in addresses.chunks(self.batch_size) {
            // Convert [u8; 32] to Pubkey for the RPC call.
            let pubkeys: Vec<Pubkey> = chunk
                .iter()
                .map(|bytes| Pubkey::new_from_array(*bytes))
                .collect();

            let accounts = self.rpc_client.get_multiple_accounts(&pubkeys).await?;

            for (i, maybe_account) in accounts.into_iter().enumerate() {
                let address_bytes = chunk[i];

                let account = match maybe_account {
                    Some(a) => a,
                    None => {
                        tracing::trace!(
                            address = hex::encode(address_bytes),
                            slot,
                            "Account not found on chain, skipping",
                        );
                        continue;
                    }
                };

                let owner_bytes = account.owner.to_bytes();

                // Build raw read for storage.
                let live_read = LiveAccountRead {
                    slot,
                    block_time,
                    account_address: address_bytes,
                    owner: owner_bytes,
                    data: account.data.clone(),
                    lamports: account.lamports,
                    executable: account.executable,
                };
                all_reads.push(live_read);

                // Resolve a human-readable source name: prefer the trigger's
                // source_name, fall back to program_names map.
                let effective_source = if !source_name.is_empty() {
                    source_name.to_string()
                } else {
                    self.program_names
                        .get(&owner_bytes)
                        .cloned()
                        .unwrap_or_else(|| hex::encode(owner_bytes))
                };

                // Decode the account data.
                if let Some(decoded) = self.account_decoder.decode_account(
                    owner_bytes,
                    address_bytes,
                    &account.data,
                    slot,
                    block_time,
                    &effective_source,
                ) {
                    decoded_by_source_and_type
                        .entry((effective_source.clone(), decoded.account_type.clone()))
                        .or_default()
                        .push(decoded);
                }
            }
        }

        // Persist raw reads to live storage.
        if !all_reads.is_empty() {
            let resolved_sources: Vec<&str> = decoded_by_source_and_type
                .keys()
                .map(|(source, _)| source.as_str())
                .collect();

            self.storage.write_accounts(slot, &all_reads)?;

            tracing::debug!(
                slot,
                requested_source = %source_name,
                resolved_sources = ?resolved_sources,
                raw_reads = all_reads.len(),
                decoded_types = decoded_by_source_and_type.len(),
                "Account reads complete",
            );
        }

        // Persist decoded states for retry and send them downstream, grouped by
        // (effective_source, account_type).
        for ((effective_source, account_type), states) in decoded_by_source_and_type {
            self.storage.write_decoded(
                "accounts",
                slot,
                &effective_source,
                &account_type,
                &states,
            )?;

            let msg = DecodedAccountStatesMessage {
                range_start: slot,
                range_end: slot + 1,
                source_name: effective_source.clone(),
                account_type,
                account_states: states,
            };

            if let Err(e) = account_states_tx.send(msg).await {
                // Expected when no transformation engine is wired: the
                // pipeline stubs this sender with a dropped receiver to
                // prevent the reader from blocking.
                tracing::debug!(
                    slot,
                    source = %effective_source,
                    error = %e,
                    "Account-states channel has no consumer (receiver dropped)",
                );
                break; // No point sending more to a closed channel
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_reader(defer_account_completion: bool) -> SolanaLiveAccountReader {
        SolanaLiveAccountReader::new(
            Arc::new(
                SolanaRpcClient::new(
                    "http://127.0.0.1:8899",
                    crate::types::config::solana::SolanaCommitment::Confirmed,
                    None,
                    None,
                )
                .expect("rpc client"),
            ),
            Arc::new(SolanaAccountDecoder::new(Vec::new())),
            SolanaLiveStorage::with_base_dir(
                std::env::temp_dir().join("solana-live-account-reader-tests"),
            ),
            Arc::new(HashMap::new()),
            None,
            HashMap::new(),
            defer_account_completion,
        )
    }

    #[test]
    fn explicit_trigger_completion_respects_barrier_flag() {
        let reader = make_reader(true);
        let startup_trigger = AccountReadTrigger {
            slot: 42,
            block_time: Some(1_700_000_123),
            source_name: "program".to_string(),
            addresses: vec![[0xAA; 32]],
            await_completion_barrier: false,
            mark_complete_only: false,
        };
        let discovery_trigger = AccountReadTrigger {
            await_completion_barrier: true,
            ..AccountReadTrigger {
                slot: 42,
                block_time: Some(1_700_000_123),
                source_name: "program".to_string(),
                addresses: vec![[0xBB; 32]],
                await_completion_barrier: false,
                mark_complete_only: false,
            }
        };

        assert!(!reader.should_defer_explicit_trigger_completion(&startup_trigger));
        assert!(reader.should_defer_explicit_trigger_completion(&discovery_trigger));
        assert!(!make_reader(false).should_defer_explicit_trigger_completion(&discovery_trigger));
    }
}
