//! Types for Solana live mode slot processing.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// A slot in live mode, stored as bincode for fast serialization.
/// Parallels the EVM `LiveBlock` but with Solana slot fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSlot {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub parent_slot: u64,
    pub blockhash: [u8; 32],
    pub previous_blockhash: [u8; 32],
    pub transaction_count: u32,
}

impl LiveSlot {
    /// Create a new LiveSlot from raw data.
    #[allow(dead_code)]
    pub fn new(
        slot: u64,
        block_time: Option<i64>,
        block_height: Option<u64>,
        parent_slot: u64,
        blockhash: [u8; 32],
        previous_blockhash: [u8; 32],
        transaction_count: u32,
    ) -> Self {
        Self {
            slot,
            block_time,
            block_height,
            parent_slot,
            blockhash,
            previous_blockhash,
            transaction_count,
        }
    }
}

/// Transaction metadata for live mode (debugging/audit trail).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTransaction {
    pub slot: u64,
    pub block_time: Option<i64>,
    #[serde(with = "serde_big_array::BigArray")]
    pub signature: [u8; 64],
    pub is_err: bool,
    pub err_msg: Option<String>,
    pub fee: u64,
    pub compute_units_consumed: Option<u64>,
    pub log_messages: Vec<String>,
    pub account_keys: Vec<[u8; 32]>,
}

/// Raw account state fetch result (live-only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveAccountRead {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub account_address: [u8; 32],
    pub owner: [u8; 32],
    pub data: Vec<u8>,
    pub lamports: u64,
    pub executable: bool,
}

/// Status tracking for a live slot's processing pipeline.
/// Parallels EVM's `LiveBlockStatus`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveSlotStatus {
    /// Slot notification received from WebSocket.
    pub collected: bool,
    /// Full block data fetched via getBlock RPC.
    pub block_fetched: bool,
    /// Events extracted from block transactions.
    pub events_extracted: bool,
    /// Instructions extracted from block transactions.
    pub instructions_extracted: bool,
    /// Events decoded via program decoders.
    pub events_decoded: bool,
    /// Instructions decoded via program decoders.
    pub instructions_decoded: bool,
    /// Account state reads completed (live-only).
    pub accounts_read: bool,
    /// Account state decoded via program decoders (live-only).
    pub accounts_decoded: bool,
    /// All transformation handlers complete.
    pub transformed: bool,
    /// Handler keys that have completed processing for this slot.
    #[serde(default)]
    pub completed_handlers: HashSet<String>,
    /// Handler keys that failed processing (will be retried).
    #[serde(default)]
    pub failed_handlers: HashSet<String>,
}

impl LiveSlotStatus {
    /// Create a new status indicating only initial collection.
    pub fn collected() -> Self {
        Self {
            collected: true,
            ..Default::default()
        }
    }

    /// Check if the slot is fully processed and ready for compaction.
    pub fn is_complete(&self) -> bool {
        self.collected
            && self.block_fetched
            && self.events_extracted
            && self.instructions_extracted
            && self.events_decoded
            && self.instructions_decoded
            && self.accounts_read
            && self.accounts_decoded
            && self.transformed
    }

    /// Check completeness using the active runtime pipeline expectations.
    pub fn is_complete_with(&self, expectations: &SolanaLivePipelineExpectations) -> bool {
        self.collected
            && self.block_fetched
            && self.events_extracted
            && self.instructions_extracted
            && (!expectations.expect_event_decode || self.events_decoded)
            && (!expectations.expect_instruction_decode || self.instructions_decoded)
            && (!expectations.expect_account_reads || self.accounts_read)
            && (!expectations.expect_account_reads || self.accounts_decoded)
            && (!expectations.expect_transformations || self.transformed)
    }

    /// Check whether all inputs needed for transformation are ready.
    ///
    /// Returns true when all expected decode stages have completed,
    /// meaning transformation handlers can safely run.
    pub fn transform_inputs_ready_with(
        &self,
        expectations: &SolanaLivePipelineExpectations,
    ) -> bool {
        (!expectations.expect_event_decode || self.events_decoded)
            && (!expectations.expect_instruction_decode || self.instructions_decoded)
            && (!expectations.expect_account_reads || self.accounts_decoded)
    }

    /// Normalize optional stage flags to match disabled runtime expectations.
    ///
    /// Stages that are not expected by the pipeline are set to `true` so that
    /// completeness checks pass without waiting for them.
    pub fn apply_expectations(&mut self, expectations: &SolanaLivePipelineExpectations) {
        if !expectations.expect_event_decode {
            self.events_decoded = true;
        }

        if !expectations.expect_instruction_decode {
            self.instructions_decoded = true;
        }

        if !expectations.expect_account_reads {
            self.accounts_read = true;
            self.accounts_decoded = true;
        }

        if !expectations.expect_transformations {
            self.transformed = true;
        }
    }
}

/// Runtime expectations for optional live-mode stages.
///
/// These are derived from the active pipeline wiring rather than persisted in
/// status files, allowing status reconstruction to remain backward compatible.
#[derive(Debug, Clone, Copy, Default)]
pub struct SolanaLivePipelineExpectations {
    /// Whether decoded event output is expected from the live decoder.
    pub expect_event_decode: bool,
    /// Whether decoded instruction output is expected from the live decoder.
    pub expect_instruction_decode: bool,
    /// Whether live account reads are enabled for this chain.
    pub expect_account_reads: bool,
    /// Whether transformation handlers are expected to run for the slot.
    pub expect_transformations: bool,
}

/// Messages for Solana live mode processing.
#[derive(Debug)]
pub enum SolanaLiveMessage {
    /// A new slot was received.
    Slot(LiveSlot),
    /// A reorg was detected (slot fork).
    Reorg {
        /// The slot number of the common ancestor (last valid slot).
        common_ancestor: u64,
        /// Slot numbers that were orphaned.
        orphaned: Vec<u64>,
    },
    /// All live processing should stop (shutdown signal).
    Shutdown,
}

/// Trigger for the account reader from the collector.
///
/// Sent after block data is fetched to initiate account state reads
/// for addresses relevant to a particular source/program.
#[derive(Debug)]
pub struct AccountReadTrigger {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub source_name: String,
    pub addresses: Vec<[u8; 32]>,
    /// When true, this explicit read participates in the deferred
    /// `mark_complete_only` barrier when deferred account completion is active.
    /// Startup Once reads set this false because they are not followed by that
    /// slot-scoped barrier.
    pub await_completion_barrier: bool,
    /// When true, skip reads and just mark account flags complete.
    /// Sent by the discovery loop after it has enqueued all expected
    /// discovery-managed account reads for a slot.
    pub mark_complete_only: bool,
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_slot_new() {
        let slot = LiveSlot::new(
            100,
            Some(1_700_000_000),
            Some(90),
            99,
            [1u8; 32],
            [2u8; 32],
            42,
        );
        assert_eq!(slot.slot, 100);
        assert_eq!(slot.block_time, Some(1_700_000_000));
        assert_eq!(slot.block_height, Some(90));
        assert_eq!(slot.parent_slot, 99);
        assert_eq!(slot.transaction_count, 42);
    }

    #[test]
    fn test_live_slot_serde_roundtrip() {
        let slot = LiveSlot {
            slot: 250_000_000,
            block_time: Some(1_700_000_000),
            block_height: Some(200_000_000),
            parent_slot: 249_999_999,
            blockhash: [0xAA; 32],
            previous_blockhash: [0xBB; 32],
            transaction_count: 100,
        };

        let bytes = bincode::serialize(&slot).unwrap();
        let deserialized: LiveSlot = bincode::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.slot, slot.slot);
        assert_eq!(deserialized.block_time, slot.block_time);
        assert_eq!(deserialized.blockhash, slot.blockhash);
        assert_eq!(deserialized.transaction_count, slot.transaction_count);
    }

    #[test]
    fn test_live_transaction_serde_roundtrip() {
        let tx = LiveTransaction {
            slot: 100,
            block_time: Some(1_700_000_000),
            signature: [0xCC; 64],
            is_err: false,
            err_msg: None,
            fee: 5000,
            compute_units_consumed: Some(200_000),
            log_messages: vec!["Program log: hello".to_string()],
            account_keys: vec![[0xDD; 32], [0xEE; 32]],
        };

        let bytes = bincode::serialize(&tx).unwrap();
        let deserialized: LiveTransaction = bincode::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.slot, 100);
        assert_eq!(deserialized.signature, [0xCC; 64]);
        assert_eq!(deserialized.fee, 5000);
        assert_eq!(deserialized.log_messages.len(), 1);
        assert_eq!(deserialized.account_keys.len(), 2);
    }

    #[test]
    fn test_live_account_read_serde_roundtrip() {
        let read = LiveAccountRead {
            slot: 100,
            block_time: Some(1_700_000_000),
            account_address: [0x11; 32],
            owner: [0x22; 32],
            data: vec![1, 2, 3, 4, 5],
            lamports: 1_000_000_000,
            executable: false,
        };

        let bytes = bincode::serialize(&read).unwrap();
        let deserialized: LiveAccountRead = bincode::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.account_address, [0x11; 32]);
        assert_eq!(deserialized.lamports, 1_000_000_000);
        assert!(!deserialized.executable);
        assert_eq!(deserialized.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_status_collected() {
        let status = LiveSlotStatus::collected();
        assert!(status.collected);
        assert!(!status.block_fetched);
        assert!(!status.events_extracted);
        assert!(!status.transformed);
    }

    #[test]
    fn test_status_is_complete() {
        let mut status = LiveSlotStatus {
            collected: true,
            block_fetched: true,
            events_extracted: true,
            instructions_extracted: true,
            events_decoded: true,
            instructions_decoded: true,
            accounts_read: true,
            accounts_decoded: true,
            transformed: true,
            ..Default::default()
        };
        assert!(status.is_complete());

        status.events_decoded = false;
        assert!(!status.is_complete());
    }

    #[test]
    fn test_status_is_complete_with_expectations() {
        let status = LiveSlotStatus {
            collected: true,
            block_fetched: true,
            events_extracted: true,
            instructions_extracted: true,
            events_decoded: false,
            instructions_decoded: false,
            accounts_read: false,
            accounts_decoded: false,
            transformed: false,
            ..Default::default()
        };

        // No optional stages expected => complete
        let expectations = SolanaLivePipelineExpectations::default();
        assert!(status.is_complete_with(&expectations));

        // Expect event decode => not complete
        let expectations = SolanaLivePipelineExpectations {
            expect_event_decode: true,
            ..Default::default()
        };
        assert!(!status.is_complete_with(&expectations));
    }

    #[test]
    fn test_status_transform_inputs_ready() {
        let status = LiveSlotStatus {
            events_decoded: true,
            instructions_decoded: true,
            accounts_decoded: false,
            ..Default::default()
        };

        let expectations = SolanaLivePipelineExpectations {
            expect_event_decode: true,
            expect_instruction_decode: true,
            expect_account_reads: false,
            expect_transformations: true,
        };
        assert!(status.transform_inputs_ready_with(&expectations));

        let expectations_with_accounts = SolanaLivePipelineExpectations {
            expect_event_decode: true,
            expect_instruction_decode: true,
            expect_account_reads: true,
            expect_transformations: true,
        };
        assert!(!status.transform_inputs_ready_with(&expectations_with_accounts));
    }

    #[test]
    fn test_status_apply_expectations() {
        let mut status = LiveSlotStatus::default();
        assert!(!status.events_decoded);
        assert!(!status.instructions_decoded);
        assert!(!status.accounts_read);
        assert!(!status.accounts_decoded);
        assert!(!status.transformed);

        let expectations = SolanaLivePipelineExpectations {
            expect_event_decode: false,
            expect_instruction_decode: true,
            expect_account_reads: false,
            expect_transformations: false,
        };

        status.apply_expectations(&expectations);

        assert!(status.events_decoded);
        assert!(!status.instructions_decoded);
        assert!(status.accounts_read);
        assert!(status.accounts_decoded);
        assert!(status.transformed);
    }

    #[test]
    fn test_status_completed_handlers() {
        let mut status = LiveSlotStatus::default();
        status.completed_handlers.insert("handler_a".to_string());
        status.completed_handlers.insert("handler_b".to_string());
        status.failed_handlers.insert("handler_c".to_string());

        assert!(status.completed_handlers.contains("handler_a"));
        assert!(status.completed_handlers.contains("handler_b"));
        assert!(status.failed_handlers.contains("handler_c"));
        assert_eq!(status.completed_handlers.len(), 2);
        assert_eq!(status.failed_handlers.len(), 1);
    }

    #[test]
    fn test_status_json_roundtrip() {
        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.completed_handlers.insert("my_handler".to_string());

        let json = serde_json::to_string_pretty(&status).unwrap();
        let deserialized: LiveSlotStatus = serde_json::from_str(&json).unwrap();

        assert!(deserialized.collected);
        assert!(deserialized.block_fetched);
        assert!(!deserialized.events_extracted);
        assert!(deserialized.completed_handlers.contains("my_handler"));
    }
}
