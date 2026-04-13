//! Record types and errors for Solana raw data collection.

#[cfg(feature = "solana")]
use solana_transaction_status::UiConfirmedBlock;

use serde::{Deserialize, Serialize};

/// Raw slot/block header data for parquet storage.
#[derive(Debug, Clone)]
pub struct SolanaSlotRecord {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub parent_slot: u64,
    pub blockhash: [u8; 32],
    pub previous_blockhash: [u8; 32],
    pub transaction_count: u32,
    pub transaction_signatures: Vec<[u8; 64]>,
}

/// Raw event extracted from logMessages via ProgramLogParser.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaEventRecord {
    pub slot: u64,
    pub block_time: Option<i64>,
    #[serde(with = "serde_big_array::BigArray")]
    pub transaction_signature: [u8; 64],
    pub program_id: [u8; 32],
    pub event_discriminator: [u8; 8],
    pub event_data: Vec<u8>,
    pub log_index: u32,
    pub instruction_index: u16,
    pub inner_instruction_index: Option<u16>,
}

/// Raw instruction extracted from transaction instruction tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaInstructionRecord {
    pub slot: u64,
    pub block_time: Option<i64>,
    #[serde(with = "serde_big_array::BigArray")]
    pub transaction_signature: [u8; 64],
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
    pub accounts: Vec<[u8; 32]>,
    pub instruction_index: u16,
    pub inner_instruction_index: Option<u16>,
}

/// Channel message from slot collector to extractor.
#[cfg(feature = "solana")]
pub struct SlotData {
    pub slot: u64,
    pub block: UiConfirmedBlock,
}

/// Errors from Solana raw data collection.
#[derive(Debug, thiserror::Error)]
pub enum SolanaCollectionError {
    #[error("RPC error: {0}")]
    Rpc(#[from] crate::solana::rpc::SolanaRpcError),

    #[error("Parquet write error: {0}")]
    ParquetWrite(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel send error: {0}")]
    ChannelSend(String),

    #[error("Join error: {0}")]
    JoinError(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_record_construction() {
        let record = SolanaSlotRecord {
            slot: 250_000_000,
            block_time: Some(1_700_000_000),
            block_height: Some(200_000_000),
            parent_slot: 249_999_999,
            blockhash: [1u8; 32],
            previous_blockhash: [2u8; 32],
            transaction_count: 42,
            transaction_signatures: vec![[3u8; 64], [4u8; 64]],
        };
        assert_eq!(record.slot, 250_000_000);
        assert_eq!(record.block_time, Some(1_700_000_000));
        assert_eq!(record.block_height, Some(200_000_000));
        assert_eq!(record.parent_slot, 249_999_999);
        assert_eq!(record.transaction_count, 42);
        assert_eq!(record.transaction_signatures.len(), 2);
    }

    #[test]
    fn test_slot_record_optional_fields_none() {
        let record = SolanaSlotRecord {
            slot: 1,
            block_time: None,
            block_height: None,
            parent_slot: 0,
            blockhash: [0u8; 32],
            previous_blockhash: [0u8; 32],
            transaction_count: 0,
            transaction_signatures: vec![],
        };
        assert!(record.block_time.is_none());
        assert!(record.block_height.is_none());
        assert!(record.transaction_signatures.is_empty());
    }

    #[test]
    fn test_event_record_construction() {
        let record = SolanaEventRecord {
            slot: 100,
            block_time: Some(1_700_000_000),
            transaction_signature: [5u8; 64],
            program_id: [6u8; 32],
            event_discriminator: [7u8; 8],
            event_data: vec![8, 9, 10],
            log_index: 3,
            instruction_index: 1,
            inner_instruction_index: Some(2),
        };
        assert_eq!(record.slot, 100);
        assert_eq!(record.log_index, 3);
        assert_eq!(record.inner_instruction_index, Some(2));
        assert_eq!(record.event_data, vec![8, 9, 10]);
    }

    #[test]
    fn test_event_record_no_inner_instruction() {
        let record = SolanaEventRecord {
            slot: 100,
            block_time: None,
            transaction_signature: [0u8; 64],
            program_id: [0u8; 32],
            event_discriminator: [0u8; 8],
            event_data: vec![],
            log_index: 0,
            instruction_index: 0,
            inner_instruction_index: None,
        };
        assert!(record.inner_instruction_index.is_none());
    }

    #[test]
    fn test_instruction_record_construction() {
        let record = SolanaInstructionRecord {
            slot: 200,
            block_time: Some(1_700_000_000),
            transaction_signature: [11u8; 64],
            program_id: [12u8; 32],
            data: vec![13, 14, 15, 16],
            accounts: vec![[17u8; 32], [18u8; 32], [19u8; 32]],
            instruction_index: 0,
            inner_instruction_index: None,
        };
        assert_eq!(record.slot, 200);
        assert_eq!(record.data.len(), 4);
        assert_eq!(record.accounts.len(), 3);
        assert!(record.inner_instruction_index.is_none());
    }

    #[test]
    fn test_instruction_record_inner_instruction() {
        let record = SolanaInstructionRecord {
            slot: 200,
            block_time: None,
            transaction_signature: [0u8; 64],
            program_id: [0u8; 32],
            data: vec![],
            accounts: vec![],
            instruction_index: 2,
            inner_instruction_index: Some(5),
        };
        assert_eq!(record.instruction_index, 2);
        assert_eq!(record.inner_instruction_index, Some(5));
    }

    #[test]
    fn test_collection_error_display() {
        let err = SolanaCollectionError::ParquetWrite("test error".to_string());
        assert_eq!(err.to_string(), "Parquet write error: test error");

        let err = SolanaCollectionError::ChannelSend("closed".to_string());
        assert_eq!(err.to_string(), "Channel send error: closed");

        let err = SolanaCollectionError::JoinError("task panicked".to_string());
        assert_eq!(err.to_string(), "Join error: task panicked");
    }

    #[test]
    fn test_event_record_serde_roundtrip() {
        let record = SolanaEventRecord {
            slot: 42,
            block_time: Some(1_700_000_000),
            transaction_signature: [0xAB; 64],
            program_id: [0xCD; 32],
            event_discriminator: [0xEF; 8],
            event_data: vec![1, 2, 3],
            log_index: 7,
            instruction_index: 3,
            inner_instruction_index: Some(1),
        };
        let json = serde_json::to_string(&record).unwrap();
        let deserialized: SolanaEventRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.slot, 42);
        assert_eq!(deserialized.transaction_signature, [0xAB; 64]);
        assert_eq!(deserialized.event_discriminator, [0xEF; 8]);
    }

    #[test]
    fn test_instruction_record_serde_roundtrip() {
        let record = SolanaInstructionRecord {
            slot: 99,
            block_time: None,
            transaction_signature: [0x11; 64],
            program_id: [0x22; 32],
            data: vec![0xFF, 0xFE],
            accounts: vec![[0x33; 32]],
            instruction_index: 5,
            inner_instruction_index: None,
        };
        let json = serde_json::to_string(&record).unwrap();
        let deserialized: SolanaInstructionRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.slot, 99);
        assert!(deserialized.block_time.is_none());
        assert!(deserialized.inner_instruction_index.is_none());
    }
}
