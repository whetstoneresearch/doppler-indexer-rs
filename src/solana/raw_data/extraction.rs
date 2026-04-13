//! Combined single-pass extraction of events and instructions from a block.
//!
//! Iterates over all transactions in a [`UiConfirmedBlock`] once, extracting
//! both event records (from log messages) and instruction records (from the
//! instruction tree) in parallel, skipping failed transactions.

use std::collections::HashSet;

use solana_transaction_status::{EncodedTransaction, UiConfirmedBlock};

use super::events::extract_events_from_logs;
use super::instructions::extract_instructions_from_transaction;
use super::types::{SolanaEventRecord, SolanaInstructionRecord};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Extract both events and instructions from all transactions in a block.
///
/// Performs a single pass over the block's transaction list. For each
/// transaction that did not fail:
/// 1. Extracts events from `logMessages` via [`extract_events_from_logs`]
/// 2. Extracts instructions from the instruction tree via
///    [`extract_instructions_from_transaction`]
///
/// Failed transactions are skipped per the design spec (section 8.6).
pub fn extract_events_and_instructions(
    block: &UiConfirmedBlock,
    slot: u64,
    configured_programs: &HashSet<[u8; 32]>,
) -> (Vec<SolanaEventRecord>, Vec<SolanaInstructionRecord>) {
    let block_time = block.block_time;

    let transactions = match &block.transactions {
        Some(txs) => txs,
        None => return (Vec::new(), Vec::new()),
    };

    let mut all_events = Vec::new();
    let mut all_instructions = Vec::new();

    for encoded_tx in transactions {
        // Skip transactions with no metadata.
        let meta = match &encoded_tx.meta {
            Some(m) => m,
            None => continue,
        };

        // Skip failed transactions.
        if meta.err.is_some() {
            continue;
        }

        // --- Events from log messages ---
        let log_messages: Option<&Vec<String>> = meta.log_messages.as_ref().into();
        if let Some(logs) = log_messages {
            if let Some(sig) = extract_signature_from_tx(&encoded_tx.transaction) {
                let events = extract_events_from_logs(
                    logs,
                    slot,
                    block_time,
                    &sig,
                    configured_programs,
                );
                all_events.extend(events);
            }
        }

        // --- Instructions from instruction tree ---
        let instructions = extract_instructions_from_transaction(
            encoded_tx,
            slot,
            block_time,
            configured_programs,
        );
        all_instructions.extend(instructions);
    }

    (all_events, all_instructions)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Extract the 64-byte signature from an encoded transaction.
///
/// Uses `EncodedTransaction::decode()` for binary-encoded transactions,
/// falls back to parsing the JSON signature string.
fn extract_signature_from_tx(encoded_tx: &EncodedTransaction) -> Option<[u8; 64]> {
    // Try decoding the binary transaction first (covers Binary and LegacyBinary).
    if let Some(versioned_tx) = encoded_tx.decode() {
        let sig = versioned_tx.signatures.first()?;
        let mut out = [0u8; 64];
        out.copy_from_slice(sig.as_ref());
        return Some(out);
    }

    // Fall back to JSON path.
    if let EncodedTransaction::Json(ui_tx) = encoded_tx {
        let sig_str = ui_tx.signatures.first()?;
        let sig: solana_sdk::signature::Signature = sig_str.parse().ok()?;
        let mut out = [0u8; 64];
        out.copy_from_slice(sig.as_ref());
        return Some(out);
    }

    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use solana_transaction_status::{
        option_serializer::OptionSerializer, EncodedTransactionWithStatusMeta,
        TransactionBinaryEncoding, UiTransactionStatusMeta,
    };

    /// Helper to build minimal test metadata.
    fn test_meta(
        err: Option<solana_sdk::transaction::TransactionError>,
    ) -> UiTransactionStatusMeta {
        UiTransactionStatusMeta {
            err: err.clone(),
            status: match err {
                Some(e) => Err(e),
                None => Ok(()),
            },
            fee: 5000,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: OptionSerializer::None,
            log_messages: OptionSerializer::None,
            pre_token_balances: OptionSerializer::None,
            post_token_balances: OptionSerializer::None,
            rewards: OptionSerializer::None,
            loaded_addresses: OptionSerializer::None,
            return_data: OptionSerializer::None,
            compute_units_consumed: OptionSerializer::None,
            cost_units: OptionSerializer::None,
        }
    }

    #[test]
    fn test_extract_from_empty_block() {
        let block = UiConfirmedBlock {
            previous_blockhash: String::new(),
            blockhash: String::new(),
            parent_slot: 0,
            transactions: None,
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: None,
            block_height: None,
        };

        let (events, instructions) =
            extract_events_and_instructions(&block, 100, &HashSet::new());
        assert!(events.is_empty());
        assert!(instructions.is_empty());
    }

    #[test]
    fn test_extract_from_block_with_empty_transactions() {
        let block = UiConfirmedBlock {
            previous_blockhash: String::new(),
            blockhash: String::new(),
            parent_slot: 0,
            transactions: Some(vec![]),
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: Some(1_700_000_000),
            block_height: Some(200_000_000),
        };

        let (events, instructions) =
            extract_events_and_instructions(&block, 200, &HashSet::new());
        assert!(events.is_empty());
        assert!(instructions.is_empty());
    }

    #[test]
    fn test_extract_skips_failed_transactions() {
        let meta = test_meta(Some(
            solana_sdk::transaction::TransactionError::AccountNotFound,
        ));

        let tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(
                String::new(),
                TransactionBinaryEncoding::Base64,
            ),
            meta: Some(meta),
            version: None,
        };

        let block = UiConfirmedBlock {
            previous_blockhash: String::new(),
            blockhash: String::new(),
            parent_slot: 0,
            transactions: Some(vec![tx]),
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: None,
            block_height: None,
        };

        let (events, instructions) =
            extract_events_and_instructions(&block, 300, &HashSet::new());
        assert!(events.is_empty());
        assert!(instructions.is_empty());
    }

    #[test]
    fn test_extract_skips_tx_without_meta() {
        let tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(
                String::new(),
                TransactionBinaryEncoding::Base64,
            ),
            meta: None,
            version: None,
        };

        let block = UiConfirmedBlock {
            previous_blockhash: String::new(),
            blockhash: String::new(),
            parent_slot: 0,
            transactions: Some(vec![tx]),
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: None,
            block_height: None,
        };

        let (events, instructions) =
            extract_events_and_instructions(&block, 400, &HashSet::new());
        assert!(events.is_empty());
        assert!(instructions.is_empty());
    }

    #[test]
    fn test_extract_combined_events_and_instructions() {
        // Build a real transaction that has both a configured program instruction
        // and log messages containing an event.
        use solana_sdk::hash::Hash;
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_sdk::message::Message;
        use solana_sdk::pubkey::Pubkey;
        use solana_sdk::signer::keypair::Keypair;
        use solana_sdk::signer::Signer;
        use solana_sdk::transaction::Transaction;

        let payer = Keypair::new();
        let program_id = Pubkey::new_unique();
        let account = Pubkey::new_unique();

        let ix = Instruction {
            program_id,
            accounts: vec![AccountMeta::new(account, false)],
            data: vec![1, 2, 3],
        };

        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, Hash::new_unique());

        let tx_bytes = bincode::serialize(&tx).unwrap();
        let b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &tx_bytes,
        );

        // Build log messages with an event from program_id.
        let event_bytes: Vec<u8> = vec![10, 20, 30, 40, 50, 60, 70, 80, 0xAA, 0xBB];
        let event_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &event_bytes,
        );

        let logs = vec![
            format!("Program {} invoke [1]", program_id),
            format!("Program data: {}", event_b64),
            format!("Program {} success", program_id),
        ];

        let mut meta = test_meta(None);
        meta.log_messages = OptionSerializer::Some(logs);

        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(b64, TransactionBinaryEncoding::Base64),
            meta: Some(meta),
            version: None,
        };

        let block = UiConfirmedBlock {
            previous_blockhash: String::new(),
            blockhash: String::new(),
            parent_slot: 0,
            transactions: Some(vec![encoded_tx]),
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: Some(1_700_000_000),
            block_height: Some(200_000_000),
        };

        let mut configured = HashSet::new();
        configured.insert(program_id.to_bytes());

        let (events, instructions) =
            extract_events_and_instructions(&block, 500, &configured);

        // Should have 1 event and 1 instruction.
        assert_eq!(events.len(), 1);
        assert_eq!(instructions.len(), 1);

        // Verify event.
        assert_eq!(events[0].slot, 500);
        assert_eq!(events[0].block_time, Some(1_700_000_000));
        assert_eq!(events[0].program_id, program_id.to_bytes());
        assert_eq!(events[0].event_discriminator, [10, 20, 30, 40, 50, 60, 70, 80]);
        assert_eq!(events[0].event_data, vec![0xAA, 0xBB]);

        // Verify instruction.
        assert_eq!(instructions[0].slot, 500);
        assert_eq!(instructions[0].block_time, Some(1_700_000_000));
        assert_eq!(instructions[0].program_id, program_id.to_bytes());
        assert_eq!(instructions[0].data, vec![1, 2, 3]);
    }
}
