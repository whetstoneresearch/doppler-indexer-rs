//! Instruction extraction from Solana transaction instruction trees.
//!
//! Extracts both top-level and inner (CPI) instructions from
//! [`EncodedTransactionWithStatusMeta`], resolving account indices to
//! 32-byte pubkey arrays and filtering by a set of configured program IDs.

use std::collections::HashSet;

use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, UiCompiledInstruction,
    UiTransactionStatusMeta,
};

use super::types::SolanaInstructionRecord;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Extract instructions from a single transaction's instruction tree.
///
/// Handles both top-level instructions (from the decoded transaction message)
/// and inner (CPI) instructions (from the metadata). Resolves account indices
/// to actual pubkey bytes and filters to only include instructions targeting
/// one of the `configured_programs`.
///
/// Returns an empty vec if:
/// - The transaction failed (`meta.err.is_some()`)
/// - The transaction metadata is missing
/// - The transaction cannot be decoded
pub fn extract_instructions_from_transaction(
    encoded_tx: &EncodedTransactionWithStatusMeta,
    slot: u64,
    block_time: Option<i64>,
    configured_programs: &HashSet<[u8; 32]>,
) -> Vec<SolanaInstructionRecord> {
    // Skip transactions without metadata.
    let Some(meta) = &encoded_tx.meta else {
        return Vec::new();
    };

    // Skip failed transactions.
    if meta.err.is_some() {
        return Vec::new();
    }

    // Decode the binary transaction to get the VersionedTransaction.
    let versioned_tx = match encoded_tx.transaction.decode() {
        Some(tx) => tx,
        None => {
            // JSON-encoded transactions: fall back to UiMessage parsing.
            return extract_from_json_transaction(
                encoded_tx,
                meta,
                slot,
                block_time,
                configured_programs,
            );
        }
    };

    let tx_sig = match versioned_tx.signatures.first() {
        Some(sig) => {
            let mut out = [0u8; 64];
            out.copy_from_slice(sig.as_ref());
            out
        }
        None => return Vec::new(),
    };

    // Build the full account key list: static keys + loaded addresses (ALT).
    let mut account_keys: Vec<Pubkey> = versioned_tx.message.static_account_keys().to_vec();
    append_loaded_addresses(&mut account_keys, meta);

    let mut records = Vec::new();

    // --- Top-level instructions ---
    let instructions = versioned_tx.message.instructions();
    for (ix_index, ix) in instructions.iter().enumerate() {
        let program_id = match account_keys.get(ix.program_id_index as usize) {
            Some(pk) => pk.to_bytes(),
            None => continue,
        };

        if !configured_programs.contains(&program_id) {
            continue;
        }

        let accounts: Vec<[u8; 32]> = ix
            .accounts
            .iter()
            .filter_map(|&idx| account_keys.get(idx as usize).map(|pk| pk.to_bytes()))
            .collect();

        records.push(SolanaInstructionRecord {
            slot,
            block_time,
            transaction_signature: tx_sig,
            program_id,
            data: ix.data.clone(),
            accounts,
            instruction_index: ix_index as u16,
            inner_instruction_index: None,
        });
    }

    // --- Inner (CPI) instructions from metadata ---
    extract_inner_instructions_into(
        &mut records,
        meta,
        &account_keys,
        slot,
        block_time,
        &tx_sig,
        configured_programs,
    );

    records
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Fallback extraction path for JSON-encoded transactions (`UiMessage`).
///
/// This handles the case where `EncodedTransaction::decode()` returns `None`,
/// typically when the transaction was requested with JSON encoding.
fn extract_from_json_transaction(
    encoded_tx: &EncodedTransactionWithStatusMeta,
    meta: &UiTransactionStatusMeta,
    slot: u64,
    block_time: Option<i64>,
    configured_programs: &HashSet<[u8; 32]>,
) -> Vec<SolanaInstructionRecord> {
    let EncodedTransaction::Json(ui_tx) = &encoded_tx.transaction else {
        return Vec::new();
    };

    // Extract signature.
    let tx_sig = match ui_tx.signatures.first() {
        Some(sig_str) => match sig_str.parse::<solana_sdk::signature::Signature>() {
            Ok(sig) => {
                let mut out = [0u8; 64];
                out.copy_from_slice(sig.as_ref());
                out
            }
            Err(_) => return Vec::new(),
        },
        None => return Vec::new(),
    };

    // Extract account keys from UiMessage.
    let (account_key_strings, instructions) = match &ui_tx.message {
        solana_transaction_status::UiMessage::Raw(raw) => {
            (&raw.account_keys, raw.instructions.clone())
        }
        solana_transaction_status::UiMessage::Parsed(_) => {
            // Parsed messages don't have UiCompiledInstruction directly.
            // We skip these since our RPC always uses Base64 encoding.
            return Vec::new();
        }
    };

    let mut account_keys: Vec<Pubkey> = account_key_strings
        .iter()
        .filter_map(|s| s.parse::<Pubkey>().ok())
        .collect();
    append_loaded_addresses(&mut account_keys, meta);

    let mut records = Vec::new();

    for (ix_index, ix) in instructions.iter().enumerate() {
        let program_id = match account_keys.get(ix.program_id_index as usize) {
            Some(pk) => pk.to_bytes(),
            None => continue,
        };

        if !configured_programs.contains(&program_id) {
            continue;
        }

        let accounts: Vec<[u8; 32]> = ix
            .accounts
            .iter()
            .filter_map(|&idx| account_keys.get(idx as usize).map(|pk| pk.to_bytes()))
            .collect();

        let data = match bs58::decode(&ix.data).into_vec() {
            Ok(d) => d,
            Err(_) => continue,
        };

        records.push(SolanaInstructionRecord {
            slot,
            block_time,
            transaction_signature: tx_sig,
            program_id,
            data,
            accounts,
            instruction_index: ix_index as u16,
            inner_instruction_index: None,
        });
    }

    // Inner instructions from metadata.
    extract_inner_instructions_into(
        &mut records,
        meta,
        &account_keys,
        slot,
        block_time,
        &tx_sig,
        configured_programs,
    );

    records
}

/// Extract inner (CPI) instructions from metadata and append to `records`.
fn extract_inner_instructions_into(
    records: &mut Vec<SolanaInstructionRecord>,
    meta: &UiTransactionStatusMeta,
    account_keys: &[Pubkey],
    slot: u64,
    block_time: Option<i64>,
    tx_sig: &[u8; 64],
    configured_programs: &HashSet<[u8; 32]>,
) {
    let inner_groups: Option<&Vec<_>> = meta.inner_instructions.as_ref().into();
    let Some(inner_groups) = inner_groups else {
        return;
    };

    for group in inner_groups {
        let parent_index = group.index;
        for (inner_idx, ui_ix) in group.instructions.iter().enumerate() {
            // Only handle compiled instructions.
            let ix = match ui_ix {
                solana_transaction_status::UiInstruction::Compiled(compiled) => compiled,
                _ => continue,
            };

            let program_id = match account_keys.get(ix.program_id_index as usize) {
                Some(pk) => pk.to_bytes(),
                None => continue,
            };

            if !configured_programs.contains(&program_id) {
                continue;
            }

            let accounts: Vec<[u8; 32]> = ix
                .accounts
                .iter()
                .filter_map(|&idx| account_keys.get(idx as usize).map(|pk| pk.to_bytes()))
                .collect();

            let data = match bs58::decode(&ix.data).into_vec() {
                Ok(d) => d,
                Err(_) => continue,
            };

            records.push(SolanaInstructionRecord {
                slot,
                block_time,
                transaction_signature: *tx_sig,
                program_id,
                data,
                accounts,
                instruction_index: parent_index as u16,
                inner_instruction_index: Some(inner_idx as u16),
            });
        }
    }
}

/// Append writable + readonly loaded addresses from metadata to the key list.
///
/// For v0 transactions, the RPC node resolves Address Lookup Table entries
/// and includes them in `meta.loaded_addresses`. We append writable first,
/// then readonly, matching the Solana convention.
fn append_loaded_addresses(keys: &mut Vec<Pubkey>, meta: &UiTransactionStatusMeta) {
    let loaded: Option<&solana_transaction_status::UiLoadedAddresses> =
        meta.loaded_addresses.as_ref().into();
    let Some(loaded) = loaded else {
        return;
    };

    for addr_str in &loaded.writable {
        if let Ok(pk) = addr_str.parse::<Pubkey>() {
            keys.push(pk);
        }
    }
    for addr_str in &loaded.readonly {
        if let Ok(pk) = addr_str.parse::<Pubkey>() {
            keys.push(pk);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use solana_transaction_status::option_serializer::OptionSerializer;
    use solana_transaction_status::TransactionBinaryEncoding;

    /// Helper to build a minimal `UiTransactionStatusMeta`.
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
    fn test_extract_instructions_no_meta() {
        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(
                String::new(),
                TransactionBinaryEncoding::Base64,
            ),
            meta: None,
            version: None,
        };

        let result = extract_instructions_from_transaction(&encoded_tx, 100, None, &HashSet::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_instructions_failed_tx() {
        let meta = test_meta(Some(
            solana_sdk::transaction::TransactionError::AccountNotFound,
        ));
        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(
                String::new(),
                TransactionBinaryEncoding::Base64,
            ),
            meta: Some(meta),
            version: None,
        };

        let result = extract_instructions_from_transaction(&encoded_tx, 100, None, &HashSet::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_instructions_invalid_binary() {
        // Non-decodable base64 string — should return empty gracefully.
        let meta = test_meta(None);
        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(
                "not-valid-base64!!!".to_string(),
                TransactionBinaryEncoding::Base64,
            ),
            meta: Some(meta),
            version: None,
        };

        let result = extract_instructions_from_transaction(&encoded_tx, 100, None, &HashSet::new());
        // The decode() call returns None, then we try JSON path which also
        // returns empty since it's not JSON.
        assert!(result.is_empty());
    }

    #[test]
    fn test_append_loaded_addresses_none() {
        let meta = test_meta(None);
        let mut keys: Vec<Pubkey> = vec![Pubkey::new_unique()];
        append_loaded_addresses(&mut keys, &meta);
        assert_eq!(keys.len(), 1); // no addresses appended
    }

    #[test]
    fn test_append_loaded_addresses_with_values() {
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let pk3 = Pubkey::new_unique();

        let loaded = solana_transaction_status::UiLoadedAddresses {
            writable: vec![pk1.to_string(), pk2.to_string()],
            readonly: vec![pk3.to_string()],
        };

        let mut meta = test_meta(None);
        meta.loaded_addresses = OptionSerializer::Some(loaded);

        let mut keys: Vec<Pubkey> = vec![Pubkey::new_unique()];
        append_loaded_addresses(&mut keys, &meta);
        assert_eq!(keys.len(), 4); // original + 2 writable + 1 readonly
        assert_eq!(keys[1], pk1);
        assert_eq!(keys[2], pk2);
        assert_eq!(keys[3], pk3);
    }

    #[test]
    fn test_extract_with_real_binary_transaction() {
        // Build a minimal legacy transaction, serialize it, and test extraction.
        use solana_sdk::hash::Hash;
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_sdk::message::Message;
        use solana_sdk::signer::keypair::Keypair;
        use solana_sdk::signer::Signer;
        use solana_sdk::transaction::Transaction;

        let payer = Keypair::new();
        let program_id = Pubkey::new_unique();
        let account1 = Pubkey::new_unique();
        let account2 = Pubkey::new_unique();

        let ix = Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(account1, false),
                AccountMeta::new_readonly(account2, false),
            ],
            data: vec![1, 2, 3, 4],
        };

        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let recent_blockhash = Hash::new_unique();
        let tx = Transaction::new(&[&payer], msg, recent_blockhash);

        // Serialize and base64-encode.
        let tx_bytes = bincode::serialize(&tx).unwrap();
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);

        let meta = test_meta(None);
        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(b64, TransactionBinaryEncoding::Base64),
            meta: Some(meta),
            version: None,
        };

        // Without the program configured, no instructions extracted.
        let result =
            extract_instructions_from_transaction(&encoded_tx, 42, Some(999), &HashSet::new());
        assert!(result.is_empty());

        // With the program configured, we get the instruction.
        let mut configured = HashSet::new();
        configured.insert(program_id.to_bytes());

        let result = extract_instructions_from_transaction(&encoded_tx, 42, Some(999), &configured);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].slot, 42);
        assert_eq!(result[0].block_time, Some(999));
        assert_eq!(result[0].program_id, program_id.to_bytes());
        assert_eq!(result[0].data, vec![1, 2, 3, 4]);
        assert_eq!(result[0].instruction_index, 0);
        assert!(result[0].inner_instruction_index.is_none());
        assert_eq!(result[0].accounts.len(), 2);
        assert_eq!(result[0].accounts[0], account1.to_bytes());
        assert_eq!(result[0].accounts[1], account2.to_bytes());
    }

    #[test]
    fn test_extract_with_inner_instructions() {
        // Build a transaction and attach inner instructions via metadata.
        use solana_sdk::hash::Hash;
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_sdk::message::Message;
        use solana_sdk::signer::keypair::Keypair;
        use solana_sdk::signer::Signer;
        use solana_sdk::transaction::Transaction;

        let payer = Keypair::new();
        let outer_program = Pubkey::new_unique();
        let inner_program = Pubkey::new_unique();
        let account1 = Pubkey::new_unique();

        // Top-level instruction.
        let ix = Instruction {
            program_id: outer_program,
            accounts: vec![AccountMeta::new(account1, false)],
            data: vec![10, 20],
        };

        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let recent_blockhash = Hash::new_unique();
        let tx = Transaction::new(&[&payer], msg, recent_blockhash);

        let tx_bytes = bincode::serialize(&tx).unwrap();
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);

        // Figure out inner_program's index in the account keys.
        // The transaction's account keys are: [payer, account1, outer_program].
        // We need inner_program to be in the keys. We'll add it via loaded_addresses.
        let inner_program_idx = 3u8; // After payer, account1, outer_program

        let inner_ix = UiCompiledInstruction {
            program_id_index: inner_program_idx,
            accounts: vec![1], // account1
            data: bs58::encode(vec![30, 40, 50]).into_string(),
            stack_height: Some(2),
        };

        let inner_group = solana_transaction_status::UiInnerInstructions {
            index: 0, // parent instruction index
            instructions: vec![solana_transaction_status::UiInstruction::Compiled(inner_ix)],
        };

        let loaded = solana_transaction_status::UiLoadedAddresses {
            writable: vec![inner_program.to_string()],
            readonly: vec![],
        };

        let mut meta = test_meta(None);
        meta.inner_instructions = OptionSerializer::Some(vec![inner_group]);
        meta.loaded_addresses = OptionSerializer::Some(loaded);

        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(b64, TransactionBinaryEncoding::Base64),
            meta: Some(meta),
            version: None,
        };

        // Configure both programs.
        let mut configured = HashSet::new();
        configured.insert(outer_program.to_bytes());
        configured.insert(inner_program.to_bytes());

        let result = extract_instructions_from_transaction(&encoded_tx, 100, None, &configured);

        assert_eq!(result.len(), 2);

        // Top-level instruction.
        assert_eq!(result[0].program_id, outer_program.to_bytes());
        assert_eq!(result[0].instruction_index, 0);
        assert!(result[0].inner_instruction_index.is_none());

        // Inner instruction.
        assert_eq!(result[1].program_id, inner_program.to_bytes());
        assert_eq!(result[1].instruction_index, 0);
        assert_eq!(result[1].inner_instruction_index, Some(0));
        assert_eq!(result[1].data, vec![30, 40, 50]);
    }

    #[test]
    fn test_extract_filters_by_configured_programs() {
        // Two top-level instructions to different programs, only one configured.
        use solana_sdk::hash::Hash;
        use solana_sdk::instruction::{AccountMeta, Instruction};
        use solana_sdk::message::Message;
        use solana_sdk::signer::keypair::Keypair;
        use solana_sdk::signer::Signer;
        use solana_sdk::transaction::Transaction;

        let payer = Keypair::new();
        let program_a = Pubkey::new_unique();
        let program_b = Pubkey::new_unique();
        let account = Pubkey::new_unique();

        let ix_a = Instruction {
            program_id: program_a,
            accounts: vec![AccountMeta::new(account, false)],
            data: vec![1],
        };
        let ix_b = Instruction {
            program_id: program_b,
            accounts: vec![AccountMeta::new(account, false)],
            data: vec![2],
        };

        let msg = Message::new(&[ix_a, ix_b], Some(&payer.pubkey()));
        let recent_blockhash = Hash::new_unique();
        let tx = Transaction::new(&[&payer], msg, recent_blockhash);

        let tx_bytes = bincode::serialize(&tx).unwrap();
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);

        let meta = test_meta(None);
        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(b64, TransactionBinaryEncoding::Base64),
            meta: Some(meta),
            version: None,
        };

        // Only configure program_b.
        let mut configured = HashSet::new();
        configured.insert(program_b.to_bytes());

        let result = extract_instructions_from_transaction(&encoded_tx, 500, None, &configured);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].program_id, program_b.to_bytes());
        assert_eq!(result[0].instruction_index, 1);
        assert_eq!(result[0].data, vec![2]);
    }
}
