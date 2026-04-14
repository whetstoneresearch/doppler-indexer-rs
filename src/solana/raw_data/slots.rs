//! Slot-range raw data collector for Solana.
//!
//! Mirrors the EVM block collector (`raw_data/historical/catchup/blocks.rs`)
//! for Solana slots. For each range:
//!
//! 1. Fetch blocks via `get_blocks_batch`
//! 2. Track skipped slots
//! 3. Extract events and instructions from each block
//! 4. Write parquet files (slots, events, instructions)
//! 5. Update skipped slots index
//! 6. Send decoded events/instructions downstream via channels

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use solana_transaction_status::{EncodedTransaction, UiConfirmedBlock};
use tokio::sync::mpsc::Sender;

use crate::decoding::DecoderMessage;
use crate::solana::rpc::SolanaRpcClient;
use crate::storage::paths::{
    raw_solana_events_dir, raw_solana_instructions_dir, raw_solana_slots_dir, BlockRange,
};
use crate::storage::skipped_slots::{self, SkippedSlotsIndex};

use super::extraction::extract_events_and_instructions;
use super::parquet::{
    build_event_schema, build_instruction_schema, build_slot_schema, read_events_from_parquet,
    read_instructions_from_parquet, read_slots_from_parquet, write_events_to_parquet_async,
    write_instructions_to_parquet_async, write_slots_to_parquet_async,
};
use super::types::{SolanaCollectionError, SolanaSlotRecord};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Collect raw Solana data for a range of slots.
///
/// For each aligned range within `[start_slot, end_slot)`:
/// 1. Fetch blocks via `get_blocks_batch`
/// 2. Track skipped slots
/// 3. Extract events and instructions from each block
/// 4. Write parquet files (slots, events, instructions)
/// 5. Update skipped slots index
/// 6. Send decoded events/instructions downstream via channels
#[allow(clippy::too_many_arguments)]
pub async fn collect_solana_raw_data(
    chain_name: &str,
    rpc_client: &SolanaRpcClient,
    start_slot: u64,
    end_slot: u64, // exclusive
    range_size: u64,
    configured_programs: &HashSet<[u8; 32]>,
    event_decoder_tx: Option<&Sender<DecoderMessage>>,
    instr_decoder_tx: Option<&Sender<DecoderMessage>>,
    _catch_up_only: bool,
) -> Result<(), SolanaCollectionError> {
    let slots_dir = raw_solana_slots_dir(chain_name);
    let events_dir = raw_solana_events_dir(chain_name);
    let instructions_dir = raw_solana_instructions_dir(chain_name);

    // Create directories
    std::fs::create_dir_all(&slots_dir)?;
    std::fs::create_dir_all(&events_dir)?;
    std::fs::create_dir_all(&instructions_dir)?;

    let slot_schema = build_slot_schema();
    let event_schema = build_event_schema();
    let instruction_schema = build_instruction_schema();

    // Load skipped slots index for resume logic
    let mut skipped_index = skipped_slots::read_skipped_slots_index(&events_dir);

    // Compute ranges to fetch
    let ranges = compute_slot_ranges_to_fetch(
        start_slot,
        end_slot,
        range_size,
        &events_dir,
        &skipped_index,
    );

    if ranges.is_empty() {
        tracing::info!(chain = chain_name, "No slot ranges to collect");
        return Ok(());
    }

    tracing::info!(
        chain = chain_name,
        ranges = ranges.len(),
        start = ranges.first().unwrap().start,
        end = ranges.last().unwrap().end,
        "Starting Solana raw data collection"
    );

    for range in &ranges {
        // Build list of all slots in this range
        let slots: Vec<u64> = (range.start..range.end).collect();

        // Fetch blocks in batch
        let block_results = rpc_client.get_blocks_batch(&slots).await?;

        let mut slot_records: BTreeMap<u64, SolanaSlotRecord> = BTreeMap::new();
        let mut all_events = Vec::new();
        let mut all_instructions = Vec::new();
        let mut range_skipped_slots = Vec::new();

        for (slot, maybe_block) in block_results {
            match maybe_block {
                None => {
                    range_skipped_slots.push(slot);
                }
                Some(block) => {
                    // Build slot record
                    let slot_record = build_slot_record(slot, &block);
                    slot_records.insert(slot, slot_record);

                    // Extract events and instructions
                    let (events, instructions) =
                        extract_events_and_instructions(&block, slot, configured_programs);
                    all_events.extend(events);
                    all_instructions.extend(instructions);
                }
            }
        }

        let rk = skipped_slots::range_key(range.start, range.end_inclusive());

        // Write parquet files
        let slot_records_vec: Vec<_> = slot_records.into_values().collect();

        if !slot_records_vec.is_empty() {
            let slot_path = slots_dir.join(range.file_name("slots"));
            write_slots_to_parquet_async(slot_records_vec, slot_schema.clone(), slot_path).await?;
        }

        let events_path = events_dir.join(range.file_name("events"));
        write_events_to_parquet_async(all_events.clone(), event_schema.clone(), events_path)
            .await?;

        let instructions_path = instructions_dir.join(range.file_name("instructions"));
        write_instructions_to_parquet_async(
            all_instructions.clone(),
            instruction_schema.clone(),
            instructions_path,
        )
        .await?;

        // Update skipped slots index
        skipped_index.insert(rk, range_skipped_slots);
        skipped_slots::write_skipped_slots_index(&events_dir, &skipped_index)?;

        // Send downstream
        if let Some(tx) = event_decoder_tx {
            let msg = DecoderMessage::SolanaEventsReady {
                range_start: range.start,
                range_end: range.end,
                events: all_events,
                live_mode: false,
            };
            tx.send(msg)
                .await
                .map_err(|e: tokio::sync::mpsc::error::SendError<DecoderMessage>| {
                    SolanaCollectionError::ChannelSend(e.to_string())
                })?;
        }

        if let Some(tx) = instr_decoder_tx {
            let msg = DecoderMessage::SolanaInstructionsReady {
                range_start: range.start,
                range_end: range.end,
                instructions: all_instructions,
                live_mode: false,
            };
            tx.send(msg)
                .await
                .map_err(|e: tokio::sync::mpsc::error::SendError<DecoderMessage>| {
                    SolanaCollectionError::ChannelSend(e.to_string())
                })?;
        }

        tracing::info!(
            chain = chain_name,
            range_start = range.start,
            range_end = range.end,
            "Completed slot range"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Range computation
// ---------------------------------------------------------------------------

/// Compute the list of slot ranges that need to be fetched.
///
/// Aligns `start` down to the nearest multiple of `range_size`, generates all
/// aligned ranges up to `end` (exclusive), and filters out ranges that are
/// already marked complete in the skipped slots index (indicating all parquet
/// files for that range have already been written).
pub fn compute_slot_ranges_to_fetch(
    start: u64,
    end: u64,
    range_size: u64,
    dir: &Path,
    skipped_index: &SkippedSlotsIndex,
) -> Vec<BlockRange> {
    if start >= end || range_size == 0 {
        return Vec::new();
    }

    let aligned_start = (start / range_size) * range_size;

    // Scan existing parquet files for completed ranges
    let existing_files = crate::storage::paths::scan_parquet_filenames(dir, "events_");

    let mut ranges = Vec::new();
    let mut current = aligned_start;
    while current < end {
        let range_end = std::cmp::min(current + range_size, end);
        let range = BlockRange {
            start: current,
            end: range_end,
        };

        let rk = skipped_slots::range_key(range.start, range.end_inclusive());

        // Skip if already complete (exists in skipped index, meaning we processed it)
        let already_complete = skipped_slots::is_range_complete(skipped_index, &rk);

        // Also skip if parquet file already exists
        let file_exists = existing_files.contains(&range.file_name("events"));

        if !already_complete && !file_exists {
            ranges.push(range);
        }

        current += range_size;
    }

    ranges
}

// ---------------------------------------------------------------------------
// Slot record building
// ---------------------------------------------------------------------------

/// Build a [`SolanaSlotRecord`] from a slot number and its block.
///
/// Extracts header fields from the block, decoding base58 blockhash strings
/// to `[u8; 32]` and collecting transaction signatures.
pub fn build_slot_record(slot: u64, block: &UiConfirmedBlock) -> SolanaSlotRecord {
    let blockhash = decode_blockhash(&block.blockhash);
    let previous_blockhash = decode_blockhash(&block.previous_blockhash);

    let transaction_signatures = extract_block_signatures(block);
    let transaction_count = transaction_signatures.len() as u32;

    SolanaSlotRecord {
        slot,
        block_time: block.block_time,
        block_height: block.block_height,
        parent_slot: block.parent_slot,
        blockhash,
        previous_blockhash,
        transaction_count,
        transaction_signatures,
    }
}

/// Decode a base58-encoded blockhash string into `[u8; 32]`.
///
/// Returns `[0u8; 32]` if decoding fails (should not happen for valid blocks).
fn decode_blockhash(hash_str: &str) -> [u8; 32] {
    match bs58::decode(hash_str).into_vec() {
        Ok(bytes) if bytes.len() == 32 => {
            let mut out = [0u8; 32];
            out.copy_from_slice(&bytes);
            out
        }
        Ok(bytes) => {
            tracing::warn!(
                blockhash = hash_str,
                len = bytes.len(),
                "Unexpected blockhash length, expected 32 bytes"
            );
            [0u8; 32]
        }
        Err(e) => {
            tracing::warn!(blockhash = hash_str, error = %e, "Failed to decode blockhash");
            [0u8; 32]
        }
    }
}

/// Extract all transaction signatures from a block as `[u8; 64]` arrays.
fn extract_block_signatures(block: &UiConfirmedBlock) -> Vec<[u8; 64]> {
    let transactions = match &block.transactions {
        Some(txs) => txs,
        None => return Vec::new(),
    };

    let mut signatures = Vec::with_capacity(transactions.len());

    for encoded_tx in transactions {
        // Try decoding binary transaction first
        if let Some(versioned_tx) = encoded_tx.transaction.decode() {
            if let Some(sig) = versioned_tx.signatures.first() {
                let mut out = [0u8; 64];
                out.copy_from_slice(sig.as_ref());
                signatures.push(out);
                continue;
            }
        }

        // Fall back to JSON path
        if let EncodedTransaction::Json(ui_tx) = &encoded_tx.transaction {
            if let Some(sig_str) = ui_tx.signatures.first() {
                if let Ok(sig) = sig_str.parse::<solana_sdk::signature::Signature>() {
                    let mut out = [0u8; 64];
                    out.copy_from_slice(sig.as_ref());
                    signatures.push(out);
                }
            }
        }
    }

    signatures
}

// ---------------------------------------------------------------------------
// Selective slot collection (for signature-driven backfill)
// ---------------------------------------------------------------------------

/// Collect raw Solana data for a specific set of slots within a range.
///
/// Similar to [`collect_solana_raw_data`] but only fetches the given slots
/// instead of every slot in the range. Used by the signature-driven backfill
/// which knows exactly which slots contain relevant transactions.
///
/// When `repair_slots` is `Some`, only those specific slots are re-fetched and
/// the new data is **merged** with existing parquet records for the range.
/// Records from the repaired slots are replaced; records from other slots in
/// the range are preserved.
#[allow(clippy::too_many_arguments)]
pub async fn collect_slots_selective(
    chain_name: &str,
    rpc_client: &SolanaRpcClient,
    range: &BlockRange,
    target_slots: &BTreeSet<u64>,
    configured_programs: &HashSet<[u8; 32]>,
    event_decoder_tx: Option<&Sender<DecoderMessage>>,
    instr_decoder_tx: Option<&Sender<DecoderMessage>>,
    repair_slots: Option<&BTreeSet<u64>>,
    repair_program_ids: Option<&HashSet<[u8; 32]>>,
) -> Result<(), SolanaCollectionError> {
    let slots_dir = raw_solana_slots_dir(chain_name);
    let events_dir = raw_solana_events_dir(chain_name);
    let instructions_dir = raw_solana_instructions_dir(chain_name);

    let slot_schema = build_slot_schema();
    let event_schema = build_event_schema();
    let instruction_schema = build_instruction_schema();

    // Only fetch slots that fall within this range
    let slots_to_fetch: Vec<u64> = target_slots
        .range(range.start..range.end)
        .copied()
        .collect();

    if slots_to_fetch.is_empty() {
        return Ok(());
    }

    let block_results = rpc_client.get_blocks_batch(&slots_to_fetch).await?;

    let mut slot_records: BTreeMap<u64, SolanaSlotRecord> = BTreeMap::new();
    let mut all_events = Vec::new();
    let mut all_instructions = Vec::new();
    let mut skipped_slots_list = Vec::new();

    for (slot, maybe_block) in block_results {
        match maybe_block {
            None => {
                skipped_slots_list.push(slot);
            }
            Some(block) => {
                let slot_record = build_slot_record(slot, &block);
                slot_records.insert(slot, slot_record);

                let (events, instructions) =
                    extract_events_and_instructions(&block, slot, configured_programs);
                all_events.extend(events);
                all_instructions.extend(instructions);
            }
        }
    }

    // In repair mode, merge fresh data with existing records from the parquet
    // file so that non-repaired slots in the same aligned range are preserved.
    // When repair_program_ids is set (source-scoped repair), only drop records
    // that match both the repaired slot AND the repaired program; records from
    // other programs in the same slot are retained.
    //
    // The merged result is sent to decoders (not just the fresh subset) so that
    // decoded parquet output is complete for the full aligned range.
    if let Some(repaired) = repair_slots {
        let is_being_repaired = |slot: u64, program_id: &[u8; 32]| -> bool {
            if !repaired.contains(&slot) {
                return false;
            }
            match repair_program_ids {
                Some(programs) => programs.contains(program_id),
                None => true, // unscoped repair: all programs in repaired slots
            }
        };

        let events_path = events_dir.join(range.file_name("events"));
        if events_path.exists() {
            let existing = read_events_from_parquet(&events_path)?;
            let retained: Vec<_> = existing
                .into_iter()
                .filter(|r| !is_being_repaired(r.slot, &r.program_id))
                .collect();
            all_events.extend(retained);
        }
        all_events.sort_by_key(|r| (r.slot, r.log_index));

        let instr_path = instructions_dir.join(range.file_name("instructions"));
        if instr_path.exists() {
            let existing = read_instructions_from_parquet(&instr_path)?;
            let retained: Vec<_> = existing
                .into_iter()
                .filter(|r| !is_being_repaired(r.slot, &r.program_id))
                .collect();
            all_instructions.extend(retained);
        }
        all_instructions.sort_by_key(|r| (r.slot, r.instruction_index));
    }

    // In repair mode, merge slot records with existing so non-repaired slots
    // in the same aligned range are preserved.
    if let Some(repaired) = repair_slots {
        let slot_path = slots_dir.join(range.file_name("slots"));
        if slot_path.exists() {
            let existing = read_slots_from_parquet(&slot_path)?;
            for record in existing {
                if !repaired.contains(&record.slot) {
                    slot_records.entry(record.slot).or_insert(record);
                }
            }
        }
    }

    // Write parquet files
    let slot_records_vec: Vec<_> = slot_records.into_values().collect();

    if !slot_records_vec.is_empty() {
        let slot_path = slots_dir.join(range.file_name("slots"));
        write_slots_to_parquet_async(slot_records_vec, slot_schema, slot_path).await?;
    }

    let events_path = events_dir.join(range.file_name("events"));
    write_events_to_parquet_async(all_events.clone(), event_schema, events_path).await?;

    let instructions_path = instructions_dir.join(range.file_name("instructions"));
    write_instructions_to_parquet_async(all_instructions.clone(), instruction_schema, instructions_path).await?;

    // Update skipped slots index
    let mut skipped_index = skipped_slots::read_skipped_slots_index(&events_dir);
    let rk = skipped_slots::range_key(range.start, range.end_inclusive());
    if let Some(repaired) = repair_slots {
        // Merge: keep existing skipped slots for non-repaired slots,
        // add new skipped slots from repaired slots
        let mut merged = skipped_index
            .get(&rk)
            .cloned()
            .unwrap_or_default();
        // Remove skipped entries for slots being repaired (they may no longer be skipped)
        merged.retain(|s| !repaired.contains(s));
        // Add newly discovered skipped slots
        merged.extend(&skipped_slots_list);
        merged.sort_unstable();
        merged.dedup();
        skipped_index.insert(rk, merged);
    } else {
        skipped_index.insert(rk, skipped_slots_list);
    }
    skipped_slots::write_skipped_slots_index(&events_dir, &skipped_index)?;

    if let Some(tx) = event_decoder_tx {
        let msg = DecoderMessage::SolanaEventsReady {
            range_start: range.start,
            range_end: range.end,
            events: all_events,
            live_mode: false,
        };
        tx.send(msg)
            .await
            .map_err(|e: tokio::sync::mpsc::error::SendError<DecoderMessage>| {
                SolanaCollectionError::ChannelSend(e.to_string())
            })?;
    }

    if let Some(tx) = instr_decoder_tx {
        let msg = DecoderMessage::SolanaInstructionsReady {
            range_start: range.start,
            range_end: range.end,
            instructions: all_instructions,
            live_mode: false,
        };
        tx.send(msg)
            .await
            .map_err(|e: tokio::sync::mpsc::error::SendError<DecoderMessage>| {
                SolanaCollectionError::ChannelSend(e.to_string())
            })?;
    }

    tracing::info!(
        chain = chain_name,
        range_start = range.start,
        range_end = range.end,
        fetched_slots = slots_to_fetch.len(),
        "Completed selective slot range"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::skipped_slots::SkippedSlotsIndex;

    #[test]
    fn test_compute_slot_ranges_basic() {
        let empty_index = SkippedSlotsIndex::new();
        let dir = tempfile::TempDir::new().unwrap();

        let ranges = compute_slot_ranges_to_fetch(0, 3000, 1000, dir.path(), &empty_index);

        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1000);
        assert_eq!(ranges[1].start, 1000);
        assert_eq!(ranges[1].end, 2000);
        assert_eq!(ranges[2].start, 2000);
        assert_eq!(ranges[2].end, 3000);
    }

    #[test]
    fn test_compute_slot_ranges_alignment() {
        let empty_index = SkippedSlotsIndex::new();
        let dir = tempfile::TempDir::new().unwrap();

        // start_slot=500 should align down to 0
        let ranges = compute_slot_ranges_to_fetch(500, 2500, 1000, dir.path(), &empty_index);

        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1000);
        assert_eq!(ranges[1].start, 1000);
        assert_eq!(ranges[1].end, 2000);
        assert_eq!(ranges[2].start, 2000);
        assert_eq!(ranges[2].end, 2500);
    }

    #[test]
    fn test_compute_slot_ranges_skips_completed() {
        let mut index = SkippedSlotsIndex::new();
        // Mark range 1000-1999 as complete
        index.insert(skipped_slots::range_key(1000, 1999), vec![1050]);

        let dir = tempfile::TempDir::new().unwrap();

        let ranges = compute_slot_ranges_to_fetch(0, 3000, 1000, dir.path(), &index);

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1000);
        assert_eq!(ranges[1].start, 2000);
        assert_eq!(ranges[1].end, 3000);
    }

    #[test]
    fn test_compute_slot_ranges_empty_when_all_complete() {
        let mut index = SkippedSlotsIndex::new();
        index.insert(skipped_slots::range_key(0, 999), vec![]);
        index.insert(skipped_slots::range_key(1000, 1999), vec![]);

        let dir = tempfile::TempDir::new().unwrap();

        let ranges = compute_slot_ranges_to_fetch(0, 2000, 1000, dir.path(), &index);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_compute_slot_ranges_empty_input() {
        let empty_index = SkippedSlotsIndex::new();
        let dir = tempfile::TempDir::new().unwrap();

        // start >= end
        let ranges = compute_slot_ranges_to_fetch(1000, 1000, 1000, dir.path(), &empty_index);
        assert!(ranges.is_empty());

        let ranges = compute_slot_ranges_to_fetch(2000, 1000, 1000, dir.path(), &empty_index);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_compute_slot_ranges_zero_range_size() {
        let empty_index = SkippedSlotsIndex::new();
        let dir = tempfile::TempDir::new().unwrap();

        let ranges = compute_slot_ranges_to_fetch(0, 1000, 0, dir.path(), &empty_index);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_build_slot_record_basic() {
        use solana_transaction_status::UiConfirmedBlock;

        // Create a valid base58 32-byte blockhash
        let blockhash_bytes = [1u8; 32];
        let blockhash_b58 = bs58::encode(&blockhash_bytes).into_string();

        let prev_hash_bytes = [2u8; 32];
        let prev_hash_b58 = bs58::encode(&prev_hash_bytes).into_string();

        let block = UiConfirmedBlock {
            previous_blockhash: prev_hash_b58,
            blockhash: blockhash_b58,
            parent_slot: 99,
            transactions: None,
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: Some(1_700_000_000),
            block_height: Some(200_000_000),
        };

        let record = build_slot_record(100, &block);

        assert_eq!(record.slot, 100);
        assert_eq!(record.block_time, Some(1_700_000_000));
        assert_eq!(record.block_height, Some(200_000_000));
        assert_eq!(record.parent_slot, 99);
        assert_eq!(record.blockhash, blockhash_bytes);
        assert_eq!(record.previous_blockhash, prev_hash_bytes);
        assert_eq!(record.transaction_count, 0);
        assert!(record.transaction_signatures.is_empty());
    }

    #[test]
    fn test_build_slot_record_optional_fields_none() {
        use solana_transaction_status::UiConfirmedBlock;

        let blockhash_b58 = bs58::encode([0u8; 32]).into_string();
        let prev_hash_b58 = bs58::encode([0u8; 32]).into_string();

        let block = UiConfirmedBlock {
            previous_blockhash: prev_hash_b58,
            blockhash: blockhash_b58,
            parent_slot: 0,
            transactions: Some(vec![]),
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: None,
            block_height: None,
        };

        let record = build_slot_record(1, &block);

        assert_eq!(record.slot, 1);
        assert!(record.block_time.is_none());
        assert!(record.block_height.is_none());
        assert_eq!(record.transaction_count, 0);
    }

    #[test]
    fn test_build_slot_record_with_transactions() {
        use solana_sdk::hash::Hash;
        use solana_sdk::message::Message;
        use solana_sdk::pubkey::Pubkey;
        use solana_sdk::signer::keypair::Keypair;
        use solana_sdk::signer::Signer;
        use solana_sdk::system_instruction;
        use solana_sdk::transaction::Transaction;
        use solana_transaction_status::{
            EncodedTransaction, EncodedTransactionWithStatusMeta, TransactionBinaryEncoding,
            UiConfirmedBlock,
        };

        let payer = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&payer.pubkey(), &to, 1_000_000);
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, Hash::new_unique());

        let expected_sig = tx.signatures[0];

        let tx_bytes = bincode::serialize(&tx).unwrap();
        let b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &tx_bytes,
        );

        let encoded_tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(b64, TransactionBinaryEncoding::Base64),
            meta: None,
            version: None,
        };

        let blockhash_b58 = bs58::encode([5u8; 32]).into_string();
        let prev_hash_b58 = bs58::encode([6u8; 32]).into_string();

        let block = UiConfirmedBlock {
            previous_blockhash: prev_hash_b58,
            blockhash: blockhash_b58,
            parent_slot: 99,
            transactions: Some(vec![encoded_tx]),
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: Some(1_700_000_000),
            block_height: Some(200_000_000),
        };

        let record = build_slot_record(100, &block);

        assert_eq!(record.transaction_count, 1);
        assert_eq!(record.transaction_signatures.len(), 1);

        let mut expected_bytes = [0u8; 64];
        expected_bytes.copy_from_slice(expected_sig.as_ref());
        assert_eq!(record.transaction_signatures[0], expected_bytes);
    }

    #[test]
    fn test_decode_blockhash_valid() {
        let original = [42u8; 32];
        let encoded = bs58::encode(&original).into_string();
        let decoded = decode_blockhash(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_blockhash_invalid() {
        // Invalid base58 should return zeros
        let decoded = decode_blockhash("not-valid-base58!!!");
        assert_eq!(decoded, [0u8; 32]);
    }
}
