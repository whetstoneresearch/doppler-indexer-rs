//! Signature-driven historical backfill for Solana raw data.
//!
//! Instead of iterating every slot (many of which may have no relevant
//! transactions), this module uses `getSignaturesForAddress` to discover
//! exactly which slots contain transactions for the configured programs,
//! then fetches only those slots.
//!
//! Flow:
//! 1. For each configured program, paginate `getSignaturesForAddress` backward
//!    from chain head to the program's `start_slot`.
//! 2. Collect all discovered slots into a deduplicated set.
//! 3. Group slots into aligned ranges matching `range_size`.
//! 4. For each range, fetch the relevant blocks, extract events + instructions,
//!    and write parquet files.
//! 5. Resume from the latest processed slot on restart.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::sync::mpsc::Sender;

use crate::decoding::DecoderMessage;
use crate::solana::rpc::{SolanaRpcClient, SolanaRpcError};
use crate::storage::paths::{
    raw_solana_events_dir, raw_solana_instructions_dir, raw_solana_slots_dir, BlockRange,
};
use crate::storage::skipped_slots;
use crate::types::config::solana::SolanaPrograms;
use crate::types::shared::repair::RepairScope;

use super::slots::collect_slots_selective;
use super::types::SolanaCollectionError;

/// Maximum number of signatures returned per RPC page.
const SIGNATURES_PAGE_SIZE: usize = 1000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Signature-driven historical backfill.
///
/// For each configured program:
/// 1. Paginate `getSignaturesForAddress` backward from chain head to `start_slot`
/// 2. Deduplicate discovered slots across all programs
/// 3. Group slots into aligned ranges
/// 4. For each range: fetch blocks, extract events + instructions, write parquet
/// 5. Resume from latest processed slot on restart
#[allow(clippy::too_many_arguments)]
pub async fn signature_driven_backfill(
    chain_name: &str,
    rpc_client: &SolanaRpcClient,
    programs: &SolanaPrograms,
    range_size: u64,
    configured_programs: &HashSet<[u8; 32]>,
    event_decoder_tx: Option<&Sender<DecoderMessage>>,
    instr_decoder_tx: Option<&Sender<DecoderMessage>>,
    repair_scope: Option<&RepairScope>,
) -> Result<(), SolanaCollectionError> {
    if programs.is_empty() {
        tracing::info!(chain = chain_name, "No Solana programs configured, nothing to backfill");
        return Ok(());
    }

    let is_repair = repair_scope.is_some();

    // Create output directories
    let slots_dir = raw_solana_slots_dir(chain_name);
    let events_dir = raw_solana_events_dir(chain_name);
    let instructions_dir = raw_solana_instructions_dir(chain_name);

    std::fs::create_dir_all(&slots_dir)?;
    std::fs::create_dir_all(&events_dir)?;
    std::fs::create_dir_all(&instructions_dir)?;

    // Determine global start_slot (minimum across all programs)
    let global_start = programs
        .values()
        .filter_map(|p| p.start_slot)
        .min()
        .unwrap_or(0);

    // In repair mode, from_block overrides resume and global start.
    // In normal mode, resume from existing data.
    let effective_start = if let Some(scope) = repair_scope {
        scope.from_block.unwrap_or(global_start)
    } else {
        let resume_slot = find_resume_slot(chain_name);
        if let Some(slot) = resume_slot {
            tracing::info!(
                chain = chain_name,
                resume_slot = slot,
                "Resuming signature-driven backfill from existing data"
            );
        }
        match resume_slot {
            Some(s) if s >= global_start => s + 1,
            _ => global_start,
        }
    };

    // In repair mode, to_block bounds the signature scan.
    let end_slot = repair_scope.and_then(|s| s.to_block);

    tracing::info!(
        chain = chain_name,
        global_start,
        effective_start,
        ?end_slot,
        is_repair,
        programs = programs.len(),
        "Starting signature-driven backfill"
    );

    // Collect all signatures for all programs, grouped by slot
    let slots_with_sigs =
        collect_all_signatures(rpc_client, programs, effective_start, end_slot).await?;

    if slots_with_sigs.is_empty() {
        tracing::info!(
            chain = chain_name,
            "No new signatures found, backfill complete"
        );
        return Ok(());
    }

    let total_slots = slots_with_sigs.len();
    let min_slot = *slots_with_sigs.keys().next().unwrap();
    let max_slot = *slots_with_sigs.keys().next_back().unwrap();

    tracing::info!(
        chain = chain_name,
        total_slots,
        min_slot,
        max_slot,
        "Discovered slots with relevant transactions"
    );

    // Group slots into aligned ranges
    let target_slots: BTreeSet<u64> = slots_with_sigs.keys().copied().collect();
    let ranges = group_slots_into_ranges(&target_slots, range_size);

    // Filter ranges: skip already-completed ranges (unless repair mode).
    // In repair mode, also apply repair_scope range bounds.
    let skipped_index = skipped_slots::read_skipped_slots_index(&events_dir);

    let ranges_to_process: Vec<BlockRange> = ranges
        .into_iter()
        .filter(|range| {
            // In repair mode, skip the "already completed" check to force re-processing
            if !is_repair {
                let rk = skipped_slots::range_key(range.start, range.end_inclusive());
                if skipped_slots::is_range_complete(&skipped_index, &rk) {
                    return false;
                }
            }
            // Apply repair scope range bounds
            if let Some(scope) = repair_scope {
                if !scope.matches_range(range.start, range.end) {
                    return false;
                }
            }
            true
        })
        .collect();

    if ranges_to_process.is_empty() {
        tracing::info!(
            chain = chain_name,
            "All discovered ranges already processed"
        );
        return Ok(());
    }

    tracing::info!(
        chain = chain_name,
        ranges = ranges_to_process.len(),
        is_repair,
        "Processing signature-discovered ranges"
    );

    for range in &ranges_to_process {
        collect_slots_selective(
            chain_name,
            rpc_client,
            range,
            &target_slots,
            configured_programs,
            event_decoder_tx,
            instr_decoder_tx,
        )
        .await?;
    }

    tracing::info!(
        chain = chain_name,
        ranges = ranges_to_process.len(),
        "Signature-driven backfill complete"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Signature collection
// ---------------------------------------------------------------------------

/// Paginate `getSignaturesForAddress` for all configured programs, collecting
/// every discovered slot and its transaction signatures.
///
/// Returns a `BTreeMap<slot, Vec<signature_bytes>>` sorted by slot ascending.
/// Multiple programs may contribute transactions to the same slot; their
/// signatures are merged.
async fn collect_all_signatures(
    rpc_client: &SolanaRpcClient,
    programs: &SolanaPrograms,
    start_slot: u64,
    end_slot: Option<u64>,
) -> Result<BTreeMap<u64, Vec<[u8; 64]>>, SolanaRpcError> {
    let mut all_slots: BTreeMap<u64, Vec<[u8; 64]>> = BTreeMap::new();

    for (program_name, config) in programs {
        let pubkey: Pubkey = config
            .program_id
            .parse()
            .map_err(|_| SolanaRpcError::InvalidPubkey(config.program_id.clone()))?;

        // Use program-specific start_slot if available, otherwise the global one
        let program_start = config.start_slot.unwrap_or(start_slot);
        let effective_start = std::cmp::max(program_start, start_slot);

        tracing::info!(
            program = program_name.as_str(),
            pubkey = %pubkey,
            start_slot = effective_start,
            ?end_slot,
            "Collecting signatures for program"
        );

        let mut before: Option<Signature> = None;
        let mut page_count = 0u64;

        loop {
            let sigs = rpc_client
                .get_signatures_for_address(&pubkey, before, Some(SIGNATURES_PAGE_SIZE))
                .await?;

            if sigs.is_empty() {
                break;
            }

            page_count += 1;
            let page_len = sigs.len();
            let mut reached_start = false;

            for sig_info in &sigs {
                // Stop if we've gone past our start slot
                if sig_info.slot < effective_start {
                    reached_start = true;
                    break;
                }

                // Skip slots above the end bound (repair scoping)
                if let Some(end) = end_slot {
                    if sig_info.slot > end {
                        continue;
                    }
                }

                // Parse the signature string
                if let Ok(sig) = sig_info.signature.parse::<Signature>() {
                    let mut sig_bytes = [0u8; 64];
                    sig_bytes.copy_from_slice(sig.as_ref());
                    all_slots.entry(sig_info.slot).or_default().push(sig_bytes);
                }
            }

            if reached_start {
                tracing::debug!(
                    program = program_name.as_str(),
                    pages = page_count,
                    "Reached start slot, stopping pagination"
                );
                break;
            }

            // Set up pagination cursor: use the last signature as `before`
            if page_len < SIGNATURES_PAGE_SIZE {
                // Less than a full page means we've reached the end
                break;
            }

            // Parse the last signature for pagination
            if let Some(last_sig) = sigs.last() {
                if let Ok(sig) = last_sig.signature.parse::<Signature>() {
                    before = Some(sig);
                } else {
                    tracing::warn!(
                        program = program_name.as_str(),
                        signature = last_sig.signature.as_str(),
                        "Failed to parse pagination signature"
                    );
                    break;
                }
            }
        }

        let slots_found = all_slots.len();
        tracing::info!(
            program = program_name.as_str(),
            pages = page_count,
            total_slots = slots_found,
            "Finished collecting signatures for program"
        );
    }

    Ok(all_slots)
}

// ---------------------------------------------------------------------------
// Range grouping
// ---------------------------------------------------------------------------

/// Group a set of slot numbers into aligned `BlockRange`s.
///
/// Each range is aligned to multiples of `range_size`. For example, with
/// `range_size=1000`, slot 1500 falls into range `[1000, 2000)`.
pub fn group_slots_into_ranges(slots: &BTreeSet<u64>, range_size: u64) -> Vec<BlockRange> {
    if slots.is_empty() || range_size == 0 {
        return Vec::new();
    }

    let mut seen_ranges: BTreeSet<u64> = BTreeSet::new();

    for &slot in slots {
        let range_start = (slot / range_size) * range_size;
        seen_ranges.insert(range_start);
    }

    seen_ranges
        .into_iter()
        .map(|start| BlockRange {
            start,
            end: start + range_size,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Resume logic
// ---------------------------------------------------------------------------

/// Scan existing parquet files to find the highest fully-completed slot range.
///
/// A range is only counted if it has a corresponding entry in the
/// `skipped_slots.json` index — parquet file existence alone is not
/// sufficient because a crash between parquet writes and index update
/// leaves an incomplete range that must be re-processed.
pub fn find_resume_slot(chain_name: &str) -> Option<u64> {
    let events_dir = raw_solana_events_dir(chain_name);
    let skipped_index = crate::storage::skipped_slots::read_skipped_slots_index(&events_dir);

    let dirs = [
        raw_solana_slots_dir(chain_name),
        events_dir,
        raw_solana_instructions_dir(chain_name),
    ];

    let mut max_slot: Option<u64> = None;

    for dir in &dirs {
        if let Ok(ranges) = crate::storage::paths::scan_parquet_ranges(dir) {
            for (start, end_inclusive, _path) in ranges {
                let rk = crate::storage::skipped_slots::range_key(start, end_inclusive);
                if crate::storage::skipped_slots::is_range_complete(&skipped_index, &rk) {
                    max_slot =
                        Some(max_slot.map_or(end_inclusive, |m: u64| m.max(end_inclusive)));
                }
            }
        }
    }

    max_slot
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_slots_into_ranges_basic() {
        let slots: BTreeSet<u64> = [100, 200, 300].into_iter().collect();
        let ranges = group_slots_into_ranges(&slots, 1000);

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1000);
    }

    #[test]
    fn test_group_slots_into_ranges_multiple() {
        let slots: BTreeSet<u64> = [100, 1500, 2999, 3000].into_iter().collect();
        let ranges = group_slots_into_ranges(&slots, 1000);

        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1000);
        assert_eq!(ranges[1].start, 1000);
        assert_eq!(ranges[1].end, 2000);
        assert_eq!(ranges[2].start, 2000);
        assert_eq!(ranges[2].end, 3000);
        assert_eq!(ranges[3].start, 3000);
        assert_eq!(ranges[3].end, 4000);
    }

    #[test]
    fn test_group_slots_into_ranges_empty() {
        let slots: BTreeSet<u64> = BTreeSet::new();
        let ranges = group_slots_into_ranges(&slots, 1000);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_group_slots_into_ranges_zero_size() {
        let slots: BTreeSet<u64> = [100].into_iter().collect();
        let ranges = group_slots_into_ranges(&slots, 0);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_group_slots_into_ranges_deduplication() {
        // Multiple slots in the same range should produce a single range
        let slots: BTreeSet<u64> = [1000, 1001, 1500, 1999].into_iter().collect();
        let ranges = group_slots_into_ranges(&slots, 1000);

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 1000);
        assert_eq!(ranges[0].end, 2000);
    }

    #[test]
    fn test_group_slots_into_ranges_spanning_many() {
        // Slots spanning a wide range
        let slots: BTreeSet<u64> = [0, 5000, 10000, 999999].into_iter().collect();
        let ranges = group_slots_into_ranges(&slots, 1000);

        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1000);
        assert_eq!(ranges[1].start, 5000);
        assert_eq!(ranges[1].end, 6000);
        assert_eq!(ranges[2].start, 10000);
        assert_eq!(ranges[2].end, 11000);
        assert_eq!(ranges[3].start, 999000);
        assert_eq!(ranges[3].end, 1000000);
    }

    #[test]
    fn test_group_slots_into_ranges_exact_boundary() {
        // Slot at exact boundary goes into the next range
        let slots: BTreeSet<u64> = [1000].into_iter().collect();
        let ranges = group_slots_into_ranges(&slots, 1000);

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 1000);
        assert_eq!(ranges[0].end, 2000);
    }

    #[test]
    fn test_find_resume_slot_no_data() {
        // With a non-existent chain name, should return None
        let result = find_resume_slot("nonexistent_chain_xyz_123");
        assert!(result.is_none());
    }
}
