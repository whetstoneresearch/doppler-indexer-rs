//! Solana reorg detector using parent-slot chain verification.
//!
//! Tracks recent slots and their parent relationships to detect forks.
//! Unlike EVM where blocks are sequential, Solana slots can be skipped,
//! so the detector operates on parent_slot linkage rather than N-1 assumptions.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
struct TrackedSlot {
    parent_slot: u64,
    blockhash: [u8; 32],
}

/// Reorg event containing information about the detected fork.
#[derive(Debug, Clone)]
pub struct SolanaReorgEvent {
    /// The slot number of the last valid slot (common ancestor).
    pub common_ancestor: u64,
    /// Slot numbers that were orphaned by the reorg.
    pub orphaned: Vec<u64>,
    /// The slot that triggered reorg detection.
    pub new_slot: u64,
    /// How deep the reorg went (number of orphaned slots).
    pub depth: u64,
    /// Whether the reorg extends beyond our retention window.
    pub is_deep: bool,
}

/// Detects chain reorganizations on Solana by tracking recent slots and their
/// parent-slot linkage.
///
/// Solana slots are not necessarily contiguous: the leader may skip slots, so
/// the detector cannot assume slot N-1 is the parent. Instead it follows
/// explicit `parent_slot` pointers stored alongside each tracked slot.
///
/// Recommended retention depths by commitment level:
/// - `Processed`: 150
/// - `Confirmed`: 32
/// - `Finalized`: 0 (no reorg possible, but tracking is useful for gap detection)
#[derive(Debug)]
pub struct SolanaReorgDetector {
    /// Recent slots: slot_number -> TrackedSlot.
    recent_slots: BTreeMap<u64, TrackedSlot>,
    /// How many recent slots to retain. Slots older than
    /// `latest_slot - retention_depth` are pruned after each insertion.
    retention_depth: u64,
}

impl SolanaReorgDetector {
    /// Create a new detector with the given retention depth.
    pub fn new(retention_depth: u64) -> Self {
        Self {
            recent_slots: BTreeMap::new(),
            retention_depth,
        }
    }

    /// Process a new slot and check for reorgs.
    ///
    /// Returns `Some(SolanaReorgEvent)` if a reorg is detected, `None` otherwise.
    /// The slot is always inserted (or replaced) regardless of reorg status, and
    /// old entries beyond the retention window are pruned.
    pub fn process_slot(
        &mut self,
        slot: u64,
        parent_slot: u64,
        blockhash: [u8; 32],
    ) -> Option<SolanaReorgEvent> {
        let reorg_event = self.check_for_reorg(slot, parent_slot, blockhash);

        // Remove orphaned slots from tracking so subsequent checks see the
        // new canonical chain.
        if let Some(ref event) = reorg_event {
            for &orphaned_slot in &event.orphaned {
                self.recent_slots.remove(&orphaned_slot);
            }
        }

        // Insert (or replace) the new slot.
        self.recent_slots.insert(
            slot,
            TrackedSlot {
                parent_slot,
                blockhash,
            },
        );

        // Prune entries outside the retention window.
        self.prune_old_slots(slot);

        reorg_event
    }

    /// Seed the detector with known slots (e.g., loaded from storage on restart).
    ///
    /// Each item is `(slot, parent_slot, blockhash)`.
    pub fn seed(&mut self, slots: impl IntoIterator<Item = (u64, u64, [u8; 32])>) {
        for (slot, parent_slot, blockhash) in slots {
            self.recent_slots.insert(
                slot,
                TrackedSlot {
                    parent_slot,
                    blockhash,
                },
            );
        }
    }

    /// The highest tracked slot, or `None` if the detector is empty.
    pub fn latest_slot(&self) -> Option<u64> {
        self.recent_slots.keys().next_back().copied()
    }

    /// Number of slots currently tracked.
    pub fn tracked_count(&self) -> usize {
        self.recent_slots.len()
    }

    /// Clear all tracked slots (e.g., after a deep reorg recovery).
    pub fn clear(&mut self) {
        self.recent_slots.clear();
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Core reorg-detection logic.
    ///
    /// Three cases trigger a reorg:
    /// 1. We already track this slot number with a **different blockhash** (replacement).
    /// 2. We track the parent_slot and its blockhash is consistent, but slots above
    ///    the parent on our current chain differ (fork after parent).
    /// 3. The parent_slot isn't tracked (or points to a different chain), meaning
    ///    the fork is deeper than our window.
    fn check_for_reorg(
        &self,
        slot: u64,
        parent_slot: u64,
        blockhash: [u8; 32],
    ) -> Option<SolanaReorgEvent> {
        if self.recent_slots.is_empty() {
            return None;
        }

        // Case 1: same slot number, different blockhash -> replacement reorg.
        if let Some(existing) = self.recent_slots.get(&slot) {
            if existing.blockhash == blockhash {
                // Duplicate delivery, not a reorg.
                return None;
            }

            tracing::warn!(
                slot,
                tracked_hash = %hex::encode(existing.blockhash),
                new_hash = %hex::encode(blockhash),
                "replacement reorg detected at slot",
            );

            let (common_ancestor, orphaned, is_deep) = self.find_common_ancestor(slot, parent_slot);
            let depth = orphaned.len() as u64;

            return Some(SolanaReorgEvent {
                common_ancestor,
                orphaned,
                new_slot: slot,
                depth,
                is_deep,
            });
        }

        // Case 2 & 3: new slot that claims a parent.
        // If we track the parent and it matches, check whether our chain above
        // the parent diverges from the new slot.
        if let Some(tracked_parent) = self.recent_slots.get(&parent_slot) {
            // Parent exists in our chain. Are there any tracked slots above
            // parent_slot that would be on a different fork?
            // If the next slot we have above parent_slot is NOT this new slot,
            // the new slot introduces a fork that orphans whatever we tracked
            // above the parent.
            let slots_above_parent: Vec<u64> = self
                .recent_slots
                .range((parent_slot + 1)..)
                .map(|(&s, _)| s)
                .collect();

            if slots_above_parent.is_empty() {
                // We're extending the tip; no reorg.
                return None;
            }

            // Verify parent-slot chain consistency. Walk forward from
            // parent_slot through our tracked entries and see if the chain
            // that leads to `slot` is compatible.
            // The simplest check: does the first slot above parent_slot claim
            // parent_slot as its parent? If so, the chains are compatible only
            // if that first slot IS this slot (which can't be, since we checked
            // get() above and it was None). So there's a fork.
            //
            // However, if the existing chain above parent also has the same
            // parent_slot, we need to walk further. In practice the chain above
            // parent_slot may consist of entries whose parent chain traces back
            // through parent_slot — those are on the OLD fork and become
            // orphaned.

            // Walk back from the highest existing slot to verify it chains
            // through parent_slot. Collect everything above parent_slot that
            // traces back through the parent — those are orphaned.
            let orphaned = self.collect_descendants(parent_slot);
            if orphaned.is_empty() {
                return None;
            }

            tracing::warn!(
                slot,
                parent_slot,
                parent_hash = %hex::encode(tracked_parent.blockhash),
                orphaned_count = orphaned.len(),
                "fork detected above tracked parent",
            );

            let depth = orphaned.len() as u64;
            return Some(SolanaReorgEvent {
                common_ancestor: parent_slot,
                orphaned,
                new_slot: slot,
                depth,
                is_deep: false,
            });
        }

        // The parent_slot is not tracked at all. If we have any tracked slots,
        // this could be a deep reorg that goes beyond our retention window, OR
        // simply a large gap in slot numbers (e.g., after downtime).
        //
        // Heuristic: if the new slot is above our latest tracked slot and we
        // have no tracked parent, it's likely a gap — not a reorg. But if the
        // new slot is at or below our latest tracked slot, it's almost
        // certainly a fork.
        let latest = match self.latest_slot() {
            Some(l) => l,
            None => return None,
        };

        if slot > latest {
            // Gap scenario: the new slot is ahead and we can't link it to our
            // chain. Not a reorg — just missing data.
            return None;
        }

        // The new slot is at or below our tip but its parent chain doesn't
        // connect to anything we track. This is a deep reorg.
        let (common_ancestor, orphaned, _) = self.find_common_ancestor(slot, parent_slot);
        let depth = orphaned.len() as u64;

        tracing::warn!(
            slot,
            parent_slot,
            orphaned_count = orphaned.len(),
            "deep reorg detected — parent outside retention window",
        );

        Some(SolanaReorgEvent {
            common_ancestor,
            orphaned,
            new_slot: slot,
            depth,
            is_deep: true,
        })
    }

    /// Walk backward through tracked slots to find the common ancestor between
    /// the new chain (represented by `parent_slot`) and our existing chain.
    ///
    /// Returns `(common_ancestor_slot, orphaned_slots, is_deep)`.
    fn find_common_ancestor(&self, new_slot: u64, new_parent_slot: u64) -> (u64, Vec<u64>, bool) {
        // Collect the new chain's ancestry as far as we can trace it through
        // our tracked slots.
        let mut new_chain_ancestors = std::collections::BTreeSet::new();
        let mut cursor = new_parent_slot;
        loop {
            if let Some(tracked) = self.recent_slots.get(&cursor) {
                new_chain_ancestors.insert(cursor);
                if tracked.parent_slot == cursor {
                    // Self-referencing (genesis-like); stop.
                    break;
                }
                cursor = tracked.parent_slot;
            } else {
                break;
            }
        }

        // If we found any ancestors in common, the highest one that is on the
        // new chain is the common ancestor. All tracked slots above it that
        // are NOT in the new chain's ancestor set are orphaned.
        if !new_chain_ancestors.is_empty() {
            // The common ancestor is the *highest* slot that is shared between
            // the old chain and the new chain.  Since we walked backward from
            // `new_parent_slot`, the highest element in the set is
            // `new_parent_slot` itself — the fork point.
            let common_ancestor = *new_chain_ancestors.iter().next_back().unwrap();

            // Orphaned: tracked slots above the common ancestor that are NOT
            // in the new chain's ancestor set. This intentionally includes
            // the old slot at `new_slot`'s number (if any) — the caller will
            // remove orphans and then insert the replacement.
            let orphaned: Vec<u64> = self
                .recent_slots
                .range((common_ancestor + 1)..)
                .map(|(&s, _)| s)
                .filter(|s| !new_chain_ancestors.contains(s))
                .collect();

            (common_ancestor, orphaned, false)
        } else {
            // Couldn't find any common ancestor in the retention window.
            // Best-effort: orphan everything we track.
            let common_ancestor = self
                .recent_slots
                .keys()
                .next()
                .copied()
                .unwrap_or(new_slot)
                .saturating_sub(1);

            let orphaned: Vec<u64> = self.recent_slots.keys().copied().collect();

            (common_ancestor, orphaned, true)
        }
    }

    /// Collect all tracked slots above `ancestor` that chain back through it.
    fn collect_descendants(&self, ancestor: u64) -> Vec<u64> {
        // Simple approach: gather every tracked slot above `ancestor` whose
        // parent chain eventually reaches `ancestor`.
        let mut descendants = Vec::new();
        for (&slot, tracked) in self.recent_slots.range((ancestor + 1)..) {
            // Walk backward from this slot's parent to see if it reaches
            // `ancestor`.
            if self.chains_to(slot, ancestor) {
                descendants.push(slot);
            } else {
                // If the slot doesn't chain back to ancestor, it belongs to
                // a completely different fork — not relevant here.
                let _ = tracked;
            }
        }
        descendants
    }

    /// Check whether `slot` eventually chains back to `target` via parent
    /// links in our tracked set.
    fn chains_to(&self, slot: u64, target: u64) -> bool {
        let mut cursor = slot;
        let mut steps = 0u64;
        loop {
            if cursor == target {
                return true;
            }
            if cursor < target || steps > self.retention_depth {
                return false;
            }
            match self.recent_slots.get(&cursor) {
                Some(tracked) => {
                    if tracked.parent_slot == cursor {
                        // Self-referencing; stop.
                        return false;
                    }
                    cursor = tracked.parent_slot;
                    steps += 1;
                }
                None => return false,
            }
        }
    }

    /// Remove tracked slots that fall outside the retention window.
    fn prune_old_slots(&mut self, current_slot: u64) {
        if self.retention_depth == 0 {
            return;
        }
        let min_slot = current_slot.saturating_sub(self.retention_depth);
        // BTreeMap::split_off returns everything >= key, which is exactly what
        // we want to keep.
        self.recent_slots = self.recent_slots.split_off(&min_slot);
    }
}

impl Default for SolanaReorgDetector {
    fn default() -> Self {
        // Confirmed commitment default.
        Self::new(32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a blockhash deterministically from a u8 seed.
    fn hash(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    // ------------------------------------------------------------------
    // Basic: sequential slots, no reorg
    // ------------------------------------------------------------------

    #[test]
    fn sequential_slots_no_reorg() {
        let mut det = SolanaReorgDetector::new(10);

        assert!(det.process_slot(100, 99, hash(1)).is_none());
        assert!(det.process_slot(101, 100, hash(2)).is_none());
        assert!(det.process_slot(102, 101, hash(3)).is_none());

        assert_eq!(det.tracked_count(), 3);
        assert_eq!(det.latest_slot(), Some(102));
    }

    // ------------------------------------------------------------------
    // Skipped slots: non-contiguous but valid chain
    // ------------------------------------------------------------------

    #[test]
    fn skipped_slots_no_reorg() {
        let mut det = SolanaReorgDetector::new(20);

        // Slots 100, 103, 107 — gaps are normal on Solana.
        assert!(det.process_slot(100, 99, hash(1)).is_none());
        assert!(det.process_slot(103, 100, hash(2)).is_none());
        assert!(det.process_slot(107, 103, hash(3)).is_none());

        assert_eq!(det.tracked_count(), 3);
        assert_eq!(det.latest_slot(), Some(107));
    }

    // ------------------------------------------------------------------
    // Simple reorg: replacement of a single slot
    // ------------------------------------------------------------------

    #[test]
    fn single_slot_replacement_reorg() {
        let mut det = SolanaReorgDetector::new(10);

        det.process_slot(100, 99, hash(1));
        det.process_slot(105, 100, hash(2));
        det.process_slot(110, 105, hash(3));

        // Slot 110 re-appears with a different blockhash, still claiming
        // parent 105 (single-slot replacement).
        let event = det.process_slot(110, 105, hash(33));
        assert!(event.is_some());

        let event = event.unwrap();
        assert_eq!(event.new_slot, 110);
        assert_eq!(event.common_ancestor, 105);
        assert_eq!(event.orphaned, vec![110]);
        assert_eq!(event.depth, 1);
        assert!(!event.is_deep);
    }

    // ------------------------------------------------------------------
    // Multi-slot reorg: several slots orphaned
    // ------------------------------------------------------------------

    #[test]
    fn multi_slot_fork_reorg() {
        let mut det = SolanaReorgDetector::new(20);

        // Build chain: 100 -> 105 -> 110 -> 115
        det.process_slot(100, 99, hash(1));
        det.process_slot(105, 100, hash(2));
        det.process_slot(110, 105, hash(3));
        det.process_slot(115, 110, hash(4));

        // New slot 112 arrives claiming parent 100 — this forks off after 100,
        // orphaning 105, 110, 115.
        let event = det.process_slot(112, 100, hash(50));
        assert!(event.is_some());

        let event = event.unwrap();
        assert_eq!(event.common_ancestor, 100);
        assert!(event.orphaned.contains(&105));
        assert!(event.orphaned.contains(&110));
        assert!(event.orphaned.contains(&115));
        assert_eq!(event.depth, 3);
        assert!(!event.is_deep);
    }

    // ------------------------------------------------------------------
    // Replacement reorg that re-parents
    // ------------------------------------------------------------------

    #[test]
    fn replacement_reorg_different_parent() {
        let mut det = SolanaReorgDetector::new(20);

        // Chain: 100 -> 105 -> 110
        det.process_slot(100, 99, hash(1));
        det.process_slot(105, 100, hash(2));
        det.process_slot(110, 105, hash(3));

        // Slot 110 re-appears with a different hash AND a different parent (100
        // instead of 105), orphaning slot 105 and old 110.
        let event = det.process_slot(110, 100, hash(33));
        assert!(event.is_some());

        let event = event.unwrap();
        assert_eq!(event.common_ancestor, 100);
        assert!(event.orphaned.contains(&105));
        assert!(event.orphaned.contains(&110));
        assert_eq!(event.depth, 2);
        assert!(!event.is_deep);
    }

    // ------------------------------------------------------------------
    // Deep reorg beyond retention
    // ------------------------------------------------------------------

    #[test]
    fn deep_reorg_beyond_retention() {
        let mut det = SolanaReorgDetector::new(5);

        // Build a short chain.
        det.process_slot(100, 99, hash(1));
        det.process_slot(101, 100, hash(2));
        det.process_slot(102, 101, hash(3));

        // New slot 102 with a parent that is NOT tracked at all (slot 50),
        // signaling a deep reorg.
        let event = det.process_slot(102, 50, hash(99));
        assert!(event.is_some());

        let event = event.unwrap();
        assert!(event.is_deep);
        // All previously tracked slots should be orphaned.
        assert!(event.orphaned.contains(&100));
        assert!(event.orphaned.contains(&101));
        assert!(event.orphaned.contains(&102));
    }

    // ------------------------------------------------------------------
    // Seed from storage and detect
    // ------------------------------------------------------------------

    #[test]
    fn seed_and_detect_reorg() {
        let mut det = SolanaReorgDetector::new(20);

        det.seed(vec![
            (200, 199, hash(10)),
            (205, 200, hash(11)),
            (210, 205, hash(12)),
        ]);

        assert_eq!(det.tracked_count(), 3);
        assert_eq!(det.latest_slot(), Some(210));

        // Normal extension — no reorg.
        assert!(det.process_slot(215, 210, hash(13)).is_none());

        // Fork: slot 212 claims parent 200, orphaning 205 and 210.
        let event = det.process_slot(212, 200, hash(50));
        assert!(event.is_some());

        let event = event.unwrap();
        assert_eq!(event.common_ancestor, 200);
        assert!(event.orphaned.contains(&205));
        assert!(event.orphaned.contains(&210));
        // 215 was on the old fork (chains through 210) so it's orphaned too.
        assert!(event.orphaned.contains(&215));
        assert!(!event.is_deep);
    }

    // ------------------------------------------------------------------
    // Empty detector returns None
    // ------------------------------------------------------------------

    #[test]
    fn empty_detector_returns_none() {
        let mut det = SolanaReorgDetector::new(10);

        assert!(det.process_slot(100, 99, hash(1)).is_none());
        assert!(det.latest_slot().is_some()); // now has one entry
    }

    #[test]
    fn empty_detector_latest_slot_is_none() {
        let det = SolanaReorgDetector::new(10);
        assert_eq!(det.latest_slot(), None);
        assert_eq!(det.tracked_count(), 0);
    }

    // ------------------------------------------------------------------
    // Clear resets state
    // ------------------------------------------------------------------

    #[test]
    fn clear_resets_state() {
        let mut det = SolanaReorgDetector::new(10);

        det.process_slot(100, 99, hash(1));
        det.process_slot(101, 100, hash(2));
        assert_eq!(det.tracked_count(), 2);

        det.clear();
        assert_eq!(det.tracked_count(), 0);
        assert_eq!(det.latest_slot(), None);
    }

    // ------------------------------------------------------------------
    // Pruning respects retention depth with skipped slots
    // ------------------------------------------------------------------

    #[test]
    fn pruning_with_skipped_slots() {
        let mut det = SolanaReorgDetector::new(10);

        // Insert slots with gaps.
        det.process_slot(100, 99, hash(1));
        det.process_slot(103, 100, hash(2));
        det.process_slot(106, 103, hash(3));
        det.process_slot(109, 106, hash(4));
        det.process_slot(112, 109, hash(5));

        // All slots are within 112 - 10 = 102..112, so slot 100 should be
        // pruned.
        assert!(det.recent_slots.get(&100).is_none());
        assert!(det.recent_slots.get(&103).is_some());
        assert_eq!(det.latest_slot(), Some(112));
    }

    // ------------------------------------------------------------------
    // Duplicate slot delivery is not a reorg
    // ------------------------------------------------------------------

    #[test]
    fn duplicate_slot_not_reorg() {
        let mut det = SolanaReorgDetector::new(10);

        det.process_slot(100, 99, hash(1));
        det.process_slot(105, 100, hash(2));

        // Exact same slot delivered again — no reorg.
        assert!(det.process_slot(105, 100, hash(2)).is_none());
        assert_eq!(det.tracked_count(), 2);
    }

    // ------------------------------------------------------------------
    // After reorg, orphaned slots are removed from tracking
    // ------------------------------------------------------------------

    #[test]
    fn orphaned_slots_removed_after_reorg() {
        let mut det = SolanaReorgDetector::new(20);

        det.process_slot(100, 99, hash(1));
        det.process_slot(105, 100, hash(2));
        det.process_slot(110, 105, hash(3));

        // Fork after 100, orphaning 105 and 110.
        let event = det.process_slot(108, 100, hash(50));
        assert!(event.is_some());

        // 105 and 110 should be gone.
        assert!(det.recent_slots.get(&105).is_none());
        assert!(det.recent_slots.get(&110).is_none());
        // 100 and the new 108 should remain.
        assert!(det.recent_slots.get(&100).is_some());
        assert!(det.recent_slots.get(&108).is_some());
    }

    // ------------------------------------------------------------------
    // Default uses confirmed commitment depth (32)
    // ------------------------------------------------------------------

    #[test]
    fn default_retention_depth() {
        let det = SolanaReorgDetector::default();
        assert_eq!(det.retention_depth, 32);
    }
}
