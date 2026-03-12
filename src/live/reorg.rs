//! Reorg detection for live mode.
//!
//! Tracks recent block hashes and detects chain reorganizations by checking
//! parent hash consistency.

use std::collections::BTreeMap;

use super::types::LiveBlock;

#[derive(Debug, Clone, Copy)]
struct TrackedBlock {
    hash: [u8; 32],
}

/// Event indicating a chain reorganization was detected.
#[derive(Debug, Clone)]
pub struct ReorgEvent {
    /// The block number of the common ancestor (last valid block before reorg).
    pub common_ancestor: u64,
    /// Block numbers that were orphaned by the reorg.
    pub orphaned: Vec<u64>,
    /// The new block that triggered the reorg detection.
    pub _new_block_number: u64,
    /// How many blocks deep the reorg was.
    pub depth: u64,
}

/// Detects chain reorganizations by tracking recent block hashes.
#[derive(Debug)]
pub struct ReorgDetector {
    /// Recent blocks: block_number -> block_hash
    recent_blocks: BTreeMap<u64, TrackedBlock>,
    /// How many recent blocks to keep track of.
    retention_depth: u64,
}

impl ReorgDetector {
    /// Create a new ReorgDetector.
    ///
    /// # Arguments
    /// * `retention_depth` - Number of recent blocks to track (default: 128)
    pub fn new(retention_depth: u64) -> Self {
        Self {
            recent_blocks: BTreeMap::new(),
            retention_depth,
        }
    }

    /// Process a new block and check for reorgs.
    ///
    /// Returns `Some(ReorgEvent)` if a reorg is detected, `None` otherwise.
    pub fn process_block(&mut self, block: &LiveBlock) -> Option<ReorgEvent> {
        let reorg_event = self.check_for_reorg(block);

        if let Some(ref event) = reorg_event {
            // Remove orphaned blocks from tracking
            for orphaned_number in &event.orphaned {
                self.recent_blocks.remove(orphaned_number);
            }
        }

        // Add this block to tracking
        self.recent_blocks
            .insert(block.number, TrackedBlock { hash: block.hash });

        // Prune old blocks beyond retention depth
        self.prune_old_blocks(block.number);

        reorg_event
    }

    /// Check if a block represents a reorg.
    fn check_for_reorg(&self, block: &LiveBlock) -> Option<ReorgEvent> {
        // If this is the first block we're tracking, no reorg possible
        if self.recent_blocks.is_empty() {
            return None;
        }

        if let Some(existing) = self.recent_blocks.get(&block.number) {
            if existing.hash != block.hash {
                tracing::warn!(
                    "Reorg detected at block {}: tracked hash {:?}, got {:?}",
                    block.number,
                    hex::encode(existing.hash),
                    hex::encode(block.hash)
                );

                let (common_ancestor, orphaned) = self.find_common_ancestor(block);
                let depth = block.number.saturating_sub(common_ancestor);

                return Some(ReorgEvent {
                    common_ancestor,
                    orphaned,
                    _new_block_number: block.number,
                    depth,
                });
            }

            return None;
        }

        // Get the expected parent (block at number - 1)
        let parent_number = block.number.checked_sub(1)?;

        // If we don't have the parent tracked, we can't detect a reorg
        let expected_parent = self.recent_blocks.get(&parent_number)?;

        // If parent hash matches, no reorg
        if expected_parent.hash == block.parent_hash {
            return None;
        }

        // Parent hash mismatch - we have a reorg!
        // Walk back to find the common ancestor
        tracing::warn!(
            "Reorg detected at block {}: expected parent {:?}, got {:?}",
            block.number,
            hex::encode(expected_parent.hash),
            hex::encode(block.parent_hash)
        );

        let (common_ancestor, orphaned) = self.find_common_ancestor(block);

        let depth = block.number - common_ancestor;

        Some(ReorgEvent {
            common_ancestor,
            orphaned,
            _new_block_number: block.number,
            depth,
        })
    }

    /// Find the common ancestor by walking back through tracked blocks.
    ///
    /// Returns (common_ancestor_number, orphaned_block_numbers).
    fn find_common_ancestor(&self, new_block: &LiveBlock) -> (u64, Vec<u64>) {
        let retained_ancestor = self.recent_blocks.range(..new_block.number).rev().find_map(
            |(&block_number, tracked)| {
                (tracked.hash == new_block.parent_hash).then_some(block_number)
            },
        );

        match retained_ancestor {
            Some(common_ancestor) => {
                let orphaned = self
                    .recent_blocks
                    .range((common_ancestor + 1)..)
                    .map(|(&block_number, _)| block_number)
                    .collect();
                (common_ancestor, orphaned)
            }
            None => {
                let common_ancestor = self
                    .recent_blocks
                    .keys()
                    .next()
                    .copied()
                    .unwrap_or(new_block.number)
                    .saturating_sub(1);
                let orphaned: Vec<u64> = self.recent_blocks.keys().copied().collect();

                tracing::warn!(
                    "Reorg ancestor for block {} is outside retention window; orphaning {} tracked blocks",
                    new_block.number,
                    orphaned.len()
                );

                (common_ancestor, orphaned)
            }
        }
    }

    /// Remove blocks older than retention_depth.
    ///
    /// After pruning, exactly `retention_depth` blocks are kept (including the current block).
    /// For example, with retention_depth=5 and current_block=10, keeps blocks 6-10.
    fn prune_old_blocks(&mut self, current_block: u64) {
        // To keep exactly retention_depth blocks, we need min_block such that
        // the range [min_block, current_block] has retention_depth elements.
        // That means: current_block - min_block + 1 = retention_depth
        // So: min_block = current_block - retention_depth + 1
        let min_block = current_block.saturating_sub(self.retention_depth.saturating_sub(1));

        // Remove all blocks before min_block
        self.recent_blocks = self.recent_blocks.split_off(&min_block);
    }

    #[allow(dead_code)]
    /// Get the most recent block number we're tracking.
    pub fn latest_block(&self) -> Option<u64> {
        self.recent_blocks.keys().next_back().copied()
    }

    #[allow(dead_code)]
    /// Get the hash of a specific block if we're tracking it.
    pub fn get_block_hash(&self, block_number: u64) -> Option<[u8; 32]> {
        self.recent_blocks
            .get(&block_number)
            .map(|tracked| tracked.hash)
    }

    #[allow(dead_code)]
    /// Get the number of blocks currently being tracked.
    pub fn tracked_count(&self) -> usize {
        self.recent_blocks.len()
    }

    #[allow(dead_code)]
    /// Clear all tracked blocks (e.g., after a deep reorg recovery).
    pub fn clear(&mut self) {
        self.recent_blocks.clear();
    }

    /// Update the tracked hash for a single block (e.g., after catchup refetch).
    pub fn update_block_hash(&mut self, block_number: u64, hash: [u8; 32]) {
        self.recent_blocks
            .insert(block_number, TrackedBlock { hash });
    }

    /// Seed the detector with known block hashes (e.g., on startup).
    pub fn seed(&mut self, blocks: impl IntoIterator<Item = LiveBlock>) {
        for block in blocks {
            self.recent_blocks
                .insert(block.number, TrackedBlock { hash: block.hash });
        }
    }
}

impl Default for ReorgDetector {
    fn default() -> Self {
        Self::new(128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(number: u64, hash: u8, parent_hash: u8) -> LiveBlock {
        LiveBlock {
            number,
            hash: [hash; 32],
            parent_hash: [parent_hash; 32],
            timestamp: number * 12,
            tx_hashes: vec![],
        }
    }

    #[test]
    fn test_normal_chain_progression() {
        let mut detector = ReorgDetector::new(10);

        // Normal chain: 1 -> 2 -> 3
        assert!(detector.process_block(&make_block(1, 1, 0)).is_none());
        assert!(detector.process_block(&make_block(2, 2, 1)).is_none());
        assert!(detector.process_block(&make_block(3, 3, 2)).is_none());

        assert_eq!(detector.tracked_count(), 3);
        assert_eq!(detector.latest_block(), Some(3));
    }

    #[test]
    fn test_simple_reorg() {
        let mut detector = ReorgDetector::new(10);

        // Build initial chain: 1 -> 2 -> 3
        detector.process_block(&make_block(1, 1, 0));
        detector.process_block(&make_block(2, 2, 1));
        detector.process_block(&make_block(3, 3, 2));

        // Reorg: new block 3 attaches to retained block 1 instead of block 2.
        // This replaces blocks 2 and 3.
        let reorg_block = LiveBlock {
            number: 3,
            hash: [33; 32],       // Different hash
            parent_hash: [1; 32], // Parent is tracked block 1
            timestamp: 36,
            tx_hashes: vec![],
        };

        let event = detector.process_block(&reorg_block);
        assert!(event.is_some());

        let event = event.unwrap();
        assert_eq!(event.common_ancestor, 1);
        assert_eq!(event.orphaned, vec![2, 3]);
        assert_eq!(event.depth, 2);
    }

    #[test]
    fn test_reorg_does_not_orphan_entire_history() {
        let mut detector = ReorgDetector::new(10);

        for i in 1..=6 {
            let parent = if i == 1 { 0 } else { i - 1 };
            detector.process_block(&make_block(i, i as u8, parent as u8));
        }

        let reorg_block = LiveBlock {
            number: 6,
            hash: [66; 32],
            parent_hash: [4; 32],
            timestamp: 72,
            tx_hashes: vec![],
        };

        let event = detector.process_block(&reorg_block).unwrap();
        assert_eq!(event.common_ancestor, 4);
        assert_eq!(event.orphaned, vec![5, 6]);
        assert!(detector.get_block_hash(4).is_some());
        assert!(detector.get_block_hash(5).is_none());
    }

    #[test]
    fn test_pruning() {
        let mut detector = ReorgDetector::new(5);

        // Add 10 blocks
        for i in 1..=10 {
            let parent = if i == 1 { 0 } else { i - 1 };
            detector.process_block(&make_block(i, i as u8, parent as u8));
        }

        // Should only track last 5 blocks (6-10)
        assert!(detector.tracked_count() <= 6);
        assert!(detector.get_block_hash(1).is_none());
        assert!(detector.get_block_hash(10).is_some());
    }

    #[test]
    fn test_pruning_exact_retention_depth() {
        // Test that pruning keeps exactly retention_depth blocks
        let mut detector = ReorgDetector::new(5);

        // Add 20 blocks to ensure pruning happens multiple times
        for i in 1..=20 {
            let parent = if i == 1 { 0 } else { i - 1 };
            detector.process_block(&make_block(i, i as u8, parent as u8));
        }

        // Should keep exactly 5 blocks: 16, 17, 18, 19, 20
        assert_eq!(detector.tracked_count(), 5);
        assert!(detector.get_block_hash(15).is_none());
        assert!(detector.get_block_hash(16).is_some());
        assert!(detector.get_block_hash(17).is_some());
        assert!(detector.get_block_hash(18).is_some());
        assert!(detector.get_block_hash(19).is_some());
        assert!(detector.get_block_hash(20).is_some());
    }

    #[test]
    fn test_pruning_retention_depth_boundary() {
        // Test edge cases for retention depth
        let mut detector = ReorgDetector::new(3);

        // Add exactly 3 blocks
        detector.process_block(&make_block(1, 1, 0));
        detector.process_block(&make_block(2, 2, 1));
        detector.process_block(&make_block(3, 3, 2));

        // All 3 should be present
        assert_eq!(detector.tracked_count(), 3);

        // Add one more
        detector.process_block(&make_block(4, 4, 3));

        // Should now have exactly 3: blocks 2, 3, 4
        assert_eq!(detector.tracked_count(), 3);
        assert!(detector.get_block_hash(1).is_none());
        assert!(detector.get_block_hash(2).is_some());
        assert!(detector.get_block_hash(3).is_some());
        assert!(detector.get_block_hash(4).is_some());
    }

    #[test]
    fn test_seed() {
        let mut detector = ReorgDetector::new(10);

        detector.seed(vec![
            make_block(100, 100, 99),
            make_block(101, 101, 100),
            make_block(102, 102, 101),
        ]);

        assert_eq!(detector.tracked_count(), 3);
        assert_eq!(detector.get_block_hash(101), Some([101; 32]));
    }

    #[test]
    fn test_same_height_replacement_is_detected() {
        let mut detector = ReorgDetector::new(10);

        detector.process_block(&make_block(1, 1, 0));
        detector.process_block(&make_block(2, 2, 1));
        detector.process_block(&make_block(3, 3, 2));

        let replacement = LiveBlock {
            number: 3,
            hash: [30; 32],
            parent_hash: [2; 32],
            timestamp: 36,
            tx_hashes: vec![],
        };

        let event = detector.process_block(&replacement).unwrap();
        assert_eq!(event.common_ancestor, 2);
        assert_eq!(event.orphaned, vec![3]);
        assert_eq!(detector.get_block_hash(3), Some([30; 32]));
    }

    #[test]
    fn test_deep_reorg_orphans_retained_window() {
        let mut detector = ReorgDetector::new(3);

        detector.process_block(&make_block(8, 8, 7));
        detector.process_block(&make_block(9, 9, 8));
        detector.process_block(&make_block(10, 10, 9));

        let reorg_block = LiveBlock {
            number: 11,
            hash: [111; 32],
            parent_hash: [1; 32],
            timestamp: 132,
            tx_hashes: vec![],
        };

        let event = detector.process_block(&reorg_block).unwrap();
        assert_eq!(event.orphaned, vec![8, 9, 10]);
        assert!(detector.get_block_hash(8).is_none());
        assert_eq!(detector.get_block_hash(11), Some([111; 32]));
    }
}
