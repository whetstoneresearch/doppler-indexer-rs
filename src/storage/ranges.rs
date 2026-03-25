//! Unified block range type used across all raw-data collectors.
//!
//! This replaces the four identical `BlockRange` definitions that were scattered
//! across `logs.rs`, `receipts.rs`, `catchup/blocks.rs`, and `eth_calls/types.rs`.

/// A half-open block range `[start, end)`.
///
/// Used to name parquet output files and track which block ranges have been
/// collected or decoded.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockRange {
    pub start: u64,
    /// Exclusive upper bound.
    pub end: u64,
}

impl BlockRange {
    /// Create a new block range `[start, end)`.
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Generate a parquet filename for this range.
    ///
    /// The end value in the filename is **inclusive** (`end - 1`) to match
    /// the existing naming convention (`blocks_0-999.parquet`).
    ///
    /// - `file_name(Some("logs"))`   -> `"logs_0-999.parquet"`
    /// - `file_name(None)`           -> `"0-999.parquet"`
    pub fn file_name(&self, prefix: Option<&str>) -> String {
        match prefix {
            Some(p) => format!("{}_{}-{}.parquet", p, self.start, self.end - 1),
            None => format!("{}-{}.parquet", self.start, self.end - 1),
        }
    }

    /// Number of blocks in this range.
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Whether the range contains zero blocks.
    pub fn is_empty(&self) -> bool {
        self.end <= self.start
    }

    /// Whether `block` falls within `[start, end)`.
    pub fn contains(&self, block: u64) -> bool {
        block >= self.start && block < self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // file_name
    // -----------------------------------------------------------------------

    #[test]
    fn file_name_with_logs_prefix() {
        let r = BlockRange::new(0, 1000);
        assert_eq!(r.file_name(Some("logs")), "logs_0-999.parquet");
    }

    #[test]
    fn file_name_with_blocks_prefix() {
        let r = BlockRange::new(5000, 6000);
        assert_eq!(r.file_name(Some("blocks")), "blocks_5000-5999.parquet");
    }

    #[test]
    fn file_name_with_receipts_prefix() {
        let r = BlockRange::new(1000, 2000);
        assert_eq!(r.file_name(Some("receipts")), "receipts_1000-1999.parquet");
    }

    #[test]
    fn file_name_without_prefix() {
        let r = BlockRange::new(0, 1000);
        assert_eq!(r.file_name(None), "0-999.parquet");
    }

    #[test]
    fn file_name_without_prefix_nonzero_start() {
        let r = BlockRange::new(10000, 20000);
        assert_eq!(r.file_name(None), "10000-19999.parquet");
    }

    // -----------------------------------------------------------------------
    // contains
    // -----------------------------------------------------------------------

    #[test]
    fn contains_start_boundary() {
        let r = BlockRange::new(100, 200);
        assert!(r.contains(100));
    }

    #[test]
    fn contains_end_boundary_is_exclusive() {
        let r = BlockRange::new(100, 200);
        assert!(!r.contains(200));
    }

    #[test]
    fn contains_just_before_end() {
        let r = BlockRange::new(100, 200);
        assert!(r.contains(199));
    }

    #[test]
    fn contains_below_start() {
        let r = BlockRange::new(100, 200);
        assert!(!r.contains(99));
    }

    #[test]
    fn contains_middle() {
        let r = BlockRange::new(100, 200);
        assert!(r.contains(150));
    }

    // -----------------------------------------------------------------------
    // len / is_empty
    // -----------------------------------------------------------------------

    #[test]
    fn len_normal() {
        let r = BlockRange::new(0, 1000);
        assert_eq!(r.len(), 1000);
    }

    #[test]
    fn len_single_block() {
        let r = BlockRange::new(5, 6);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn is_empty_when_start_equals_end() {
        let r = BlockRange::new(10, 10);
        assert!(r.is_empty());
    }

    #[test]
    fn is_empty_when_start_exceeds_end() {
        let r = BlockRange::new(20, 10);
        assert!(r.is_empty());
    }

    #[test]
    fn is_not_empty_for_valid_range() {
        let r = BlockRange::new(0, 1);
        assert!(!r.is_empty());
    }
}
