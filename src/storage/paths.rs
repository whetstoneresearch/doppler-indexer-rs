//! Shared path builders, range parsing, and parquet file scanning utilities.
//!
//! These functions centralise duplicated file I/O patterns that were previously
//! scattered across the collector / catchup modules.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Range separator conventions
// ---------------------------------------------------------------------------

/// Separator used in parquet filenames (e.g., "blocks_0-999.parquet").
#[allow(dead_code)]
pub const PARQUET_RANGE_SEPARATOR: char = '-';

/// Separator used in marker filenames (e.g., "0_999.marker").
#[allow(dead_code)]
pub const MARKER_RANGE_SEPARATOR: char = '_';

// ---------------------------------------------------------------------------
// Path builders
// ---------------------------------------------------------------------------

/// `data/{chain}/historical/raw/blocks`
pub fn raw_blocks_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/raw/blocks", chain))
}

/// `data/{chain}/historical/raw/receipts`
pub fn raw_receipts_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/raw/receipts", chain))
}

/// `data/{chain}/historical/raw/logs`
pub fn raw_logs_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/raw/logs", chain))
}

/// `data/{chain}/historical/raw/eth_calls`
pub fn raw_eth_calls_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/raw/eth_calls", chain))
}

/// `data/{chain}/historical/factories`
pub fn factories_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/factories", chain))
}

/// `data/{chain}/historical/decoded`
pub fn decoded_base_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/decoded", chain))
}

/// `data/{chain}/historical/decoded/logs`
pub fn decoded_logs_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/decoded/logs", chain))
}

/// `data/{chain}/historical/decoded/eth_calls`
pub fn decoded_eth_calls_dir(chain: &str) -> PathBuf {
    PathBuf::from(format!("data/{}/historical/decoded/eth_calls", chain))
}

// ---------------------------------------------------------------------------
// Unified BlockRange
// ---------------------------------------------------------------------------

/// A half-open block range `[start, end)` used for naming parquet files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRange {
    pub start: u64,
    /// Exclusive upper bound.
    pub end: u64,
}

impl BlockRange {
    #[allow(dead_code)]
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Build a parquet file name for this range.
    ///
    /// When `prefix` is non-empty the format is `"{prefix}_{start}-{end_inclusive}.parquet"`,
    /// otherwise `"{start}-{end_inclusive}.parquet"`.
    pub fn file_name(&self, prefix: &str) -> String {
        let end_inclusive = self.end_inclusive();
        if prefix.is_empty() {
            format!("{}-{}.parquet", self.start, end_inclusive)
        } else {
            format!("{}_{}-{}.parquet", prefix, self.start, end_inclusive)
        }
    }

    /// Return the inclusive end (`end - 1`).
    pub fn end_inclusive(&self) -> u64 {
        self.end - 1
    }
}

// ---------------------------------------------------------------------------
// Range parsing
// ---------------------------------------------------------------------------

/// Parse a filename like `"blocks_0-999.parquet"`, `"logs_0-999.parquet"`,
/// `"decoded_0-999.parquet"`, or `"0-999.parquet"` into `(start, end_inclusive)`.
///
/// Returns `None` when the filename does not match any recognised pattern.
pub fn parse_range_from_filename(path: &Path) -> Option<(u64, u64)> {
    let stem = path.file_stem()?.to_str()?;

    // Strip any known prefix to get the "START-END" part.
    let range_part = stem
        .strip_prefix("blocks_")
        .or_else(|| stem.strip_prefix("receipts_"))
        .or_else(|| stem.strip_prefix("logs_"))
        .or_else(|| stem.strip_prefix("decoded_"))
        .unwrap_or(stem);

    let mut parts = range_part.splitn(2, '-');
    let start: u64 = parts.next()?.parse().ok()?;
    let end: u64 = parts.next()?.parse().ok()?;

    Some((start, end))
}

// ---------------------------------------------------------------------------
// Flat-directory parquet scanners
// ---------------------------------------------------------------------------

/// List parquet files in a flat directory whose names start with `prefix`
/// (e.g. `"blocks_"`). Returns a `HashSet` of file names (not full paths).
pub fn scan_parquet_filenames(dir: &Path, prefix: &str) -> HashSet<String> {
    let mut files = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(prefix) && name.ends_with(".parquet") {
                    files.insert(name.to_string());
                }
            }
        }
    }
    files
}

/// Scan a flat directory for `.parquet` files, parse ranges from filenames,
/// and return sorted `(start, end_inclusive, path)` tuples.
///
/// Returns an I/O error if the directory cannot be read, so callers that need
/// fail-fast behaviour (e.g. `HistoricalDataReader::rebuild_index`) can
/// propagate the error instead of silently treating it as empty.
pub fn scan_parquet_ranges(dir: &Path) -> std::io::Result<Vec<(u64, u64, PathBuf)>> {
    let mut results = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "parquet").unwrap_or(false) {
            if let Some((start, end)) = parse_range_from_filename(&path) {
                results.push((start, end, path));
            }
        }
    }
    results.sort_by_key(|(start, _, _)| *start);
    Ok(results)
}

// ---------------------------------------------------------------------------
// Nested-directory parquet scanners
// ---------------------------------------------------------------------------

/// Scan a two-level nested directory (`dir/sub1/sub2/*.parquet`) and return
/// relative paths like `"ContractName/functionName/0-999.parquet"`.
///
/// Used by eth_calls where files live at `eth_calls/{contract}/{function}/`.
pub fn scan_nested_parquet_files_2(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();
    let Ok(level1) = std::fs::read_dir(dir) else {
        return files;
    };
    for l1 in level1.flatten() {
        let l1_path = l1.path();
        if !l1_path.is_dir() {
            continue;
        }
        let l1_name = match l1.file_name().to_str() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let Ok(level2) = std::fs::read_dir(&l1_path) else {
            continue;
        };
        for l2 in level2.flatten() {
            let l2_path = l2.path();
            if !l2_path.is_dir() {
                continue;
            }
            let l2_name = match l2.file_name().to_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            let Ok(file_entries) = std::fs::read_dir(&l2_path) else {
                continue;
            };
            for fe in file_entries.flatten() {
                if let Some(name) = fe.file_name().to_str() {
                    if name.ends_with(".parquet") {
                        files.insert(format!("{}/{}/{}", l1_name, l2_name, name));
                    }
                }
            }
        }
    }
    files
}

/// Scan a one-level nested directory (`dir/sub/*.parquet`) and return
/// relative paths like `"collection/0-999.parquet"`.
///
/// Used by factories where files live at `factories/{collection}/`.
pub fn scan_nested_parquet_files_1(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();
    let Ok(level1) = std::fs::read_dir(dir) else {
        return files;
    };
    for l1 in level1.flatten() {
        let l1_path = l1.path();
        if !l1_path.is_dir() {
            continue;
        }
        let l1_name = match l1.file_name().to_str() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let Ok(file_entries) = std::fs::read_dir(&l1_path) else {
            continue;
        };
        for fe in file_entries.flatten() {
            if let Some(name) = fe.file_name().to_str() {
                if name.ends_with(".parquet") {
                    files.insert(format!("{}/{}", l1_name, name));
                }
            }
        }
    }
    files
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_range_blocks() {
        let p = Path::new("blocks_0-999.parquet");
        assert_eq!(parse_range_from_filename(p), Some((0, 999)));
    }

    #[test]
    fn parse_range_logs() {
        let p = Path::new("logs_1000-1999.parquet");
        assert_eq!(parse_range_from_filename(p), Some((1000, 1999)));
    }

    #[test]
    fn parse_range_decoded() {
        let p = Path::new("decoded_0-9999.parquet");
        assert_eq!(parse_range_from_filename(p), Some((0, 9999)));
    }

    #[test]
    fn parse_range_bare() {
        let p = Path::new("0-999.parquet");
        assert_eq!(parse_range_from_filename(p), Some((0, 999)));
    }

    #[test]
    fn parse_range_invalid() {
        assert_eq!(parse_range_from_filename(Path::new("foo.parquet")), None);
        assert_eq!(
            parse_range_from_filename(Path::new("blocks_abc.parquet")),
            None
        );
    }

    #[test]
    fn path_builders() {
        assert_eq!(
            raw_blocks_dir("ethereum"),
            PathBuf::from("data/ethereum/historical/raw/blocks")
        );
        assert_eq!(
            raw_receipts_dir("base"),
            PathBuf::from("data/base/historical/raw/receipts")
        );
        assert_eq!(
            raw_logs_dir("base"),
            PathBuf::from("data/base/historical/raw/logs")
        );
        assert_eq!(
            raw_eth_calls_dir("base"),
            PathBuf::from("data/base/historical/raw/eth_calls")
        );
        assert_eq!(
            factories_dir("base"),
            PathBuf::from("data/base/historical/factories")
        );
        assert_eq!(
            decoded_base_dir("base"),
            PathBuf::from("data/base/historical/decoded")
        );
        assert_eq!(
            decoded_logs_dir("ethereum"),
            PathBuf::from("data/ethereum/historical/decoded/logs")
        );
        assert_eq!(
            decoded_eth_calls_dir("ethereum"),
            PathBuf::from("data/ethereum/historical/decoded/eth_calls")
        );
    }

    #[test]
    fn block_range_file_name_with_prefix() {
        let r = BlockRange::new(0, 1000);
        assert_eq!(r.file_name("blocks"), "blocks_0-999.parquet");
        assert_eq!(r.file_name("logs"), "logs_0-999.parquet");
        assert_eq!(r.file_name("receipts"), "receipts_0-999.parquet");
    }

    #[test]
    fn block_range_file_name_no_prefix() {
        let r = BlockRange::new(1000, 2000);
        assert_eq!(r.file_name(""), "1000-1999.parquet");
    }

    #[test]
    fn block_range_end_inclusive() {
        let r = BlockRange::new(0, 1000);
        assert_eq!(r.end_inclusive(), 999);
    }
}
