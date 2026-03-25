//! Helpers for indexing existing decoded parquet files.
//!
//! The [`scan_existing_decoded_files`] function provides a simple existence check
//! used by catchup phases to skip already-decoded files.
//!
//! Note: `HistoricalDataReader::index_directory` in `src/transformations/historical.rs`
//! performs a similar directory scan but returns structured range metadata
//! (`HashMap<(String, String), Vec<(u64, u64, PathBuf)>>`) for range-based queries.
//! These serve different purposes and are intentionally kept separate.

use std::collections::HashSet;
use std::path::Path;

/// Recursively scan a directory for `.parquet` files and return their
/// paths relative to `output_base` as a set.
///
/// Used by catchup phases to determine which decoded files already exist,
/// avoiding redundant re-decoding.
pub fn scan_existing_decoded_files(output_base: &Path) -> HashSet<String> {
    let mut files = HashSet::new();

    fn scan_recursive(dir: &Path, base: &Path, files: &mut HashSet<String>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan_recursive(&path, base, files);
                } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    if let Ok(rel) = path.strip_prefix(base) {
                        files.insert(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    scan_recursive(output_base, output_base, &mut files);
    files
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_scan_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        let result = scan_existing_decoded_files(dir.path());
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_nonexistent_directory() {
        let result = scan_existing_decoded_files(Path::new("/nonexistent/path"));
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_finds_parquet_files() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("source").join("event");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("0-999.parquet"), b"fake").unwrap();
        fs::write(sub.join("1000-1999.parquet"), b"fake").unwrap();
        // Non-parquet files should be ignored
        fs::write(sub.join("index.json"), b"{}").unwrap();

        let result = scan_existing_decoded_files(dir.path());
        assert_eq!(result.len(), 2);
        assert!(result.contains("source/event/0-999.parquet"));
        assert!(result.contains("source/event/1000-1999.parquet"));
    }
}
