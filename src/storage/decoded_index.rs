//! Shared decoded-data indexing utilities.
//!
//! Provides two reusable helpers for scanning decoded parquet output directories:
//!
//! - [`scan_existing_decoded_files`] — recursively collects relative paths of all
//!   parquet files under a directory.  Used by the catchup decoders to determine
//!   which output files already exist so they can be skipped.
//!
//! - [`build_decoded_file_index`] — scans a two-level `{source}/{event_or_function}/`
//!   directory structure and returns a structured index of parquet file ranges.
//!   Used by `HistoricalDataReader` to build its in-memory file index.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::storage::paths::scan_parquet_ranges;

/// Index mapping `(source_name, event_or_function_name)` to a sorted list of
/// `(range_start, range_end_inclusive, file_path)` tuples.
pub type DecodedFileIndex = HashMap<(String, String), Vec<(u64, u64, PathBuf)>>;

/// Recursively scan a decoded output directory and return the relative paths of
/// all `.parquet` files found underneath it.
///
/// The returned strings are forward-slash–joined relative paths such as
/// `"source/event/0-999.parquet"`.  If the directory does not exist or cannot
/// be read the function returns an empty set (it never errors).
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

/// Build a structured index of decoded parquet files.
///
/// Scans a two-level directory structure:
///
/// ```text
/// base_dir/{source}/{event_or_function}/*.parquet
/// ```
///
/// Returns a map keyed by `(source_name, event_or_function_name)` whose values
/// are sorted vectors of `(range_start, range_end_inclusive, file_path)`.
///
/// This is the same logic previously embedded in
/// `HistoricalDataReader::index_directory`.
pub fn build_decoded_file_index(
    base_dir: &Path,
) -> Result<DecodedFileIndex, std::io::Error> {
    let mut index = DecodedFileIndex::new();

    // Iterate over source directories
    for source_entry in std::fs::read_dir(base_dir)? {
        let source_entry = source_entry?;
        if !source_entry.file_type()?.is_dir() {
            continue;
        }
        let source_name = source_entry.file_name().to_string_lossy().to_string();

        // Iterate over event/function directories
        for name_entry in std::fs::read_dir(source_entry.path())? {
            let name_entry = name_entry?;
            if !name_entry.file_type()?.is_dir() {
                continue;
            }
            let name = name_entry.file_name().to_string_lossy().to_string();

            // Find parquet files and extract ranges (already sorted by start)
            let files = scan_parquet_ranges(&name_entry.path())?;

            let key = (source_name.clone(), name);
            index.insert(key, files);
        }
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // scan_existing_decoded_files
    // -----------------------------------------------------------------------

    #[test]
    fn scan_existing_decoded_files_finds_nested_parquet() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();

        // Create nested structure: source1/event1/0-999.parquet
        let event_dir = base.join("source1").join("event1");
        std::fs::create_dir_all(&event_dir).unwrap();
        std::fs::write(event_dir.join("0-999.parquet"), b"").unwrap();

        // Another deeper path: source2/func/1000-1999.parquet
        let func_dir = base.join("source2").join("func");
        std::fs::create_dir_all(&func_dir).unwrap();
        std::fs::write(func_dir.join("1000-1999.parquet"), b"").unwrap();

        // A non-parquet file that should be ignored
        std::fs::write(event_dir.join("index.json"), b"{}").unwrap();

        let result = scan_existing_decoded_files(base);
        assert_eq!(result.len(), 2);
        assert!(result.contains("source1/event1/0-999.parquet"));
        assert!(result.contains("source2/func/1000-1999.parquet"));
    }

    #[test]
    fn scan_existing_decoded_files_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let result = scan_existing_decoded_files(tmp.path());
        assert!(result.is_empty());
    }

    #[test]
    fn scan_existing_decoded_files_nonexistent_dir() {
        let result = scan_existing_decoded_files(Path::new("/tmp/nonexistent_decoded_index_test"));
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // build_decoded_file_index
    // -----------------------------------------------------------------------

    #[test]
    fn build_decoded_file_index_returns_correct_keys_and_ranges() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();

        // source/event/0-999.parquet
        let event_dir = base.join("v3").join("Swap");
        std::fs::create_dir_all(&event_dir).unwrap();
        std::fs::write(event_dir.join("0-999.parquet"), b"").unwrap();
        std::fs::write(event_dir.join("1000-1999.parquet"), b"").unwrap();

        let index = build_decoded_file_index(base).unwrap();
        assert_eq!(index.len(), 1);

        let key = ("v3".to_string(), "Swap".to_string());
        let files = index.get(&key).unwrap();
        assert_eq!(files.len(), 2);
        // Verify sorted order
        assert_eq!(files[0].0, 0);
        assert_eq!(files[0].1, 999);
        assert_eq!(files[1].0, 1000);
        assert_eq!(files[1].1, 1999);
    }

    #[test]
    fn build_decoded_file_index_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let index = build_decoded_file_index(tmp.path()).unwrap();
        assert!(index.is_empty());
    }

    #[test]
    fn build_decoded_file_index_nonexistent_dir() {
        let result = build_decoded_file_index(Path::new("/tmp/nonexistent_decoded_index_test"));
        assert!(result.is_err());
    }

    #[test]
    fn build_decoded_file_index_multiple_sources() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();

        // Two sources, each with one event
        let dir1 = base.join("v3").join("Swap");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::write(dir1.join("0-999.parquet"), b"").unwrap();

        let dir2 = base.join("v4").join("Initialize");
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(dir2.join("0-999.parquet"), b"").unwrap();

        let index = build_decoded_file_index(base).unwrap();
        assert_eq!(index.len(), 2);
        assert!(index.contains_key(&("v3".to_string(), "Swap".to_string())));
        assert!(index.contains_key(&("v4".to_string(), "Initialize".to_string())));
    }

    #[test]
    fn build_decoded_file_index_skips_non_dir_entries() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();

        // A regular file at the top level should be skipped
        std::fs::write(base.join("some_file.txt"), b"").unwrap();

        // A valid nested structure
        let dir = base.join("v3").join("Swap");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("0-999.parquet"), b"").unwrap();

        let index = build_decoded_file_index(base).unwrap();
        assert_eq!(index.len(), 1);
    }
}
