//! Skipped slots index for Solana raw data collection.
//!
//! Tracks which slots were skipped (no block produced) within each range.
//! This is persisted as a `skipped_slots.json` sidecar alongside parquet files,
//! allowing the catchup process to know which slots legitimately had no block
//! and avoid re-fetching them.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;

/// Maps range keys (e.g. "250000000-250000999") to sorted lists of skipped slot numbers.
pub type SkippedSlotsIndex = HashMap<String, Vec<u64>>;

const INDEX_FILENAME: &str = "skipped_slots.json";

// ---------------------------------------------------------------------------
// Range key helper
// ---------------------------------------------------------------------------

/// Build a range key string from start and end_inclusive.
pub fn range_key(start: u64, end_inclusive: u64) -> String {
    format!("{}-{}", start, end_inclusive)
}

// ---------------------------------------------------------------------------
// Completeness check
// ---------------------------------------------------------------------------

/// Check if a range is marked as complete in the index.
///
/// A range is complete if its key exists in the index. The value may be an
/// empty list (meaning zero slots were skipped in that range).
pub fn is_range_complete(index: &SkippedSlotsIndex, rk: &str) -> bool {
    index.contains_key(rk)
}

// ---------------------------------------------------------------------------
// I/O - sync (called via spawn_blocking)
// ---------------------------------------------------------------------------

/// Read `skipped_slots.json` from `dir`. Returns an empty index if the file
/// is missing or cannot be parsed.
pub fn read_skipped_slots_index(dir: &Path) -> SkippedSlotsIndex {
    let path = dir.join(INDEX_FILENAME);
    match std::fs::read_to_string(&path) {
        Ok(content) => {
            let index: SkippedSlotsIndex = serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read skipped slots index from {}: {} ranges tracked",
                path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No skipped slots index at {} ({}), starting fresh",
                path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Atomically write `skipped_slots.json` to `dir`.
///
/// Uses the write-to-temp + sync + rename pattern so a crash never leaves a
/// partially-written file.
pub fn write_skipped_slots_index(dir: &Path, index: &SkippedSlotsIndex) -> Result<(), io::Error> {
    std::fs::create_dir_all(dir)?;

    let path = dir.join(INDEX_FILENAME);
    let content = serde_json::to_string_pretty(index)
        .map_err(|e| io::Error::other(format!("JSON serialize error: {}", e)))?;

    // Atomic write: temp file with random suffix -> sync -> rename.
    let random_suffix: u64 = rand::random();
    let tmp_name = format!("{}.{:x}.tmp", INDEX_FILENAME, random_suffix);
    let tmp_path = dir.join(tmp_name);

    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }

    std::fs::rename(&tmp_path, &path)?;

    tracing::debug!(
        "Wrote skipped slots index to {}: {} ranges tracked",
        path.display(),
        index.len()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// I/O - async wrappers
// ---------------------------------------------------------------------------

/// Async wrapper around [`read_skipped_slots_index`].
pub async fn read_skipped_slots_index_async(dir: std::path::PathBuf) -> SkippedSlotsIndex {
    tokio::task::spawn_blocking(move || read_skipped_slots_index(&dir))
        .await
        .unwrap_or_default()
}

/// Async wrapper around [`write_skipped_slots_index`].
pub async fn write_skipped_slots_index_async(
    dir: std::path::PathBuf,
    index: SkippedSlotsIndex,
) -> Result<(), io::Error> {
    tokio::task::spawn_blocking(move || write_skipped_slots_index(&dir, &index))
        .await
        .map_err(|e| io::Error::other(format!("JoinError: {}", e)))?
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_index() -> SkippedSlotsIndex {
        let mut index = SkippedSlotsIndex::new();
        index.insert(
            "250000000-250000999".to_string(),
            vec![250_000_003, 250_000_050, 250_000_777],
        );
        index.insert("250001000-250001999".to_string(), vec![]);
        index
    }

    #[test]
    fn test_range_key() {
        assert_eq!(range_key(0, 999), "0-999");
        assert_eq!(range_key(250_000_000, 250_000_999), "250000000-250000999");
    }

    #[test]
    fn test_is_range_complete_true() {
        let index = sample_index();
        assert!(is_range_complete(&index, "250000000-250000999"));
        assert!(is_range_complete(&index, "250001000-250001999"));
    }

    #[test]
    fn test_is_range_complete_false() {
        let index = sample_index();
        assert!(!is_range_complete(&index, "999000000-999000999"));
        assert!(!is_range_complete(&index, "0-999"));
    }

    #[test]
    fn test_is_range_complete_empty_index() {
        let index = SkippedSlotsIndex::new();
        assert!(!is_range_complete(&index, "0-999"));
    }

    #[test]
    fn test_read_write_roundtrip() {
        let dir = TempDir::new().unwrap();
        let index = sample_index();

        write_skipped_slots_index(dir.path(), &index).unwrap();
        let loaded = read_skipped_slots_index(dir.path());
        assert_eq!(loaded, index);
    }

    #[test]
    fn test_read_missing_file() {
        let dir = TempDir::new().unwrap();
        let loaded = read_skipped_slots_index(dir.path());
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_write_creates_directory() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("deeply").join("nested").join("path");

        let index = sample_index();
        write_skipped_slots_index(&nested, &index).unwrap();

        let loaded = read_skipped_slots_index(&nested);
        assert_eq!(loaded, index);
    }

    #[test]
    fn test_empty_index_roundtrip() {
        let dir = TempDir::new().unwrap();
        let index = SkippedSlotsIndex::new();

        write_skipped_slots_index(dir.path(), &index).unwrap();
        let loaded = read_skipped_slots_index(dir.path());
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_overwrite_existing_index() {
        let dir = TempDir::new().unwrap();

        let mut index1 = SkippedSlotsIndex::new();
        index1.insert("0-999".to_string(), vec![5, 10]);
        write_skipped_slots_index(dir.path(), &index1).unwrap();

        let mut index2 = SkippedSlotsIndex::new();
        index2.insert("1000-1999".to_string(), vec![1500]);
        write_skipped_slots_index(dir.path(), &index2).unwrap();

        let loaded = read_skipped_slots_index(dir.path());
        assert_eq!(loaded, index2);
        assert!(!loaded.contains_key("0-999"));
    }

    #[tokio::test]
    async fn test_async_read_write_roundtrip() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        let index = sample_index();

        write_skipped_slots_index_async(dir_path.clone(), index.clone())
            .await
            .unwrap();

        let loaded = read_skipped_slots_index_async(dir_path).await;
        assert_eq!(loaded, index);
    }

    #[tokio::test]
    async fn test_async_read_missing_returns_empty() {
        let dir = TempDir::new().unwrap();
        let loaded = read_skipped_slots_index_async(dir.path().to_path_buf()).await;
        assert!(loaded.is_empty());
    }
}
