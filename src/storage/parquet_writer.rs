//! Atomic parquet file writer.
//!
//! Writes parquet data to a temporary file, syncs to disk, then atomically
//! renames to the final path. This prevents corrupt parquet files from
//! partially-written data when the process is killed mid-write (parquet
//! writes the footer last, so an interrupted write leaves a file with no
//! valid footer).

use std::fs;
use std::path::Path;

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Atomically write a RecordBatch to a Snappy-compressed parquet file with `sync_all()`.
///
/// Use for data that must survive power loss (e.g., live compaction where the
/// source bincode files are deleted after writing).
///
/// 1. Creates parent directories if needed.
/// 2. Writes to a temporary file (`.tmp.{random}` suffix) in the same directory.
/// 3. Calls `sync_all()` to ensure data is durable on disk.
/// 4. Atomically renames temp file to the final path.
/// 5. Removes the temp file on any error.
pub fn atomic_write_parquet(batch: &RecordBatch, output_path: &Path) -> Result<(), std::io::Error> {
    atomic_write_parquet_inner(batch, output_path, true)
}

/// Atomically write a RecordBatch to a Snappy-compressed parquet file without `sync_all()`.
///
/// Use for rebuildable data (historical collection, decoding) where the data can
/// be re-fetched from RPC if lost in a crash. Skips fsync for better throughput.
///
/// Still uses temp-file + flush + rename for atomicity (prevents corrupt partial writes).
pub fn atomic_write_parquet_fast(
    batch: &RecordBatch,
    output_path: &Path,
) -> Result<(), std::io::Error> {
    atomic_write_parquet_inner(batch, output_path, false)
}

fn atomic_write_parquet_inner(
    batch: &RecordBatch,
    output_path: &Path,
    durable: bool,
) -> Result<(), std::io::Error> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let random_suffix: u32 = rand::random();
    let temp_name = format!(
        "{}.tmp.{}",
        output_path.file_name().unwrap().to_string_lossy(),
        random_suffix
    );
    let temp_path = output_path.with_file_name(temp_name);

    match write_parquet_to_file(batch, &temp_path, durable) {
        Ok(()) => {
            fs::rename(&temp_path, output_path)?;
            Ok(())
        }
        Err(e) => {
            let _ = fs::remove_file(&temp_path);
            Err(e)
        }
    }
}

fn write_parquet_to_file(
    batch: &RecordBatch,
    path: &Path,
    durable: bool,
) -> Result<(), std::io::Error> {
    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(std::io::Error::other)?;
    writer.write(batch).map_err(std::io::Error::other)?;

    // into_inner() writes the footer and returns the underlying File,
    // letting us sync before the atomic rename.
    let file = writer.into_inner().map_err(std::io::Error::other)?;
    if durable {
        file.sync_all()?;
    }

    Ok(())
}
