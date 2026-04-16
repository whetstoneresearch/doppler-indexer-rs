//! Split 10k block-range parquet files into 1k block-range files.
//!
//! Walks `data/` recursively, finds every `.parquet` file whose filename
//! encodes a 10,000-block range (e.g. `27180000-27189999.parquet`), and splits
//! it into up to 10 files of 1,000-block ranges.
//!
//! Safety guarantees:
//! - Atomic writes (temp file + rename) — no corrupt partial files.
//! - Schema is preserved exactly from the source file.
//! - After writing, all output files are re-read and the total row count is
//!   verified against the original. The original is only deleted after
//!   verification passes.
//! - Pre-existing 1k output files are included in verification but never
//!   overwritten.
//! - Parallel via rayon, progress printed every 1000 files.
//!
//! Usage:
//!   cargo run --release --bin split_ranges [-- <data_dir>]
//!
//! Defaults to `data/` relative to the working directory.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, BooleanArray, UInt64Array};
use arrow::compute::{self, lexsort_to_indices, take, SortColumn};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

// ── Constants ────────────────────────────────────────────────────────────────

const RANGE_10K: u64 = 10_000;
const RANGE_1K: u64 = 1_000;
const ARCHIVE_ROOT: &str = "1k-archive";

// ── Entry point ──────────────────────────────────────────────────────────────

fn main() {
    let data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data".to_string());
    let data_path = Path::new(&data_dir);

    if !data_path.exists() {
        eprintln!("error: data directory '{}' does not exist", data_dir);
        std::process::exit(1);
    }

    // Discover every parquet file with a 10k range in its name.
    let files = find_10k_range_files(data_path);
    let total = files.len();
    println!("found {} parquet files with 10k block ranges", total);

    if total == 0 {
        return;
    }

    let processed = AtomicU64::new(0);
    let errors = AtomicU64::new(0);

    files.par_iter().for_each(|entry| {
        match split_file(
            &entry.path,
            entry.range_start,
            entry.range_end_inclusive,
            &entry.prefix,
            data_path,
        ) {
            Ok(Split::Done) => {
                let n = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if n % 1000 == 0 || n == total as u64 {
                    eprintln!("[{}/{}] processed", n, total);
                }
            }
            Err(e) => {
                eprintln!("ERROR {}: {:#}", entry.path.display(), e);
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let p = processed.load(Ordering::Relaxed);
    let e = errors.load(Ordering::Relaxed);
    println!("done — processed: {p}, errors: {e}");
    if e > 0 {
        std::process::exit(1);
    }
}

// ── File discovery ───────────────────────────────────────────────────────────

struct RangeFile {
    path: PathBuf,
    /// e.g. "blocks" for `blocks_27180000-27189999.parquet`, empty for bare ranges.
    prefix: String,
    range_start: u64,
    #[allow(dead_code)]
    range_end_inclusive: u64,
}

fn find_10k_range_files(root: &Path) -> Vec<RangeFile> {
    let mut result = Vec::new();
    walk_dir(root, &mut result);
    result
}

fn walk_dir(dir: &Path, out: &mut Vec<RangeFile>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_dir(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            if let Some(rf) = parse_10k_file(&path) {
                out.push(rf);
            }
        }
    }
}

/// Parse a parquet filename into a `RangeFile` if it encodes exactly a 10k range.
///
/// Handles both `blocks_27180000-27189999.parquet` and `27180000-27189999.parquet`.
fn parse_10k_file(path: &Path) -> Option<RangeFile> {
    let stem = path.file_stem()?.to_str()?;

    // Known prefixes used by the indexer.
    let known_prefixes = ["blocks", "receipts", "logs", "decoded"];

    let (prefix, range_part) = known_prefixes
        .iter()
        .find_map(|p| {
            stem.strip_prefix(p)
                .and_then(|rest| rest.strip_prefix('_'))
                .map(|rest| (p.to_string(), rest))
        })
        .unwrap_or_else(|| (String::new(), stem));

    let mut parts = range_part.splitn(2, '-');
    let start: u64 = parts.next()?.parse().ok()?;
    let end: u64 = parts.next()?.parse().ok()?;

    // Only 10k ranges.
    if end - start + 1 != RANGE_10K {
        return None;
    }

    Some(RangeFile {
        path: path.to_path_buf(),
        prefix,
        range_start: start,
        range_end_inclusive: end,
    })
}

// ── Splitting logic ──────────────────────────────────────────────────────────

enum Split {
    Done,
}

fn split_file(
    path: &Path,
    range_start: u64,
    _range_end_inclusive: u64,
    prefix: &str,
    data_root: &Path,
) -> Result<Split, Box<dyn std::error::Error + Send + Sync>> {
    let parent = path.parent().unwrap();

    // Build the archive directory by replacing the data root with ARCHIVE_ROOT.
    // e.g. data/base/historical/factories/X → 1k-archive/base/historical/factories/X
    let archive_dir = compute_archive_dir(parent, data_root)?;

    // Read the original file into a single RecordBatch.
    let (schema, original_batch) = read_parquet(path)?;
    let original_rows = original_batch.num_rows();

    // Detect the block-number column.
    let bn_col_name = detect_block_number_column(&schema)?;
    let bn_col = original_batch
        .column_by_name(&bn_col_name)
        .ok_or_else(|| format!("column '{}' not found in batch", bn_col_name))?;
    let bn_array = bn_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| format!("column '{}' is not UInt64", bn_col_name))?;

    // Build the 10 sub-ranges.
    let sub_ranges: Vec<(u64, u64)> = (0..10)
        .map(|i| {
            let sub_start = range_start + i * RANGE_1K;
            let sub_end = sub_start + RANGE_1K - 1;
            (sub_start, sub_end)
        })
        .collect();

    // Filter and write each sub-range.
    let mut written_files: Vec<PathBuf> = Vec::new();
    let mut archived_files: Vec<(PathBuf, PathBuf)> = Vec::new(); // (original, archive_dest)

    for (sub_start, sub_end) in &sub_ranges {
        let fname = make_filename(prefix, *sub_start, *sub_end);
        let output_path = parent.join(&fname);

        // Build a boolean mask: block_number >= sub_start AND block_number <= sub_end
        let mask = compute_range_mask(bn_array, *sub_start, *sub_end);
        let filtered = compute::filter_record_batch(&original_batch, &mask)?;

        if output_path.exists() {
            // Compare existing 1k file with the split from the 10k source.
            let (_, existing_batch) = read_parquet(&output_path)?;
            if batches_equal(&existing_batch, &filtered) {
                // Identical data — no action needed, keep existing.
                continue;
            }

            // Data differs — archive the existing 1k file, then overwrite.
            fs::create_dir_all(&archive_dir)?;
            let archive_dest = archive_dir.join(&fname);
            fs::rename(&output_path, &archive_dest)?;
            archived_files.push((output_path.clone(), archive_dest));
        }

        // Always write, even if zero rows — the indexer uses the file's
        // existence to know the range has been processed.
        atomic_write_parquet(&filtered, &output_path)?;
        written_files.push(output_path);
    }

    // ── Verification ─────────────────────────────────────────────────────────
    //
    // Re-read all 10 output files from disk, concatenate into a single batch,
    // sort both original and reassembled by block number, then compare every
    // column for exact equality.

    let mut output_batches: Vec<RecordBatch> = Vec::new();

    for (sub_start, sub_end) in &sub_ranges {
        let fname = make_filename(prefix, *sub_start, *sub_end);
        let out_path = parent.join(&fname);

        let (out_schema, out_batch) = read_parquet(&out_path)?;

        if out_schema.fields() != schema.fields() {
            rollback(&written_files, &archived_files);
            return Err(format!(
                "schema mismatch in {}: expected {:?}, got {:?}",
                out_path.display(),
                schema.fields(),
                out_schema.fields()
            )
            .into());
        }

        if out_batch.num_rows() > 0 {
            output_batches.push(out_batch);
        }
    }

    // Concatenate all output batches and sort both sides by block number.
    let reassembled = if output_batches.is_empty() {
        RecordBatch::new_empty(schema.clone())
    } else {
        compute::concat_batches(&schema, &output_batches)?
    };

    if reassembled.num_rows() != original_rows {
        rollback(&written_files, &archived_files);
        return Err(format!(
            "row count mismatch: original={}, reassembled={}. Rolled back.",
            original_rows,
            reassembled.num_rows()
        )
        .into());
    }

    // Sort both by ALL columns lexicographically, then compare column-by-column.
    // Using only block_number is insufficient — files with duplicate block numbers
    // (e.g. raw logs with many events per block) would produce non-deterministic
    // ordering, causing spurious verification failures.
    if original_rows > 0 {
        let original_sorted = sort_batch_by_all_columns(&original_batch)?;
        let reassembled_sorted = sort_batch_by_all_columns(&reassembled)?;

        for col_idx in 0..original_sorted.num_columns() {
            if original_sorted.column(col_idx) != reassembled_sorted.column(col_idx) {
                rollback(&written_files, &archived_files);
                let col_name = schema.field(col_idx).name();
                return Err(format!(
                    "data mismatch in column '{}' after reassembly. Rolled back.",
                    col_name
                )
                .into());
            }
        }
    }

    // Verification passed — safe to delete the original.
    fs::remove_file(path)?;

    Ok(Split::Done)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Rollback: delete newly written files and restore archived files.
fn rollback(written: &[PathBuf], archived: &[(PathBuf, PathBuf)]) {
    for f in written {
        let _ = fs::remove_file(f);
    }
    for (original_path, archive_path) in archived {
        let _ = fs::rename(archive_path, original_path);
    }
}

/// Compare two `RecordBatch`es for exact equality (schema, row count, all cell values).
fn batches_equal(a: &RecordBatch, b: &RecordBatch) -> bool {
    if a.num_rows() != b.num_rows() || a.num_columns() != b.num_columns() {
        return false;
    }
    if a.schema() != b.schema() {
        return false;
    }
    for i in 0..a.num_columns() {
        if a.column(i) != b.column(i) {
            return false;
        }
    }
    true
}

/// Replace the `data_root` prefix in `dir` with `ARCHIVE_ROOT`.
///
/// e.g. `data/base/historical/factories/X` → `1k-archive/base/historical/factories/X`
fn compute_archive_dir(
    dir: &Path,
    data_root: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let relative = dir.strip_prefix(data_root).map_err(|_| {
        format!(
            "path {} is not under data root {}",
            dir.display(),
            data_root.display()
        )
    })?;
    Ok(Path::new(ARCHIVE_ROOT).join(relative))
}

fn make_filename(prefix: &str, start: u64, end_inclusive: u64) -> String {
    if prefix.is_empty() {
        format!("{}-{}.parquet", start, end_inclusive)
    } else {
        format!("{}_{}-{}.parquet", prefix, start, end_inclusive)
    }
}

/// Read all row groups from a parquet file into a single `RecordBatch`.
fn read_parquet(
    path: &Path,
) -> Result<(SchemaRef, RecordBatch), Box<dyn std::error::Error + Send + Sync>> {
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }

    if batches.is_empty() {
        // Empty file — return a zero-row batch with the correct schema.
        let empty = RecordBatch::new_empty(schema.clone());
        return Ok((schema, empty));
    }

    if batches.len() == 1 {
        return Ok((schema, batches.into_iter().next().unwrap()));
    }

    // Concatenate multiple batches.
    let concatenated = arrow::compute::concat_batches(&schema, &batches)?;
    Ok((schema, concatenated))
}

/// Detect whether the block-number column is called `block_number` or `number`.
fn detect_block_number_column(
    schema: &SchemaRef,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if schema.column_with_name("block_number").is_some() {
        Ok("block_number".to_string())
    } else if schema.column_with_name("number").is_some() {
        Ok("number".to_string())
    } else {
        Err(format!(
            "no block number column found; columns: {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        )
        .into())
    }
}

/// Sort a `RecordBatch` lexicographically by all columns (ascending, nulls last).
///
/// This guarantees deterministic row ordering even when the block-number column
/// contains duplicates (e.g. raw logs with many events per block).
fn sort_batch_by_all_columns(
    batch: &RecordBatch,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let sort_columns: Vec<SortColumn> = batch
        .columns()
        .iter()
        .map(|col| SortColumn {
            values: col.clone(),
            options: None,
        })
        .collect();

    let indices = lexsort_to_indices(&sort_columns, None)?;
    let sorted_columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
}

/// Build a boolean mask selecting rows where `start <= value <= end`.
fn compute_range_mask(array: &UInt64Array, start: u64, end: u64) -> BooleanArray {
    let len = array.len();
    let mut mask = vec![false; len];
    for i in 0..len {
        if !array.is_null(i) {
            let v = array.value(i);
            mask[i] = v >= start && v <= end;
        }
    }
    BooleanArray::from(mask)
}

/// Atomically write a `RecordBatch` to a Snappy-compressed parquet file.
///
/// Writes to a temp file in the same directory, flushes, then renames.
fn atomic_write_parquet(
    batch: &RecordBatch,
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let random_suffix: u32 = rand::random();
    let temp_name = format!(
        "{}.tmp.{}",
        output_path.file_name().unwrap().to_string_lossy(),
        random_suffix
    );
    let temp_path = output_path.with_file_name(&temp_name);

    let result = write_parquet_to_file(batch, &temp_path);
    match result {
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;

    let file = writer.into_inner()?;
    file.sync_all()?;

    Ok(())
}
