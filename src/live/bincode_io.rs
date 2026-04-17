//! Shared bincode I/O helpers for live mode storage.
//!
//! Used by both EVM `LiveStorage` and Solana `SolanaLiveStorage` to avoid
//! duplicating atomic write, read, and deletion logic.

use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Block not found: {0}")]
    NotFound(u64),
}

/// Atomically write to a file using a caller-supplied write function.
///
/// Uses write-to-temp, flush, rename pattern for atomicity.
/// When `durable` is true, also calls `sync_all()` before rename for crash safety.
/// Uses unique temp file names to avoid race conditions between concurrent writers.
pub fn atomic_write_with<F>(path: &Path, durable: bool, write_fn: F) -> Result<(), StorageError>
where
    F: FnOnce(&mut BufWriter<fs::File>) -> Result<(), StorageError>,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let random_suffix: u32 = rand::random();
    let temp_name = format!(
        "{}.tmp.{}",
        path.file_name().unwrap().to_string_lossy(),
        random_suffix
    );
    let temp_path = path.with_file_name(temp_name);

    let file = fs::File::create(&temp_path)?;
    let mut writer = BufWriter::new(file);

    let result = (|| -> Result<(), StorageError> {
        write_fn(&mut writer)?;
        writer.flush()?;
        let file = writer.into_inner().map_err(std::io::Error::other)?;
        if durable {
            file.sync_all()?;
        }
        fs::rename(&temp_path, path)?;
        Ok(())
    })();

    if let Err(e) = &result {
        tracing::debug!(
            "Cleaning up temp file after write error: {:?} ({})",
            temp_path,
            e
        );
        let _ = fs::remove_file(&temp_path);
    }

    result
}

/// Atomically write bincode-serialized data to a file without sync_all().
///
/// Data writes skip fsync because they are rebuildable — the status file
/// is the crash-safety gatekeeper.
pub fn write_bincode<T: Serialize + ?Sized>(path: &Path, data: &T) -> Result<(), StorageError> {
    atomic_write_with(path, false, |writer| {
        bincode::serialize_into(writer, data)?;
        Ok(())
    })
}

pub fn read_bincode<T: DeserializeOwned>(path: &Path) -> Result<T, StorageError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let data = bincode::deserialize_from(reader)?;
    Ok(data)
}

/// Map IO NotFound errors to StorageError::NotFound for bincode operations.
pub fn map_not_found(err: StorageError, block_number: u64) -> StorageError {
    match err {
        StorageError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            StorageError::NotFound(block_number)
        }
        other => other,
    }
}

/// Map raw IO NotFound errors to StorageError::NotFound.
pub fn map_io_not_found(err: std::io::Error, block_number: u64) -> StorageError {
    if err.kind() == std::io::ErrorKind::NotFound {
        StorageError::NotFound(block_number)
    } else {
        StorageError::Io(err)
    }
}

/// Check if a file is a temp file (has `.tmp.{random}` suffix).
pub fn is_temp_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|name| name.contains(".tmp."))
}

/// Parse `.bin` / `.json` file stems as `u64` block/slot numbers.
pub fn list_numbered_entries(dir: &Path) -> Result<Vec<u64>, StorageError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(stem) = path.file_stem() {
            if let Some(stem_str) = stem.to_str() {
                if let Ok(number) = stem_str.parse::<u64>() {
                    entries.push(number);
                }
            }
        }
    }
    entries.sort_unstable();
    Ok(entries)
}

/// TOCTOU-safe file deletion. Ignores NotFound errors since the file
/// may have been deleted between check and remove (or never existed).
pub fn safe_delete(path: &Path) -> Result<(), StorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// TOCTOU-safe directory deletion. Ignores NotFound errors since the directory
/// may have been deleted between check and remove (or never existed).
pub fn safe_delete_dir_all(path: &Path) -> Result<(), StorageError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::Io(e)),
    }
}

/// Generate path, write, read, and delete methods for a bincode storage entity.
///
/// Two forms:
/// - `slice`: write takes `&[T]`, read returns `Vec<T>`
/// - `ref`: write takes `&T`, read returns `T`
#[macro_export]
macro_rules! storage_entity {
    // Slice variant: write_foo(number, &[T]), read_foo(number) -> Vec<T>
    (slice $name:ident, $type:ty, $subdir:expr) => {
        paste::paste! {
            fn [<$name _path>](&self, number: u64) -> std::path::PathBuf {
                self.base_dir.join(format!(concat!($subdir, "/{}.bin"), number))
            }

            pub fn [<write_ $name>](
                &self,
                number: u64,
                data: &[$type],
            ) -> Result<(), $crate::live::bincode_io::StorageError> {
                $crate::live::bincode_io::write_bincode(&self.[<$name _path>](number), data)
            }

            pub fn [<read_ $name>](
                &self,
                number: u64,
            ) -> Result<Vec<$type>, $crate::live::bincode_io::StorageError> {
                $crate::live::bincode_io::read_bincode(&self.[<$name _path>](number))
                    .map_err(|e| $crate::live::bincode_io::map_not_found(e, number))
            }

            pub fn [<delete_ $name>](&self, number: u64) -> Result<(), $crate::live::bincode_io::StorageError> {
                $crate::live::bincode_io::safe_delete(&self.[<$name _path>](number))
            }
        }
    };
    // Ref variant: write_foo(number, &T), read_foo(number) -> T
    (ref $name:ident, $type:ty, $subdir:expr) => {
        paste::paste! {
            fn [<$name _path>](&self, number: u64) -> std::path::PathBuf {
                self.base_dir.join(format!(concat!($subdir, "/{}.bin"), number))
            }

            pub fn [<write_ $name>](
                &self,
                number: u64,
                data: &$type,
            ) -> Result<(), $crate::live::bincode_io::StorageError> {
                $crate::live::bincode_io::write_bincode(&self.[<$name _path>](number), data)
            }

            pub fn [<read_ $name>](
                &self,
                number: u64,
            ) -> Result<$type, $crate::live::bincode_io::StorageError> {
                $crate::live::bincode_io::read_bincode(&self.[<$name _path>](number))
                    .map_err(|e| $crate::live::bincode_io::map_not_found(e, number))
            }

            pub fn [<delete_ $name>](&self, number: u64) -> Result<(), $crate::live::bincode_io::StorageError> {
                $crate::live::bincode_io::safe_delete(&self.[<$name _path>](number))
            }
        }
    };
}
