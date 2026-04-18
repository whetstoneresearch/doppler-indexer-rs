//! Shared library crate for backfill and utility binaries.
//!
//! Exposes all internal modules so that binaries in `src/bin/` can import
//! from the same code as the main indexer binary without duplication.

// Helpers required by runtime.rs (which references crate::has_items / crate::optional_channel).
#[allow(dead_code)]
pub(crate) fn has_items<T>(opt: &Option<Vec<T>>) -> bool {
    opt.as_ref().is_some_and(|v| !v.is_empty())
}

#[allow(dead_code)]
pub(crate) fn optional_channel<T>(
    enabled: bool,
    capacity: usize,
) -> (
    Option<tokio::sync::mpsc::Sender<T>>,
    Option<tokio::sync::mpsc::Receiver<T>>,
) {
    if enabled {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    }
}

#[cfg(feature = "bench")]
pub mod bench;
pub mod db;
pub mod decoding;
pub mod live;
pub mod metrics;
pub mod raw_data;
pub mod rpc;
pub mod runtime;
pub mod storage;
pub mod transformations;
pub mod types;
