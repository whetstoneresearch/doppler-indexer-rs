//! CatchupLoader: per-WorkItem data loading, handler invocation, and DB write.
//!
//! This module contains:
//! - Free functions shared by `CatchupLoader` and the engine's live-path methods
//!   (`read_decoded_parquet`, `read_receipt_addresses`).
//! - [`CatchupPayload`]: opaque payload carried in a [`WorkItem`].
//! - [`CatchupLoader`]: executes one [`WorkItem`] end-to-end.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::task::JoinSet;

use super::dag::WorkItem;
use crate::db::DbPool;
use crate::rpc::UnifiedRpcClient;
use crate::transformations::context::{DecodedCall, DecodedEvent, TransactionAddresses, TransformationContext};
use crate::transformations::engine::HandlerKind;
use crate::transformations::error::TransformationError;
use crate::transformations::executor::inject_source_version;
use crate::transformations::finalizer::RangeFinalizer;
use crate::transformations::historical::HistoricalDataReader;
use crate::transformations::retry::{filter_calls_by_start_block, filter_events_by_start_block};
use crate::transformations::traits::TransformationHandler;
use crate::types::config::contract::Contracts;

// ─── Free functions ──────────────────────────────────────────────────────────

/// Generic parquet reader shared by CatchupLoader and engine's live-path methods.
///
/// Spawns one blocking task per file path returned by `resolve_paths`, collects
/// all items, and returns the merged result.
pub(crate) async fn read_decoded_parquet<T>(
    historical_reader: Arc<HistoricalDataReader>,
    triggers: &[(String, String)],
    resolve_paths: impl Fn(&str, &str) -> Vec<PathBuf>,
    read_file: impl Fn(Arc<HistoricalDataReader>, &Path, &str, &str) -> Result<Vec<T>, TransformationError>
        + Send
        + Sync
        + 'static,
) -> Result<Vec<T>, TransformationError>
where
    T: Send + 'static,
{
    let mut read_tasks: JoinSet<Result<Vec<T>, TransformationError>> = JoinSet::new();
    let read_fn = Arc::new(read_file);

    for (source_name, secondary_name) in triggers {
        let paths = resolve_paths(source_name, secondary_name);

        for file_path in paths {
            if !file_path.exists() {
                continue;
            }

            let reader = historical_reader.clone();
            let src = source_name.clone();
            let name = secondary_name.clone();
            let read_fn = read_fn.clone();

            read_tasks.spawn_blocking(move || {
                tracing::debug!("Reading decoded data from {}", file_path.display());
                match read_fn(reader, &file_path, &src, &name) {
                    Ok(items) => {
                        tracing::debug!(
                            "Read {} items from {}",
                            items.len(),
                            file_path.display()
                        );
                        Ok(items)
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read decoded data from {}: {}",
                            file_path.display(),
                            e
                        );
                        Ok(Vec::new())
                    }
                }
            });
        }
    }

    let mut all_items = Vec::new();
    while let Some(result) = read_tasks.join_next().await {
        match result {
            Ok(Ok(items)) => all_items.extend(items),
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(TransformationError::IoError(std::io::Error::other(
                    e.to_string(),
                )))
            }
        }
    }

    Ok(all_items)
}

/// Read transaction addresses from a receipts parquet file.
///
/// Returns a map from tx_hash → `TransactionAddresses`. Returns an empty map
/// if the file is missing or cannot be read.
pub(crate) fn read_receipt_addresses(
    raw_receipts_dir: &Path,
    range_start: u64,
    range_end: u64,
) -> HashMap<[u8; 32], TransactionAddresses> {
    use arrow::array::{Array, FixedSizeBinaryArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file_name = format!("receipts_{}-{}.parquet", range_start, range_end - 1);
    let file_path = raw_receipts_dir.join(&file_name);

    if !file_path.exists() {
        tracing::debug!(
            "No receipt file found at {}, tx addresses unavailable for range {}-{}",
            file_path.display(),
            range_start,
            range_end - 1
        );
        return HashMap::new();
    }

    let file = match std::fs::File::open(&file_path) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("Failed to open receipt file {}: {}", file_path.display(), e);
            return HashMap::new();
        }
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                "Failed to read receipt parquet {}: {}",
                file_path.display(),
                e
            );
            return HashMap::new();
        }
    };

    let reader = match builder.build() {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                "Failed to build receipt reader {}: {}",
                file_path.display(),
                e
            );
            return HashMap::new();
        }
    };

    let mut addresses = HashMap::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "Failed to read receipt batch from {}: {}",
                    file_path.display(),
                    e
                );
                continue;
            }
        };

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        let tx_hash_col = match batch.column_by_name("transaction_hash") {
            Some(col) => match col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                Some(arr) => arr.clone(),
                None => continue,
            },
            None => continue,
        };

        let from_col = match batch.column_by_name("from_address") {
            Some(col) => match col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                Some(arr) => arr.clone(),
                None => continue,
            },
            None => continue,
        };

        let to_col = batch
            .column_by_name("to_address")
            .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>().cloned());

        for row in 0..num_rows {
            let tx_hash: [u8; 32] = match tx_hash_col.value(row).try_into() {
                Ok(h) => h,
                Err(_) => continue,
            };

            let from_address: [u8; 20] = match from_col.value(row).try_into() {
                Ok(a) => a,
                Err(_) => continue,
            };

            let to_address = to_col.as_ref().and_then(|col| {
                if col.is_null(row) {
                    None
                } else {
                    col.value(row).try_into().ok()
                }
            });

            addresses.insert(
                tx_hash,
                TransactionAddresses {
                    from_address,
                    to_address,
                },
            );
        }
    }

    tracing::debug!(
        "Read {} transaction addresses from {}",
        addresses.len(),
        file_path.display()
    );

    addresses
}

// ─── CatchupPayload ──────────────────────────────────────────────────────────

/// Opaque payload carried inside a [`WorkItem`] for catchup execution.
pub(crate) struct CatchupPayload {
    pub handler: Arc<dyn TransformationHandler>,
    pub handler_key: String,
    pub handler_name: &'static str,
    pub handler_version: u32,
    pub triggers: Vec<(String, String)>,
    pub call_deps: Vec<(String, String)>,
    pub kind: HandlerKind,
}

// ─── CatchupLoader ───────────────────────────────────────────────────────────

/// Shared state needed to execute one [`WorkItem`] during catchup.
///
/// Constructed once per `run_handler_catchup` invocation and shared via `Arc`
/// across all spawned tasks inside [`DagScheduler::execute`].
pub(crate) struct CatchupLoader {
    pub decoded_logs_dir: PathBuf,
    pub decoded_calls_dir: PathBuf,
    pub raw_receipts_dir: PathBuf,
    pub historical_reader: Arc<HistoricalDataReader>,
    pub rpc_client: Arc<UnifiedRpcClient>,
    pub contracts: Arc<Contracts>,
    pub chain_name: String,
    pub chain_id: u64,
    pub db_pool: Arc<DbPool>,
    pub finalizer: Arc<RangeFinalizer>,
}

impl CatchupLoader {
    /// Load data, invoke handler, execute DB ops, and record progress for one [`WorkItem`].
    pub(crate) async fn run(&self, item: WorkItem) -> Result<(), TransformationError> {
        let range_start = item.range_start;
        let range_end = item.range_end;

        let payload = item
            .payload
            .downcast::<CatchupPayload>()
            .expect("CatchupPayload type mismatch");

        let handler_key = &payload.handler_key;

        let (events, calls) = match payload.kind {
            HandlerKind::Event => {
                let evts = self
                    .load_events(range_start, range_end, &payload.triggers)
                    .await?;
                let evts = filter_events_by_start_block(&self.contracts, evts);

                let calls = if !evts.is_empty() && !payload.call_deps.is_empty() {
                    let raw_calls = self
                        .load_calls(range_start, range_end, &payload.call_deps)
                        .await?;
                    filter_calls_by_start_block(&self.contracts, raw_calls)
                } else {
                    Vec::new()
                };

                (evts, calls)
            }
            HandlerKind::Call => {
                let raw_calls = self
                    .load_calls(range_start, range_end, &payload.triggers)
                    .await?;
                let calls = filter_calls_by_start_block(&self.contracts, raw_calls);
                (Vec::new(), calls)
            }
        };

        let has_data = match payload.kind {
            HandlerKind::Event => !events.is_empty(),
            HandlerKind::Call => !calls.is_empty(),
        };

        if !has_data {
            self.finalizer
                .record_completed_range_for_handler(handler_key, range_start, range_end)
                .await?;
            return Ok(());
        }

        let tx_addresses = match payload.kind {
            HandlerKind::Event => {
                read_receipt_addresses(&self.raw_receipts_dir, range_start, range_end)
            }
            HandlerKind::Call => HashMap::new(),
        };

        let ctx = TransformationContext::new(
            self.chain_name.clone(),
            self.chain_id,
            range_start,
            range_end,
            Arc::new(events),
            Arc::new(calls),
            tx_addresses,
            self.historical_reader.clone(),
            self.rpc_client.clone(),
            self.contracts.clone(),
        );

        let ops = payload.handler.handle(&ctx).await?;

        if !ops.is_empty() {
            let ops = inject_source_version(ops, payload.handler_name, payload.handler_version);
            self.db_pool.execute_transaction(ops).await?;
        }

        self.finalizer
            .record_completed_range_for_handler(handler_key, range_start, range_end)
            .await?;

        Ok(())
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    async fn load_events(
        &self,
        range_start: u64,
        range_end: u64,
        triggers: &[(String, String)],
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let logs_dir = self.decoded_logs_dir.clone();

        read_decoded_parquet(
            self.historical_reader.clone(),
            triggers,
            move |source, event| vec![logs_dir.join(source).join(event).join(&file_name)],
            |reader, path, src, evt| reader.read_events_from_file(path, src, evt),
        )
        .await
    }

    async fn load_calls(
        &self,
        range_start: u64,
        range_end: u64,
        triggers: &[(String, String)],
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
        let calls_dir = self.decoded_calls_dir.clone();

        read_decoded_parquet(
            self.historical_reader.clone(),
            triggers,
            move |source, function| {
                let base_dir = calls_dir.join(source).join(function);
                // Use first-match semantics: only the first existing path is returned,
                // matching the original behavior where base > on_events > once priority
                // prevents duplicate data when multiple paths exist.
                [
                    base_dir.join(&file_name),
                    base_dir.join("on_events").join(&file_name),
                    base_dir.join("once").join(&file_name),
                ]
                .into_iter()
                .find(|p| p.exists())
                .into_iter()
                .collect()
            },
            |reader, path, src, func| reader.read_calls_from_file(path, src, func),
        )
        .await
    }
}
