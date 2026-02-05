//! Transformation engine that orchestrates handler execution.
//!
//! The engine receives decoded events and calls, invokes registered handlers,
//! and writes results to PostgreSQL. It tracks its own progress and performs
//! catchup from decoded parquet files on startup.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;

use super::context::{DecodedCall, DecodedEvent, TransformationContext};
use super::error::TransformationError;
use super::historical::HistoricalDataReader;
use super::registry::TransformationRegistry;
use crate::db::{DbOperation, DbPool, DbValue};
use crate::rpc::UnifiedRpcClient;

/// Message containing decoded events for a block range.
#[derive(Debug)]
pub struct DecodedEventsMessage {
    pub range_start: u64,
    pub range_end: u64,
    pub source_name: String,
    pub event_name: String,
    pub events: Vec<DecodedEvent>,
}

/// Message containing decoded call results for a block range.
#[derive(Debug)]
pub struct DecodedCallsMessage {
    pub range_start: u64,
    pub range_end: u64,
    pub source_name: String,
    pub function_name: String,
    pub calls: Vec<DecodedCall>,
}

/// Signal that all decoding for a range is complete.
#[derive(Debug)]
pub struct RangeCompleteMessage {
    pub range_start: u64,
    pub range_end: u64,
}

/// Execution mode for the transformation engine.
#[derive(Debug, Clone, Copy)]
pub enum ExecutionMode {
    /// Process data as it arrives (for live/real-time data).
    Streaming,
    /// Process in larger batches (for historical catchup).
    Batch { batch_size: usize },
}

impl Default for ExecutionMode {
    fn default() -> Self {
        Self::Streaming
    }
}

/// The transformation engine processes decoded data and invokes handlers.
pub struct TransformationEngine {
    registry: Arc<TransformationRegistry>,
    db_pool: Arc<DbPool>,
    rpc_client: Arc<UnifiedRpcClient>,
    historical_reader: Arc<HistoricalDataReader>,
    chain_name: String,
    chain_id: u64,
    mode: ExecutionMode,
    decoded_logs_dir: PathBuf,
}

impl TransformationEngine {
    /// Create a new transformation engine.
    pub async fn new(
        registry: Arc<TransformationRegistry>,
        db_pool: Arc<DbPool>,
        rpc_client: Arc<UnifiedRpcClient>,
        chain_name: String,
        chain_id: u64,
        mode: ExecutionMode,
    ) -> Result<Self, TransformationError> {
        let historical_reader = Arc::new(HistoricalDataReader::new(&chain_name)?);
        let decoded_logs_dir = PathBuf::from(format!("data/derived/{}/decoded/logs", chain_name));

        Ok(Self {
            registry,
            db_pool,
            rpc_client,
            historical_reader,
            chain_name,
            chain_id,
            mode,
            decoded_logs_dir,
        })
    }

    /// Initialize all handlers.
    pub async fn initialize(&self) -> Result<(), TransformationError> {
        for handler in self.registry.all_handlers() {
            tracing::debug!("Initializing handler: {}", handler.name());
            handler.initialize(&self.db_pool).await?;
        }
        Ok(())
    }

    /// Get completed ranges from the database.
    async fn get_completed_ranges(&self) -> Result<HashSet<u64>, TransformationError> {
        let rows = self
            .db_pool
            .query(
                "SELECT range_start FROM _transformation_progress WHERE chain_id = $1",
                &[&(self.chain_id as i64)],
            )
            .await?;

        let mut completed = HashSet::new();
        for row in rows {
            let range_start: i64 = row.get(0);
            completed.insert(range_start as u64);
        }

        Ok(completed)
    }

    /// Record a completed range in the database.
    async fn record_completed_range(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> Result<(), TransformationError> {
        self.db_pool
            .execute_transaction(vec![DbOperation::Upsert {
                table: "_transformation_progress".to_string(),
                columns: vec![
                    "chain_id".to_string(),
                    "range_start".to_string(),
                    "range_end".to_string(),
                ],
                values: vec![
                    DbValue::Int64(self.chain_id as i64),
                    DbValue::Int64(range_start as i64),
                    DbValue::Int64(range_end as i64),
                ],
                conflict_columns: vec!["chain_id".to_string(), "range_start".to_string()],
                update_columns: vec!["range_end".to_string()],
            }])
            .await?;

        Ok(())
    }

    /// Scan decoded parquet files to find available ranges.
    fn scan_available_ranges(&self) -> Result<Vec<(u64, u64)>, TransformationError> {
        let mut ranges = HashSet::new();

        // Scan all subdirectories for parquet files
        if !self.decoded_logs_dir.exists() {
            return Ok(Vec::new());
        }

        fn scan_recursive(dir: &std::path::Path, ranges: &mut HashSet<(u64, u64)>) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        scan_recursive(&path, ranges);
                    } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        // Parse range from filename: 28990000-28999999.parquet
                        if let Some(file_name) = path.file_stem().and_then(|s| s.to_str()) {
                            let parts: Vec<&str> = file_name.split('-').collect();
                            if parts.len() == 2 {
                                if let (Ok(start), Ok(end)) =
                                    (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                                {
                                    ranges.insert((start, end + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        scan_recursive(&self.decoded_logs_dir, &mut ranges);

        let mut sorted: Vec<_> = ranges.into_iter().collect();
        sorted.sort_by_key(|(start, _)| *start);

        Ok(sorted)
    }

    /// Run catchup phase: process decoded parquet files that haven't been transformed yet.
    pub async fn run_catchup(&self) -> Result<(), TransformationError> {
        let completed = self.get_completed_ranges().await?;
        let available = self.scan_available_ranges()?;

        let to_process: Vec<_> = available
            .into_iter()
            .filter(|(start, _)| !completed.contains(start))
            .collect();

        if to_process.is_empty() {
            tracing::info!(
                "Transformation catchup: no new ranges to process for chain {}",
                self.chain_name
            );
            return Ok(());
        }

        let event_triggers = self.registry.all_event_triggers();
        tracing::info!(
            "Transformation catchup: processing {} ranges for chain {} (looking for {} event types: {:?})",
            to_process.len(),
            self.chain_name,
            event_triggers.len(),
            event_triggers
        );

        let total = to_process.len();
        let mut processed = 0;
        let mut with_events = 0;
        let mut total_events = 0;

        for (range_start, range_end) in to_process {
            // Read decoded events from parquet files for this range
            let events = self.read_decoded_events_for_range(range_start, range_end)?;

            processed += 1;

            if events.is_empty() {
                // No events for handlers we care about, but still mark as complete
                self.record_completed_range(range_start, range_end).await?;
            } else {
                with_events += 1;
                total_events += events.len();

                tracing::info!(
                    "Transformation catchup: processing range {}-{} with {} events ({}/{})",
                    range_start,
                    range_end - 1,
                    events.len(),
                    processed,
                    total
                );

                // Process the range
                self.process_range(range_start, range_end, events, Vec::new())
                    .await?;

                // Record completion
                self.record_completed_range(range_start, range_end).await?;
            }

            // Log progress every 50 ranges
            if processed % 50 == 0 {
                tracing::info!(
                    "Transformation catchup progress: {}/{} ranges processed ({} with events, {} total events)",
                    processed,
                    total,
                    with_events,
                    total_events
                );
            }
        }

        tracing::info!(
            "Transformation catchup complete for chain {}: processed {} ranges, {} had events ({} total events)",
            self.chain_name,
            processed,
            with_events,
            total_events
        );

        Ok(())
    }

    /// Read decoded events from parquet files for a specific range.
    fn read_decoded_events_for_range(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let mut all_events = Vec::new();
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

        // Get event triggers we care about from the registry
        let event_triggers = self.registry.all_event_triggers();

        for (source_name, event_name) in event_triggers {
            let file_path = self
                .decoded_logs_dir
                .join(&source_name)
                .join(&event_name)
                .join(&file_name);

            if !file_path.exists() {
                continue;
            }

            tracing::debug!(
                "Reading decoded events from {}",
                file_path.display()
            );

            match self.historical_reader.read_events_from_file(
                &file_path,
                &source_name,
                &event_name,
            ) {
                Ok(events) => {
                    tracing::debug!(
                        "Read {} events from {}",
                        events.len(),
                        file_path.display()
                    );
                    all_events.extend(events);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to read decoded events from {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }

        Ok(all_events)
    }

    /// Run the transformation engine, processing messages from channels.
    ///
    /// This is spawned as a task similar to decode_logs and decode_eth_calls.
    /// Events and calls are processed immediately as they arrive.
    pub async fn run(
        &self,
        mut events_rx: Receiver<DecodedEventsMessage>,
        mut calls_rx: Receiver<DecodedCallsMessage>,
        mut _complete_rx: Receiver<RangeCompleteMessage>,
    ) -> Result<(), TransformationError> {
        // Run catchup first
        self.run_catchup().await?;

        tracing::info!(
            "Transformation engine started for chain {} in {:?} mode",
            self.chain_name,
            self.mode
        );

        loop {
            tokio::select! {
                biased;

                Some(msg) = events_rx.recv() => {
                    if msg.events.is_empty() {
                        continue;
                    }

                    tracing::debug!(
                        "Processing {} events for {}/{} in range {}-{}",
                        msg.events.len(),
                        msg.source_name,
                        msg.event_name,
                        msg.range_start,
                        msg.range_end
                    );

                    // Process events immediately
                    self.process_range(
                        msg.range_start,
                        msg.range_end,
                        msg.events,
                        Vec::new(),
                    ).await?;

                    // Record completion for live data
                    self.record_completed_range(msg.range_start, msg.range_end).await?;
                }

                Some(msg) = calls_rx.recv() => {
                    if msg.calls.is_empty() {
                        continue;
                    }

                    tracing::debug!(
                        "Processing {} calls for {}/{} in range {}-{}",
                        msg.calls.len(),
                        msg.source_name,
                        msg.function_name,
                        msg.range_start,
                        msg.range_end
                    );

                    // Process calls immediately
                    self.process_range(
                        msg.range_start,
                        msg.range_end,
                        Vec::new(),
                        msg.calls,
                    ).await?;

                    // Record completion for live data
                    self.record_completed_range(msg.range_start, msg.range_end).await?;
                }

                else => {
                    // All channels closed
                    tracing::info!("All channels closed, transformation engine shutting down");
                    break;
                }
            }
        }

        tracing::info!(
            "Transformation engine completed for chain {}",
            self.chain_name
        );
        Ok(())
    }

    /// Process a block range when all data is ready.
    async fn process_range(
        &self,
        range_start: u64,
        range_end: u64,
        events: Vec<DecodedEvent>,
        calls: Vec<DecodedCall>,
    ) -> Result<(), TransformationError> {
        tracing::debug!(
            "Processing range {}-{} with {} events and {} calls",
            range_start,
            range_end,
            events.len(),
            calls.len()
        );

        // Create context for handlers
        let ctx = TransformationContext::new(
            &self.chain_name,
            self.chain_id,
            range_start,
            range_end,
            &events,
            &calls,
            self.historical_reader.clone(),
            self.rpc_client.clone(),
        );

        // Collect all database operations from all handlers
        let mut all_ops = Vec::new();

        // Get unique triggers from events
        let event_triggers: HashSet<_> = events
            .iter()
            .map(|e| (e.source_name.clone(), e.event_name.clone()))
            .collect();

        // Invoke event handlers
        for (source, event_name) in event_triggers {
            for handler in self.registry.handlers_for_event(&source, &event_name) {
                tracing::debug!(
                    "Invoking handler {} for event {}/{}",
                    handler.name(),
                    source,
                    event_name
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        tracing::debug!(
                            "Handler {} produced {} operations",
                            handler.name(),
                            ops.len()
                        );
                        all_ops.extend(ops);
                    }
                    Err(e) => {
                        // Halt on error as per requirements
                        tracing::error!(
                            "Handler {} failed for event {}/{}: {}",
                            handler.name(),
                            source,
                            event_name,
                            e
                        );
                        return Err(TransformationError::HandlerError {
                            handler_name: handler.name().to_string(),
                            message: e.to_string(),
                        });
                    }
                }
            }
        }

        // Get unique triggers from calls
        let call_triggers: HashSet<_> = calls
            .iter()
            .map(|c| (c.source_name.clone(), c.function_name.clone()))
            .collect();

        // Invoke call handlers
        for (source, function_name) in call_triggers {
            for handler in self.registry.handlers_for_call(&source, &function_name) {
                tracing::trace!(
                    "Invoking handler {} for call {}/{}",
                    handler.name(),
                    source,
                    function_name
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        tracing::trace!(
                            "Handler {} produced {} operations",
                            handler.name(),
                            ops.len()
                        );
                        all_ops.extend(ops);
                    }
                    Err(e) => {
                        return Err(TransformationError::HandlerError {
                            handler_name: handler.name().to_string(),
                            message: e.to_string(),
                        });
                    }
                }
            }
        }

        // Execute all operations in a single transaction (per-block atomicity)
        if !all_ops.is_empty() {
            tracing::info!(
                "Executing {} database operations for range {}-{}",
                all_ops.len(),
                range_start,
                range_end
            );

            match self.db_pool.execute_transaction(all_ops).await {
                Ok(()) => {
                    tracing::debug!("Database transaction committed for range {}-{}", range_start, range_end);
                }
                Err(e) => {
                    tracing::error!("Database transaction failed for range {}-{}: {:?}", range_start, range_end, e);
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}
