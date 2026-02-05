//! Transformation engine that orchestrates handler execution.
//!
//! The engine receives decoded events and calls, invokes registered handlers,
//! and writes results to PostgreSQL. It tracks progress per handler and performs
//! per-handler catchup from decoded parquet files on startup.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;

use super::context::{DecodedCall, DecodedEvent, TransformationContext};
use super::error::TransformationError;
use super::historical::HistoricalDataReader;
use super::registry::{extract_event_name, TransformationRegistry};
use crate::db::{DbPool, DbValue, DbOperation};
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
///
/// Progress is tracked per handler (keyed by `handler_key()`) in the
/// `_handler_progress` table, enabling independent catchup and versioning.
pub struct TransformationEngine {
    registry: Arc<TransformationRegistry>,
    db_pool: Arc<DbPool>,
    rpc_client: Arc<UnifiedRpcClient>,
    historical_reader: Arc<HistoricalDataReader>,
    chain_name: String,
    chain_id: u64,
    mode: ExecutionMode,
    decoded_logs_dir: PathBuf,
    decoded_calls_dir: PathBuf,
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
        let decoded_calls_dir =
            PathBuf::from(format!("data/derived/{}/decoded/eth_calls", chain_name));

        Ok(Self {
            registry,
            db_pool,
            rpc_client,
            historical_reader,
            chain_name,
            chain_id,
            mode,
            decoded_logs_dir,
            decoded_calls_dir,
        })
    }

    /// Initialize the engine: run handler migrations, then initialize handlers.
    pub async fn initialize(&self) -> Result<(), TransformationError> {
        // Run handler-specified migrations first
        self.run_handler_migrations().await?;

        // Then run handler initialization
        for handler in self.registry.all_handlers() {
            tracing::debug!(
                "Initializing handler: {} ({})",
                handler.name(),
                handler.handler_key()
            );
            handler.initialize(&self.db_pool).await?;
        }
        Ok(())
    }

    // ─── Handler Migrations ──────────────────────────────────────────

    /// Run handler migration files by scanning each handler's `migration_folder()/v{version}/`.
    /// All `.sql` files in the version subfolder are run in alphabetical order.
    /// Each migration is tracked in the `_migrations` table with a `handlers/` prefix.
    async fn run_handler_migrations(&self) -> Result<(), TransformationError> {
        // Collect migration directories from all handlers (deduplicate by resolved path)
        let mut migration_dirs: Vec<(PathBuf, String)> = Vec::new();
        let mut seen_dirs = HashSet::new();

        for handler in self.registry.all_handlers() {
            if let Some(folder) = handler.migration_folder() {
                let version_dir =
                    PathBuf::from(format!("{}/v{}", folder, handler.version()));
                if seen_dirs.insert(version_dir.clone()) {
                    migration_dirs.push((version_dir, handler.handler_key()));
                }
            }
        }

        if migration_dirs.is_empty() {
            return Ok(());
        }

        // Check which migrations have already been applied
        let pool = self.db_pool.inner();
        let applied: HashSet<String> = {
            let client = pool.get().await.map_err(|e| {
                TransformationError::DatabaseError(crate::db::DbError::PoolError(e))
            })?;
            let rows = client
                .query("SELECT name FROM _migrations", &[])
                .await
                .map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
                })?;
            rows.iter().map(|r| r.get(0)).collect()
        };

        for (version_dir, handler_key) in &migration_dirs {
            if !version_dir.exists() {
                tracing::warn!(
                    "Handler {} migration directory does not exist: {}",
                    handler_key,
                    version_dir.display()
                );
                continue;
            }

            // Collect and sort .sql files in the version directory
            let mut sql_files: Vec<_> = std::fs::read_dir(version_dir)?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|x| x == "sql")
                        .unwrap_or(false)
                })
                .collect();
            sql_files.sort_by_key(|e| e.file_name());

            for entry in sql_files {
                let file_name = entry.file_name().to_string_lossy().to_string();
                // Track as "handlers/{version_dir_relative}/{filename}"
                let migration_name =
                    format!("handlers/{}", version_dir.join(&file_name).display());

                if applied.contains(&migration_name) {
                    continue;
                }

                let sql = std::fs::read_to_string(entry.path())?;

                let mut client = pool.get().await.map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::PoolError(e))
                })?;
                let tx = client.transaction().await.map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
                })?;

                tx.batch_execute(&sql).await.map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::MigrationError(
                        format!(
                            "Handler migration {} failed: {}",
                            migration_name, e
                        ),
                    ))
                })?;

                tx.execute(
                    "INSERT INTO _migrations (name) VALUES ($1)",
                    &[&migration_name],
                )
                .await
                .map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
                })?;

                tx.commit().await.map_err(|e| {
                    TransformationError::DatabaseError(crate::db::DbError::PostgresError(e))
                })?;

                tracing::info!("Applied handler migration: {}", migration_name);
            }
        }

        Ok(())
    }

    // ─── Per-Handler Progress Tracking ───────────────────────────────

    /// Get completed ranges for a specific handler from the database.
    async fn get_completed_ranges_for_handler(
        &self,
        handler_key: &str,
    ) -> Result<HashSet<u64>, TransformationError> {
        let rows = self
            .db_pool
            .query(
                "SELECT range_start FROM _handler_progress WHERE chain_id = $1 AND handler_key = $2",
                &[&(self.chain_id as i64), &handler_key.to_string()],
            )
            .await?;

        let mut completed = HashSet::new();
        for row in rows {
            let range_start: i64 = row.get(0);
            completed.insert(range_start as u64);
        }

        Ok(completed)
    }

    /// Record a completed range for a specific handler.
    async fn record_completed_range_for_handler(
        &self,
        handler_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<(), TransformationError> {
        self.db_pool
            .execute_transaction(vec![DbOperation::Upsert {
                table: "_handler_progress".to_string(),
                columns: vec![
                    "chain_id".to_string(),
                    "handler_key".to_string(),
                    "range_start".to_string(),
                    "range_end".to_string(),
                ],
                values: vec![
                    DbValue::Int64(self.chain_id as i64),
                    DbValue::Text(handler_key.to_string()),
                    DbValue::Int64(range_start as i64),
                    DbValue::Int64(range_end as i64),
                ],
                conflict_columns: vec![
                    "chain_id".to_string(),
                    "handler_key".to_string(),
                    "range_start".to_string(),
                ],
                update_columns: vec!["range_end".to_string()],
            }])
            .await?;

        Ok(())
    }

    // ─── Range Scanning ──────────────────────────────────────────────

    /// Scan decoded parquet files to find available ranges.
    fn scan_available_ranges(&self, base_dir: &PathBuf) -> Result<Vec<(u64, u64)>, TransformationError> {
        let mut ranges = HashSet::new();

        if !base_dir.exists() {
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

        scan_recursive(base_dir, &mut ranges);

        let mut sorted: Vec<_> = ranges.into_iter().collect();
        sorted.sort_by_key(|(start, _)| *start);

        Ok(sorted)
    }

    // ─── Per-Handler Catchup ─────────────────────────────────────────

    /// Run catchup phase: process decoded parquet files per handler.
    /// Each handler catches up independently based on its own progress.
    pub async fn run_catchup(&self) -> Result<(), TransformationError> {
        // Catch up event handlers
        self.run_event_handler_catchup().await?;

        // Catch up call handlers
        self.run_call_handler_catchup().await?;

        Ok(())
    }

    /// Run catchup for all event handlers.
    async fn run_event_handler_catchup(&self) -> Result<(), TransformationError> {
        let available = self.scan_available_ranges(&self.decoded_logs_dir)?;

        if available.is_empty() {
            tracing::info!(
                "Event handler catchup: no parquet ranges found for chain {}",
                self.chain_name
            );
            return Ok(());
        }

        for handler_info in self.registry.unique_event_handlers() {
            let handler = &handler_info.handler;
            let handler_key = handler.handler_key();
            let event_triggers: Vec<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|t| (t.source.clone(), extract_event_name(&t.event_signature)))
                .collect();

            let completed = self.get_completed_ranges_for_handler(&handler_key).await?;

            let to_process: Vec<_> = available
                .iter()
                .filter(|(start, _)| !completed.contains(start))
                .cloned()
                .collect();

            if to_process.is_empty() {
                tracing::info!("Handler {} catchup: already up to date", handler_key);
                continue;
            }

            tracing::info!(
                "Handler {} catchup: processing {} ranges (triggers: {:?})",
                handler_key,
                to_process.len(),
                event_triggers
            );

            let total = to_process.len();
            let mut processed = 0;
            let mut errored = false;

            for (range_start, range_end) in &to_process {
                let events = self.read_decoded_events_for_triggers(
                    *range_start,
                    *range_end,
                    &event_triggers,
                )?;

                processed += 1;

                if !events.is_empty() {
                    let ctx = TransformationContext::new(
                        &self.chain_name,
                        self.chain_id,
                        *range_start,
                        *range_end,
                        &events,
                        &[],
                        self.historical_reader.clone(),
                        self.rpc_client.clone(),
                    );

                    match handler.handle(&ctx).await {
                        Ok(ops) => {
                            if !ops.is_empty() {
                                self.db_pool.execute_transaction(ops).await?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Handler {} failed on range {}-{}: {}. Stopping catchup for this handler.",
                                handler_key, range_start, range_end, e
                            );
                            errored = true;
                            break;
                        }
                    }
                }

                // Record progress for this handler
                self.record_completed_range_for_handler(&handler_key, *range_start, *range_end)
                    .await?;

                if processed % 50 == 0 {
                    tracing::info!(
                        "Handler {} catchup progress: {}/{}",
                        handler_key,
                        processed,
                        total
                    );
                }
            }

            if !errored {
                tracing::info!(
                    "Handler {} catchup complete: processed {} ranges",
                    handler_key,
                    processed
                );
            }
        }

        Ok(())
    }

    /// Run catchup for all call handlers.
    async fn run_call_handler_catchup(&self) -> Result<(), TransformationError> {
        let available = self.scan_available_ranges(&self.decoded_calls_dir)?;

        if available.is_empty() {
            tracing::info!(
                "Call handler catchup: no parquet ranges found for chain {}",
                self.chain_name
            );
            return Ok(());
        }

        for handler_info in self.registry.unique_call_handlers() {
            let handler = &handler_info.handler;
            let handler_key = handler.handler_key();
            let call_triggers: Vec<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|t| (t.source.clone(), t.function_name.clone()))
                .collect();

            let completed = self.get_completed_ranges_for_handler(&handler_key).await?;

            let to_process: Vec<_> = available
                .iter()
                .filter(|(start, _)| !completed.contains(start))
                .cloned()
                .collect();

            if to_process.is_empty() {
                tracing::info!("Handler {} catchup: already up to date", handler_key);
                continue;
            }

            tracing::info!(
                "Handler {} catchup: processing {} ranges (triggers: {:?})",
                handler_key,
                to_process.len(),
                call_triggers
            );

            let total = to_process.len();
            let mut processed = 0;
            let mut errored = false;

            for (range_start, range_end) in &to_process {
                let calls = self.read_decoded_calls_for_triggers(
                    *range_start,
                    *range_end,
                    &call_triggers,
                )?;

                processed += 1;

                if !calls.is_empty() {
                    let ctx = TransformationContext::new(
                        &self.chain_name,
                        self.chain_id,
                        *range_start,
                        *range_end,
                        &[],
                        &calls,
                        self.historical_reader.clone(),
                        self.rpc_client.clone(),
                    );

                    match handler.handle(&ctx).await {
                        Ok(ops) => {
                            if !ops.is_empty() {
                                self.db_pool.execute_transaction(ops).await?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Handler {} failed on range {}-{}: {}. Stopping catchup for this handler.",
                                handler_key, range_start, range_end, e
                            );
                            errored = true;
                            break;
                        }
                    }
                }

                self.record_completed_range_for_handler(&handler_key, *range_start, *range_end)
                    .await?;

                if processed % 50 == 0 {
                    tracing::info!(
                        "Handler {} catchup progress: {}/{}",
                        handler_key,
                        processed,
                        total
                    );
                }
            }

            if !errored {
                tracing::info!(
                    "Handler {} catchup complete: processed {} ranges",
                    handler_key,
                    processed
                );
            }
        }

        Ok(())
    }

    // ─── Parquet Reading ─────────────────────────────────────────────

    /// Read decoded events from parquet files for specific triggers only.
    fn read_decoded_events_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        event_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedEvent>, TransformationError> {
        let mut all_events = Vec::new();
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

        for (source_name, event_name) in event_triggers {
            let file_path = self
                .decoded_logs_dir
                .join(source_name)
                .join(event_name)
                .join(&file_name);

            if !file_path.exists() {
                continue;
            }

            tracing::debug!("Reading decoded events from {}", file_path.display());

            match self
                .historical_reader
                .read_events_from_file(&file_path, source_name, event_name)
            {
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

    /// Read decoded calls from parquet files for specific triggers only.
    fn read_decoded_calls_for_triggers(
        &self,
        range_start: u64,
        range_end: u64,
        call_triggers: &[(String, String)],
    ) -> Result<Vec<DecodedCall>, TransformationError> {
        let mut all_calls = Vec::new();
        let file_name = format!("{}-{}.parquet", range_start, range_end - 1);

        for (source_name, function_name) in call_triggers {
            let file_path = self
                .decoded_calls_dir
                .join(source_name)
                .join(function_name)
                .join(&file_name);

            if !file_path.exists() {
                continue;
            }

            tracing::debug!("Reading decoded calls from {}", file_path.display());

            match self
                .historical_reader
                .read_calls_from_file(&file_path, source_name, function_name)
            {
                Ok(calls) => {
                    tracing::debug!(
                        "Read {} calls from {}",
                        calls.len(),
                        file_path.display()
                    );
                    all_calls.extend(calls);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to read decoded calls from {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }

        Ok(all_calls)
    }

    // ─── Live Processing ─────────────────────────────────────────────

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

                    // Process events with per-handler transactions and progress
                    self.process_range(
                        msg.range_start,
                        msg.range_end,
                        msg.events,
                        Vec::new(),
                    ).await?;
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

                    // Process calls with per-handler transactions and progress
                    self.process_range(
                        msg.range_start,
                        msg.range_end,
                        Vec::new(),
                        msg.calls,
                    ).await?;
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

    /// Process a block range with per-handler transactions.
    /// Each handler's operations execute in their own transaction and
    /// progress is recorded independently.
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

        // Get unique triggers from events
        let event_triggers: HashSet<_> = events
            .iter()
            .map(|e| (e.source_name.clone(), e.event_name.clone()))
            .collect();

        // Invoke each event handler independently with its own transaction
        for (source, event_name) in &event_triggers {
            for handler in self.registry.handlers_for_event(source, event_name) {
                let handler_key = handler.handler_key();

                tracing::debug!(
                    "Invoking handler {} for event {}/{}",
                    handler_key,
                    source,
                    event_name
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            tracing::debug!(
                                "Handler {} produced {} operations",
                                handler_key,
                                ops.len()
                            );

                            // Per-handler transaction
                            if let Err(e) = self.db_pool.execute_transaction(ops).await {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key,
                                    range_start,
                                    range_end,
                                    e
                                );
                                // Continue with other handlers
                                continue;
                            }
                        }

                        // Record per-handler progress
                        self.record_completed_range_for_handler(
                            &handler_key,
                            range_start,
                            range_end,
                        )
                        .await?;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for event {}/{}: {}",
                            handler_key,
                            source,
                            event_name,
                            e
                        );
                        // Continue with other handlers
                        continue;
                    }
                }
            }
        }

        // Get unique triggers from calls
        let call_triggers: HashSet<_> = calls
            .iter()
            .map(|c| (c.source_name.clone(), c.function_name.clone()))
            .collect();

        // Invoke each call handler independently with its own transaction
        for (source, function_name) in &call_triggers {
            for handler in self.registry.handlers_for_call(source, function_name) {
                let handler_key = handler.handler_key();

                tracing::trace!(
                    "Invoking handler {} for call {}/{}",
                    handler_key,
                    source,
                    function_name
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            tracing::trace!(
                                "Handler {} produced {} operations",
                                handler_key,
                                ops.len()
                            );

                            if let Err(e) = self.db_pool.execute_transaction(ops).await {
                                tracing::error!(
                                    "Handler {} transaction failed for range {}-{}: {:?}",
                                    handler_key,
                                    range_start,
                                    range_end,
                                    e
                                );
                                continue;
                            }
                        }

                        self.record_completed_range_for_handler(
                            &handler_key,
                            range_start,
                            range_end,
                        )
                        .await?;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Handler {} failed for call {}/{}: {}",
                            handler_key,
                            source,
                            function_name,
                            e
                        );
                        continue;
                    }
                }
            }
        }

        Ok(())
    }
}
