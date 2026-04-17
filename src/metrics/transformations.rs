//! Metrics for the transformation engine subsystem.
//!
//! Tracks handler execution duration, errors, event/call throughput,
//! retry outcomes, range finalization, and concurrency gauges.

use std::time::Instant;

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

/// Register metric descriptions (call once at startup).
pub fn describe_transformation_metrics() {
    describe_histogram!(
        "transformation_handler_duration_seconds",
        "Duration of individual handler executions in seconds"
    );
    describe_counter!(
        "transformation_handler_errors_total",
        "Total handler execution errors and rollbacks"
    );
    describe_counter!(
        "transformation_events_processed_total",
        "Total decoded events processed through the engine"
    );
    describe_counter!(
        "transformation_calls_processed_total",
        "Total decoded calls processed through the engine"
    );
    describe_counter!(
        "transformation_retry_attempts_total",
        "Total retry attempts per handler"
    );
    describe_histogram!(
        "transformation_range_finalization_duration_seconds",
        "Duration of range finalization in seconds"
    );
    describe_counter!(
        "transformation_catchup_ranges_completed_total",
        "Total ranges completed per handler during catchup"
    );
    describe_gauge!(
        "transformation_catchup_ranges_remaining",
        "Remaining work items for catchup (set at start of each pass)"
    );
    describe_gauge!(
        "transformation_catchup_blocks_remaining",
        "Estimated remaining blocks for catchup across all handlers"
    );
    describe_histogram!(
        "transformation_catchup_pass_duration_seconds",
        "Duration of a single catchup scheduler pass in seconds"
    );
    describe_gauge!(
        "transformation_pending_events",
        "Pending event batches waiting for dependencies"
    );
    describe_counter!(
        "transformation_handlers_completed_total",
        "Total handler executions completed (success or error)"
    );
    describe_gauge!(
        "transformation_handlers_in_flight",
        "Handler tasks currently executing"
    );
    describe_gauge!(
        "transformation_handler_completed_block",
        "Highest block number (range_end) completed by each handler, per chain"
    );
    describe_gauge!(
        "transformation_chain_head_block",
        "Latest block number seen by the transformation engine, per chain"
    );
}

/// Update the highest completed block gauge for a handler.
///
/// Call this after a handler successfully records progress for a range.
pub fn record_handler_completed_block(handler_key: &str, chain: &str, block_end: u64) {
    gauge!(
        "transformation_handler_completed_block",
        "handler_key" => handler_key.to_string(),
        "chain" => chain.to_string(),
    )
    .set(block_end as f64);
}

/// Update the chain head block gauge.
///
/// Call this when the engine receives or begins processing a new block range,
/// so the gauge reflects the latest block the engine is aware of.
pub fn record_chain_head_block(chain: &str, block: u64) {
    gauge!(
        "transformation_chain_head_block",
        "chain" => chain.to_string(),
    )
    .set(block as f64);
}

/// RAII guard for recording handler execution metrics.
///
/// Increments the in-flight gauge on creation and decrements it when the
/// guard is consumed via [`success`](Self::success) or
/// [`failure`](Self::failure), or when dropped without completion (e.g.
/// panic).
pub struct HandlerMetricsGuard {
    handler_key: String,
    chain: String,
    mode: &'static str,
    blocks: u64,
    start: Instant,
    completed: bool,
}

impl HandlerMetricsGuard {
    /// Create a new guard, incrementing `transformation_handlers_in_flight`.
    ///
    /// `blocks` is the number of blocks in this unit of work: for catchup
    /// ranges pass `range_end - range_start + 1`, for live pass `1`.
    pub fn new(handler_key: &str, chain: &str, mode: &'static str, blocks: u64) -> Self {
        gauge!(
            "transformation_handlers_in_flight",
            "handler_key" => handler_key.to_string(),
            "chain" => chain.to_string(),
            "mode" => mode,
        )
        .increment(1.0);

        Self {
            handler_key: handler_key.to_string(),
            chain: chain.to_string(),
            mode,
            blocks,
            start: Instant::now(),
            completed: false,
        }
    }

    /// Record successful completion.
    pub fn success(mut self) {
        self.completed = true;
        self.record_completion("success");
    }

    /// Record failure with an error type category.
    pub fn failure(mut self, error_type: &'static str) {
        self.completed = true;

        counter!(
            "transformation_handler_errors_total",
            "handler_key" => self.handler_key.clone(),
            "chain" => self.chain.clone(),
            "error_type" => error_type,
        )
        .increment(1);

        self.record_completion("error");
    }

    fn record_completion(&self, status: &'static str) {
        let duration = self.start.elapsed().as_secs_f64();

        histogram!(
            "transformation_handler_duration_seconds",
            "handler_key" => self.handler_key.clone(),
            "chain" => self.chain.clone(),
            "status" => status,
            "mode" => self.mode,
        )
        .record(duration);

        counter!(
            "transformation_handlers_completed_total",
            "handler_key" => self.handler_key.clone(),
            "chain" => self.chain.clone(),
            "status" => status,
            "mode" => self.mode,
        )
        .increment(self.blocks);

        gauge!(
            "transformation_handlers_in_flight",
            "handler_key" => self.handler_key.clone(),
            "chain" => self.chain.clone(),
            "mode" => self.mode,
        )
        .decrement(1.0);
    }
}

impl Drop for HandlerMetricsGuard {
    fn drop(&mut self) {
        if !self.completed {
            gauge!(
                "transformation_handlers_in_flight",
                "handler_key" => self.handler_key.clone(),
                "chain" => self.chain.clone(),
                "mode" => self.mode,
            )
            .decrement(1.0);
        }
    }
}
