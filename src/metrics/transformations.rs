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
    describe_gauge!(
        "transformation_pending_events",
        "Pending event batches waiting for dependencies"
    );
    describe_gauge!(
        "transformation_handlers_in_flight",
        "Handler tasks currently executing"
    );
}

/// RAII guard for recording handler execution metrics.
///
/// Increments the in-flight gauge on creation and decrements it when the
/// guard is consumed via [`success`](Self::success) or
/// [`failure`](Self::failure), or when dropped without completion (e.g.
/// panic).
pub struct HandlerMetricsGuard {
    handler_key: String,
    mode: &'static str,
    start: Instant,
    completed: bool,
}

impl HandlerMetricsGuard {
    /// Create a new guard, incrementing `transformation_handlers_in_flight`.
    pub fn new(handler_key: &str, mode: &'static str) -> Self {
        gauge!(
            "transformation_handlers_in_flight",
            "handler_key" => handler_key.to_string(),
        )
        .increment(1.0);

        Self {
            handler_key: handler_key.to_string(),
            mode,
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
            "status" => status,
            "mode" => self.mode,
        )
        .record(duration);

        gauge!(
            "transformation_handlers_in_flight",
            "handler_key" => self.handler_key.clone(),
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
            )
            .decrement(1.0);
        }
    }
}
