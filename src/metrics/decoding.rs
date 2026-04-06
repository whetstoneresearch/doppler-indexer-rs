use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

/// Register metric descriptions for the decoding subsystem (call once at startup).
pub fn describe_decoding_metrics() {
    // Log decoding counters
    describe_counter!(
        "decode_logs_total",
        "Total raw logs processed by the decoder"
    );
    describe_counter!(
        "decode_logs_decoded_total",
        "Total successfully decoded log events"
    );
    describe_counter!(
        "decode_logs_unmatched_total",
        "Logs that matched no event matcher"
    );
    describe_histogram!(
        "decode_logs_duration_seconds",
        "Duration of log decode operations in seconds"
    );

    // Eth call decoding counters
    describe_counter!(
        "decode_eth_calls_success_total",
        "Total successful eth_call decodes"
    );
    describe_counter!(
        "decode_eth_calls_failure_total",
        "Total failed eth_call decodes"
    );
    describe_histogram!(
        "decode_eth_calls_duration_seconds",
        "Duration of eth_call decode operations in seconds"
    );

    // Event matcher gauge
    describe_gauge!(
        "decode_event_matchers",
        "Number of compiled event matchers"
    );

    // Parquet write duration
    describe_histogram!(
        "decode_parquet_write_duration_seconds",
        "Duration of decoded data parquet writes in seconds"
    );
}

/// Record log decoding metrics for a batch of logs.
pub fn record_log_decode_metrics(
    chain: &str,
    mode: &str,
    total_logs: u64,
    decoded_count: u64,
    duration: std::time::Duration,
) {
    let unmatched = total_logs.saturating_sub(decoded_count);

    counter!(
        "decode_logs_total",
        "chain" => chain.to_string(),
        "mode" => mode.to_string()
    )
    .increment(total_logs);

    counter!(
        "decode_logs_decoded_total",
        "chain" => chain.to_string(),
        "mode" => mode.to_string()
    )
    .increment(decoded_count);

    counter!(
        "decode_logs_unmatched_total",
        "chain" => chain.to_string(),
        "mode" => mode.to_string()
    )
    .increment(unmatched);

    histogram!(
        "decode_logs_duration_seconds",
        "chain" => chain.to_string(),
        "mode" => mode.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Record eth_call decoding metrics for a batch of results.
pub fn record_eth_call_decode_metrics(
    chain: &str,
    mode: &str,
    successes: u64,
    failures: u64,
    duration: std::time::Duration,
) {
    counter!(
        "decode_eth_calls_success_total",
        "chain" => chain.to_string(),
        "mode" => mode.to_string()
    )
    .increment(successes);

    counter!(
        "decode_eth_calls_failure_total",
        "chain" => chain.to_string(),
        "mode" => mode.to_string()
    )
    .increment(failures);

    histogram!(
        "decode_eth_calls_duration_seconds",
        "chain" => chain.to_string(),
        "mode" => mode.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Set the number of compiled event matchers.
pub fn set_event_matcher_counts(chain: &str, regular_count: usize, factory_count: usize) {
    gauge!(
        "decode_event_matchers",
        "chain" => chain.to_string(),
        "type" => "regular"
    )
    .set(regular_count as f64);

    gauge!(
        "decode_event_matchers",
        "chain" => chain.to_string(),
        "type" => "factory"
    )
    .set(factory_count as f64);
}

/// Record parquet write duration.
pub fn record_parquet_write_duration(chain: &str, kind: &str, duration: std::time::Duration) {
    histogram!(
        "decode_parquet_write_duration_seconds",
        "chain" => chain.to_string(),
        "kind" => kind.to_string()
    )
    .record(duration.as_secs_f64());
}
