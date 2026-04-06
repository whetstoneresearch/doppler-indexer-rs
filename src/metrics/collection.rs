use std::path::Path;

use metrics::{counter, describe_counter, describe_histogram, histogram};

/// Register metric descriptions for block/receipt/log/eth_call collection
/// (call once at startup).
pub fn describe_collection_metrics() {
    describe_counter!(
        "collection_blocks_processed_total",
        "Total blocks processed during collection"
    );
    describe_histogram!(
        "collection_block_processing_duration_seconds",
        "Duration to process a range of blocks"
    );
    describe_histogram!(
        "collection_block_transactions",
        "Number of transactions per block"
    );
    describe_histogram!(
        "collection_parquet_write_duration_seconds",
        "Duration of parquet file writes"
    );
    describe_histogram!(
        "collection_parquet_file_size_bytes",
        "Size of written parquet files in bytes"
    );
    describe_counter!(
        "collection_eth_calls_total",
        "Total eth_call results collected"
    );
    describe_histogram!(
        "collection_eth_call_batch_duration_seconds",
        "Duration of eth_call RPC batch execution"
    );
    describe_counter!(
        "collection_receipts_processed_total",
        "Total receipts processed"
    );
}

/// Record metrics for a parquet file write: duration and file size.
pub fn record_parquet_write(chain: &str, data_type: &str, duration_secs: f64, file_path: &Path) {
    histogram!(
        "collection_parquet_write_duration_seconds",
        "chain" => chain.to_string(),
        "data_type" => data_type.to_string()
    )
    .record(duration_secs);

    if let Ok(metadata) = std::fs::metadata(file_path) {
        histogram!(
            "collection_parquet_file_size_bytes",
            "chain" => chain.to_string(),
            "data_type" => data_type.to_string()
        )
        .record(metadata.len() as f64);
    }
}
