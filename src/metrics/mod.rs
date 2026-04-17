mod collection;
mod db;
mod rpc;
mod transformations;

pub use collection::{describe_collection_metrics, record_parquet_write};
pub use db::{describe_db_metrics, sample_pool_stats};
pub use rpc::{
    chain_label_from_url, describe_rpc_metrics, record_batch_size, record_rate_limit_wait,
    record_retries_exhausted, record_retry_attempt, set_cu_usage, set_receipt_pipeline_state,
    set_semaphore_utilization, with_metrics, RpcMethod,
};
pub use transformations::{
    describe_transformation_metrics, record_chain_head_block, record_handler_completed_block,
    HandlerMetricsGuard,
};

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;

/// Initialize the metrics exporter and HTTP server.
///
/// Call this once at application startup before any metrics are recorded.
/// The server exposes metrics at the `/metrics` endpoint.
pub fn init_metrics_server(addr: SocketAddr) {
    let builder = PrometheusBuilder::new().with_http_listener(addr);

    builder
        .install()
        .expect("failed to install Prometheus exporter");

    tracing::info!("Metrics server listening on http://{}/metrics", addr);
}
