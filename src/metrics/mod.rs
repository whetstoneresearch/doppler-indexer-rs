mod live;
mod rpc;

pub use live::describe_live_metrics;
pub use rpc::{chain_label_from_url, describe_rpc_metrics, with_metrics, RpcMethod};

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
