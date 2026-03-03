use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Address to bind the metrics HTTP server (e.g., "0.0.0.0:9090")
    pub addr: String,
}
