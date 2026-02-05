//! Configuration for the transformation system.

use serde::Deserialize;

/// Configuration for the transformation system.
///
/// Transformations are enabled when this config is present AND there are
/// registered handlers. No explicit "enabled" field is needed.
#[derive(Debug, Clone, Deserialize)]
pub struct TransformationConfig {
    /// PostgreSQL connection string environment variable.
    #[serde(default = "default_database_url_env_var")]
    pub database_url_env_var: String,

    /// Execution mode configuration.
    #[serde(default)]
    pub mode: TransformationModeConfig,

    /// Number of concurrent handler executions.
    #[serde(default = "default_handler_concurrency")]
    pub handler_concurrency: usize,

    /// Maximum operations per transaction batch.
    #[serde(default = "default_batch_size")]
    pub max_batch_size: usize,
}

/// Execution mode configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TransformationModeConfig {
    /// Use batch mode for historical catchup (larger batches, more memory).
    #[serde(default = "default_batch_for_catchup")]
    pub batch_for_catchup: bool,

    /// Batch size in blocks for batch mode.
    #[serde(default = "default_catchup_batch_size")]
    pub catchup_batch_size: usize,
}

fn default_database_url_env_var() -> String {
    "DATABASE_URL".to_string()
}

fn default_handler_concurrency() -> usize {
    4
}

fn default_batch_size() -> usize {
    1000
}

fn default_batch_for_catchup() -> bool {
    true
}

fn default_catchup_batch_size() -> usize {
    10000
}

impl Default for TransformationConfig {
    fn default() -> Self {
        Self {
            database_url_env_var: default_database_url_env_var(),
            mode: TransformationModeConfig::default(),
            handler_concurrency: default_handler_concurrency(),
            max_batch_size: default_batch_size(),
        }
    }
}
