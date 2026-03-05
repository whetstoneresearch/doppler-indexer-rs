//! Storage configuration types for S3 and local caching.

use serde::Deserialize;

/// Top-level storage configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageConfig {
    /// S3 configuration (if not set, uses local-only mode)
    pub s3: Option<S3Config>,

    /// Local cache configuration
    #[serde(default)]
    pub cache: Option<CacheConfig>,

    /// Synchronization configuration for S3
    #[serde(default)]
    pub sync: Option<SyncConfig>,
}

/// S3-compatible storage configuration.
///
/// All credentials are read from environment variables for security.
#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    /// Environment variable name containing the S3 endpoint URL
    /// (e.g., "S3_ENDPOINT" -> reads from $S3_ENDPOINT)
    pub endpoint_env_var: String,

    /// Environment variable name containing the access key ID
    pub access_key_env_var: String,

    /// Environment variable name containing the secret access key
    pub secret_key_env_var: String,

    /// Environment variable name containing the bucket name
    pub bucket_env_var: String,

    /// AWS region (default: "us-east-1")
    #[serde(default = "default_region")]
    pub region: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// Local cache configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// Maximum cache size in gigabytes (default: 100)
    #[serde(default = "default_max_size_gb")]
    pub max_size_gb: u64,

    /// Prefixes that are pinned (never evicted from cache)
    /// Default: ["factories", "decoded"]
    #[serde(default = "default_pinned_prefixes")]
    pub pinned_prefixes: Vec<String>,

    /// Eviction threshold as a fraction of max_size_gb (default: 0.8)
    /// When cache exceeds this threshold, LRU eviction kicks in.
    #[serde(default = "default_eviction_threshold")]
    pub eviction_threshold: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_gb: default_max_size_gb(),
            pinned_prefixes: default_pinned_prefixes(),
            eviction_threshold: default_eviction_threshold(),
        }
    }
}

fn default_max_size_gb() -> u64 {
    100
}

fn default_pinned_prefixes() -> Vec<String> {
    vec!["factories".to_string(), "decoded".to_string()]
}

fn default_eviction_threshold() -> f64 {
    0.8
}

/// Synchronization configuration for S3.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    /// Number of recent ranges to check markers directly (vs relying on manifest)
    /// Used for freshness: manifest may be slightly stale, so recent ranges are checked
    /// directly via marker files.
    #[serde(default = "default_marker_freshness_ranges")]
    pub marker_freshness_ranges: u64,

    /// How often to refresh the cached manifest from S3 (in seconds)
    #[serde(default = "default_manifest_refresh_secs")]
    pub manifest_refresh_secs: u64,

    /// Retry interval for failed uploads (in seconds)
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,

    /// Maximum number of retry attempts before giving up
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            marker_freshness_ranges: default_marker_freshness_ranges(),
            manifest_refresh_secs: default_manifest_refresh_secs(),
            retry_interval_secs: default_retry_interval_secs(),
            max_retries: default_max_retries(),
        }
    }
}

fn default_marker_freshness_ranges() -> u64 {
    10
}

fn default_manifest_refresh_secs() -> u64 {
    60
}

fn default_retry_interval_secs() -> u64 {
    30
}

fn default_max_retries() -> u32 {
    10
}
