use std::future::Future;
use std::time::Instant;

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use url::Url;

use crate::rpc::RpcError;

/// RPC method names for metric labels
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum RpcMethod {
    GetBlockNumber,
    GetBlock,
    GetTransaction,
    GetTransactionReceipt,
    GetBlockReceipts,
    GetLogs,
    GetBalance,
    GetCode,
    EthCall,
    // Batch/concurrent variants
    GetBlocksBatch,
    GetTransactionReceiptsBatch,
    GetLogsBatch,
    EthCallBatch,
    GetBlockReceiptsConcurrent,
}

impl RpcMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GetBlockNumber => "eth_blockNumber",
            Self::GetBlock => "eth_getBlockByNumber",
            Self::GetTransaction => "eth_getTransactionByHash",
            Self::GetTransactionReceipt => "eth_getTransactionReceipt",
            Self::GetBlockReceipts => "eth_getBlockReceipts",
            Self::GetLogs => "eth_getLogs",
            Self::GetBalance => "eth_getBalance",
            Self::GetCode => "eth_getCode",
            Self::EthCall => "eth_call",
            Self::GetBlocksBatch => "eth_getBlockByNumber_batch",
            Self::GetTransactionReceiptsBatch => "eth_getTransactionReceipt_batch",
            Self::GetLogsBatch => "eth_getLogs_batch",
            Self::EthCallBatch => "eth_call_batch",
            Self::GetBlockReceiptsConcurrent => "eth_getBlockReceipts_concurrent",
        }
    }
}

/// Error categories for metric labels
#[derive(Debug, Clone, Copy)]
pub enum RpcErrorCategory {
    Transport,
    RateLimit,
    InvalidUrl,
    Batch,
    Provider,
}

impl RpcErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Transport => "transport",
            Self::RateLimit => "rate_limit",
            Self::InvalidUrl => "invalid_url",
            Self::Batch => "batch",
            Self::Provider => "provider",
        }
    }

    /// Categorize an RpcError into a metric label
    pub fn from_error(err: &RpcError) -> Self {
        match err {
            RpcError::Transport(_) => Self::Transport,
            RpcError::RateLimitExceeded => Self::RateLimit,
            RpcError::InvalidUrl(_) => Self::InvalidUrl,
            RpcError::BatchError(_) => Self::Batch,
            RpcError::ProviderError(_) => Self::Provider,
        }
    }
}

/// Register metric descriptions (call once at startup)
pub fn describe_rpc_metrics() {
    describe_histogram!(
        "rpc_request_duration_seconds",
        "Duration of RPC requests in seconds"
    );
    describe_counter!("rpc_requests_total", "Total number of RPC requests");
    describe_counter!("rpc_errors_total", "Total number of RPC errors");
    describe_gauge!(
        "rpc_requests_in_flight",
        "Number of RPC requests currently in progress"
    );
    describe_gauge!(
        "rpc_rate_limiter_cu_used",
        "Current compute units consumed in sliding window"
    );
    describe_gauge!(
        "rpc_rate_limiter_cu_capacity",
        "Max compute units for the window"
    );
    describe_histogram!(
        "rpc_rate_limit_wait_seconds",
        "Time waiting for rate limiter capacity"
    );
    describe_counter!("rpc_retry_attempts_total", "Each retry attempt");
    describe_counter!("rpc_retries_exhausted_total", "Retries fully exhausted");
    describe_gauge!("rpc_semaphore_acquired", "Currently held semaphore permits");
    describe_gauge!(
        "rpc_semaphore_capacity",
        "Total semaphore permits configured"
    );
    describe_histogram!("rpc_batch_size", "Items per batch execution");
    describe_gauge!(
        "receipt_pipeline_in_flight_fetches",
        "Number of receipt fetch tasks currently in flight"
    );
    describe_gauge!(
        "receipt_pipeline_pending_tx_hashes",
        "Number of transaction hashes pending in fallback batch buffer"
    );
}

/// Record time spent waiting for rate limiter capacity.
pub fn record_rate_limit_wait(chain: &str, duration_secs: f64) {
    histogram!(
        "rpc_rate_limit_wait_seconds",
        "chain" => chain.to_string()
    )
    .record(duration_secs);
}

/// Record a retry attempt with its outcome.
pub fn record_retry_attempt(chain: &str, operation: &str, outcome: &str) {
    counter!(
        "rpc_retry_attempts_total",
        "chain" => chain.to_string(),
        "operation" => operation.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

/// Record that retries were fully exhausted for an operation.
pub fn record_retries_exhausted(chain: &str, operation: &str) {
    counter!(
        "rpc_retries_exhausted_total",
        "chain" => chain.to_string(),
        "operation" => operation.to_string()
    )
    .increment(1);
}

/// Record the size of a batch execution.
pub fn record_batch_size(chain: &str, method: &str, size: usize) {
    histogram!(
        "rpc_batch_size",
        "chain" => chain.to_string(),
        "method" => method.to_string()
    )
    .record(size as f64);
}

/// Set current compute unit usage and capacity gauges.
pub fn set_cu_usage(chain: &str, current: f64, capacity: f64) {
    gauge!(
        "rpc_rate_limiter_cu_used",
        "chain" => chain.to_string()
    )
    .set(current);
    gauge!(
        "rpc_rate_limiter_cu_capacity",
        "chain" => chain.to_string()
    )
    .set(capacity);
}

/// Set semaphore utilization gauges.
pub fn set_semaphore_utilization(chain: &str, acquired: f64, capacity: f64) {
    gauge!(
        "rpc_semaphore_acquired",
        "chain" => chain.to_string()
    )
    .set(acquired);
    gauge!(
        "rpc_semaphore_capacity",
        "chain" => chain.to_string()
    )
    .set(capacity);
}

/// Set receipt collection pipeline gauges for in-flight work and buffered tx hashes.
pub fn set_receipt_pipeline_state(chain: &str, in_flight_fetches: usize, pending_tx_hashes: usize) {
    gauge!(
        "receipt_pipeline_in_flight_fetches",
        "chain" => chain.to_string()
    )
    .set(in_flight_fetches as f64);
    gauge!(
        "receipt_pipeline_pending_tx_hashes",
        "chain" => chain.to_string()
    )
    .set(pending_tx_hashes as f64);
}

/// Extract a sanitized chain identifier from an RPC URL.
///
/// Converts URLs like "https://eth-mainnet.g.alchemy.com/v2/xxx" to "alchemy_eth_mainnet"
/// or falls back to the host if parsing fails.
pub fn chain_label_from_url(url: &Url) -> String {
    let host = url.host_str().unwrap_or("unknown");

    // For Alchemy URLs, extract the network from the subdomain
    if host.contains("alchemy") {
        if let Some(subdomain) = host.split('.').next() {
            return format!("alchemy_{}", subdomain.replace('-', "_"));
        }
    }

    // For Infura URLs
    if host.contains("infura") {
        if let Some(subdomain) = host.split('.').next() {
            return format!("infura_{}", subdomain.replace('-', "_"));
        }
    }

    // For QuickNode or other providers, sanitize the hostname
    host.replace(['.', '-'], "_")
}

/// RAII guard for recording RPC request metrics.
///
/// Records duration and updates counters when dropped.
/// Use `success()` or `failure()` to record the outcome before dropping.
pub struct RpcMetricsGuard {
    method: &'static str,
    chain: String,
    start: Instant,
    completed: bool,
}

impl RpcMetricsGuard {
    pub fn new(method: RpcMethod, chain: &str) -> Self {
        let method_str = method.as_str();

        // Increment in-flight gauge
        gauge!(
            "rpc_requests_in_flight",
            "method" => method_str,
            "chain" => chain.to_string()
        )
        .increment(1.0);

        Self {
            method: method_str,
            chain: chain.to_string(),
            start: Instant::now(),
            completed: false,
        }
    }

    /// Record successful completion
    pub fn success(mut self) {
        self.completed = true;
        self.record_completion("success");
    }

    /// Record failure with error categorization
    pub fn failure(mut self, error: &RpcError) {
        self.completed = true;
        let error_category = RpcErrorCategory::from_error(error);

        counter!(
            "rpc_errors_total",
            "method" => self.method,
            "chain" => self.chain.clone(),
            "error_type" => error_category.as_str()
        )
        .increment(1);

        self.record_completion("error");
    }

    fn record_completion(&self, status: &'static str) {
        let duration = self.start.elapsed().as_secs_f64();

        // Record histogram
        histogram!(
            "rpc_request_duration_seconds",
            "method" => self.method,
            "chain" => self.chain.clone(),
            "status" => status
        )
        .record(duration);

        // Increment total counter
        counter!(
            "rpc_requests_total",
            "method" => self.method,
            "chain" => self.chain.clone(),
            "status" => status
        )
        .increment(1);

        // Decrement in-flight gauge
        gauge!(
            "rpc_requests_in_flight",
            "method" => self.method,
            "chain" => self.chain.clone()
        )
        .decrement(1.0);
    }
}

impl Drop for RpcMetricsGuard {
    fn drop(&mut self) {
        // If dropped without calling success/failure (e.g., panic), still decrement in-flight
        if !self.completed {
            gauge!(
                "rpc_requests_in_flight",
                "method" => self.method,
                "chain" => self.chain.clone()
            )
            .decrement(1.0);
        }
    }
}

/// Execute an async operation with metrics tracking.
///
/// This is a convenience wrapper that eliminates the repetitive pattern of:
/// 1. Creating an RpcMetricsGuard
/// 2. Executing the operation
/// 3. Calling guard.success() or guard.failure() based on result
///
/// # Example
/// ```ignore
/// let result = with_metrics(RpcMethod::GetBlock, &chain, || async {
///     // your async operation
/// }).await;
/// ```
pub async fn with_metrics<T, F, Fut>(
    method: RpcMethod,
    chain: &str,
    operation: F,
) -> Result<T, RpcError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, RpcError>>,
{
    let guard = RpcMetricsGuard::new(method, chain);
    let result = operation().await;
    match &result {
        Ok(_) => guard.success(),
        Err(e) => guard.failure(e),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_label_from_url_alchemy() {
        let url = Url::parse("https://eth-mainnet.g.alchemy.com/v2/abc123").unwrap();
        assert_eq!(chain_label_from_url(&url), "alchemy_eth_mainnet");

        let url = Url::parse("https://base-mainnet.g.alchemy.com/v2/abc123").unwrap();
        assert_eq!(chain_label_from_url(&url), "alchemy_base_mainnet");
    }

    #[test]
    fn test_chain_label_from_url_infura() {
        let url = Url::parse("https://mainnet.infura.io/v3/abc123").unwrap();
        assert_eq!(chain_label_from_url(&url), "infura_mainnet");
    }

    #[test]
    fn test_chain_label_from_url_other() {
        let url = Url::parse("https://rpc.ankr.com/eth").unwrap();
        assert_eq!(chain_label_from_url(&url), "rpc_ankr_com");
    }
}
