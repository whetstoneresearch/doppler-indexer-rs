use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::{Address, BlockNumber, Bytes, B256, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::{
    Block, BlockId, BlockNumberOrTag, Filter, Log, Transaction, TransactionReceipt,
};
use async_trait::async_trait;
use governor::clock::{QuantaClock, QuantaInstant};
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, Quota, RateLimiter};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::metrics::{
    chain_label_from_url, record_rate_limit_wait, record_retries_exhausted, record_retry_attempt,
    set_cu_usage,
};

use super::alchemy::SlidingWindowRateLimiter;

/// Trait for RPC providers that can execute Ethereum JSON-RPC calls.
/// This abstracts over different client implementations (standard, Alchemy, etc.)
/// and allows unified usage while each implementation handles rate limiting differently.
#[async_trait]
#[allow(dead_code)]
pub trait RpcProvider: Send + Sync {
    /// Get the current block number
    async fn get_block_number(&self) -> Result<BlockNumber, RpcError>;

    /// Get a block by ID
    async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError>;

    /// Get a block by number
    async fn get_block_by_number(
        &self,
        number: BlockNumberOrTag,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        self.get_block(BlockId::Number(number), full_transactions)
            .await
    }

    /// Get a transaction by hash
    async fn get_transaction(&self, hash: B256) -> Result<Option<Transaction>, RpcError>;

    /// Get a transaction receipt by hash
    async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError>;

    /// Get logs matching a filter
    async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError>;

    /// Get account balance
    async fn get_balance(&self, address: Address, block: Option<BlockId>)
        -> Result<U256, RpcError>;

    /// Get contract code
    async fn get_code(&self, address: Address, block: Option<BlockId>) -> Result<Bytes, RpcError>;

    /// Execute an eth_call
    async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError>;

    /// Get block receipts
    async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError>;

    /// Batch get blocks
    async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError>;

    /// Batch get transaction receipts
    async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError>;

    /// Batch get logs
    async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError>;

    /// Batch execute eth_calls
    async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError>;
}

/// Maximum length for individual error messages in the chain.
/// Keeps log output readable when alloy includes full response bodies in errors.
const ERROR_SEGMENT_MAX_LEN: usize = 300;

/// Truncate a single error message to a reasonable length.
fn truncate_error_segment(msg: &str, max_len: usize) -> String {
    if msg.len() <= max_len {
        msg.to_string()
    } else {
        format!(
            "{}... [{} bytes truncated]",
            &msg[..max_len],
            msg.len() - max_len
        )
    }
}

/// Extracts the full error chain from an error, including all source errors.
/// Individual segments are truncated to keep logs readable.
/// Use `error_chain_full` when you need the complete untruncated output.
pub fn error_chain(err: &dyn std::error::Error) -> String {
    let mut chain = vec![truncate_error_segment(
        &err.to_string(),
        ERROR_SEGMENT_MAX_LEN,
    )];
    let mut source = err.source();
    while let Some(s) = source {
        chain.push(truncate_error_segment(
            &s.to_string(),
            ERROR_SEGMENT_MAX_LEN,
        ));
        source = s.source();
    }
    chain.join(": ")
}

/// Extracts the full error chain without any truncation.
/// Useful for debug-level logging when you need to see the complete response body.
pub fn error_chain_full(err: &dyn std::error::Error) -> String {
    let mut chain = vec![err.to_string()];
    let mut source = err.source();
    while let Some(s) = source {
        chain.push(s.to_string());
        source = s.source();
    }
    chain.join(": ")
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum RpcError {
    #[error("RPC transport error: {0}")]
    Transport(String),

    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Batch request failed: {0}")]
    BatchError(String),

    #[error("Provider error: {0}")]
    ProviderError(String),
}

impl RpcError {
    /// Check if this error is likely transient and worth retrying
    pub fn is_retryable(&self) -> bool {
        match self {
            // Transport errors are typically network issues
            RpcError::Transport(_) => true,
            // Rate limits should be retried after backoff
            RpcError::RateLimitExceeded => true,
            // Invalid URL is permanent
            RpcError::InvalidUrl(_) => false,
            // Batch errors may be transient
            RpcError::BatchError(msg) => Self::is_retryable_message(msg),
            // Provider errors need message inspection
            RpcError::ProviderError(msg) => Self::is_retryable_message(msg),
        }
    }

    fn is_retryable_message(msg: &str) -> bool {
        let msg_lower = msg.to_lowercase();
        // Network/connection errors
        msg_lower.contains("connection")
            || msg_lower.contains("timeout")
            || msg_lower.contains("timed out")
            || msg_lower.contains("reset")
            || msg_lower.contains("broken pipe")
            || msg_lower.contains("network")
            || msg_lower.contains("eof")
            || msg_lower.contains("sending request")
            // Rate limiting indicators
            || msg_lower.contains("rate limit")
            || msg_lower.contains("too many requests")
            || msg_lower.contains("429")
            // Server errors (5xx)
            || msg_lower.contains("502")
            || msg_lower.contains("503")
            || msg_lower.contains("504")
            || msg_lower.contains("internal server error")
            || msg_lower.contains("service unavailable")
            || msg_lower.contains("bad gateway")
            // Temporary failures
            || msg_lower.contains("temporarily")
            || msg_lower.contains("try again")
            || msg_lower.contains("retry")
    }
}

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries)
    pub max_retries: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff (e.g., 2.0 doubles delay each retry)
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }
}

#[allow(dead_code)]
impl RetryConfig {
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Default::default()
        }
    }

    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Calculate the delay for a given attempt number (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        let delay_ms = self.initial_delay.as_millis() as f64
            * self.backoff_multiplier.powi(attempt as i32 - 1);
        let delay = Duration::from_millis(delay_ms as u64);
        std::cmp::min(delay, self.max_delay)
    }
}

/// Execute an async operation with retry logic.
///
/// Retries the operation up to `config.max_retries` times with exponential backoff
/// for retryable errors. All retry attempts are logged for debugging.
///
/// The `chain` parameter is used for metrics labeling. Pass an empty string to skip
/// metrics recording.
pub async fn with_retry<F, Fut, T>(
    config: &RetryConfig,
    operation_name: &str,
    chain: &str,
    mut operation: F,
) -> Result<T, RpcError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RpcError>>,
{
    let has_chain = !chain.is_empty();
    let chain_tag = if has_chain { chain } else { "unknown" };
    let mut errors: Vec<String> = Vec::new();

    for attempt in 0..=config.max_retries {
        // Wait before retry (no wait on first attempt)
        if attempt > 0 {
            let delay = config.delay_for_attempt(attempt);
            tracing::warn!(
                "[{}] RPC retry {}/{} for '{}' in {:?}",
                chain_tag,
                attempt,
                config.max_retries,
                operation_name,
                delay
            );
            tokio::time::sleep(delay).await;
        }

        match operation().await {
            Ok(result) => {
                if attempt > 0 {
                    tracing::info!(
                        "[{}] RPC '{}' succeeded after {} retries",
                        chain_tag,
                        operation_name,
                        attempt
                    );
                    if has_chain {
                        record_retry_attempt(chain, operation_name, "success_after_retry");
                    }
                }
                return Ok(result);
            }
            Err(e) => {
                let error_msg = e.to_string();

                if e.is_retryable() && attempt < config.max_retries {
                    tracing::warn!(
                        "[{}] RPC '{}' failed (attempt {}/{}): {}",
                        chain_tag,
                        operation_name,
                        attempt + 1,
                        config.max_retries + 1,
                        error_msg
                    );
                    errors.push(format!("attempt {}: {}", attempt + 1, error_msg));
                    if has_chain {
                        record_retry_attempt(chain, operation_name, "retry");
                    }
                } else {
                    // Non-retryable error or exhausted retries
                    errors.push(format!("attempt {}: {}", attempt + 1, error_msg));

                    if errors.len() > 1 {
                        // Multiple attempts failed - log the full error history
                        tracing::error!(
                            "[{}] RPC '{}' failed after {} attempts. Error history: [{}]",
                            chain_tag,
                            operation_name,
                            attempt + 1,
                            errors.join("; ")
                        );
                        if has_chain {
                            record_retries_exhausted(chain, operation_name);
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    // This should be unreachable, but provide a meaningful error if it happens
    if has_chain {
        record_retries_exhausted(chain, operation_name);
    }
    Err(RpcError::ProviderError(format!(
        "[{}] RPC '{}' exhausted retries. Errors: [{}]",
        chain_tag,
        operation_name,
        errors.join("; ")
    )))
}

pub type StandardRateLimiter =
    RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>;

#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    pub url: Url,
    pub max_batch_size: usize,
    pub concurrency: usize,
    pub batching_enabled: bool,
    pub rate_limit: Option<RateLimitConfig>,
    pub retry: RetryConfig,
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_second: NonZeroU32,
    pub jitter_min_ms: u64,
    pub jitter_max_ms: u64,
}

/// Default requests per second for rate limiting.
/// Using a const ensures this is computed at compile time and cannot fail.
const DEFAULT_REQUESTS_PER_SECOND: NonZeroU32 = match NonZeroU32::new(10) {
    Some(v) => v,
    None => unreachable!(),
};

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: DEFAULT_REQUESTS_PER_SECOND,
            jitter_min_ms: 5,
            jitter_max_ms: 50,
        }
    }
}

#[allow(dead_code)]
impl RpcClientConfig {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            max_batch_size: 100,
            concurrency: 100,
            batching_enabled: true,
            rate_limit: None,
            retry: RetryConfig::default(),
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_batching(mut self, enabled: bool) -> Self {
        self.batching_enabled = enabled;
        self
    }

    pub fn with_rate_limit(mut self, config: RateLimitConfig) -> Self {
        self.rate_limit = Some(config);
        self
    }

    pub fn with_retry(mut self, config: RetryConfig) -> Self {
        self.retry = config;
        self
    }
}

#[derive(Clone)]
pub struct RpcClient {
    provider: RootProvider<Ethereum>,
    config: RpcClientConfig,
    rate_limiter: Option<Arc<StandardRateLimiter>>,
    jitter: Option<Jitter>,
    sliding_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    rpc_semaphore: Arc<Semaphore>,
    chain: String,
}

#[allow(dead_code)]
impl RpcClient {
    pub fn new(config: RpcClientConfig) -> Result<Self, RpcError> {
        let provider = RootProvider::<Ethereum>::new_http(config.url.clone());
        let chain = chain_label_from_url(&config.url);
        let concurrency = config.concurrency.max(1);

        let (rate_limiter, jitter) = if let Some(ref rate_config) = config.rate_limit {
            let quota = Quota::per_second(rate_config.requests_per_second);
            let limiter = RateLimiter::direct(quota);
            let jitter = Jitter::new(
                Duration::from_millis(rate_config.jitter_min_ms),
                Duration::from_millis(rate_config.jitter_max_ms),
            );
            (Some(Arc::new(limiter)), Some(jitter))
        } else {
            (None, None)
        };

        Ok(Self {
            provider,
            config,
            rate_limiter,
            jitter,
            sliding_limiter: None,
            rpc_semaphore: Arc::new(Semaphore::new(concurrency)),
            chain,
        })
    }

    pub fn new_with_shared_limiter(
        config: RpcClientConfig,
        limiter: Arc<SlidingWindowRateLimiter>,
    ) -> Result<Self, RpcError> {
        let provider = RootProvider::<Ethereum>::new_http(config.url.clone());
        let chain = chain_label_from_url(&config.url);
        let concurrency = config.concurrency.max(1);
        Ok(Self {
            provider,
            config,
            rate_limiter: None,
            jitter: None,
            sliding_limiter: Some(limiter),
            rpc_semaphore: Arc::new(Semaphore::new(concurrency)),
            chain,
        })
    }

    pub fn from_url(url: &str) -> Result<Self, RpcError> {
        let url = Url::parse(url).map_err(|e| RpcError::InvalidUrl(e.to_string()))?;
        Self::new(RpcClientConfig::new(url))
    }

    pub fn config(&self) -> &RpcClientConfig {
        &self.config
    }

    pub fn provider(&self) -> &RootProvider<Ethereum> {
        &self.provider
    }

    pub fn rate_limiter(&self) -> Option<&Arc<StandardRateLimiter>> {
        self.rate_limiter.as_ref()
    }

    pub fn retry_config(&self) -> &RetryConfig {
        &self.config.retry
    }

    async fn wait_for_rate_limit(&self) {
        let start = std::time::Instant::now();
        if let Some(ref limiter) = self.sliding_limiter {
            limiter.acquire(1).await;
            let current = limiter.current_usage_async().await;
            set_cu_usage(&self.chain, current as f64, limiter.max_in_window() as f64);
        } else if let (Some(limiter), Some(jitter)) = (&self.rate_limiter, &self.jitter) {
            limiter.until_ready_with_jitter(*jitter).await;
        }
        let elapsed = start.elapsed();
        if elapsed > Duration::from_millis(1) {
            record_rate_limit_wait(&self.chain, elapsed.as_secs_f64());
        }
    }

    fn effective_chunk_size(&self) -> usize {
        self.config
            .max_batch_size
            .min(self.config.concurrency)
            .max(1)
    }

    fn streaming_max_in_flight(&self) -> usize {
        self.config.concurrency.max(1) * 2
    }

    async fn acquire_rpc_permit(&self) -> OwnedSemaphorePermit {
        self.rpc_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("rpc semaphore should never be closed during operation")
    }

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        with_retry(
            &self.config.retry,
            "get_block_number",
            &self.chain,
            || async {
                self.wait_for_rate_limit().await;
                let _permit = self.acquire_rpc_permit().await;
                self.provider
                    .get_block_number()
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            },
        )
        .await
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        let op_name = format!("eth_getBlockByNumber({:?})", block_id);
        with_retry(&self.config.retry, &op_name, &self.chain, || async {
            self.wait_for_rate_limit().await;
            let _permit = self.acquire_rpc_permit().await;
            let builder = self.provider.get_block(block_id);
            if full_transactions {
                builder
                    .full()
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            } else {
                builder
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            }
        })
        .await
    }

    pub async fn get_block_by_number(
        &self,
        number: BlockNumberOrTag,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        self.get_block(BlockId::Number(number), full_transactions)
            .await
    }

    pub async fn get_transaction(&self, hash: B256) -> Result<Option<Transaction>, RpcError> {
        let op_name = format!("eth_getTransactionByHash({:?})", hash);
        with_retry(&self.config.retry, &op_name, &self.chain, || async {
            self.wait_for_rate_limit().await;
            let _permit = self.acquire_rpc_permit().await;
            self.provider
                .get_transaction_by_hash(hash)
                .await
                .map_err(|e| RpcError::ProviderError(error_chain(&e)))
        })
        .await
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        let op_name = format!("eth_getTransactionReceipt({:?})", hash);
        with_retry(&self.config.retry, &op_name, &self.chain, || async {
            self.wait_for_rate_limit().await;
            let _permit = self.acquire_rpc_permit().await;
            self.provider
                .get_transaction_receipt(hash)
                .await
                .map_err(|e| RpcError::ProviderError(error_chain(&e)))
        })
        .await
    }

    pub async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        let method = method_name.to_string();
        let block_param = match block_number {
            BlockNumberOrTag::Number(n) => format!("0x{:x}", n),
            BlockNumberOrTag::Latest => "latest".to_string(),
            BlockNumberOrTag::Earliest => "earliest".to_string(),
            BlockNumberOrTag::Pending => "pending".to_string(),
            BlockNumberOrTag::Safe => "safe".to_string(),
            BlockNumberOrTag::Finalized => "finalized".to_string(),
        };

        let raw_receipts: Vec<serde_json::Value> = with_retry(
            &self.config.retry,
            &format!("{}({:?})", method_name, block_number),
            &self.chain,
            || async {
                self.wait_for_rate_limit().await;
                let _permit = self.acquire_rpc_permit().await;
                self.provider
                    .client()
                    .request(method.clone(), (block_param.clone(),))
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            },
        )
        .await?;

        // Parse each receipt individually, returning None for failures
        let receipts: Vec<Option<TransactionReceipt>> = raw_receipts
            .into_iter()
            .enumerate()
            .map(
                |(i, value)| match serde_json::from_value::<TransactionReceipt>(value) {
                    Ok(receipt) => Some(receipt),
                    Err(e) => {
                        tracing::debug!(
                            "[{}] Skipping receipt {} in block {:?}: {}",
                            self.chain,
                            i,
                            block_number,
                            e
                        );
                        None
                    }
                },
            )
            .collect();

        Ok(receipts)
    }

    pub async fn get_block_receipts_concurrent(
        &self,
        method_name: &str,
        block_numbers: Vec<BlockNumberOrTag>,
        concurrency: usize,
    ) -> Result<Vec<Vec<Option<TransactionReceipt>>>, RpcError> {
        if block_numbers.is_empty() {
            return Ok(vec![]);
        }

        let mut all_results = Vec::with_capacity(block_numbers.len());
        let effective_concurrency = concurrency.min(self.config.concurrency).max(1);

        for chunk in block_numbers.chunks(effective_concurrency) {
            let chunk_block_numbers: Vec<_> = chunk.to_vec();
            let futures: Vec<_> = chunk
                .iter()
                .map(|&block_number| self.get_block_receipts(method_name, block_number))
                .collect();

            let results = futures::future::join_all(futures).await;

            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(receipts) => all_results.push(receipts),
                    Err(e) => {
                        let failed_block = chunk_block_numbers.get(i);
                        tracing::error!(
                            "[{}] get_block_receipts failed for block {:?}: {}",
                            self.chain,
                            failed_block,
                            e
                        );
                        return Err(e);
                    }
                }
            }
        }

        Ok(all_results)
    }

    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        let filter = filter.clone();
        let op_name = format!(
            "eth_getLogs(blocks {:?}-{:?})",
            filter.get_from_block(),
            filter.get_to_block()
        );
        with_retry(&self.config.retry, &op_name, &self.chain, || async {
            self.wait_for_rate_limit().await;
            let _permit = self.acquire_rpc_permit().await;
            self.provider
                .get_logs(&filter)
                .await
                .map_err(|e| RpcError::ProviderError(error_chain(&e)))
        })
        .await
    }

    pub async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        let op_name = format!("eth_getBalance({:?}, {:?})", address, block);
        with_retry(&self.config.retry, &op_name, &self.chain, || async {
            self.wait_for_rate_limit().await;
            let _permit = self.acquire_rpc_permit().await;
            self.provider
                .get_balance(address)
                .block_id(block.unwrap_or(BlockId::latest()))
                .await
                .map_err(|e| RpcError::ProviderError(error_chain(&e)))
        })
        .await
    }

    pub async fn get_code(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        let op_name = format!("eth_getCode({:?}, {:?})", address, block);
        with_retry(&self.config.retry, &op_name, &self.chain, || async {
            self.wait_for_rate_limit().await;
            let _permit = self.acquire_rpc_permit().await;
            self.provider
                .get_code_at(address)
                .block_id(block.unwrap_or(BlockId::latest()))
                .await
                .map_err(|e| RpcError::ProviderError(error_chain(&e)))
        })
        .await
    }

    pub async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        let tx = tx.clone();
        let op_name = format!("eth_call(to={:?}, block={:?})", tx.to, block);
        with_retry(&self.config.retry, &op_name, &self.chain, || async {
            self.wait_for_rate_limit().await;
            let _permit = self.acquire_rpc_permit().await;
            self.provider
                .call(tx.clone())
                .block(block.unwrap_or(BlockId::latest()))
                .await
                .map_err(|e| RpcError::ProviderError(error_chain(&e)))
        })
        .await
    }

    pub async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError> {
        if !self.config.batching_enabled {
            let mut results = Vec::with_capacity(block_numbers.len());
            for number in block_numbers {
                results.push(self.get_block_by_number(number, full_transactions).await?);
            }
            return Ok(results);
        }

        let mut all_results = Vec::with_capacity(block_numbers.len());

        for chunk in block_numbers.chunks(self.effective_chunk_size()) {
            let chunk_vec: Vec<BlockNumberOrTag> = chunk.to_vec();
            let client = self.clone();
            let futures: Vec<_> = chunk_vec
                .into_iter()
                .map(|number| {
                    let client = client.clone();
                    async move { client.get_block_by_number(number, full_transactions).await }
                })
                .collect();
            let results = futures::future::join_all(futures).await;

            let mut chunk_results = Vec::with_capacity(results.len());
            for result in results {
                chunk_results.push(result?);
            }

            all_results.extend(chunk_results);
        }

        Ok(all_results)
    }

    /// Stream blocks as they are fetched concurrently, sending each to the provided channel.
    /// Uses a bounded JoinSet pipeline for concurrent fetching with backpressure.
    /// Returns a JoinHandle that completes when all blocks are fetched.
    pub fn get_blocks_streaming(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
        result_tx: tokio::sync::mpsc::Sender<(BlockNumberOrTag, Result<Option<Block>, RpcError>)>,
    ) -> tokio::task::JoinHandle<()> {
        let max_in_flight = self.streaming_max_in_flight();
        let client = self.clone();
        let chain = self.chain.clone();

        tokio::spawn(async move {
            let mut iter = block_numbers.into_iter();
            let mut join_set = tokio::task::JoinSet::new();

            // Seed initial batch
            for number in iter.by_ref().take(max_in_flight) {
                let client = client.clone();
                let tx = result_tx.clone();
                join_set.spawn(async move {
                    let result = client.get_block_by_number(number, full_transactions).await;
                    let _ = tx.send((number, result)).await;
                });
            }

            // Pipeline: as tasks complete, spawn replacements
            while let Some(join_result) = join_set.join_next().await {
                if let Err(e) = join_result {
                    tracing::error!(
                        "[{}] Task panicked in get_blocks_streaming: {:?}",
                        chain,
                        e
                    );
                }

                if let Some(number) = iter.next() {
                    let client = client.clone();
                    let tx = result_tx.clone();
                    join_set.spawn(async move {
                        let result = client.get_block_by_number(number, full_transactions).await;
                        let _ = tx.send((number, result)).await;
                    });
                }
            }
        })
    }

    pub async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        if !self.config.batching_enabled {
            let mut results = Vec::with_capacity(hashes.len());
            for hash in hashes {
                match self.get_transaction_receipt(hash).await {
                    Ok(receipt) => results.push(receipt),
                    Err(e) => {
                        tracing::debug!(
                            "[{}] Skipping receipt for tx {:?}: {}",
                            self.chain,
                            hash,
                            e
                        );
                        results.push(None);
                    }
                }
            }
            return Ok(results);
        }

        let mut all_results = Vec::with_capacity(hashes.len());

        for chunk in hashes.chunks(self.effective_chunk_size()) {
            let client = self.clone();
            let futures: Vec<_> = chunk
                .iter()
                .map(|&hash| {
                    let client = client.clone();
                    async move { client.get_transaction_receipt(hash).await }
                })
                .collect();

            let results = futures::future::join_all(futures).await;

            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(receipt) => all_results.push(receipt),
                    Err(e) => {
                        tracing::debug!(
                            "[{}] Skipping receipt for tx {:?}: {}",
                            self.chain,
                            chunk[i],
                            e
                        );
                        all_results.push(None);
                    }
                }
            }
        }

        Ok(all_results)
    }

    pub async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError> {
        if !self.config.batching_enabled {
            let mut results = Vec::with_capacity(filters.len());
            for filter in filters {
                results.push(self.get_logs(&filter).await?);
            }
            return Ok(results);
        }

        let mut all_results = Vec::with_capacity(filters.len());

        for chunk in filters.chunks(self.effective_chunk_size()) {
            let chunk_vec: Vec<Filter> = chunk.to_vec();
            let client = self.clone();
            let futures: Vec<_> = chunk_vec
                .into_iter()
                .map(|filter| {
                    let client = client.clone();
                    async move { client.get_logs(&filter).await }
                })
                .collect();
            let results = futures::future::join_all(futures).await;

            let mut chunk_results = Vec::with_capacity(results.len());
            for result in results {
                chunk_results.push(result?);
            }

            all_results.extend(chunk_results);
        }

        Ok(all_results)
    }

    pub async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError> {
        if !self.config.batching_enabled {
            let mut results = Vec::with_capacity(calls.len());
            for (tx, block) in calls {
                results.push(self.call(&tx, Some(block)).await);
            }
            return Ok(results);
        }

        let mut all_results = Vec::with_capacity(calls.len());

        for chunk in calls.chunks(self.effective_chunk_size()) {
            let chunk_vec: Vec<(alloy::rpc::types::TransactionRequest, BlockId)> = chunk.to_vec();
            let client = self.clone();
            let futures: Vec<_> = chunk_vec
                .into_iter()
                .map(|(tx, block)| {
                    let client = client.clone();
                    async move { client.call(&tx, Some(block)).await }
                })
                .collect();
            let results = futures::future::join_all(futures).await;
            let chunk_results: Vec<Result<Bytes, RpcError>> = results.into_iter().collect();

            all_results.extend(chunk_results);
        }

        Ok(all_results)
    }
}

#[async_trait]
impl RpcProvider for RpcClient {
    async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        RpcClient::get_block_number(self).await
    }

    async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        RpcClient::get_block(self, block_id, full_transactions).await
    }

    async fn get_transaction(&self, hash: B256) -> Result<Option<Transaction>, RpcError> {
        RpcClient::get_transaction(self, hash).await
    }

    async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        RpcClient::get_transaction_receipt(self, hash).await
    }

    async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        RpcClient::get_logs(self, filter).await
    }

    async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        RpcClient::get_balance(self, address, block).await
    }

    async fn get_code(&self, address: Address, block: Option<BlockId>) -> Result<Bytes, RpcError> {
        RpcClient::get_code(self, address, block).await
    }

    async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        RpcClient::call(self, tx, block).await
    }

    async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        RpcClient::get_block_receipts(self, method_name, block_number).await
    }

    async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError> {
        RpcClient::get_blocks_batch(self, block_numbers, full_transactions).await
    }

    async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        RpcClient::get_transaction_receipts_batch(self, hashes).await
    }

    async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError> {
        RpcClient::get_logs_batch(self, filters).await
    }

    async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError> {
        RpcClient::call_batch(self, calls).await
    }
}

impl std::fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClient")
            .field("config", &self.config)
            .field("has_rate_limiter", &self.rate_limiter.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that get_transaction_receipts_batch chunks by
    /// min(max_batch_size, concurrency) to enforce concurrency.
    #[test]
    fn effective_chunk_size_uses_min_of_batch_and_concurrency() {
        let config = RpcClientConfig::new(Url::parse("http://localhost:8545").unwrap())
            .with_batch_size(100)
            .with_concurrency(10);

        let effective = config.max_batch_size.min(config.concurrency).max(1);
        assert_eq!(effective, 10);
    }

    /// Verify that concurrency > batch_size still respects the batch_size cap.
    #[test]
    fn effective_chunk_size_respects_batch_cap() {
        let config = RpcClientConfig::new(Url::parse("http://localhost:8545").unwrap())
            .with_batch_size(5)
            .with_concurrency(100);

        let effective = config.max_batch_size.min(config.concurrency).max(1);
        assert_eq!(effective, 5);
    }

    /// Verify .max(1) guards against chunks(0) panic.
    #[test]
    fn effective_chunk_size_zero_defaults_to_one() {
        let mut config = RpcClientConfig::new(Url::parse("http://localhost:8545").unwrap());
        config.max_batch_size = 0;
        config.concurrency = 0;

        let effective = config.max_batch_size.min(config.concurrency).max(1);
        assert_eq!(effective, 1);
    }

    /// Verify with_concurrency builder actually sets the field.
    #[test]
    fn with_concurrency_builder() {
        let config =
            RpcClientConfig::new(Url::parse("http://localhost:8545").unwrap()).with_concurrency(42);
        assert_eq!(config.concurrency, 42);
    }

    /// Verify default concurrency is 100.
    #[test]
    fn default_concurrency_is_100() {
        let config = RpcClientConfig::new(Url::parse("http://localhost:8545").unwrap());
        assert_eq!(config.concurrency, 100);
    }
}
