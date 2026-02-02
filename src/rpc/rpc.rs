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
use governor::clock::{QuantaClock, QuantaInstant};
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, Quota, RateLimiter};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
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

/// Execute an async operation with retry logic
pub async fn with_retry<F, Fut, T>(
    config: &RetryConfig,
    operation_name: &str,
    mut operation: F,
) -> Result<T, RpcError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RpcError>>,
{
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        // Wait before retry (no wait on first attempt)
        if attempt > 0 {
            let delay = config.delay_for_attempt(attempt);
            tracing::warn!(
                "RPC retry {}/{} for '{}' in {:?}",
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
                        "RPC '{}' succeeded after {} retries",
                        operation_name,
                        attempt
                    );
                }
                return Ok(result);
            }
            Err(e) => {
                if e.is_retryable() && attempt < config.max_retries {
                    tracing::warn!(
                        "RPC '{}' failed (attempt {}/{}): {}",
                        operation_name,
                        attempt + 1,
                        config.max_retries + 1,
                        e
                    );
                    last_error = Some(e);
                } else {
                    // Non-retryable error or exhausted retries
                    if attempt > 0 {
                        tracing::error!(
                            "RPC '{}' failed after {} attempts: {}",
                            operation_name,
                            attempt + 1,
                            e
                        );
                    }
                    return Err(e);
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| RpcError::ProviderError("Unknown error".to_string())))
}

pub type StandardRateLimiter =
    RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>;

#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    pub url: Url,
    pub max_batch_size: usize,
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

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: NonZeroU32::new(10).unwrap(),
            jitter_min_ms: 5,
            jitter_max_ms: 50,
        }
    }
}

impl RpcClientConfig {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            max_batch_size: 100,
            batching_enabled: true,
            rate_limit: None,
            retry: RetryConfig::default(),
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
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

pub struct RpcClient {
    provider: RootProvider<Ethereum>,
    config: RpcClientConfig,
    rate_limiter: Option<Arc<StandardRateLimiter>>,
    jitter: Option<Jitter>,
}

impl RpcClient {
    pub fn new(config: RpcClientConfig) -> Result<Self, RpcError> {
        let provider = RootProvider::<Ethereum>::new_http(config.url.clone());

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
        if let (Some(limiter), Some(jitter)) = (&self.rate_limiter, &self.jitter) {
            limiter.until_ready_with_jitter(*jitter).await;
        }
    }

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        with_retry(&self.config.retry, "get_block_number", || async {
            self.wait_for_rate_limit().await;
            self.provider
                .get_block_number()
                .await
                .map_err(|e| RpcError::ProviderError(e.to_string()))
        })
        .await
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        let op_name = format!("eth_getBlockByNumber({:?})", block_id);
        with_retry(&self.config.retry, &op_name, || async {
            self.wait_for_rate_limit().await;
            let builder = self.provider.get_block(block_id);
            if full_transactions {
                builder
                    .full()
                    .await
                    .map_err(|e| RpcError::ProviderError(e.to_string()))
            } else {
                builder
                    .await
                    .map_err(|e| RpcError::ProviderError(e.to_string()))
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
        with_retry(&self.config.retry, &op_name, || async {
            self.wait_for_rate_limit().await;
            self.provider
                .get_transaction_by_hash(hash)
                .await
                .map_err(|e| RpcError::ProviderError(e.to_string()))
        })
        .await
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        let op_name = format!("eth_getTransactionReceipt({:?})", hash);
        with_retry(&self.config.retry, &op_name, || async {
            self.wait_for_rate_limit().await;
            self.provider
                .get_transaction_receipt(hash)
                .await
                .map_err(|e| RpcError::ProviderError(e.to_string()))
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
            || async {
                self.wait_for_rate_limit().await;
                self.provider
                    .client()
                    .request(method.clone(), (block_param.clone(),))
                    .await
                    .map_err(|e| RpcError::ProviderError(e.to_string()))
            },
        )
        .await?;

        // Parse each receipt individually, returning None for failures
        let receipts: Vec<Option<TransactionReceipt>> = raw_receipts
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                match serde_json::from_value::<TransactionReceipt>(value) {
                    Ok(receipt) => Some(receipt),
                    Err(e) => {
                        tracing::debug!(
                            "Skipping receipt {} in block {:?}: {}",
                            i,
                            block_number,
                            e
                        );
                        None
                    }
                }
            })
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

        for chunk in block_numbers.chunks(concurrency) {
            self.wait_for_rate_limit().await;

            let chunk_block_numbers: Vec<_> = chunk.to_vec();
            let futures: Vec<_> = chunk
                .iter()
                .map(|&block_number| {
                    self.get_block_receipts(method_name, block_number)
                })
                .collect();

            let results = futures::future::join_all(futures).await;

            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(receipts) => all_results.push(receipts),
                    Err(e) => {
                        let failed_block = chunk_block_numbers.get(i);
                        tracing::error!(
                            "get_block_receipts failed for block {:?}: {}",
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
        with_retry(&self.config.retry, &op_name, || async {
            self.wait_for_rate_limit().await;
            self.provider
                .get_logs(&filter)
                .await
                .map_err(|e| RpcError::ProviderError(e.to_string()))
        })
        .await
    }

    pub async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        let op_name = format!("eth_getBalance({:?}, {:?})", address, block);
        with_retry(&self.config.retry, &op_name, || async {
            self.wait_for_rate_limit().await;
            self.provider
                .get_balance(address)
                .block_id(block.unwrap_or(BlockId::latest()))
                .await
                .map_err(|e| RpcError::ProviderError(e.to_string()))
        })
        .await
    }

    pub async fn get_code(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        let op_name = format!("eth_getCode({:?}, {:?})", address, block);
        with_retry(&self.config.retry, &op_name, || async {
            self.wait_for_rate_limit().await;
            self.provider
                .get_code_at(address)
                .block_id(block.unwrap_or(BlockId::latest()))
                .await
                .map_err(|e| RpcError::ProviderError(e.to_string()))
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
        with_retry(&self.config.retry, &op_name, || async {
            self.wait_for_rate_limit().await;
            self.provider
                .call(tx.clone())
                .block(block.unwrap_or(BlockId::latest()))
                .await
                .map_err(|e| RpcError::ProviderError(e.to_string()))
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

        for (chunk_idx, chunk) in block_numbers.chunks(self.config.max_batch_size).enumerate() {
            let chunk_vec: Vec<BlockNumberOrTag> = chunk.to_vec();
            let first_block = chunk_vec.first().map(|b| format!("{:?}", b)).unwrap_or_default();
            let last_block = chunk_vec.last().map(|b| format!("{:?}", b)).unwrap_or_default();
            let op_name = format!(
                "eth_getBlockByNumber[batch {}] blocks {}-{}",
                chunk_idx, first_block, last_block
            );
            let chunk_results: Vec<Option<Block>> = with_retry(
                &self.config.retry,
                &op_name,
                || async {
                    self.wait_for_rate_limit().await;

                    let futures: Vec<_> = chunk_vec
                        .iter()
                        .map(|&number| {
                            let builder = self.provider.get_block(BlockId::Number(number));
                            async move {
                                if full_transactions {
                                    builder.full().await
                                } else {
                                    builder.await
                                }
                            }
                        })
                        .collect();

                    let results = futures::future::join_all(futures).await;

                    let mut chunk_results = Vec::with_capacity(results.len());
                    for result in results {
                        chunk_results
                            .push(result.map_err(|e| RpcError::ProviderError(e.to_string()))?);
                    }
                    Ok(chunk_results)
                },
            )
            .await?;

            all_results.extend(chunk_results);
        }

        Ok(all_results)
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
                        tracing::debug!("Skipping receipt for tx {:?}: {}", hash, e);
                        results.push(None);
                    }
                }
            }
            return Ok(results);
        }

        let mut all_results = Vec::with_capacity(hashes.len());

        for chunk in hashes.chunks(self.config.max_batch_size) {
            self.wait_for_rate_limit().await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|&hash| self.provider.get_transaction_receipt(hash))
                .collect();

            let results = futures::future::join_all(futures).await;

            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(receipt) => all_results.push(receipt),
                    Err(e) => {
                        tracing::debug!("Skipping receipt for tx {:?}: {}", chunk[i], e);
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

        for (chunk_idx, chunk) in filters.chunks(self.config.max_batch_size).enumerate() {
            let chunk_vec: Vec<Filter> = chunk.to_vec();
            let op_name = format!(
                "eth_getLogs[batch {}] ({} filters)",
                chunk_idx,
                chunk_vec.len()
            );
            let chunk_results: Vec<Vec<Log>> = with_retry(
                &self.config.retry,
                &op_name,
                || async {
                    self.wait_for_rate_limit().await;

                    let futures: Vec<_> = chunk_vec
                        .iter()
                        .map(|filter| self.provider.get_logs(filter))
                        .collect();

                    let results = futures::future::join_all(futures).await;

                    let mut chunk_results = Vec::with_capacity(results.len());
                    for result in results {
                        chunk_results
                            .push(result.map_err(|e| RpcError::ProviderError(e.to_string()))?);
                    }
                    Ok(chunk_results)
                },
            )
            .await?;

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

        for (chunk_idx, chunk) in calls.chunks(self.config.max_batch_size).enumerate() {
            let chunk_vec: Vec<(alloy::rpc::types::TransactionRequest, BlockId)> =
                chunk.to_vec();
            let first_block = chunk_vec.first().map(|(_, b)| format!("{:?}", b)).unwrap_or_default();
            let last_block = chunk_vec.last().map(|(_, b)| format!("{:?}", b)).unwrap_or_default();
            let to_addr = chunk_vec.first().and_then(|(tx, _)| tx.to).map(|a| format!("{:?}", a)).unwrap_or_else(|| "unknown".to_string());
            let op_name = format!(
                "eth_call[batch {}] to={} blocks {}-{} ({} calls)",
                chunk_idx, to_addr, first_block, last_block, chunk_vec.len()
            );
            let chunk_results: Vec<Result<Bytes, RpcError>> = with_retry(
                &self.config.retry,
                &op_name,
                || async {
                    self.wait_for_rate_limit().await;

                    let futures: Vec<_> = chunk_vec
                        .iter()
                        .map(|(tx, block)| async move {
                            self.provider.call(tx.clone()).block(*block).await
                        })
                        .collect();

                    let results = futures::future::join_all(futures).await;

                    Ok(results
                        .into_iter()
                        .map(|r| r.map_err(|e| RpcError::ProviderError(e.to_string())))
                        .collect())
                },
            )
            .await?;

            all_results.extend(chunk_results);
        }

        Ok(all_results)
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
