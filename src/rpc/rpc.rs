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

pub type StandardRateLimiter =
    RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>;

#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    pub url: Url,
    pub max_batch_size: usize,
    pub batching_enabled: bool,
    pub rate_limit: Option<RateLimitConfig>,
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

    async fn wait_for_rate_limit(&self) {
        if let (Some(limiter), Some(jitter)) = (&self.rate_limiter, &self.jitter) {
            limiter.until_ready_with_jitter(*jitter).await;
        }
    }

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        self.wait_for_rate_limit().await;
        self.provider
            .get_block_number()
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
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
        self.wait_for_rate_limit().await;
        self.provider
            .get_transaction_by_hash(hash)
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        self.wait_for_rate_limit().await;
        self.provider
            .get_transaction_receipt(hash)
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        self.wait_for_rate_limit().await;

        let block_param = match block_number {
            BlockNumberOrTag::Number(n) => format!("0x{:x}", n),
            BlockNumberOrTag::Latest => "latest".to_string(),
            BlockNumberOrTag::Earliest => "earliest".to_string(),
            BlockNumberOrTag::Pending => "pending".to_string(),
            BlockNumberOrTag::Safe => "safe".to_string(),
            BlockNumberOrTag::Finalized => "finalized".to_string(),
        };

        let method = method_name.to_string();

        let raw_receipts: Vec<serde_json::Value> = self
            .provider
            .client()
            .request(method, (block_param,))
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))?;

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

            let futures: Vec<_> = chunk
                .iter()
                .map(|&block_number| {
                    self.get_block_receipts(method_name, block_number)
                })
                .collect();

            let results = futures::future::join_all(futures).await;

            for result in results {
                all_results.push(result?);
            }
        }

        Ok(all_results)
    }

    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        self.wait_for_rate_limit().await;
        self.provider
            .get_logs(filter)
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        self.wait_for_rate_limit().await;
        self.provider
            .get_balance(address)
            .block_id(block.unwrap_or(BlockId::latest()))
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_code(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        self.wait_for_rate_limit().await;
        self.provider
            .get_code_at(address)
            .block_id(block.unwrap_or(BlockId::latest()))
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        self.wait_for_rate_limit().await;
        self.provider
            .call(tx.clone())
            .block(block.unwrap_or(BlockId::latest()))
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
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

        for chunk in block_numbers.chunks(self.config.max_batch_size) {
            self.wait_for_rate_limit().await;

            let futures: Vec<_> = chunk
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

            for result in results {
                all_results.push(result.map_err(|e| RpcError::ProviderError(e.to_string()))?);
            }
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

        for chunk in filters.chunks(self.config.max_batch_size) {
            self.wait_for_rate_limit().await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|filter| self.provider.get_logs(filter))
                .collect();

            let results = futures::future::join_all(futures).await;

            for result in results {
                all_results.push(result.map_err(|e| RpcError::ProviderError(e.to_string()))?);
            }
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

        for chunk in calls.chunks(self.config.max_batch_size) {
            self.wait_for_rate_limit().await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|(tx, block)| async move {
                    self.provider
                        .call(tx.clone())
                        .block(*block)
                        .await
                })
                .collect();

            let results = futures::future::join_all(futures).await;

            for result in results {
                all_results.push(result.map_err(|e| RpcError::ProviderError(e.to_string())));
            }
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
