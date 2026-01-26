use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, BlockNumber, Bytes, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{
    Block, BlockId, BlockNumberOrTag, Filter, Log, Transaction, TransactionReceipt,
};
use governor::clock::{QuantaClock, QuantaInstant};
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Jitter, Quota, RateLimiter};
use url::Url;

use crate::rpc::rpc::{RpcClient, RpcClientConfig, RpcError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputeUnitCost(pub u32);

impl ComputeUnitCost {
    pub const BLOCK_NUMBER: Self = Self(10);
    pub const GET_BLOCK_BY_NUMBER: Self = Self(16);
    pub const GET_BLOCK_BY_HASH: Self = Self(16);
    pub const GET_TRANSACTION_BY_HASH: Self = Self(17);
    pub const GET_TRANSACTION_RECEIPT: Self = Self(15);
    pub const GET_LOGS: Self = Self(75);
    pub const GET_BALANCE: Self = Self(19);
    pub const GET_CODE: Self = Self(19);
    pub const ETH_CALL: Self = Self(26);
    pub const GET_STORAGE_AT: Self = Self(17);

    pub fn cost(&self) -> u32 {
        self.0
    }
}

pub type ComputeUnitRateLimiter =
    RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>;

#[derive(Debug, Clone)]
pub struct AlchemyConfig {
    pub url: Url,
    pub compute_units_per_second: NonZeroU32,
    pub max_batch_size: usize,
    pub batching_enabled: bool,
    pub jitter_min_ms: u64,
    pub jitter_max_ms: u64,
}

impl AlchemyConfig {
    pub fn new(url: Url, compute_units_per_second: u32) -> Self {
        Self {
            url,
            compute_units_per_second: NonZeroU32::new(compute_units_per_second)
                .expect("compute_units_per_second must be > 0"),
            max_batch_size: 100,
            batching_enabled: true,
            jitter_min_ms: 5,
            jitter_max_ms: 50,
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

    pub fn with_jitter(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.jitter_min_ms = min_ms;
        self.jitter_max_ms = max_ms;
        self
    }
}

impl Default for AlchemyConfig {
    fn default() -> Self {
        Self {
            url: Url::parse("http://localhost:8545").unwrap(),
            compute_units_per_second: NonZeroU32::new(330).unwrap(),
            max_batch_size: 100,
            batching_enabled: true,
            jitter_min_ms: 5,
            jitter_max_ms: 50,
        }
    }
}

pub struct AlchemyClient {
    inner: RpcClient,
    cu_rate_limiter: Arc<ComputeUnitRateLimiter>,
    jitter: Jitter,
    config: AlchemyConfig,
}

impl AlchemyClient {
    pub fn new(config: AlchemyConfig) -> Result<Self, RpcError> {
        let rpc_config = RpcClientConfig::new(config.url.clone())
            .with_batch_size(config.max_batch_size)
            .with_batching(config.batching_enabled);

        let inner = RpcClient::new(rpc_config)?;

        let quota = Quota::per_second(config.compute_units_per_second);
        let cu_rate_limiter = Arc::new(RateLimiter::direct(quota));

        let jitter = Jitter::new(
            Duration::from_millis(config.jitter_min_ms),
            Duration::from_millis(config.jitter_max_ms),
        );

        Ok(Self {
            inner,
            cu_rate_limiter,
            jitter,
            config,
        })
    }

    pub fn from_url(url: &str, compute_units_per_second: u32) -> Result<Self, RpcError> {
        let url = Url::parse(url).map_err(|e| RpcError::InvalidUrl(e.to_string()))?;
        Self::new(AlchemyConfig::new(url, compute_units_per_second))
    }

    pub fn config(&self) -> &AlchemyConfig {
        &self.config
    }

    pub fn inner(&self) -> &RpcClient {
        &self.inner
    }

    async fn consume_compute_units(&self, cost: ComputeUnitCost) {
        let units = cost.cost();
        for _ in 0..units {
            self.cu_rate_limiter
                .until_ready_with_jitter(self.jitter)
                .await;
        }
    }

    async fn consume_compute_units_raw(&self, units: u32) {
        for _ in 0..units {
            self.cu_rate_limiter
                .until_ready_with_jitter(self.jitter)
                .await;
        }
    }

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        self.consume_compute_units(ComputeUnitCost::BLOCK_NUMBER)
            .await;
        self.inner
            .provider()
            .get_block_number()
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        let cost = match block_id {
            BlockId::Hash(_) => ComputeUnitCost::GET_BLOCK_BY_HASH,
            BlockId::Number(_) => ComputeUnitCost::GET_BLOCK_BY_NUMBER,
        };
        self.consume_compute_units(cost).await;

        let builder = self.inner.provider().get_block(block_id);
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
        self.consume_compute_units(ComputeUnitCost::GET_TRANSACTION_BY_HASH)
            .await;
        self.inner
            .provider()
            .get_transaction_by_hash(hash)
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        self.consume_compute_units(ComputeUnitCost::GET_TRANSACTION_RECEIPT)
            .await;
        self.inner
            .provider()
            .get_transaction_receipt(hash)
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        self.consume_compute_units(ComputeUnitCost::GET_LOGS).await;
        self.inner
            .provider()
            .get_logs(filter)
            .await
            .map_err(|e| RpcError::ProviderError(e.to_string()))
    }

    pub async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        self.consume_compute_units(ComputeUnitCost::GET_BALANCE)
            .await;
        self.inner
            .provider()
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
        self.consume_compute_units(ComputeUnitCost::GET_CODE).await;
        self.inner
            .provider()
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
        self.consume_compute_units(ComputeUnitCost::ETH_CALL).await;
        self.inner
            .provider()
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
        let cost_per_block = ComputeUnitCost::GET_BLOCK_BY_NUMBER.cost();

        for chunk in block_numbers.chunks(self.config.max_batch_size) {
            let total_cost = chunk.len() as u32 * cost_per_block;
            self.consume_compute_units_raw(total_cost).await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|&number| {
                    let builder = self.inner.provider().get_block(BlockId::Number(number));
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
                results.push(self.get_transaction_receipt(hash).await?);
            }
            return Ok(results);
        }

        let mut all_results = Vec::with_capacity(hashes.len());
        let cost_per_receipt = ComputeUnitCost::GET_TRANSACTION_RECEIPT.cost();

        for chunk in hashes.chunks(self.config.max_batch_size) {
            let total_cost = chunk.len() as u32 * cost_per_receipt;
            self.consume_compute_units_raw(total_cost).await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|&hash| self.inner.provider().get_transaction_receipt(hash))
                .collect();

            let results = futures::future::join_all(futures).await;

            for result in results {
                all_results.push(result.map_err(|e| RpcError::ProviderError(e.to_string()))?);
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
        let cost_per_logs = ComputeUnitCost::GET_LOGS.cost();

        for chunk in filters.chunks(self.config.max_batch_size) {
            let total_cost = chunk.len() as u32 * cost_per_logs;
            self.consume_compute_units_raw(total_cost).await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|filter| self.inner.provider().get_logs(filter))
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
        let cost_per_call = ComputeUnitCost::ETH_CALL.cost();

        for chunk in calls.chunks(self.config.max_batch_size) {
            let total_cost = chunk.len() as u32 * cost_per_call;
            self.consume_compute_units_raw(total_cost).await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|(tx, block)| async move {
                    self.inner
                        .provider()
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

impl std::fmt::Debug for AlchemyClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlchemyClient")
            .field("config", &self.config)
            .finish()
    }
}
