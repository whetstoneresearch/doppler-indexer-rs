use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, BlockNumber, Bytes, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{
    Block, BlockId, BlockNumberOrTag, Filter, Log, Transaction, TransactionReceipt,
};
use tokio::sync::Mutex;
use url::Url;

use crate::rpc::rpc::{error_chain, with_retry, RetryConfig, RpcClient, RpcClientConfig, RpcError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputeUnitCost(pub u32);

impl ComputeUnitCost {
    pub const BLOCK_NUMBER: Self = Self(10);
    pub const GET_BLOCK_BY_NUMBER: Self = Self(16);
    pub const GET_BLOCK_BY_HASH: Self = Self(16);
    pub const GET_TRANSACTION_BY_HASH: Self = Self(17);
    pub const GET_TRANSACTION_RECEIPT: Self = Self(15);
    pub const GET_BLOCK_RECEIPTS: Self = Self(500);
    pub const GET_LOGS: Self = Self(75);
    pub const GET_BALANCE: Self = Self(19);
    pub const GET_CODE: Self = Self(19);
    pub const ETH_CALL: Self = Self(26);
    pub const GET_STORAGE_AT: Self = Self(17);

    pub fn cost(&self) -> u32 {
        self.0
    }
}

/// A sliding window rate limiter that tracks compute unit usage over a 10-second window.
/// Alchemy measures rate limits over 10-second windows at 10x the per-second rate.
/// Unlike token bucket algorithms, this prevents burst accumulation - you cannot "save up"
/// compute units by being idle.
#[derive(Debug)]
pub struct SlidingWindowRateLimiter {
    /// Maximum compute units allowed in the window
    max_in_window: u32,
    /// Window duration (10 seconds for Alchemy)
    window: Duration,
    /// Record of (timestamp, units) for recent consumption
    history: Mutex<VecDeque<(Instant, u32)>>,
}

impl SlidingWindowRateLimiter {
    pub fn new(max_per_second: u32) -> Self {
        Self {
            // Alchemy uses 10-second windows at 10x the per-second rate
            max_in_window: max_per_second * 10,
            window: Duration::from_secs(10),
            history: Mutex::new(VecDeque::new()),
        }
    }

    /// Calculate current usage within the sliding window
    fn current_usage(history: &VecDeque<(Instant, u32)>, now: Instant, window: Duration) -> u32 {
        let cutoff = now.checked_sub(window).unwrap_or(now);
        history
            .iter()
            .filter(|(ts, _)| *ts > cutoff)
            .map(|(_, units)| *units)
            .sum()
    }

    /// Remove expired entries from history
    fn cleanup(history: &mut VecDeque<(Instant, u32)>, now: Instant, window: Duration) {
        let cutoff = now.checked_sub(window).unwrap_or(now);
        while let Some((ts, _)) = history.front() {
            if *ts <= cutoff {
                history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Wait until we can consume the requested units, then record them.
    /// Returns immediately if there's capacity, otherwise waits.
    pub async fn acquire(&self, units: u32) {
        if units == 0 {
            return;
        }

        loop {
            let wait_time = {
                let mut history = self.history.lock().await;
                let now = Instant::now();

                // Clean up old entries
                Self::cleanup(&mut history, now, self.window);

                let current = Self::current_usage(&history, now, self.window);
                let available = self.max_in_window.saturating_sub(current);

                if units <= available {
                    // We have capacity - record and return
                    history.push_back((now, units));
                    return;
                }

                // Calculate how long until enough capacity frees up
                // Find the oldest entry that, if expired, would give us enough room
                let needed = units - available;
                let mut accumulated = 0u32;
                let mut wait_until = now;

                for (ts, u) in history.iter() {
                    accumulated += u;
                    if accumulated >= needed {
                        // When this entry expires, we'll have enough room
                        wait_until = *ts + self.window;
                        break;
                    }
                }

                // Add a small buffer to ensure the entry has expired
                wait_until
                    .saturating_duration_since(now)
                    .saturating_add(Duration::from_millis(1))
            };

            // Wait outside the lock
            tokio::time::sleep(wait_time).await;
        }
    }

    /// Get current usage (for debugging/metrics)
    pub async fn current_usage_async(&self) -> u32 {
        let history = self.history.lock().await;
        Self::current_usage(&history, Instant::now(), self.window)
    }
}

#[derive(Debug, Clone)]
pub struct AlchemyConfig {
    pub url: Url,
    pub compute_units_per_second: NonZeroU32,
    pub max_batch_size: usize,
    pub batching_enabled: bool,
    pub retry: RetryConfig,
}

impl AlchemyConfig {
    pub fn new(url: Url, compute_units_per_second: u32) -> Self {
        Self {
            url,
            compute_units_per_second: NonZeroU32::new(compute_units_per_second)
                .expect("compute_units_per_second must be > 0"),
            max_batch_size: 100,
            batching_enabled: true,
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

    pub fn with_retry(mut self, config: RetryConfig) -> Self {
        self.retry = config;
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
            retry: RetryConfig::default(),
        }
    }
}

pub struct AlchemyClient {
    inner: RpcClient,
    rate_limiter: Arc<SlidingWindowRateLimiter>,
    config: AlchemyConfig,
}

impl AlchemyClient {
    pub fn new(config: AlchemyConfig) -> Result<Self, RpcError> {
        let rpc_config = RpcClientConfig::new(config.url.clone())
            .with_batch_size(config.max_batch_size)
            .with_batching(config.batching_enabled);

        let inner = RpcClient::new(rpc_config)?;

        let rate_limiter = Arc::new(SlidingWindowRateLimiter::new(
            config.compute_units_per_second.get(),
        ));

        Ok(Self {
            inner,
            rate_limiter,
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

    pub fn retry_config(&self) -> &RetryConfig {
        &self.config.retry
    }

    /// Consume compute units, waiting if necessary.
    /// Uses sliding window rate limiting to prevent burst accumulation.
    async fn consume_compute_units(&self, cost: ComputeUnitCost) {
        self.rate_limiter.acquire(cost.cost()).await;
    }

    /// Consume a raw number of compute units, waiting if necessary.
    async fn consume_compute_units_raw(&self, units: u32) {
        self.rate_limiter.acquire(units).await;
    }

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        self.consume_compute_units(ComputeUnitCost::BLOCK_NUMBER)
            .await;
        with_retry(&self.config.retry, "get_block_number", || async {
            self.inner
                .provider()
                .get_block_number()
                .await
                .map_err(|e| RpcError::ProviderError(error_chain(&e)))
        })
        .await
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
        let op_name = format!("eth_getBlockByNumber({:?})", block_id);

        self.consume_compute_units(cost).await;
        with_retry(&self.config.retry, &op_name, || async {
            let builder = self.inner.provider().get_block(block_id);
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
        self.consume_compute_units(ComputeUnitCost::GET_TRANSACTION_BY_HASH)
            .await;
        with_retry(&self.config.retry, &op_name, || async {
            self.inner
                .provider()
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
        self.consume_compute_units(ComputeUnitCost::GET_TRANSACTION_RECEIPT)
            .await;
        with_retry(&self.config.retry, &op_name, || async {
            self.inner
                .provider()
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
        // Note: inner.get_block_receipts already has retry logic
        self.consume_compute_units(ComputeUnitCost::GET_BLOCK_RECEIPTS)
            .await;
        self.inner.get_block_receipts(method_name, block_number).await
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
        let cost_per_block = ComputeUnitCost::GET_BLOCK_RECEIPTS.cost();

        for chunk in block_numbers.chunks(concurrency) {
            let total_cost = chunk.len() as u32 * cost_per_block;
            self.consume_compute_units_raw(total_cost).await;

            let chunk_block_numbers: Vec<_> = chunk.to_vec();
            let futures: Vec<_> = chunk
                .iter()
                .map(|&block_number| {
                    self.inner.get_block_receipts(method_name, block_number)
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
        self.consume_compute_units(ComputeUnitCost::GET_LOGS).await;
        with_retry(&self.config.retry, &op_name, || async {
            self.inner
                .provider()
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
        self.consume_compute_units(ComputeUnitCost::GET_BALANCE)
            .await;
        with_retry(&self.config.retry, &op_name, || async {
            self.inner
                .provider()
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
        self.consume_compute_units(ComputeUnitCost::GET_CODE).await;
        with_retry(&self.config.retry, &op_name, || async {
            self.inner
                .provider()
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
        self.consume_compute_units(ComputeUnitCost::ETH_CALL).await;
        with_retry(&self.config.retry, &op_name, || async {
            self.inner
                .provider()
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
        let cost_per_block = ComputeUnitCost::GET_BLOCK_BY_NUMBER.cost();

        for (chunk_idx, chunk) in block_numbers.chunks(self.config.max_batch_size).enumerate() {
            let chunk_vec: Vec<BlockNumberOrTag> = chunk.to_vec();
            let total_cost = chunk_vec.len() as u32 * cost_per_block;
            let first_block = chunk_vec.first().map(|b| format!("{:?}", b)).unwrap_or_default();
            let last_block = chunk_vec.last().map(|b| format!("{:?}", b)).unwrap_or_default();
            let op_name = format!(
                "eth_getBlockByNumber[batch {}] blocks {}-{}",
                chunk_idx, first_block, last_block
            );

            self.consume_compute_units_raw(total_cost).await;
            let chunk_results: Vec<Option<Block>> = with_retry(
                &self.config.retry,
                &op_name,
                || async {
                    let futures: Vec<_> = chunk_vec
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

                    let mut chunk_results = Vec::with_capacity(results.len());
                    for result in results {
                        chunk_results
                            .push(result.map_err(|e| RpcError::ProviderError(error_chain(&e)))?);
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
        let cost_per_receipt = ComputeUnitCost::GET_TRANSACTION_RECEIPT.cost();

        for chunk in hashes.chunks(self.config.max_batch_size) {
            let total_cost = chunk.len() as u32 * cost_per_receipt;
            self.consume_compute_units_raw(total_cost).await;

            let futures: Vec<_> = chunk
                .iter()
                .map(|&hash| self.inner.provider().get_transaction_receipt(hash))
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
        let cost_per_logs = ComputeUnitCost::GET_LOGS.cost();

        for (chunk_idx, chunk) in filters.chunks(self.config.max_batch_size).enumerate() {
            let chunk_vec: Vec<Filter> = chunk.to_vec();
            let total_cost = chunk_vec.len() as u32 * cost_per_logs;
            let op_name = format!(
                "eth_getLogs[batch {}] ({} filters)",
                chunk_idx,
                chunk_vec.len()
            );

            self.consume_compute_units_raw(total_cost).await;
            let chunk_results: Vec<Vec<Log>> = with_retry(
                &self.config.retry,
                &op_name,
                || async {
                    let futures: Vec<_> = chunk_vec
                        .iter()
                        .map(|filter| self.inner.provider().get_logs(filter))
                        .collect();

                    let results = futures::future::join_all(futures).await;

                    let mut chunk_results = Vec::with_capacity(results.len());
                    for result in results {
                        chunk_results
                            .push(result.map_err(|e| RpcError::ProviderError(error_chain(&e)))?);
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
        let cost_per_call = ComputeUnitCost::ETH_CALL.cost();

        for (chunk_idx, chunk) in calls.chunks(self.config.max_batch_size).enumerate() {
            let chunk_vec: Vec<(alloy::rpc::types::TransactionRequest, BlockId)> =
                chunk.to_vec();
            let total_cost = chunk_vec.len() as u32 * cost_per_call;
            let first_block = chunk_vec.first().map(|(_, b)| format!("{:?}", b)).unwrap_or_default();
            let last_block = chunk_vec.last().map(|(_, b)| format!("{:?}", b)).unwrap_or_default();
            let to_addr = chunk_vec.first().and_then(|(tx, _)| tx.to).map(|a| format!("{:?}", a)).unwrap_or_else(|| "unknown".to_string());
            let op_name = format!(
                "eth_call[batch {}] to={} blocks {}-{} ({} calls)",
                chunk_idx, to_addr, first_block, last_block, chunk_vec.len()
            );

            self.consume_compute_units_raw(total_cost).await;
            let chunk_results: Vec<Result<Bytes, RpcError>> = with_retry(
                &self.config.retry,
                &op_name,
                || async {
                    let futures: Vec<_> = chunk_vec
                        .iter()
                        .map(|(tx, block)| async move {
                            self.inner.provider().call(tx.clone()).block(*block).await
                        })
                        .collect();

                    let results = futures::future::join_all(futures).await;

                    Ok(results
                        .into_iter()
                        .map(|r| r.map_err(|e| RpcError::ProviderError(error_chain(&e))))
                        .collect())
                },
            )
            .await?;

            all_results.extend(chunk_results);
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
