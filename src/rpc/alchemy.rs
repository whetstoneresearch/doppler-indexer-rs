use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, BlockNumber, Bytes, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{
    Block, BlockId, BlockNumberOrTag, Filter, Log, Transaction, TransactionReceipt,
};
use async_trait::async_trait;
use tokio::sync::{Mutex, Semaphore};
use url::Url;

use crate::rpc::rpc::{
    error_chain, with_retry, RetryConfig, RpcClient, RpcClientConfig, RpcError, RpcProvider,
};

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
    /// Max concurrent in-flight RPC requests. Configurable via RPC_CONCURRENCY env var.
    pub rpc_concurrency: usize,
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
            rpc_concurrency: 100,
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

    pub fn with_rpc_concurrency(mut self, concurrency: usize) -> Self {
        self.rpc_concurrency = concurrency;
        self
    }
}

/// Default compute units per second for Alchemy.
/// Using a const ensures this is computed at compile time and cannot fail.
const DEFAULT_COMPUTE_UNITS_PER_SECOND: NonZeroU32 = match NonZeroU32::new(330) {
    Some(v) => v,
    None => unreachable!(),
};

impl Default for AlchemyConfig {
    fn default() -> Self {
        Self {
            // SAFETY: This is a valid URL literal and will always parse successfully.
            url: Url::parse("http://localhost:8545")
                .expect("default URL is a valid literal"),
            compute_units_per_second: DEFAULT_COMPUTE_UNITS_PER_SECOND,
            max_batch_size: 100,
            batching_enabled: true,
            retry: RetryConfig::default(),
            rpc_concurrency: 100,
        }
    }
}

pub struct AlchemyClient {
    inner: Arc<RpcClient>,
    rate_limiter: Arc<SlidingWindowRateLimiter>,
    config: AlchemyConfig,
}

impl AlchemyClient {
    pub fn new(config: AlchemyConfig) -> Result<Self, RpcError> {
        Self::new_with_limiter(config, None)
    }

    /// Create a new client, optionally sharing a rate limiter with other clients.
    /// If `shared_limiter` is None, creates a new rate limiter.
    pub fn new_with_limiter(
        config: AlchemyConfig,
        shared_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    ) -> Result<Self, RpcError> {
        let rpc_config = RpcClientConfig::new(config.url.clone())
            .with_batch_size(config.max_batch_size)
            .with_batching(config.batching_enabled);

        let inner = Arc::new(RpcClient::new(rpc_config)?);

        let rate_limiter = shared_limiter.unwrap_or_else(|| {
            Arc::new(SlidingWindowRateLimiter::new(
                config.compute_units_per_second.get(),
            ))
        });

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

    /// Create client with custom options for concurrency and optional shared rate limiter.
    pub fn from_url_with_options(
        url: &str,
        compute_units_per_second: u32,
        rpc_concurrency: usize,
        shared_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    ) -> Result<Self, RpcError> {
        let url = Url::parse(url).map_err(|e| RpcError::InvalidUrl(e.to_string()))?;
        let config = AlchemyConfig::new(url, compute_units_per_second)
            .with_rpc_concurrency(rpc_concurrency);
        Self::new_with_limiter(config, shared_limiter)
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

    /// Execute requests concurrently with semaphore-bounded parallelism and per-request rate limiting.
    /// Returns results in the same order as the input requests.
    ///
    /// Each request:
    /// 1. Acquires a semaphore permit (limits concurrent in-flight requests)
    /// 2. Acquires CUs from the rate limiter
    /// 3. Executes the request
    /// 4. Stores result at its original index
    ///
    /// Returns an error if any task panics.
    async fn execute_concurrent_ordered<T, Req, F, Fut>(
        &self,
        requests: Vec<Req>,
        cost_per_request: u32,
        make_request: F,
    ) -> Result<Vec<T>, RpcError>
    where
        T: Send + 'static,
        Req: Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send,
    {
        use tokio::task::JoinSet;

        if requests.is_empty() {
            return Ok(vec![]);
        }

        let num_requests = requests.len();
        let semaphore = Arc::new(Semaphore::new(self.config.rpc_concurrency));
        let rate_limiter = self.rate_limiter.clone();
        let make_request = Arc::new(make_request);

        let mut join_set = JoinSet::new();

        // Spawn all tasks immediately - permit acquisition happens inside each task.
        // This allows all tasks to be spawned without blocking, maximizing parallelism.
        for (idx, request) in requests.into_iter().enumerate() {
            let semaphore = semaphore.clone();
            let rate_limiter = rate_limiter.clone();
            let make_request = make_request.clone();

            join_set.spawn(async move {
                // Acquire semaphore permit inside the task to avoid blocking task spawning
                let permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("semaphore should never be closed during operation");
                // Acquire CUs before making request
                rate_limiter.acquire(cost_per_request).await;
                let result = make_request(request).await;
                drop(permit); // Release semaphore permit
                (idx, result)
            });
        }

        // Collect results and sort by index to preserve order
        let mut indexed_results: Vec<(usize, T)> = Vec::with_capacity(num_requests);
        let mut had_panic = false;

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((idx, value)) => indexed_results.push((idx, value)),
                Err(e) => {
                    // Task panicked - track it and continue collecting other results
                    tracing::error!("Task panicked in execute_concurrent_ordered: {:?}", e);
                    had_panic = true;
                }
            }
        }

        if had_panic {
            return Err(RpcError::ProviderError(
                "One or more concurrent tasks panicked".to_string(),
            ));
        }

        // Sort by index and extract values
        indexed_results.sort_by_key(|(idx, _)| *idx);
        Ok(indexed_results.into_iter().map(|(_, v)| v).collect())
    }

    /// Execute requests concurrently, streaming results to a channel as they complete.
    /// Results are sent in completion order (not input order) for maximum throughput.
    ///
    /// Returns a JoinHandle that resolves when all requests are complete.
    /// The caller should process results from the receiver concurrently.
    pub fn execute_streaming<T, Req, F, Fut>(
        &self,
        requests: Vec<Req>,
        cost_per_request: u32,
        result_tx: tokio::sync::mpsc::Sender<T>,
        make_request: F,
    ) -> tokio::task::JoinHandle<()>
    where
        T: Send + 'static,
        Req: Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send,
    {
        use tokio::task::JoinSet;

        let semaphore = Arc::new(Semaphore::new(self.config.rpc_concurrency));
        let rate_limiter = self.rate_limiter.clone();
        let make_request = Arc::new(make_request);

        tokio::spawn(async move {
            if requests.is_empty() {
                return;
            }

            let mut join_set = JoinSet::new();

            // Spawn all tasks immediately - permit acquisition happens inside each task.
            // This allows all tasks to be spawned without blocking, maximizing parallelism.
            for request in requests {
                let semaphore = semaphore.clone();
                let rate_limiter = rate_limiter.clone();
                let make_request = make_request.clone();
                let result_tx = result_tx.clone();

                join_set.spawn(async move {
                    // Acquire semaphore permit inside the task to avoid blocking task spawning
                    let permit = semaphore
                        .acquire_owned()
                        .await
                        .expect("semaphore should never be closed during operation");
                    // Acquire CUs before making request
                    rate_limiter.acquire(cost_per_request).await;
                    let result = make_request(request).await;
                    drop(permit); // Release semaphore permit

                    // Send result immediately - ignore error if receiver dropped
                    let _ = result_tx.send(result).await;
                });
            }

            // Wait for all tasks to complete, logging any panics
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    tracing::error!("Task panicked in execute_streaming: {:?}", e);
                }
            }
        })
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

    /// Get block receipts for multiple blocks concurrently.
    ///
    /// # Arguments
    /// * `method_name` - The RPC method name (e.g., "eth_getBlockReceipts")
    /// * `block_numbers` - Block numbers to fetch receipts for
    /// * `_concurrency` - **Deprecated and ignored.** Concurrency is controlled by
    ///   `rpc_concurrency` in `AlchemyConfig`. This parameter is kept for API compatibility.
    pub async fn get_block_receipts_concurrent(
        &self,
        method_name: &str,
        block_numbers: Vec<BlockNumberOrTag>,
        #[allow(unused_variables)] _concurrency: usize,
    ) -> Result<Vec<Vec<Option<TransactionReceipt>>>, RpcError> {
        if block_numbers.is_empty() {
            return Ok(vec![]);
        }

        let inner = self.inner.clone();
        let method_name = method_name.to_string();
        let cost_per_block = ComputeUnitCost::GET_BLOCK_RECEIPTS.cost();

        let results = self
            .execute_concurrent_ordered(block_numbers, cost_per_block, move |block_number| {
                let inner = inner.clone();
                let method_name = method_name.clone();
                async move {
                    // inner.get_block_receipts already has retry logic
                    inner.get_block_receipts(&method_name, block_number).await
                }
            })
            .await?;

        // Collect results, failing on first error
        let mut receipts = Vec::with_capacity(results.len());
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(r) => receipts.push(r),
                Err(e) => {
                    tracing::error!("get_block_receipts failed for block index {}: {}", i, e);
                    return Err(e);
                }
            }
        }
        Ok(receipts)
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

        if block_numbers.is_empty() {
            return Ok(vec![]);
        }

        let provider = self.inner.provider().clone();
        let retry_config = self.config.retry.clone();
        let cost_per_block = ComputeUnitCost::GET_BLOCK_BY_NUMBER.cost();

        let results = self
            .execute_concurrent_ordered(block_numbers, cost_per_block, move |number| {
                let provider = provider.clone();
                let retry_config = retry_config.clone();
                async move {
                    let op_name = format!("eth_getBlockByNumber({:?})", number);
                    with_retry(&retry_config, &op_name, || async {
                        let builder = provider.get_block(BlockId::Number(number));
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
            })
            .await?;

        // Collect results, failing on first error
        let mut blocks = Vec::with_capacity(results.len());
        for result in results {
            blocks.push(result?);
        }
        Ok(blocks)
    }

    /// Stream blocks as they are fetched, sending each to the provided channel.
    /// Returns a JoinHandle that completes when all blocks are fetched.
    /// Results include the block number for ordering/identification.
    pub fn get_blocks_streaming(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
        result_tx: tokio::sync::mpsc::Sender<(BlockNumberOrTag, Result<Option<Block>, RpcError>)>,
    ) -> tokio::task::JoinHandle<()> {
        let provider = self.inner.provider().clone();
        let retry_config = self.config.retry.clone();
        let cost_per_block = ComputeUnitCost::GET_BLOCK_BY_NUMBER.cost();

        self.execute_streaming(
            block_numbers,
            cost_per_block,
            result_tx,
            move |number| {
                let provider = provider.clone();
                let retry_config = retry_config.clone();
                async move {
                    let op_name = format!("eth_getBlockByNumber({:?})", number);
                    let result = with_retry(&retry_config, &op_name, || async {
                        let builder = provider.get_block(BlockId::Number(number));
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
                    .await;
                    (number, result)
                }
            },
        )
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

        if hashes.is_empty() {
            return Ok(vec![]);
        }

        let provider = self.inner.provider().clone();
        let cost_per_receipt = ComputeUnitCost::GET_TRANSACTION_RECEIPT.cost();

        let results = self
            .execute_concurrent_ordered(hashes, cost_per_receipt, move |hash| {
                let provider = provider.clone();
                async move {
                    match provider.get_transaction_receipt(hash).await {
                        Ok(receipt) => receipt,
                        Err(e) => {
                            tracing::debug!("Skipping receipt for tx {:?}: {}", hash, e);
                            None
                        }
                    }
                }
            })
            .await?;

        Ok(results)
    }

    pub async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError> {
        if !self.config.batching_enabled {
            let mut results = Vec::with_capacity(filters.len());
            for filter in filters {
                results.push(self.get_logs(&filter).await?);
            }
            return Ok(results);
        }

        if filters.is_empty() {
            return Ok(vec![]);
        }

        let provider = self.inner.provider().clone();
        let retry_config = self.config.retry.clone();
        let cost_per_logs = ComputeUnitCost::GET_LOGS.cost();

        let results = self
            .execute_concurrent_ordered(filters, cost_per_logs, move |filter| {
                let provider = provider.clone();
                let retry_config = retry_config.clone();
                async move {
                    let op_name = format!(
                        "eth_getLogs(blocks {:?}-{:?})",
                        filter.get_from_block(),
                        filter.get_to_block()
                    );
                    with_retry(&retry_config, &op_name, || async {
                        provider
                            .get_logs(&filter)
                            .await
                            .map_err(|e| RpcError::ProviderError(error_chain(&e)))
                    })
                    .await
                }
            })
            .await?;

        // Collect results, failing on first error
        let mut logs = Vec::with_capacity(results.len());
        for result in results {
            logs.push(result?);
        }
        Ok(logs)
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

        if calls.is_empty() {
            return Ok(vec![]);
        }

        let provider = self.inner.provider().clone();
        let retry_config = self.config.retry.clone();
        let cost_per_call = ComputeUnitCost::ETH_CALL.cost();

        let results = self
            .execute_concurrent_ordered(calls, cost_per_call, move |(tx, block)| {
                let provider = provider.clone();
                let retry_config = retry_config.clone();
                async move {
                    let op_name = format!("eth_call(to={:?}, block={:?})", tx.to, block);
                    with_retry(&retry_config, &op_name, || async {
                        provider
                            .call(tx.clone())
                            .block(block)
                            .await
                            .map_err(|e| RpcError::ProviderError(error_chain(&e)))
                    })
                    .await
                }
            })
            .await?;

        Ok(results)
    }
}

#[async_trait]
impl RpcProvider for AlchemyClient {
    async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        AlchemyClient::get_block_number(self).await
    }

    async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        AlchemyClient::get_block(self, block_id, full_transactions).await
    }

    async fn get_transaction(&self, hash: B256) -> Result<Option<Transaction>, RpcError> {
        AlchemyClient::get_transaction(self, hash).await
    }

    async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        AlchemyClient::get_transaction_receipt(self, hash).await
    }

    async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        AlchemyClient::get_logs(self, filter).await
    }

    async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        AlchemyClient::get_balance(self, address, block).await
    }

    async fn get_code(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        AlchemyClient::get_code(self, address, block).await
    }

    async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        AlchemyClient::call(self, tx, block).await
    }

    async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        AlchemyClient::get_block_receipts(self, method_name, block_number).await
    }

    async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError> {
        AlchemyClient::get_blocks_batch(self, block_numbers, full_transactions).await
    }

    async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        AlchemyClient::get_transaction_receipts_batch(self, hashes).await
    }

    async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError> {
        AlchemyClient::get_logs_batch(self, filters).await
    }

    async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError> {
        AlchemyClient::call_batch(self, calls).await
    }
}

impl std::fmt::Debug for AlchemyClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlchemyClient")
            .field("config", &self.config)
            .finish()
    }
}
