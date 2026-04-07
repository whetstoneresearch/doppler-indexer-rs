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

use crate::metrics::{
    chain_label_from_url, record_batch_size, set_semaphore_utilization, with_metrics, RpcMethod,
};
use crate::rpc::provider::{
    error_chain, with_retry, RetryConfig, RpcClient, RpcClientConfig, RpcError, RpcProvider,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputeUnitCost(pub u32);

#[allow(dead_code)]
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

    /// Calculate current usage within the sliding window.
    ///
    /// This counts entries strictly within the window (timestamp > cutoff).
    /// An entry exactly at the cutoff (i.e., exactly `window` duration old) is NOT counted
    /// because it is considered expired. This is consistent with `cleanup()` which removes
    /// entries at `timestamp <= cutoff`.
    ///
    /// Example with a 10-second window:
    /// - Entry at T=0, current time T=10: cutoff=0, entry is NOT counted (0 > 0 is false)
    /// - Entry at T=1, current time T=10: cutoff=0, entry IS counted (1 > 0 is true)
    fn current_usage(history: &VecDeque<(Instant, u32)>, now: Instant, window: Duration) -> u32 {
        let cutoff = now.checked_sub(window).unwrap_or(now);
        history
            .iter()
            .filter(|(ts, _)| *ts > cutoff)
            .map(|(_, units)| *units)
            .sum()
    }

    /// Remove expired entries from history.
    ///
    /// This removes entries at or before the cutoff (timestamp <= cutoff).
    /// An entry exactly at the cutoff (i.e., exactly `window` duration old) IS removed
    /// because it is considered expired. This is consistent with `current_usage()` which
    /// only counts entries with `timestamp > cutoff`.
    ///
    /// The boundary semantics are:
    /// - `cleanup()` removes entries where `timestamp <= cutoff` (expired)
    /// - `current_usage()` counts entries where `timestamp > cutoff` (active)
    /// - These are complementary: no entry is both removed and counted, and every entry
    ///   is either removed or counted.
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
    #[allow(dead_code)]
    pub async fn current_usage_async(&self) -> u32 {
        let history = self.history.lock().await;
        Self::current_usage(&history, Instant::now(), self.window)
    }

    /// Get the maximum compute units allowed in the sliding window.
    pub fn max_in_window(&self) -> u32 {
        self.max_in_window
    }

    /// Acquire compute units with metrics instrumentation.
    ///
    /// Wraps `acquire` with timing and reports the wait duration and CU usage
    /// to the metrics system.
    pub async fn acquire_with_metrics(&self, units: u32, chain: &str) {
        let start = Instant::now();
        self.acquire(units).await;
        let elapsed = start.elapsed();

        if elapsed > Duration::from_millis(1) {
            crate::metrics::record_rate_limit_wait(chain, elapsed.as_secs_f64());
        }

        let current = self.current_usage_async().await;
        crate::metrics::set_cu_usage(chain, current as f64, self.max_in_window as f64);
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

#[allow(dead_code)]
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
            url: Url::parse("http://localhost:8545").expect("default URL is a valid literal"),
            compute_units_per_second: DEFAULT_COMPUTE_UNITS_PER_SECOND,
            max_batch_size: 100,
            batching_enabled: true,
            retry: RetryConfig::default(),
            rpc_concurrency: 100,
        }
    }
}

#[derive(Clone)]
pub struct AlchemyClient {
    inner: Arc<RpcClient>,
    rate_limiter: Arc<SlidingWindowRateLimiter>,
    /// Shared semaphore across all clones — limits total concurrent in-flight RPC requests
    /// to prevent TCP connection exhaustion.
    rpc_semaphore: Arc<Semaphore>,
    config: AlchemyConfig,
}

/// Helper struct that captures concurrency control for batch operations.
/// Encapsulates semaphore-bounded parallelism and rate limiting.
struct ConcurrentExecutor {
    semaphore: Arc<Semaphore>,
    rate_limiter: Arc<SlidingWindowRateLimiter>,
    cost_per_request: u32,
    /// Maximum number of spawned tasks allowed in a JoinSet at once.
    /// Tasks beyond this limit are spawned only as earlier tasks complete,
    /// preventing unbounded task creation for large request sets.
    max_in_flight: usize,
    /// Chain label for metrics.
    chain: String,
}

impl ConcurrentExecutor {
    /// Spawn a task that acquires semaphore + rate limiter before executing.
    /// Returns indexed result for ordering.
    fn spawn_indexed<T, Fut>(
        &self,
        join_set: &mut tokio::task::JoinSet<(usize, T)>,
        idx: usize,
        fut: Fut,
    ) where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let semaphore = self.semaphore.clone();
        let rate_limiter = self.rate_limiter.clone();
        let cost = self.cost_per_request;
        let chain = self.chain.clone();

        join_set.spawn(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore should never be closed during operation");
            metrics::gauge!("rpc_semaphore_acquired", "chain" => chain.clone()).increment(1.0);
            rate_limiter.acquire_with_metrics(cost, &chain).await;
            metrics::counter!("rpc_individual_calls_total", "chain" => chain.clone()).increment(1);
            let result = fut.await;
            metrics::gauge!("rpc_semaphore_acquired", "chain" => chain.clone()).decrement(1.0);
            drop(permit);
            (idx, result)
        });
    }

    /// Spawn a task that acquires semaphore + rate limiter, then sends result to channel.
    fn spawn_streaming<T, Fut>(
        &self,
        join_set: &mut tokio::task::JoinSet<()>,
        result_tx: tokio::sync::mpsc::Sender<T>,
        fut: Fut,
    ) where
        T: Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let semaphore = self.semaphore.clone();
        let rate_limiter = self.rate_limiter.clone();
        let cost = self.cost_per_request;
        let chain = self.chain.clone();

        join_set.spawn(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore should never be closed during operation");
            metrics::gauge!("rpc_semaphore_acquired", "chain" => chain.clone()).increment(1.0);
            rate_limiter.acquire_with_metrics(cost, &chain).await;
            metrics::counter!("rpc_individual_calls_total", "chain" => chain.clone()).increment(1);
            let result = fut.await;
            metrics::gauge!("rpc_semaphore_acquired", "chain" => chain.clone()).decrement(1.0);
            drop(permit);
            let _ = result_tx.send(result).await;
        });
    }
}

#[allow(dead_code)]
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

        let rpc_semaphore = Arc::new(Semaphore::new(config.rpc_concurrency));

        Ok(Self {
            inner,
            rate_limiter,
            rpc_semaphore,
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
        max_batch_size: usize,
        shared_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    ) -> Result<Self, RpcError> {
        let url = Url::parse(url).map_err(|e| RpcError::InvalidUrl(e.to_string()))?;
        let config = AlchemyConfig::new(url, compute_units_per_second)
            .with_rpc_concurrency(rpc_concurrency)
            .with_batch_size(max_batch_size);
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

    /// Get the chain label for metrics, derived from the RPC URL.
    fn chain_label(&self) -> String {
        chain_label_from_url(&self.config.url)
    }

    /// Consume compute units, waiting if necessary.
    /// Uses sliding window rate limiting to prevent burst accumulation.
    async fn consume_compute_units(&self, cost: ComputeUnitCost) {
        let chain = self.chain_label();
        self.rate_limiter
            .acquire_with_metrics(cost.cost(), &chain)
            .await;
    }

    /// Consume a raw number of compute units, waiting if necessary.
    async fn consume_compute_units_raw(&self, units: u32) {
        let chain = self.chain_label();
        self.rate_limiter
            .acquire_with_metrics(units, &chain)
            .await;
    }

    /// Create a bounded concurrent executor with semaphore and rate limiter.
    /// This captures the common setup for both ordered and streaming execution.
    fn create_concurrent_executor(&self, cost_per_request: u32) -> ConcurrentExecutor {
        let chain = self.chain_label();
        set_semaphore_utilization(&chain, 0.0, self.config.rpc_concurrency as f64);
        ConcurrentExecutor {
            semaphore: self.rpc_semaphore.clone(),
            rate_limiter: self.rate_limiter.clone(),
            cost_per_request,
            max_in_flight: (self.config.rpc_concurrency * 2).max(1),
            chain,
        }
    }

    /// Create a concurrent executor with a caller-specified concurrency cap.
    /// The effective concurrency is the minimum of the requested value and
    /// the client's configured `rpc_concurrency`.
    fn create_concurrent_executor_with_concurrency(
        &self,
        cost_per_request: u32,
        concurrency: usize,
    ) -> ConcurrentExecutor {
        let effective = concurrency.min(self.config.rpc_concurrency);
        ConcurrentExecutor {
            semaphore: Arc::new(Semaphore::new(effective)),
            rate_limiter: self.rate_limiter.clone(),
            cost_per_request,
            max_in_flight: (effective * 2).max(1),
            chain: self.chain_label(),
        }
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
        let executor = self.create_concurrent_executor(cost_per_request);
        let make_request = Arc::new(make_request);
        let mut join_set = JoinSet::new();
        let mut requests_iter = requests.into_iter().enumerate().peekable();

        // Seed the initial batch up to the spawn window
        while requests_iter.peek().is_some() && join_set.len() < executor.max_in_flight {
            let (idx, request) = requests_iter.next().unwrap();
            let make_request = make_request.clone();
            executor.spawn_indexed(
                &mut join_set,
                idx,
                async move { make_request(request).await },
            );
        }

        // Collect results and spawn replacements as tasks complete
        let mut indexed_results: Vec<(usize, T)> = Vec::with_capacity(num_requests);
        let mut had_panic = false;
        let batch_start = tokio::time::Instant::now();
        let log_interval = num_requests / 4; // Log at 25%, 50%, 75%

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((idx, value)) => indexed_results.push((idx, value)),
                Err(e) => {
                    tracing::error!("Task panicked in execute_concurrent_ordered: {:?}", e);
                    had_panic = true;
                }
            }

            if log_interval > 0 && indexed_results.len() % log_interval == 0 && indexed_results.len() < num_requests {
                tracing::debug!(
                    "Batch progress: {}/{} ({:.0}%) in {:.1}s",
                    indexed_results.len(),
                    num_requests,
                    indexed_results.len() as f64 / num_requests as f64 * 100.0,
                    batch_start.elapsed().as_secs_f64()
                );
            }

            // Spawn replacements to keep the pipeline full
            while requests_iter.peek().is_some() && join_set.len() < executor.max_in_flight {
                let (idx, request) = requests_iter.next().unwrap();
                let make_request = make_request.clone();
                executor.spawn_indexed(
                    &mut join_set,
                    idx,
                    async move { make_request(request).await },
                );
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

        let executor = self.create_concurrent_executor(cost_per_request);
        let make_request = Arc::new(make_request);

        tokio::spawn(async move {
            if requests.is_empty() {
                return;
            }

            let mut join_set = JoinSet::new();
            let mut requests_iter = requests.into_iter().peekable();

            // Seed the initial batch up to the spawn window
            while requests_iter.peek().is_some() && join_set.len() < executor.max_in_flight {
                let request = requests_iter.next().unwrap();
                let make_request = make_request.clone();
                let result_tx = result_tx.clone();
                executor.spawn_streaming(&mut join_set, result_tx, async move {
                    make_request(request).await
                });
            }

            // Wait for tasks to complete, spawning replacements to keep the pipeline full
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    tracing::error!("Task panicked in execute_streaming: {:?}", e);
                }

                // Spawn replacements
                while requests_iter.peek().is_some() && join_set.len() < executor.max_in_flight {
                    let request = requests_iter.next().unwrap();
                    let make_request = make_request.clone();
                    let result_tx = result_tx.clone();
                    executor.spawn_streaming(&mut join_set, result_tx, async move {
                        make_request(request).await
                    });
                }
            }
        })
    }

    /// Execute a batch operation with metrics, handling common patterns:
    /// - Empty input returns empty output
    /// - Batching disabled falls back to sequential execution
    /// - Metrics are recorded on success/failure
    ///
    /// The `make_request` closure is called for each item in the batch when batching is enabled.
    /// The `fallback` closure is called when batching is disabled.
    async fn execute_batch_with_metrics<T, Req, ReqF, ReqFut, FallbackF, FallbackFut>(
        &self,
        method: RpcMethod,
        requests: Vec<Req>,
        cost_per_request: u32,
        make_request: ReqF,
        fallback: FallbackF,
    ) -> Result<Vec<T>, RpcError>
    where
        T: Send + 'static,
        Req: Send + 'static,
        ReqF: Fn(Req) -> ReqFut + Send + Sync + 'static,
        ReqFut: Future<Output = T> + Send,
        FallbackF: FnOnce(Vec<Req>) -> FallbackFut,
        FallbackFut: Future<Output = Result<Vec<T>, RpcError>>,
    {
        let chain = self.chain_label();
        with_metrics(method, &chain, || async {
            if requests.is_empty() {
                return Ok(vec![]);
            }

            record_batch_size(&chain, method.as_str(), requests.len());

            if !self.config.batching_enabled {
                return fallback(requests).await;
            }

            self.execute_concurrent_ordered(requests, cost_per_request, make_request)
                .await
        })
        .await
    }

    /// Execute a batch operation where each request returns `Result<T, RpcError>`.
    /// All results are collected and the first error causes the entire batch to fail.
    async fn execute_batch_collecting_results<T, Req, ReqF, ReqFut, FallbackF, FallbackFut>(
        &self,
        method: RpcMethod,
        requests: Vec<Req>,
        cost_per_request: u32,
        make_request: ReqF,
        fallback: FallbackF,
    ) -> Result<Vec<T>, RpcError>
    where
        T: Send + 'static,
        Req: Send + 'static,
        ReqF: Fn(Req) -> ReqFut + Send + Sync + 'static,
        ReqFut: Future<Output = Result<T, RpcError>> + Send,
        FallbackF: FnOnce(Vec<Req>) -> FallbackFut,
        FallbackFut: Future<Output = Result<Vec<T>, RpcError>>,
    {
        let chain = self.chain_label();
        with_metrics(method, &chain, || async {
            if requests.is_empty() {
                return Ok(vec![]);
            }

            record_batch_size(&chain, method.as_str(), requests.len());

            if !self.config.batching_enabled {
                return fallback(requests).await;
            }

            let results = self
                .execute_concurrent_ordered(requests, cost_per_request, make_request)
                .await?;

            // Collect results, failing on first error
            let mut collected = Vec::with_capacity(results.len());
            for result in results {
                collected.push(result?);
            }
            Ok(collected)
        })
        .await
    }

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        let chain = self.chain_label();
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();
        self.consume_compute_units(ComputeUnitCost::BLOCK_NUMBER)
            .await;
        with_metrics(RpcMethod::GetBlockNumber, &chain, || async {
            with_retry(&retry_config, "get_block_number", &chain, || async {
                provider
                    .get_block_number()
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            })
            .await
        })
        .await
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        let chain = self.chain_label();
        let cost = match block_id {
            BlockId::Hash(_) => ComputeUnitCost::GET_BLOCK_BY_HASH,
            BlockId::Number(_) => ComputeUnitCost::GET_BLOCK_BY_NUMBER,
        };
        let op_name = format!("eth_getBlockByNumber({:?})", block_id);
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();

        self.consume_compute_units(cost).await;
        with_metrics(RpcMethod::GetBlock, &chain, || async {
            with_retry(&retry_config, &op_name, &chain, || async {
                let builder = provider.get_block(block_id);
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
        let chain = self.chain_label();
        let op_name = format!("eth_getTransactionByHash({:?})", hash);
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();
        self.consume_compute_units(ComputeUnitCost::GET_TRANSACTION_BY_HASH)
            .await;
        with_metrics(RpcMethod::GetTransaction, &chain, || async {
            with_retry(&retry_config, &op_name, &chain, || async {
                provider
                    .get_transaction_by_hash(hash)
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            })
            .await
        })
        .await
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        let chain = self.chain_label();
        let op_name = format!("eth_getTransactionReceipt({:?})", hash);
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();
        self.consume_compute_units(ComputeUnitCost::GET_TRANSACTION_RECEIPT)
            .await;
        with_metrics(RpcMethod::GetTransactionReceipt, &chain, || async {
            with_retry(&retry_config, &op_name, &chain, || async {
                provider
                    .get_transaction_receipt(hash)
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            })
            .await
        })
        .await
    }

    pub async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        let chain = self.chain_label();
        let inner = self.inner.clone();
        let method_name = method_name.to_string();
        // Note: inner.get_block_receipts already has retry logic
        self.consume_compute_units(ComputeUnitCost::GET_BLOCK_RECEIPTS)
            .await;
        with_metrics(RpcMethod::GetBlockReceipts, &chain, || async {
            inner.get_block_receipts(&method_name, block_number).await
        })
        .await
    }

    /// Get block receipts for multiple blocks concurrently.
    ///
    /// # Arguments
    /// * `method_name` - The RPC method name (e.g., "eth_getBlockReceipts")
    /// * `block_numbers` - Block numbers to fetch receipts for
    /// * `concurrency` - Maximum concurrency for this batch. The effective concurrency
    ///   is the minimum of this value and the client's configured `rpc_concurrency`.
    pub async fn get_block_receipts_concurrent(
        &self,
        method_name: &str,
        block_numbers: Vec<BlockNumberOrTag>,
        concurrency: usize,
    ) -> Result<Vec<Vec<Option<TransactionReceipt>>>, RpcError> {
        let chain = self.chain_label();
        let inner = self.inner.clone();
        let method_name = method_name.to_string();

        with_metrics(RpcMethod::GetBlockReceiptsConcurrent, &chain, || async {
            if block_numbers.is_empty() {
                return Ok(vec![]);
            }

            let executor = self.create_concurrent_executor_with_concurrency(
                ComputeUnitCost::GET_BLOCK_RECEIPTS.cost(),
                concurrency,
            );
            let make_request = Arc::new(move |block_number: BlockNumberOrTag| {
                let inner = inner.clone();
                let method_name = method_name.clone();
                async move {
                    inner.get_block_receipts(&method_name, block_number).await
                }
            });

            let num_requests = block_numbers.len();
            let mut join_set = tokio::task::JoinSet::new();
            let mut requests_iter = block_numbers.into_iter().enumerate().peekable();

            // Seed initial batch
            while requests_iter.peek().is_some() && join_set.len() < executor.max_in_flight {
                let (idx, request) = requests_iter.next().unwrap();
                let make_request = make_request.clone();
                executor.spawn_indexed(
                    &mut join_set,
                    idx,
                    async move { make_request(request).await },
                );
            }

            let mut indexed_results: Vec<(usize, Result<Vec<Option<TransactionReceipt>>, RpcError>)> =
                Vec::with_capacity(num_requests);

            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok((idx, value)) => indexed_results.push((idx, value)),
                    Err(e) => {
                        return Err(RpcError::ProviderError(format!(
                            "Task panicked in get_block_receipts_concurrent: {:?}", e
                        )));
                    }
                }

                while requests_iter.peek().is_some() && join_set.len() < executor.max_in_flight {
                    let (idx, request) = requests_iter.next().unwrap();
                    let make_request = make_request.clone();
                    executor.spawn_indexed(
                        &mut join_set,
                        idx,
                        async move { make_request(request).await },
                    );
                }
            }

            indexed_results.sort_by_key(|(idx, _)| *idx);
            let mut collected = Vec::with_capacity(indexed_results.len());
            for (_, result) in indexed_results {
                collected.push(result?);
            }
            Ok(collected)
        })
        .await
    }

    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        let chain = self.chain_label();
        let filter = filter.clone();
        let op_name = format!(
            "eth_getLogs(blocks {:?}-{:?})",
            filter.get_from_block(),
            filter.get_to_block()
        );
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();
        self.consume_compute_units(ComputeUnitCost::GET_LOGS).await;
        with_metrics(RpcMethod::GetLogs, &chain, || async {
            with_retry(&retry_config, &op_name, &chain, || async {
                provider
                    .get_logs(&filter)
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            })
            .await
        })
        .await
    }

    pub async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        let chain = self.chain_label();
        let op_name = format!("eth_getBalance({:?}, {:?})", address, block);
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();
        self.consume_compute_units(ComputeUnitCost::GET_BALANCE)
            .await;
        with_metrics(RpcMethod::GetBalance, &chain, || async {
            with_retry(&retry_config, &op_name, &chain, || async {
                provider
                    .get_balance(address)
                    .block_id(block.unwrap_or(BlockId::latest()))
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            })
            .await
        })
        .await
    }

    pub async fn get_code(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        let chain = self.chain_label();
        let op_name = format!("eth_getCode({:?}, {:?})", address, block);
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();
        self.consume_compute_units(ComputeUnitCost::GET_CODE).await;
        with_metrics(RpcMethod::GetCode, &chain, || async {
            with_retry(&retry_config, &op_name, &chain, || async {
                provider
                    .get_code_at(address)
                    .block_id(block.unwrap_or(BlockId::latest()))
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            })
            .await
        })
        .await
    }

    pub async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        let chain = self.chain_label();
        let tx = tx.clone();
        let op_name = format!("eth_call(to={:?}, block={:?})", tx.to, block);
        let retry_config = self.config.retry.clone();
        let provider = self.inner.provider().clone();
        self.consume_compute_units(ComputeUnitCost::ETH_CALL).await;
        with_metrics(RpcMethod::EthCall, &chain, || async {
            with_retry(&retry_config, &op_name, &chain, || async {
                provider
                    .call(tx.clone())
                    .block(block.unwrap_or(BlockId::latest()))
                    .await
                    .map_err(|e| RpcError::ProviderError(error_chain(&e)))
            })
            .await
        })
        .await
    }

    pub async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError> {
        let provider = self.inner.provider().clone();
        let retry_config = self.config.retry.clone();
        let chain = self.chain_label();

        self.execute_batch_collecting_results(
            RpcMethod::GetBlocksBatch,
            block_numbers,
            ComputeUnitCost::GET_BLOCK_BY_NUMBER.cost(),
            move |number| {
                let provider = provider.clone();
                let retry_config = retry_config.clone();
                let chain = chain.clone();
                async move {
                    let op_name = format!("eth_getBlockByNumber({:?})", number);
                    with_retry(&retry_config, &op_name, &chain, || async {
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
            },
            |numbers| async move {
                let mut results = Vec::with_capacity(numbers.len());
                for number in numbers {
                    results.push(self.get_block_by_number(number, full_transactions).await?);
                }
                Ok(results)
            },
        )
        .await
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
        let chain = self.chain_label();

        self.execute_streaming(block_numbers, cost_per_block, result_tx, move |number| {
            let provider = provider.clone();
            let retry_config = retry_config.clone();
            let chain = chain.clone();
            async move {
                let op_name = format!("eth_getBlockByNumber({:?})", number);
                let result = with_retry(&retry_config, &op_name, &chain, || async {
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
        })
    }

    pub async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        let provider = self.inner.provider().clone();

        // This method swallows errors and returns None for failed receipts,
        // so we use execute_batch_with_metrics (no error unwrapping)
        self.execute_batch_with_metrics(
            RpcMethod::GetTransactionReceiptsBatch,
            hashes,
            ComputeUnitCost::GET_TRANSACTION_RECEIPT.cost(),
            move |hash| {
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
            },
            |hashes| async move {
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
                Ok(results)
            },
        )
        .await
    }

    pub async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError> {
        let provider = self.inner.provider().clone();
        let retry_config = self.config.retry.clone();
        let chain = self.chain_label();

        self.execute_batch_collecting_results(
            RpcMethod::GetLogsBatch,
            filters,
            ComputeUnitCost::GET_LOGS.cost(),
            move |filter| {
                let provider = provider.clone();
                let retry_config = retry_config.clone();
                let chain = chain.clone();
                async move {
                    let op_name = format!(
                        "eth_getLogs(blocks {:?}-{:?})",
                        filter.get_from_block(),
                        filter.get_to_block()
                    );
                    with_retry(&retry_config, &op_name, &chain, || async {
                        provider
                            .get_logs(&filter)
                            .await
                            .map_err(|e| RpcError::ProviderError(error_chain(&e)))
                    })
                    .await
                }
            },
            |filters| async move {
                let mut results = Vec::with_capacity(filters.len());
                for filter in filters {
                    results.push(self.get_logs(&filter).await?);
                }
                Ok(results)
            },
        )
        .await
    }

    pub async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError> {
        let provider = self.inner.provider().clone();
        let retry_config = self.config.retry.clone();
        let chain = self.chain_label();

        // call_batch returns Vec<Result<Bytes, RpcError>> directly, so we use
        // execute_batch_with_metrics (no result unwrapping)
        self.execute_batch_with_metrics(
            RpcMethod::EthCallBatch,
            calls,
            ComputeUnitCost::ETH_CALL.cost(),
            move |(tx, block)| {
                let provider = provider.clone();
                let retry_config = retry_config.clone();
                let chain = chain.clone();
                async move {
                    let op_name = format!("eth_call(to={:?}, block={:?})", tx.to, block);
                    with_retry(&retry_config, &op_name, &chain, || async {
                        provider
                            .call(tx.clone())
                            .block(block)
                            .await
                            .map_err(|e| RpcError::ProviderError(error_chain(&e)))
                    })
                    .await
                }
            },
            |calls| async move {
                let mut results = Vec::with_capacity(calls.len());
                for (tx, block) in calls {
                    results.push(self.call(&tx, Some(block)).await);
                }
                Ok(results)
            },
        )
        .await
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

    async fn get_code(&self, address: Address, block: Option<BlockId>) -> Result<Bytes, RpcError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that an entry exactly at the window boundary is considered expired.
    ///
    /// This test verifies the boundary semantics of SlidingWindowRateLimiter:
    /// - An entry exactly at the cutoff (timestamp == cutoff) should be expired
    /// - cleanup() removes entries at timestamp <= cutoff
    /// - current_usage() counts entries at timestamp > cutoff
    /// - Both methods agree: boundary entries are expired and not counted
    ///
    /// Note: This test directly tests the static methods to avoid needing real-time sleeps,
    /// since SlidingWindowRateLimiter uses std::time::Instant which is not affected by
    /// tokio's time mocking.
    #[test]
    fn test_boundary_entry_is_expired() {
        let now = Instant::now();
        let window = Duration::from_secs(10);

        // Create history with an entry exactly at the cutoff boundary
        // If now = T+10 and window = 10s, then cutoff = T+0
        // An entry at T+0 should NOT be counted (0 > 0 is false)
        let mut history: VecDeque<(Instant, u32)> = VecDeque::new();

        // Entry exactly at the cutoff (now - window)
        let entry_time = now.checked_sub(window).unwrap();
        history.push_back((entry_time, 500));

        // current_usage should return 0 (entry at boundary is expired)
        let usage = SlidingWindowRateLimiter::current_usage(&history, now, window);
        assert_eq!(
            usage, 0,
            "Entry at exactly the window boundary (timestamp == cutoff) should NOT be counted"
        );

        // cleanup should remove the entry
        SlidingWindowRateLimiter::cleanup(&mut history, now, window);
        assert!(
            history.is_empty(),
            "Entry at exactly the window boundary should be removed by cleanup"
        );
    }

    /// Test that an entry just inside the window (1ms after cutoff) is still counted.
    #[test]
    fn test_entry_inside_window_is_counted() {
        let now = Instant::now();
        let window = Duration::from_secs(10);

        let mut history: VecDeque<(Instant, u32)> = VecDeque::new();

        // Entry 1ms after the cutoff (still within window)
        let entry_time = now
            .checked_sub(window)
            .unwrap()
            .checked_add(Duration::from_millis(1))
            .unwrap();
        history.push_back((entry_time, 500));

        // current_usage should return 500 (entry just inside window)
        let usage = SlidingWindowRateLimiter::current_usage(&history, now, window);
        assert_eq!(usage, 500, "Entry 1ms inside the window should be counted");

        // cleanup should NOT remove the entry
        SlidingWindowRateLimiter::cleanup(&mut history, now, window);
        assert_eq!(
            history.len(),
            1,
            "Entry inside the window should NOT be removed by cleanup"
        );
    }

    /// Test that cleanup and current_usage are consistent - every entry is either
    /// removed by cleanup or counted by current_usage, never both, never neither.
    #[test]
    fn test_cleanup_and_usage_consistency() {
        let now = Instant::now();
        let window = Duration::from_secs(10);

        let mut history: VecDeque<(Instant, u32)> = VecDeque::new();

        // Add entries at various times relative to the cutoff
        let cutoff = now.checked_sub(window).unwrap();

        // Entry 1: 5 seconds before cutoff (should be removed, not counted)
        history.push_back((cutoff.checked_sub(Duration::from_secs(5)).unwrap(), 100));
        // Entry 2: exactly at cutoff (should be removed, not counted)
        history.push_back((cutoff, 200));
        // Entry 3: 1ms after cutoff (should NOT be removed, SHOULD be counted)
        history.push_back((cutoff.checked_add(Duration::from_millis(1)).unwrap(), 300));
        // Entry 4: 5 seconds after cutoff (should NOT be removed, SHOULD be counted)
        history.push_back((cutoff.checked_add(Duration::from_secs(5)).unwrap(), 400));

        // current_usage should only count entries 3 and 4
        let usage = SlidingWindowRateLimiter::current_usage(&history, now, window);
        assert_eq!(
            usage,
            300 + 400,
            "Only entries strictly after cutoff should be counted"
        );

        // cleanup should remove entries 1 and 2
        SlidingWindowRateLimiter::cleanup(&mut history, now, window);
        assert_eq!(
            history.len(),
            2,
            "Cleanup should remove entries at or before cutoff"
        );

        // After cleanup, current_usage should still return the same value
        let usage_after = SlidingWindowRateLimiter::current_usage(&history, now, window);
        assert_eq!(
            usage_after, usage,
            "Usage should be the same before and after cleanup"
        );
    }
}
