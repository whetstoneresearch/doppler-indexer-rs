# RPC Client Documentation

This module provides a generic Ethereum RPC client with support for rate limiting, batching, automatic retries with exponential backoff, and Alchemy-specific compute unit tracking.

## Overview

The RPC module consists of three main clients:

- **`RpcClient`** - A generic RPC client with optional request-based rate limiting (uses governor token bucket)
- **`AlchemyClient`** - An Alchemy-specific client with compute unit (CU) based rate limiting (uses sliding window)
- **`UnifiedRpcClient`** - A unified enum that automatically selects the appropriate client based on URL

All three clients implement the **`RpcProvider`** trait, enabling polymorphic usage.

The clients use [alloy](https://github.com/alloy-rs/alloy) for the underlying RPC operations. `RpcClient` uses [governor](https://github.com/bheisler/governor) for token bucket rate limiting, while `AlchemyClient` uses a custom `SlidingWindowRateLimiter` that accurately models Alchemy's 10-second window rate limiting.

## Module Structure

```
src/rpc/
├── mod.rs      # Module exports
├── rpc.rs      # Generic RPC client, RpcProvider trait, retry logic
├── alchemy.rs  # Alchemy-specific client with CU rate limiting, SlidingWindowRateLimiter
└── unified.rs  # Unified client that auto-selects based on URL
```

### Public Exports

```rust
// From mod.rs
pub use alchemy::{AlchemyClient, AlchemyConfig, ComputeUnitCost, SlidingWindowRateLimiter};
pub use rpc::{RetryConfig, RpcClient, RpcClientConfig, RpcError, RpcProvider};
pub use unified::UnifiedRpcClient;
```

Note: `RateLimitConfig` (for governor-based rate limiting in `RpcClient`) is defined in `rpc.rs` but not re-exported from the module root. To use it, either add it to the `pub use` in `mod.rs` or import directly from the submodule.

## RpcProvider Trait

All RPC clients implement the `RpcProvider` trait, which defines the common interface for Ethereum RPC operations:

```rust
use doppler_indexer_rs::rpc::RpcProvider;

// Use any client through the trait
async fn fetch_block_number(client: &dyn RpcProvider) -> Result<u64, RpcError> {
    client.get_block_number().await
}

// Or use generics
async fn fetch_blocks<P: RpcProvider>(
    client: &P,
    numbers: Vec<BlockNumberOrTag>,
) -> Result<Vec<Option<Block>>, RpcError> {
    client.get_blocks_batch(numbers, false).await
}
```

### Trait Methods

The `RpcProvider` trait includes:

- `get_block_number()` - Current block number
- `get_block(block_id, full_transactions)` - Get block by ID
- `get_block_by_number(number, full_transactions)` - Get block by number
- `get_transaction(hash)` - Get transaction by hash
- `get_transaction_receipt(hash)` - Get transaction receipt
- `get_logs(filter)` - Get logs matching filter
- `get_balance(address, block)` - Get account balance
- `get_code(address, block)` - Get contract code
- `call(tx, block)` - Execute eth_call
- `get_block_receipts(method_name, block_number)` - Get all receipts for a block
- `get_blocks_batch(block_numbers, full_transactions)` - Batch get blocks
- `get_transaction_receipts_batch(hashes)` - Batch get receipts
- `get_logs_batch(filters)` - Batch get logs
- `call_batch(calls)` - Batch execute eth_calls

## Generic RPC Client

### Basic Usage

```rust
use doppler_indexer_rs::rpc::{RpcClient, RpcClientConfig};
use url::Url;

// Simple initialization from URL string
let client = RpcClient::from_url("https://eth.drpc.org")?;

// Get the current block number
let block_number = client.get_block_number().await?;
println!("Current block: {}", block_number);
```

### Configuration

```rust
use doppler_indexer_rs::rpc::{RpcClient, RpcClientConfig, RateLimitConfig, RetryConfig};
use std::num::NonZeroU32;
use std::time::Duration;
use url::Url;

let url = Url::parse("https://eth.drpc.org")?;

let config = RpcClientConfig::new(url)
    .with_batch_size(50)           // Max requests per batch (default: 100)
    .with_batching(true)           // Enable/disable batching (default: true)
    .with_rate_limit(RateLimitConfig {
        requests_per_second: NonZeroU32::new(10).unwrap(),
        jitter_min_ms: 5,          // Minimum jitter delay
        jitter_max_ms: 50,         // Maximum jitter delay
    })
    .with_retry(RetryConfig {
        max_retries: 3,                           // Number of retry attempts
        initial_delay: Duration::from_millis(500), // First retry delay
        max_delay: Duration::from_secs(30),        // Maximum delay cap
        backoff_multiplier: 2.0,                   // Exponential backoff factor
    });

let client = RpcClient::new(config)?;
```

### Default Rate Limit Configuration

`RateLimitConfig` provides sensible defaults (using compile-time constants):

```rust
RateLimitConfig {
    requests_per_second: NonZeroU32::new(10).unwrap(),  // Compile-time constant
    jitter_min_ms: 5,
    jitter_max_ms: 50,
}
```

### Available Methods

#### Single Requests

```rust
// Get block number
let block_num: u64 = client.get_block_number().await?;

// Get block by ID (with or without full transactions)
let block = client.get_block(BlockId::Number(BlockNumberOrTag::Latest), false).await?;

// Get block by number
let block = client.get_block_by_number(BlockNumberOrTag::Number(12345678), true).await?;

// Get transaction by hash
let tx = client.get_transaction(tx_hash).await?;

// Get transaction receipt
let receipt = client.get_transaction_receipt(tx_hash).await?;

// Get logs with filter
let filter = Filter::new()
    .address(contract_address)
    .from_block(BlockNumberOrTag::Number(12345678))
    .to_block(BlockNumberOrTag::Number(12345778));
let logs = client.get_logs(&filter).await?;

// Get account balance
let balance = client.get_balance(address, Some(BlockId::latest())).await?;

// Get contract code
let code = client.get_code(contract_address, None).await?;

// Execute eth_call
let tx_request = TransactionRequest::default()
    .to(contract_address)
    .input(calldata.into());
let result = client.call(&tx_request, None).await?;
```

#### Batch Requests

Batch methods automatically chunk requests based on `max_batch_size` and execute them concurrently within each chunk using `futures::future::join_all`:

**Note:** `RpcClient` uses chunk-based batching, while `AlchemyClient` uses semaphore-bounded concurrency for better throughput with rate limiting.

```rust
// Fetch multiple blocks
let block_numbers = vec![
    BlockNumberOrTag::Number(1000000),
    BlockNumberOrTag::Number(1000001),
    BlockNumberOrTag::Number(1000002),
];
let blocks = client.get_blocks_batch(block_numbers, false).await?;

// Fetch multiple transaction receipts by hash
let tx_hashes = vec![hash1, hash2, hash3];
let receipts = client.get_transaction_receipts_batch(tx_hashes).await?;

// Fetch logs with multiple filters
let filters = vec![filter1, filter2, filter3];
let all_logs = client.get_logs_batch(filters).await?;

// Execute multiple eth_calls
let calls = vec![
    (tx_request1, BlockId::latest()),
    (tx_request2, BlockId::Number(BlockNumberOrTag::Number(12345678))),
];
let results = client.call_batch(calls).await?;
```

#### Block Receipts

Fetch all receipts for a block using `eth_getBlockReceipts` or `alchemy_getTransactionReceipts`:

```rust
// Get all receipts for a single block
let receipts = client.get_block_receipts(
    "eth_getBlockReceipts",  // or "alchemy_getTransactionReceipts"
    BlockNumberOrTag::Number(12345678)
).await?;

// Fetch receipts for multiple blocks concurrently
let block_numbers = vec![
    BlockNumberOrTag::Number(1000000),
    BlockNumberOrTag::Number(1000001),
    BlockNumberOrTag::Number(1000002),
];
let all_receipts = client.get_block_receipts_concurrent(
    "eth_getBlockReceipts",
    block_numbers,
    10  // concurrency limit
).await?;
```

### Rate Limiting

Rate limiting uses the governor crate with jitter to prevent thundering herd problems:

```rust
let config = RpcClientConfig::new(url)
    .with_rate_limit(RateLimitConfig {
        requests_per_second: NonZeroU32::new(25).unwrap(),
        jitter_min_ms: 10,
        jitter_max_ms: 100,
    });
```

When rate limiting is enabled, each request will wait until a token is available. Jitter adds a random delay between `jitter_min_ms` and `jitter_max_ms` to spread out requests.

### Disabling Batching

When batching is disabled, batch methods fall back to sequential execution:

```rust
let config = RpcClientConfig::new(url)
    .with_batching(false);
```

## Retry Logic

All RPC methods automatically retry on transient failures using exponential backoff with configurable parameters.

### Retry Configuration

```rust
use doppler_indexer_rs::rpc::RetryConfig;
use std::time::Duration;

let retry_config = RetryConfig::new(5)  // 5 retry attempts
    .with_initial_delay(Duration::from_millis(200))
    .with_max_delay(Duration::from_secs(60))
    .with_backoff_multiplier(2.0);
```

### Default Retry Configuration

```rust
RetryConfig {
    max_retries: 10,
    initial_delay: Duration::from_millis(500),
    max_delay: Duration::from_secs(30),
    backoff_multiplier: 2.0,
}
```

### Retryable Errors

Errors are automatically classified as retryable or permanent. Retryable errors include:

- **Transport errors** - Network connectivity issues
- **Rate limit errors** - 429 responses, "too many requests"
- **Server errors** - 502, 503, 504 responses
- **Timeout errors** - Connection timeouts, request timeouts
- **Temporary failures** - Errors containing "retry", "temporarily", "try again"

Non-retryable errors (like invalid URL or malformed requests) fail immediately without retry.

### Retry Logging and Error History

Retry attempts are logged at the `WARN` level with operation name and delay:

```
WARN RPC retry 1/10 for 'eth_getBlockByNumber(Number(12345678))' in 500ms
```

When all retries fail, the **full error history** is logged at the `ERROR` level:

```
ERROR RPC 'eth_getBlockByNumber(Number(12345678))' failed after 3 attempts. Error history: [attempt 1: connection reset; attempt 2: timeout; attempt 3: rate limit exceeded]
```

This helps diagnose intermittent issues by preserving all error messages, not just the final one.

## Unified RPC Client

The `UnifiedRpcClient` is an enum that automatically selects between `RpcClient` and `AlchemyClient` based on URL detection (checks for "alchemy" in the URL string).

```rust
pub enum UnifiedRpcClient {
    Standard(RpcClient),
    Alchemy(AlchemyClient),
}
```

### Basic Usage

```rust
use doppler_indexer_rs::rpc::UnifiedRpcClient;

// Automatically detects Alchemy URLs and uses AlchemyClient with 7500 CU/s
let client = UnifiedRpcClient::from_url("https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY")?;

// For non-Alchemy URLs, uses standard RpcClient (no rate limiting by default)
let client = UnifiedRpcClient::from_url("https://eth.drpc.org")?;
```

### Custom Alchemy CU Limit

```rust
// Specify custom compute units per second for Alchemy
let client = UnifiedRpcClient::from_url_with_alchemy_cu(
    "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
    660  // Growth tier limit
)?;
```

### With Shared Rate Limiter and Custom Concurrency

```rust
use std::sync::Arc;
use doppler_indexer_rs::rpc::{SlidingWindowRateLimiter, UnifiedRpcClient};

// Create shared rate limiter for account-level limiting
let shared_limiter = Arc::new(SlidingWindowRateLimiter::new(7500));

// Create clients with shared limiter and custom concurrency
let client = UnifiedRpcClient::from_url_with_options(
    "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
    7500,  // CU per second
    500,   // Max concurrent requests
    Some(shared_limiter.clone()),  // Shared limiter
)?;
```

Note: For non-Alchemy URLs, `from_url_with_options()` ignores the CU, concurrency, and limiter parameters and creates a standard `RpcClient`.

### Available Methods

`UnifiedRpcClient` implements the full `RpcProvider` trait plus additional methods:

**From RpcProvider trait:**
- `get_block_number()`
- `get_block(block_id, full_transactions)`
- `get_block_by_number(number, full_transactions)`
- `get_transaction(hash)`
- `get_transaction_receipt(hash)`
- `get_logs(filter)`
- `get_balance(address, block)`
- `get_code(address, block)`
- `call(tx, block)`
- `get_block_receipts(method_name, block_number)`
- `get_blocks_batch(block_numbers, full_transactions)`
- `get_transaction_receipts_batch(hashes)`
- `get_logs_batch(filters)`
- `call_batch(calls)`

**Additional methods (not in RpcProvider trait):**
- `get_blocks_streaming(block_numbers, full_transactions, result_tx)` - Streaming version that sends results via channel as they complete
- `get_block_receipts_concurrent(method_name, block_numbers, concurrency)` - Concurrent receipts (note: `concurrency` param is deprecated for AlchemyClient)
- `eth_call(to, data, block_number)` - Single eth_call convenience method

## Alchemy Client

The `AlchemyClient` wraps `RpcClient` and adds compute unit (CU) based rate limiting, which is how Alchemy tracks API usage.

### Environment Variables

The Alchemy client behavior can be configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ALCHEMY_CU_PER_SECOND` | 7500 | Compute units per second (match your Alchemy plan) |
| `RPC_CONCURRENCY` | 100 | Max concurrent in-flight RPC requests |
| `RPC_BATCH_SIZE` | from config | Override config's `rpc_batch_size` |

Example:
```bash
ALCHEMY_CU_PER_SECOND=7500 RPC_CONCURRENCY=500 cargo run
```

### Sliding Window Rate Limiter

The Alchemy client uses a **sliding window rate limiter** that accurately models Alchemy's rate limiting behavior:

- Alchemy measures usage over **10-second sliding windows** at 10x the per-second rate
- Unlike token bucket algorithms, you cannot "save up" compute units by being idle
- The limiter tracks exact timestamps of CU consumption and calculates available capacity
- Async-safe with `Mutex<VecDeque<(Instant, u32)>>` for history tracking

```rust
// Internal implementation
pub struct SlidingWindowRateLimiter {
    max_in_window: u32,                   // CU limit for 10-second window (cu_per_second * 10)
    window: Duration,                      // 10 seconds
    history: Mutex<VecDeque<(Instant, u32)>>,  // (timestamp, units) consumption history
}

impl SlidingWindowRateLimiter {
    pub fn new(max_per_second: u32) -> Self;

    /// Wait until we can consume the requested units, then record them.
    /// Returns immediately if there's capacity, otherwise waits.
    pub async fn acquire(&self, units: u32);

    /// Get current usage (for debugging/metrics)
    pub async fn current_usage_async(&self) -> u32;
}
```

### Shared Rate Limiter

When running multiple collectors (blocks, receipts, eth_calls), you can share a single rate limiter to enforce **account-level** rate limiting across all clients:

```rust
use std::sync::Arc;
use doppler_indexer_rs::rpc::{SlidingWindowRateLimiter, UnifiedRpcClient};

// Create shared limiter
let shared_limiter = Arc::new(SlidingWindowRateLimiter::new(7500));

// All clients share the same limiter
let blocks_client = UnifiedRpcClient::from_url_with_options(
    &rpc_url, 7500, 100, Some(shared_limiter.clone())
)?;
let receipts_client = UnifiedRpcClient::from_url_with_options(
    &rpc_url, 7500, 100, Some(shared_limiter.clone())
)?;
```

This ensures combined CU usage across all collectors stays within your Alchemy plan limits.

### Concurrent Execution with Semaphore

Batch operations use **non-blocking semaphore-bounded concurrency** via the internal `execute_concurrent_ordered()` method for maximum throughput:

1. **All tasks spawn immediately** using `JoinSet` - no blocking during task creation
2. Each task acquires a semaphore permit internally (limits concurrent in-flight requests)
3. Each task acquires CUs from the rate limiter individually
4. Results are collected, sorted by original index, and returned in order (or errors are propagated)

```rust
// Internal method signature (not public, but drives all batch operations)
async fn execute_concurrent_ordered<T, Req, F, Fut>(
    &self,
    requests: Vec<Req>,
    cost_per_request: u32,
    make_request: F,
) -> Result<Vec<T>, RpcError>
```

This approach provides:
- **Non-blocking task spawning** - 1000 requests spawn instantly, then compete for permits
- **Continuous rate limit acquisition** instead of chunk-based batching
- **Better throughput** by keeping the RPC pipeline full
- **Configurable concurrency** via `RPC_CONCURRENCY` environment variable
- **Order preservation** - results are sorted by index before returning

### Panic Handling

Concurrent batch methods (`execute_concurrent_ordered`) handle task panics gracefully:

- If any task panics, the error is logged and tracked
- The method returns `Err(RpcError::ProviderError("One or more concurrent tasks panicked"))`
- This prevents silent partial results where some requests never executed

### Streaming Execution

For pipelined collection, the `execute_streaming()` method sends results via channels as they complete, enabling immediate downstream processing without waiting for the entire batch:

```rust
use tokio::sync::mpsc;

// Start streaming block fetch (returns JoinHandle immediately)
let (tx, mut rx) = mpsc::channel(256);
let handle = client.get_blocks_streaming(block_numbers, false, tx);

// Process blocks as they arrive (in completion order, not request order)
while let Some((block_num, result)) = rx.recv().await {
    // Forward to downstream immediately
    process_block(result?);
}

// Wait for all requests to complete
handle.await?;
```

Key differences from `execute_concurrent_ordered()`:
- **Completion order** - Results are sent as they complete, not in original request order
- **Immediate processing** - Downstream can process results while other requests are in flight
- **Non-blocking return** - Returns a `JoinHandle` immediately, doesn't wait for completion

The underlying `execute_streaming()` method signature:

```rust
pub fn execute_streaming<T, Req, F, Fut>(
    &self,
    requests: Vec<Req>,
    cost_per_request: u32,
    result_tx: tokio::sync::mpsc::Sender<T>,
    make_request: F,
) -> tokio::task::JoinHandle<()>
```

### Deprecated Parameters

**`get_block_receipts_concurrent` concurrency parameter**: For `AlchemyClient`, the `concurrency` parameter is deprecated and ignored. Concurrency is controlled by `rpc_concurrency` in `AlchemyConfig`. The parameter is kept for API compatibility with `RpcClient`.

```rust
// For AlchemyClient, the concurrency parameter (10) is ignored
client.get_block_receipts_concurrent("eth_getBlockReceipts", block_numbers, 10).await?;

// Actual concurrency is controlled by config
let config = AlchemyConfig::new(url, 660)
    .with_rpc_concurrency(500);  // This controls concurrency
```

### Compute Unit Costs

Each RPC method has an associated compute unit cost:

| Method | Compute Units |
|--------|---------------|
| `eth_blockNumber` | 10 |
| `eth_getBlockByNumber` | 16 |
| `eth_getBlockByHash` | 16 |
| `eth_getTransactionByHash` | 17 |
| `eth_getTransactionReceipt` | 15 |
| `eth_getBlockReceipts` | 500 |
| `eth_getLogs` | 75 |
| `eth_getBalance` | 19 |
| `eth_getCode` | 19 |
| `eth_call` | 26 |
| `eth_getStorageAt` | 17 |

These costs are defined in `ComputeUnitCost` constants and are automatically consumed before each request.

### Basic Usage

```rust
use doppler_indexer_rs::rpc::{AlchemyClient, AlchemyConfig};
use url::Url;

// Quick initialization (330 CU/s is Alchemy's free tier limit)
let client = AlchemyClient::from_url(
    "https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY",
    330  // compute units per second
)?;

let block = client.get_block_number().await?;
```

### Configuration

```rust
use doppler_indexer_rs::rpc::{AlchemyClient, AlchemyConfig, RetryConfig};
use std::time::Duration;
use url::Url;

let url = Url::parse("https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY")?;

let config = AlchemyConfig::new(url, 660)  // Growth tier: 660 CU/s
    .with_batch_size(100)
    .with_batching(true)
    .with_rpc_concurrency(200)  // Max concurrent in-flight requests
    .with_retry(RetryConfig::new(5).with_initial_delay(Duration::from_millis(200)));

let client = AlchemyClient::new(config)?;
```

### Default Alchemy Configuration

```rust
AlchemyConfig {
    url: Url::parse("http://localhost:8545").expect("valid URL"),
    compute_units_per_second: NonZeroU32::new(330).unwrap(),  // Free tier (compile-time const)
    max_batch_size: 100,
    batching_enabled: true,
    retry: RetryConfig::default(),
    rpc_concurrency: 100,  // Max concurrent in-flight requests
}
```

**Important:** The default CU/s varies by constructor:
- `AlchemyConfig::default()`: 330 CU/s (Free tier)
- `UnifiedRpcClient::from_url()`: 7500 CU/s (Scale tier)

Use `ALCHEMY_CU_PER_SECOND` env var or explicit constructor parameters to match your actual Alchemy plan.

### Alchemy Tier Limits

| Tier | Compute Units/Second |
|------|---------------------|
| Free | 330 |
| Growth | 660 |
| Scale | 7500+ (Custom) |

Note: `UnifiedRpcClient::from_url()` defaults to 7500 CU/s, suitable for Scale tier. Use `ALCHEMY_CU_PER_SECOND` env var or `from_url_with_alchemy_cu()` to match your actual plan.

### How CU Rate Limiting Works

Before each request, the client consumes the appropriate number of compute units from the rate limiter. For batch operations, each request acquires CUs individually for continuous throughput:

```rust
// Single request: consumes 16 CU
let block = client.get_block_by_number(BlockNumberOrTag::Latest, false).await?;

// Batch of 10 blocks: each request acquires 16 CU individually
// Total: 160 CU, but acquired continuously as permits become available
let blocks = client.get_blocks_batch(block_numbers, false).await?;
```

The rate limiter ensures you don't exceed your CU/second limit, automatically throttling requests when needed.

## Error Handling

All clients return `Result<T, RpcError>`. Errors include the full error chain (including nested source errors) for better debugging via the internal `error_chain()` helper:

```rust
use doppler_indexer_rs::rpc::RpcError;

match client.get_block_number().await {
    Ok(block_num) => println!("Block: {}", block_num),
    Err(RpcError::ProviderError(msg)) => eprintln!("RPC error: {}", msg),
    Err(RpcError::InvalidUrl(msg)) => eprintln!("Invalid URL: {}", msg),
    Err(RpcError::RateLimitExceeded) => eprintln!("Rate limit exceeded"),
    Err(e) => eprintln!("Other error: {}", e),
}
```

### Error Types

| Error | Description | Retryable |
|-------|-------------|-----------|
| `RpcError::Transport` | Transport layer errors (network issues) | Yes |
| `RpcError::InvalidUrl` | URL parsing errors | No |
| `RpcError::RateLimitExceeded` | Rate limit exceeded | Yes |
| `RpcError::BatchError` | Batch operation failures | Depends on message |
| `RpcError::ProviderError` | Errors from the RPC provider | Depends on message |

### Checking Retryability

```rust
let error: RpcError = ...;
if error.is_retryable() {
    // Safe to retry after backoff
} else {
    // Permanent error, don't retry
}
```

The `is_retryable()` method checks for common transient error patterns:
- Network/connection errors: "connection", "timeout", "reset", "broken pipe", "eof"
- Rate limiting: "rate limit", "too many requests", "429"
- Server errors: "502", "503", "504", "internal server error", "service unavailable"
- Temporary failures: "temporarily", "try again", "retry"

## Accessing the Underlying Provider

Both clients expose access to the underlying alloy provider for advanced use cases:

```rust
// Generic client
let provider = client.provider();

// Alchemy client (access inner RpcClient first)
let provider = alchemy_client.inner().provider();

// Use provider directly for methods not wrapped by the client
let chain_id = provider.get_chain_id().await?;
```

## Thread Safety

Both `RpcClient` and `AlchemyClient` are designed to be used from multiple async tasks. The rate limiter uses `Arc` internally and is safe to share across tasks.

```rust
use std::sync::Arc;
use tokio::task;

let client = Arc::new(RpcClient::from_url("https://eth.drpc.org")?);

let handles: Vec<_> = (0..10).map(|i| {
    let client = Arc::clone(&client);
    task::spawn(async move {
        client.get_block_number().await
    })
}).collect();

for handle in handles {
    let result = handle.await?;
    println!("{:?}", result);
}
```

## Dependencies

The RPC module requires these dependencies in `Cargo.toml`:

```toml
[dependencies]
alloy = { version = "1.0", features = ["full", "provider-http", "rpc-types"] }
async-trait = "0.1"
governor = "0.10"
futures = "0.3"
tokio = { version = "1", features = ["full"] }
url = "2.5"
thiserror = "2.0"
```
