# RPC Client Documentation

This module provides a generic Ethereum RPC client with support for rate limiting, batching, automatic retries with exponential backoff, and Alchemy-specific compute unit tracking.

## Overview

The RPC module consists of three main clients:

- **`RpcClient`** - A generic RPC client with optional request-based rate limiting
- **`AlchemyClient`** - An Alchemy-specific client with compute unit (CU) based rate limiting
- **`UnifiedRpcClient`** - A unified enum that automatically selects the appropriate client based on URL

Both clients use [alloy](https://github.com/alloy-rs/alloy) for the underlying RPC operations and [governor](https://github.com/bheisler/governor) for rate limiting with jitter support.

## Module Structure

```
src/rpc/
├── mod.rs      # Module exports
├── rpc.rs      # Generic RPC client with retry logic
├── alchemy.rs  # Alchemy-specific client with CU rate limiting
└── unified.rs  # Unified client that auto-selects based on URL
```

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

`RateLimitConfig` provides sensible defaults:

```rust
RateLimitConfig {
    requests_per_second: NonZeroU32::new(10).unwrap(),
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

Batch methods automatically chunk requests based on `max_batch_size` and execute them concurrently:

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
    max_retries: 3,
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

### Retry Logging

Retry attempts are logged at the `WARN` level with operation name and delay:

```
WARN RPC retry 1/3 for 'eth_getBlockByNumber(Number(12345678))' in 500ms
```

## Unified RPC Client

The `UnifiedRpcClient` provides a simple way to automatically select between `RpcClient` and `AlchemyClient` based on the URL.

### Basic Usage

```rust
use doppler_indexer_rs::rpc::UnifiedRpcClient;

// Automatically detects Alchemy URLs and uses AlchemyClient with 9500 CU/s
let client = UnifiedRpcClient::from_url("https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY")?;

// For non-Alchemy URLs, uses standard RpcClient
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

### Available Methods

`UnifiedRpcClient` exposes a subset of methods available on both clients:

- `get_block_number()`
- `get_blocks_batch(block_numbers, full_transactions)`
- `get_transaction_receipts_batch(hashes)`
- `get_block_receipts(method_name, block_number)`
- `get_block_receipts_concurrent(method_name, block_numbers, concurrency)`
- `call_batch(calls)`

## Alchemy Client

The `AlchemyClient` wraps `RpcClient` and adds compute unit (CU) based rate limiting, which is how Alchemy tracks API usage.

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
    .with_jitter(5, 50)
    .with_retry(RetryConfig::new(5).with_initial_delay(Duration::from_millis(200)));

let client = AlchemyClient::new(config)?;
```

### Default Alchemy Configuration

```rust
AlchemyConfig {
    url: Url::parse("http://localhost:8545").unwrap(),
    compute_units_per_second: NonZeroU32::new(330).unwrap(),  // Free tier
    max_batch_size: 100,
    batching_enabled: true,
    jitter_min_ms: 1,
    jitter_max_ms: 10,
    retry: RetryConfig::default(),
}
```

### Alchemy Tier Limits

| Tier | Compute Units/Second |
|------|---------------------|
| Free | 330 |
| Growth | 660 |
| Scale | Custom |

### How CU Rate Limiting Works

Before each request, the client consumes the appropriate number of compute units from the rate limiter. For batch operations, the total CU cost is calculated upfront:

```rust
// Single request: consumes 16 CU
let block = client.get_block_by_number(BlockNumberOrTag::Latest, false).await?;

// Batch of 10 blocks: consumes 160 CU (10 * 16)
let blocks = client.get_blocks_batch(block_numbers, false).await?;
```

The rate limiter ensures you don't exceed your CU/second limit, automatically throttling requests when needed.

## Error Handling

Both clients return `Result<T, RpcError>`:

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
governor = "0.10"
futures = "0.3"
tokio = { version = "1", features = ["full"] }
url = "2.5"
thiserror = "2.0"
```
