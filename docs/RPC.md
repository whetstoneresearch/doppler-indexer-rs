# RPC Client Documentation

This module provides a generic Ethereum RPC client with support for rate limiting, batching, and Alchemy-specific compute unit tracking.

## Overview

The RPC module consists of two main clients:

- **`RpcClient`** - A generic RPC client with optional request-based rate limiting
- **`AlchemyClient`** - An Alchemy-specific client with compute unit (CU) based rate limiting

Both clients use [alloy](https://github.com/alloy-rs/alloy) for the underlying RPC operations and [governor](https://github.com/bheisler/governor) for rate limiting with jitter support.

## Module Structure

```
src/rpc/
├── mod.rs      # Module exports
├── rpc.rs      # Generic RPC client
└── alchemy.rs  # Alchemy-specific client with CU rate limiting
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
use doppler_indexer_rs::rpc::{RpcClient, RpcClientConfig, RateLimitConfig};
use std::num::NonZeroU32;
use url::Url;

let url = Url::parse("https://eth.drpc.org")?;

let config = RpcClientConfig::new(url)
    .with_batch_size(50)           // Max requests per batch (default: 100)
    .with_batching(true)           // Enable/disable batching (default: true)
    .with_rate_limit(RateLimitConfig {
        requests_per_second: NonZeroU32::new(10).unwrap(),
        jitter_min_ms: 5,          // Minimum jitter delay
        jitter_max_ms: 50,         // Maximum jitter delay
    });

let client = RpcClient::new(config)?;
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

// Fetch multiple transaction receipts
let tx_hashes = vec![hash1, hash2, hash3];
let receipts = client.get_transaction_receipts_batch(tx_hashes).await?;

// Fetch logs with multiple filters
let filters = vec![filter1, filter2, filter3];
let all_logs = client.get_logs_batch(filters).await?;
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
| `eth_getLogs` | 75 |
| `eth_getBalance` | 19 |
| `eth_getCode` | 19 |
| `eth_call` | 26 |
| `eth_getStorageAt` | 17 |

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
use doppler_indexer_rs::rpc::{AlchemyClient, AlchemyConfig};
use url::Url;

let url = Url::parse("https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY")?;

let config = AlchemyConfig::new(url, 660)  // Growth tier: 660 CU/s
    .with_batch_size(100)
    .with_batching(true)
    .with_jitter(5, 50);

let client = AlchemyClient::new(config)?;
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

- `RpcError::Transport` - Transport layer errors
- `RpcError::InvalidUrl` - URL parsing errors
- `RpcError::RateLimitExceeded` - Rate limit exceeded (unused currently, governor handles waiting)
- `RpcError::BatchError` - Batch operation failures
- `RpcError::ProviderError` - Errors from the RPC provider

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
