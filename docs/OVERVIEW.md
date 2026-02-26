# Doppler Indexer

Doppler Indexer is a high-performance Ethereum blockchain indexer written in Rust. It collects raw blockchain data, tracks dynamically created contracts, reads on-chain state, decodes events and call results into typed Parquet files, and supports custom transformations that write to PostgreSQL.

## Features

- **Raw Data Collection** - Fetches blocks, transaction receipts, and logs from any EVM-compatible chain
- **Factory Contract Tracking** - Automatically discovers and indexes dynamically created contracts (e.g., Uniswap pairs, token deployments)
- **Historical State Reading** - Makes `eth_call` requests at historical block heights to track on-chain state over time
- **Event-Triggered Calls** - Execute eth_calls in response to specific events (e.g., read pool state after a Swap)
- **Event Decoding** - Parses raw log data into typed columns using Solidity event signatures
- **Transformations** - Custom Rust handlers process decoded data and write to PostgreSQL with per-handler progress tracking
- **Parallel Processing** - Async task-based architecture with configurable concurrency for maximum throughput
- **Resumable** - Automatic catchup on restart, skipping already-collected ranges
- **Flexible Output** - Parquet files for analytics, PostgreSQL for application data

## Project Structure

```
src/
├── main.rs              # Entry point, orchestrates the pipeline
├── db/                  # Database layer
│   ├── pool.rs          # PostgreSQL connection pool (deadpool_postgres)
│   ├── types.rs         # DbValue, DbOperation, WhereClause types
│   ├── migrations.rs    # Migration runner
│   └── error.rs         # Database error types
├── rpc/                 # RPC client layer
│   ├── unified.rs       # UnifiedRpcClient (Standard or Alchemy)
│   ├── alchemy.rs       # Alchemy client with rate limiting
│   └── rpc.rs           # Standard RPC client
├── raw_data/            # Data collection and decoding
│   ├── historical/      # Collection pipeline
│   │   ├── catchup/     # Catchup phase (process existing ranges)
│   │   ├── current/     # Live phase (process new blocks)
│   │   ├── blocks.rs    # Block collection
│   │   ├── receipts.rs  # Receipt collection
│   │   ├── logs.rs      # Log collection
│   │   ├── eth_calls.rs # ETH call collection
│   │   └── factories.rs # Factory contract discovery
│   └── decoding/        # ABI decoding
│       ├── logs.rs      # Log decoder
│       ├── eth_calls.rs # ETH call decoder
│       ├── catchup/     # Catchup decoding
│       └── current/     # Live decoding
├── transformations/     # Transformation system
│   ├── traits.rs        # TransformationHandler, EventHandler, EthCallHandler traits
│   ├── registry.rs      # Handler registration
│   ├── engine.rs        # Transformation engine (orchestrates handlers)
│   ├── context.rs       # TransformationContext, DecodedEvent, DecodedCall
│   ├── historical.rs    # HistoricalDataReader (parquet queries)
│   ├── event/           # Event handlers
│   └── eth_call/        # ETH call handlers
└── types/               # Type definitions
    ├── config/          # Configuration types
    │   ├── indexer.rs   # IndexerConfig
    │   ├── chain.rs     # ChainConfig
    │   ├── contract.rs  # Contract definitions
    │   ├── eth_call.rs  # ETH call config (frequency, triggers)
    │   └── ...
    └── shared/          # Shared types
```

## Architecture

The indexer runs as multiple concurrent async tasks connected by channels:

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Blocks    │────►│    Receipts      │────►│    Logs     │────► Log Decoder ────┐
└──────┬──────┘     └────────┬─────────┘     └─────────────┘                      │
       │                     │                                                     │
       │                     ├────────────────►┌─────────────┐                     │
       │                     │   ◄─────────────│  Factories  │────► Decoder        │
       │                     │    (recollect)  └─────────────┘                     │
       │                     │                                                     ▼
       │                     └─────────────────►┌──────────────────┐       ┌──────────────┐
       │                    (event triggers)    │  Event-Triggered │       │              │
       └─────────────────────────────────────►  │    ETH Calls     │──────►│Transformations│──► PostgreSQL
                                                └──────────────────┘       │    Engine    │
                                                        │                  └──────────────┘
                                              ┌─────────┴──────────┐
                                              │   ETH Call Decoder │
                                              └────────────────────┘
```

Each collector runs independently, maximizing throughput while respecting RPC rate limits. The optional transformation layer processes decoded data and writes to PostgreSQL.

**Recollection Channel**: When the factory collector encounters a corrupted log file during catchup, it deletes the file and sends a recollection request back to the receipt collector. The receipt collector then re-fetches the data and sends it through the normal channels, ensuring data integrity without manual intervention.

### Two-Phase Processing

Each collector operates in two phases:
1. **Catchup Phase** - Process existing parquet files and fill gaps
2. **Current Phase** - Process new blocks as they arrive via channels

Synchronization barriers (oneshot channels) ensure proper ordering:
- Factory catchup completes before factory-dependent eth_call catchup
- ETH call catchup completes before decode catchup
- Decode catchup completes before transformation engine catchup

## Output Structure

```
data/
├── raw/{chain}/
│   ├── blocks/           # Block headers
│   ├── receipts/         # Transaction receipts
│   ├── logs/             # Raw event logs
│   └── eth_calls/        # Raw eth_call results
│       ├── {contract}/
│       │   ├── {function}/
│       │   └── once/     # One-time calls (name, symbol, etc.)
│       └── {collection}/ # Factory contract calls
│
└── derived/{chain}/
    ├── factories/        # Discovered factory addresses
    │   └── {collection}/
    └── decoded/
        ├── logs/         # Decoded events
        │   └── {contract}/{event}/
        └── eth_calls/    # Decoded call results
            └── {contract}/{function}/

migrations/
├── tables/              # Core table migrations
│   ├── 001_handler_progress.sql
│   └── ...
└── handlers/            # Per-handler migrations
    └── {handler_name}/
        └── {migration}.sql
```

When transformations are enabled, data is also written to PostgreSQL tables defined in your migrations. Each handler declares its own migration paths.

## Transformation System

The transformation system processes decoded events and eth_calls, transforming them into PostgreSQL records.

### Key Concepts

**TransformationHandler Trait**: All handlers implement this trait:
```rust
#[async_trait]
pub trait TransformationHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32 { 1 }              // Bump to reprocess all data
    fn handler_key(&self) -> String;            // "{name}_v{version}"
    fn migration_paths(&self) -> Vec<&'static str>;  // Handler-specific SQL
    async fn handle(&self, ctx: &TransformationContext) -> Result<Vec<DbOperation>, TransformationError>;
    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> { Ok(()) }
}
```

**EventHandler / EthCallHandler**: Marker traits that declare triggers:
```rust
pub trait EventHandler: TransformationHandler {
    fn triggers(&self) -> Vec<EventTrigger>;
    fn call_dependencies(&self) -> Vec<(String, String)> { vec![] }
}

pub trait EthCallHandler: TransformationHandler {
    fn triggers(&self) -> Vec<EthCallTrigger>;
}
```

**TransformationContext**: Provides handlers with:
- Chain info (name, chain_id, block range)
- Decoded events and calls for the current range
- Transaction address lookup (from/to)
- Historical data queries (parquet files)
- Ad-hoc RPC calls

**DbOperation**: Database operations returned by handlers:
- `Upsert` - INSERT with ON CONFLICT DO UPDATE
- `Insert` - Simple INSERT
- `Update` - UPDATE with WHERE clause
- `Delete` - DELETE with WHERE clause
- `RawSql` - Raw SQL for complex operations

### Handler Registration

Handlers are registered at compile-time in `src/transformations/registry.rs`:
```rust
pub fn build_registry() -> TransformationRegistry {
    let mut registry = TransformationRegistry::new();
    super::event::register_handlers(&mut registry);
    super::eth_call::register_handlers(&mut registry);
    registry
}
```

### Progress Tracking

Each handler's progress is tracked independently in the `_handler_progress` table:
- `chain_id` - Chain identifier
- `handler_key` - Handler identity ("{name}_v{version}")
- `range_start` - Last processed block range start

Bumping a handler's version causes it to reprocess all data from the beginning.

## RPC Layer

The RPC layer provides unified access to blockchain data with intelligent rate limiting.

### Client Types

**UnifiedRpcClient**: Auto-selects the appropriate client based on URL:
- `AlchemyClient` - For Alchemy endpoints (CU-based rate limiting)
- `RpcClient` - Standard client for other providers

### Rate Limiting

**SlidingWindowRateLimiter**: Implements a 10-second sliding window:
- Matches Alchemy's rate limiting behavior
- Shared across all clients for account-level limiting
- Per-request CU acquisition (not batch-based)

**Environment Variables**:
- `RPC_CONCURRENCY` - Max concurrent in-flight requests (default: 100)
- `ALCHEMY_CU_PER_SECOND` - Compute units per second (default: 7500)
- `RPC_BATCH_SIZE` - Batch size for batch RPC calls

### Streaming Collection

The block collector uses streaming for efficient throughput:
```rust
// Blocks are sent as they arrive, not waiting for the full batch
client.get_blocks_streaming(block_numbers, full_transactions, result_tx)
```

## Decode-Only Mode

Run decoders on existing raw parquet files without collection or transformations:
```bash
cargo run --release -- --decode-only
```

Useful for re-decoding data after ABI changes or profiling the decoding pipeline.

## Quick Start

1. Create a configuration file (see [Configuration](./CONFIG.md))
2. Set your RPC URL environment variable
3. Run the indexer:

```bash
cargo run --release
```

## Data Flow

### Collection Phase
1. **Block Collector** fetches block headers and sends to receipts
2. **Receipt Collector** fetches transaction receipts and extracts logs
3. **Log Collector** writes raw logs to parquet, forwards to factory collector
4. **Factory Collector** discovers new contracts from factory events
5. **ETH Call Collector** makes historical state queries (block-based and event-triggered)

### Decoding Phase
1. **Log Decoder** parses raw logs using ABI event signatures
2. **ETH Call Decoder** parses raw call results using ABI function signatures
3. Both write decoded parquet files and forward to transformation engine

### Transformation Phase
1. **Transformation Engine** receives decoded events/calls
2. Looks up registered handlers by trigger (source, event/function)
3. Invokes handlers with `TransformationContext`
4. Executes returned `DbOperation`s transactionally per handler

## Documentation

| Document | Description |
|----------|-------------|
| [Configuration](./CONFIG.md) | JSON configuration format, contracts, tokens, and options |
| [Block Collection](./BLOCK_COLLECTION.md) | Block fetching and storage |
| [Receipt Collection](./RECEIPTS_COLLECTION.md) | Transaction receipt processing |
| [Log Collection](./LOGS_COLLECTION.md) | Event log filtering and storage |
| [Factory Collection](./FACTORY_COLLECTION.md) | Dynamic contract discovery |
| [ETH Call Collection](./ETH_CALL_COLLECTION.md) | Historical state reading |
| [On-Event Calls](./ON_EVENT_CALLS.md) | Event-triggered eth_calls |
| [Decoding](./DECODING.md) | ABI-based event and call decoding |
| [Transformations](./TRANSFORMATIONS.md) | Custom handlers, PostgreSQL output, historical queries |
| [Parallelism](./PARALLELISM.md) | Task architecture and data flow |
| [RPC](./RPC.md) | RPC client, rate limiting, and retries |

## Example Configuration

```json
{
  "chains": [
    {
      "name": "base",
      "chain_id": 8453,
      "rpc_url_env_var": "BASE_RPC_URL",
      "start_block": 26602741,
      "contracts": "contracts/base",
      "tokens": "tokens/base.json"
    }
  ],
  "raw_data_collection": {
    "parquet_block_range": 10000,
    "rpc_batch_size": 100,
    "contract_logs_only": true,
    "fields": {
      "block_fields": ["number", "timestamp", "transactions"],
      "receipt_fields": ["block_number", "timestamp", "transaction_hash", "from", "to"],
      "log_fields": ["block_number", "timestamp", "transaction_hash", "log_index", "address", "topics", "data"]
    }
  },
  "transformations": {
    "database_url_env_var": "DATABASE_URL"
  }
}
```

## Use Cases

- **DEX Analytics** - Track swaps, liquidity changes, and pool state across Uniswap V2/V3/V4
- **Token Tracking** - Monitor token deployments, transfers, and balances
- **Protocol Indexing** - Index any smart contract's events and state
- **Historical Analysis** - Query on-chain data with SQL using DuckDB or similar tools
- **Application Backends** - Use transformations to power real-time APIs with PostgreSQL

## Querying Data

Example using DuckDB to query decoded swap events:

```sql
SELECT
    block_number,
    block_timestamp,
    encode(contract_address, 'hex') as pool,
    amount0,
    amount1,
    sqrtPriceX96
FROM read_parquet('data/derived/base/decoded/logs/UniswapV4PoolManager/Swap/*.parquet')
WHERE block_timestamp > 1700000000
ORDER BY block_number DESC
LIMIT 100;
```

## Performance

The indexer is designed for high throughput:

- **Parallel collection** - Multiple collectors run concurrently via async tasks
- **Streaming block fetches** - Blocks sent immediately as they arrive, not waiting for batches
- **Batch RPC requests** - Configurable batch sizes reduce overhead
- **Rate limit aware** - Sliding window rate limiter matches Alchemy's 10-second window
- **Shared rate limiter** - Single limiter across all clients for account-level limiting
- **Semaphore-bounded concurrency** - `RPC_CONCURRENCY` controls in-flight requests
- **Efficient storage** - Snappy-compressed Parquet with configurable schemas
- **Resumable** - Automatic catchup skips already-processed ranges
- **Per-handler transactions** - Handler failures don't affect other handlers
- **Concurrent handler execution** - `handler_concurrency` config controls parallelism
