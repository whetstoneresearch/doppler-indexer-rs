# Doppler Indexer

Doppler Indexer is a high-performance Ethereum blockchain indexer written in Rust. It collects raw blockchain data, tracks dynamically created contracts, reads on-chain state, decodes events and call results into typed Parquet files, and supports custom transformations that write to PostgreSQL.

## Features

- **Raw Data Collection** - Fetches blocks, transaction receipts, and logs from any EVM-compatible chain
- **Factory Contract Tracking** - Automatically discovers and indexes dynamically created contracts (e.g., Uniswap pairs, token deployments)
- **Historical State Reading** - Makes `eth_call` requests at historical block heights to track on-chain state over time
- **Event Decoding** - Parses raw log data into typed columns using Solidity event signatures
- **Transformations** - Custom Rust handlers process decoded data and write to PostgreSQL with per-block atomicity
- **Parallel Processing** - Async task-based architecture with configurable concurrency for maximum throughput
- **Resumable** - Automatic catchup on restart, skipping already-collected ranges
- **Flexible Output** - Parquet files for analytics, PostgreSQL for application data

## Architecture

The indexer runs as multiple concurrent async tasks connected by channels:

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Blocks    │────►│    Receipts      │────►│    Logs     │────► Log Decoder ────┐
└──────┬──────┘     └────────┬─────────┘     └─────────────┘                      │
       │                     │                                                     │
       │                     └────────────────►┌─────────────┐                     │
       │                           ◄───────────│  Factories  │────► Decoder        │
       │                        (recollect)    └─────────────┘                     │
       │                                                                           ▼
       └─────────────────────────────────────►┌─────────────┐              ┌──────────────┐
                                              │  ETH Calls  │────► Decoder─►│Transformations│──► PostgreSQL
                                              └─────────────┘              └──────────────┘
```

Each collector runs independently, maximizing throughput while respecting RPC rate limits. The optional transformation layer processes decoded data and writes to PostgreSQL.

**Recollection Channel**: When the factory collector encounters a corrupted log file during catchup, it deletes the file and sends a recollection request back to the receipt collector. The receipt collector then re-fetches the data and sends it through the normal channels, ensuring data integrity without manual intervention.

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

migrations/               # SQL migration files (when transformations enabled)
├── 001_initial_schema.sql
└── ...
```

When transformations are enabled, data is also written to PostgreSQL tables defined in your migrations.

## Quick Start

1. Create a configuration file (see [Configuration](./CONFIG.md))
2. Set your RPC URL environment variable
3. Run the indexer:

```bash
cargo run --release -- --config config/config.json
```

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

- **Parallel collection** - Multiple collectors run concurrently
- **Batch RPC requests** - Configurable batch sizes reduce overhead
- **Rate limit aware** - Supports Alchemy compute units and request-based limiting
- **Efficient storage** - Snappy-compressed Parquet with configurable schemas
- **Resumable** - Automatic catchup skips already-processed ranges
