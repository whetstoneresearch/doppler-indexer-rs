# Transformations

The transformation system processes decoded blockchain events and eth_call results, enabling custom data transformations that write to PostgreSQL. Handlers are written in Rust and registered at compile-time, providing type-safe access to decoded data with full chain context.

## Overview

Transformations extend the indexer's capabilities beyond Parquet file storage:

- **Custom Processing** - Write Rust handlers that process decoded events and calls
- **PostgreSQL Output** - Transform and store data in relational database tables
- **Historical Queries** - Access previously decoded data from Parquet files within handlers
- **Ad-hoc RPC Calls** - Make additional eth_calls with data from events
- **Per-Block Atomicity** - All database writes for a block range succeed or fail together

## Architecture

```
Decoded Events/Calls ──► TransformationEngine ──► Handlers ──► DbOperations ──► PostgreSQL
                               │
                               └─► TransformationContext
                                    ├─ Chain info (name, id, block range)
                                    ├─ Decoded data (events, calls)
                                    ├─ Historical queries (parquet)
                                    └─ Ad-hoc RPC calls
```

The transformation engine receives decoded data via channels from the decoders, invokes registered handlers, collects database operations, and executes them transactionally.

## Configuration

Configure transformations in your `config.json`. Transformations are automatically enabled when:
1. The `transformations` section is present in the config
2. There are registered handlers in the codebase

```json
{
  "chains": [...],
  "raw_data_collection": {...},
  "transformations": {
    "database_url_env_var": "DATABASE_URL",
    "mode": {
      "batch_for_catchup": true,
      "catchup_batch_size": 10000
    },
    "handler_concurrency": 4,
    "max_batch_size": 1000
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url_env_var` | string | `"DATABASE_URL"` | Environment variable containing PostgreSQL connection string |
| `mode.batch_for_catchup` | boolean | `true` | Use larger batches for historical catchup |
| `mode.catchup_batch_size` | integer | `10000` | Block range size for batch mode |
| `handler_concurrency` | integer | `4` | Number of concurrent handler executions |
| `max_batch_size` | integer | `1000` | Maximum database operations per transaction |

## Database Migrations

SQL migrations are stored in the `migrations/` directory and run automatically on startup:

```
migrations/
├── 001_initial_schema.sql
├── 002_add_indexes.sql
└── ...
```

Migrations are executed in alphabetical order. Each migration runs once and is tracked in a `_migrations` table.

**Example migration:**

```sql
-- migrations/001_v4_swaps.sql

CREATE TABLE IF NOT EXISTS v4_swaps (
    id BIGSERIAL PRIMARY KEY,
    pool_id BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_hash BYTEA NOT NULL,
    sender BYTEA NOT NULL,
    amount0 NUMERIC NOT NULL,
    amount1 NUMERIC NOT NULL,
    sqrt_price_x96 NUMERIC NOT NULL,
    liquidity NUMERIC NOT NULL,
    tick INT NOT NULL,
    fee INT NOT NULL,
    timestamp BIGINT NOT NULL,
    chain_id BIGINT NOT NULL,
    UNIQUE (pool_id, block_number, log_index)
);

CREATE INDEX idx_v4_swaps_pool_block ON v4_swaps (pool_id, block_number DESC);
CREATE INDEX idx_v4_swaps_timestamp ON v4_swaps (timestamp DESC);
```

## Writing Handlers

### Handler Traits

All handlers implement the `TransformationHandler` trait:

```rust
#[async_trait]
pub trait TransformationHandler: Send + Sync + 'static {
    /// Unique name for logging and error messages
    fn name(&self) -> &'static str;

    /// Process decoded data and return database operations
    async fn handle(
        &self,
        ctx: &TransformationContext<'_>,
    ) -> Result<Vec<DbOperation>, TransformationError>;

    /// Optional initialization (create indexes, warm caches, etc.)
    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        Ok(())
    }
}
```

Event handlers also implement `EventHandler`:

```rust
pub trait EventHandler: TransformationHandler {
    fn triggers(&self) -> Vec<EventTrigger>;
}
```

Call handlers implement `EthCallHandler`:

```rust
pub trait EthCallHandler: TransformationHandler {
    fn triggers(&self) -> Vec<EthCallTrigger>;
}
```

### Example: V4 Swap Handler

```rust
// src/transformations/event/v4.rs

use async_trait::async_trait;
use crate::db::{DbOperation, DbValue};
use crate::transformations::{
    TransformationHandler, EventHandler, EventTrigger,
    TransformationContext, TransformationError,
};

pub struct V4SwapHandler;

#[async_trait]
impl TransformationHandler for V4SwapHandler {
    fn name(&self) -> &'static str {
        "V4SwapHandler"
    }

    async fn handle(
        &self,
        ctx: &TransformationContext<'_>,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        // Iterate over all matching events in the current block range
        for event in ctx.events_of_type("UniswapV4PoolManager", "Swap") {
            // Extract decoded parameters by name
            let pool_id = event.get("id")?.as_bytes32()
                .ok_or_else(|| TransformationError::TypeConversion("id".into()))?;
            let sender = event.get("sender")?.as_address()
                .ok_or_else(|| TransformationError::TypeConversion("sender".into()))?;
            let amount0 = event.get("amount0")?.to_numeric_string()
                .ok_or_else(|| TransformationError::TypeConversion("amount0".into()))?;
            let tick = event.get("tick")?.as_i32()
                .ok_or_else(|| TransformationError::TypeConversion("tick".into()))?;

            // Build database operation
            ops.push(DbOperation::Upsert {
                table: "v4_swaps".to_string(),
                columns: vec![
                    "pool_id".into(), "block_number".into(), "log_index".into(),
                    "tx_hash".into(), "sender".into(), "amount0".into(),
                    "tick".into(), "chain_id".into(),
                ],
                values: vec![
                    DbValue::Bytes32(pool_id),
                    DbValue::Uint64(event.block_number),
                    DbValue::Uint64(event.log_index as u64),
                    DbValue::Bytes32(event.transaction_hash),
                    DbValue::Address(sender),
                    DbValue::Numeric(amount0),
                    DbValue::Int32(tick),
                    DbValue::Uint64(ctx.chain_id),
                ],
                conflict_columns: vec!["pool_id".into(), "block_number".into(), "log_index".into()],
                update_columns: vec!["amount0".into(), "tick".into()],
            });
        }

        Ok(ops)
    }
}

impl EventHandler for V4SwapHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "UniswapV4PoolManager",
            "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)",
        )]
    }
}
```

### Registering Handlers

Register handlers in `src/transformations/event/mod.rs`:

```rust
pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(v4::V4SwapHandler);
    // Add more handlers here
}
```

## TransformationContext

The context provides handlers with all necessary data and utilities:

### Chain Information

```rust
ctx.chain_name      // "base", "mainnet", etc.
ctx.chain_id        // 8453, 1, etc.
ctx.block_range_start
ctx.block_range_end
```

### Decoded Data Access

```rust
// All events in current range
ctx.events

// All calls in current range
ctx.calls

// Filter by type
for event in ctx.events_of_type("Contract", "EventName") { ... }
for call in ctx.calls_of_type("Contract", "functionName") { ... }

// Filter by address
for event in ctx.events_for_address(address) { ... }
for call in ctx.calls_for_address(address) { ... }
```

### Historical Queries

Query previously decoded data from Parquet files:

```rust
// Query events from past blocks
let past_events = ctx.query_events(HistoricalEventQuery {
    source: Some("UniswapV4PoolManager".into()),
    event_name: Some("Initialize".into()),
    from_block: 0,
    to_block: ctx.block_range_end,  // Cannot exceed current range
    contract_address: None,
    limit: Some(100),
}).await?;

// Query call results from past blocks
let past_calls = ctx.query_calls(HistoricalCallQuery {
    source: Some("Pool".into()),
    function_name: Some("slot0".into()),
    from_block: event.block_number - 1000,
    to_block: event.block_number + 1,
    ..Default::default()
}).await?;
```

**Important:** Historical queries cannot access future blocks. The `to_block` is automatically clamped to `block_range_end`.

### Ad-hoc RPC Calls

Make eth_calls with data from events:

```rust
// Single call
let result = ctx.eth_call(
    pool_address,                    // Contract address
    "slot0()((uint160,int24,...))",  // Function signature with return type
    vec![],                          // Parameters
    event.block_number,              // Block number
).await?;

// Batch calls (more efficient)
let results = ctx.eth_call_batch(vec![
    EthCallRequest {
        contract_address: pool1,
        function_signature: "liquidity()(uint128)".into(),
        params: vec![],
        block_number: event.block_number,
    },
    EthCallRequest {
        contract_address: pool2,
        function_signature: "liquidity()(uint128)".into(),
        params: vec![],
        block_number: event.block_number,
    },
]).await?;
```

## DecodedEvent and DecodedCall

### DecodedEvent

```rust
pub struct DecodedEvent {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub contract_address: [u8; 20],
    pub source_name: String,      // Contract or collection name
    pub event_name: String,       // "Swap", "Transfer", etc.
    pub event_signature: String,  // Full signature
    pub params: HashMap<String, DecodedValue>,  // Decoded parameters
}

// Access parameters
let value = event.get("amount0")?;           // Returns error if missing
let value = event.try_get("optional_field"); // Returns Option
```

### DecodedCall

```rust
pub struct DecodedCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub source_name: String,
    pub function_name: String,
    pub trigger_log_index: Option<u32>,  // For event-triggered calls
    pub result: HashMap<String, DecodedValue>,
}
```

### DecodedValue

```rust
pub enum DecodedValue {
    Address([u8; 20]),
    Uint256(U256),
    Int256(I256),
    Uint128(u128), Int128(i128),
    Uint64(u64), Int64(i64),
    Uint32(u32), Int32(i32),
    Uint8(u8), Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    NamedTuple(Vec<(String, DecodedValue)>),
    Array(Vec<DecodedValue>),
}

// Conversion methods
value.as_address()      // Option<[u8; 20]>
value.as_bytes32()      // Option<[u8; 32]>
value.as_uint256()      // Option<U256>
value.as_int256()       // Option<I256>
value.as_u64()          // Option<u64>
value.as_i64()          // Option<i64>
value.as_i32()          // Option<i32>
value.as_bool()         // Option<bool>
value.as_string()       // Option<&str>
value.as_bytes()        // Option<&[u8]>
value.to_numeric_string() // Option<String> (for database storage)
value.get_field("name") // Option<&DecodedValue> (for tuples)
```

## Database Operations

Handlers return `Vec<DbOperation>` which are executed transactionally:

### Upsert

```rust
DbOperation::Upsert {
    table: "swaps".into(),
    columns: vec!["id".into(), "amount".into()],
    values: vec![DbValue::Bytes32(id), DbValue::Numeric(amount)],
    conflict_columns: vec!["id".into()],
    update_columns: vec!["amount".into()],
}
// Generates: INSERT INTO swaps (id, amount) VALUES ($1, $2)
//            ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount
```

### Insert

```rust
DbOperation::Insert {
    table: "events".into(),
    columns: vec!["block".into(), "data".into()],
    values: vec![DbValue::Uint64(block), DbValue::Text(data)],
}
```

### Update

```rust
DbOperation::Update {
    table: "pools".into(),
    set_columns: vec![("liquidity".into(), DbValue::Numeric(liq))],
    where_clause: WhereClause::Eq("id".into(), DbValue::Bytes32(pool_id)),
}
```

### Delete

```rust
DbOperation::Delete {
    table: "old_data".into(),
    where_clause: WhereClause::And(vec![
        ("chain_id".into(), DbValue::Uint64(chain_id)),
        ("block".into(), DbValue::Uint64(block)),
    ]),
}
```

### Raw SQL

```rust
DbOperation::RawSql {
    query: "UPDATE pools SET total = total + $1 WHERE id = $2".into(),
    params: vec![DbValue::Numeric(amount), DbValue::Bytes32(id)],
}
```

## DbValue Types

| Variant | Rust Type | PostgreSQL Type |
|---------|-----------|-----------------|
| `Null` | - | NULL |
| `Bool(bool)` | `bool` | BOOLEAN |
| `Int32(i32)` | `i32` | INTEGER |
| `Int64(i64)` | `i64` | BIGINT |
| `Uint64(u64)` | `u64` | BIGINT |
| `Text(String)` | `String` | TEXT |
| `Bytes(Vec<u8>)` | `Vec<u8>` | BYTEA |
| `Address([u8; 20])` | `[u8; 20]` | BYTEA |
| `Bytes32([u8; 32])` | `[u8; 32]` | BYTEA |
| `Numeric(String)` | `String` | NUMERIC |
| `Timestamp(i64)` | `i64` | BIGINT |
| `Json(Value)` | `serde_json::Value` | JSONB |

## Error Handling

Transformation errors halt processing to maintain data integrity:

```rust
pub enum TransformationError {
    HandlerError { handler_name: String, message: String },
    DatabaseError(DbError),
    MissingField(String),
    MissingColumn(String),
    RpcError(String),
    FutureBlockAccess { requested: u64, current_max: u64 },
    DecodeError(String),
    TypeConversion(String),
    // ...
}
```

When any handler returns an error, the engine:

1. Logs the error with full context
2. Rolls back the current transaction
3. Halts processing

This ensures no partial data is written and issues are immediately visible.

## Execution Modes

### Streaming Mode

For live data, transformations run as each block range is decoded:

- Lower latency
- Immediate database updates
- Suitable for real-time applications

### Batch Mode

For historical catchup, transformations process larger batches:

- Higher throughput
- More efficient database writes
- Automatic when catching up from behind

Configure in `mode.batch_for_catchup` and `mode.catchup_batch_size`.

## File Structure

```
src/transformations/
├── mod.rs           # Module exports
├── traits.rs        # TransformationHandler, EventHandler, EthCallHandler
├── context.rs       # TransformationContext, DecodedEvent, DecodedCall
├── engine.rs        # TransformationEngine
├── registry.rs      # Handler registration
├── historical.rs    # Parquet reader for historical queries
├── error.rs         # TransformationError
├── event/           # Event handlers
│   ├── mod.rs
│   └── v4.rs        # Example V4 handlers
├── eth_call/        # Call handlers
│   └── mod.rs
└── util/            # Shared utilities
    └── mod.rs
```

## Performance Considerations

- **Per-block transactions** ensure atomicity but may be slower than batching
- **Historical queries** read from disk; cache results when querying the same data multiple times
- **Ad-hoc RPC calls** add latency; batch when possible
- **Handler concurrency** can be tuned via `handler_concurrency` config
- **Batch mode** for catchup processes larger ranges more efficiently

## Best Practices

1. **Keep handlers focused** - One handler per logical transformation
2. **Use upserts** - Handle re-processing gracefully with `ON CONFLICT`
3. **Add indexes** - Create indexes for common query patterns in migrations
4. **Validate data** - Return errors for unexpected data rather than silently skipping
5. **Log context** - Include relevant identifiers in error messages
6. **Test locally** - Use a local PostgreSQL for development
