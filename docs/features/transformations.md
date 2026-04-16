# Transformations

The transformation system processes decoded blockchain events and eth_call results, enabling custom data transformations that write to PostgreSQL. Handlers are written in Rust and registered at compile-time, providing type-safe access to decoded data with full chain context.

## Overview

Transformations extend the indexer's capabilities beyond Parquet file storage:

- **Custom Processing** - Write Rust handlers that process decoded events and calls
- **PostgreSQL Output** - Transform and store data in relational database tables
- **Historical Queries** - Access previously decoded data from Parquet files within handlers
- **Ad-hoc RPC Calls** - Make additional eth_calls with data from events
- **Per-Handler Transactions** - Each handler's database writes for a block range succeed or fail independently
- **Reorg Handling** - Handlers can declare tables that need rollback during chain reorganizations

## Architecture

```
Decoded Events/Calls ──► TransformationEngine ──► Handlers ──► DbOperations ──► PostgreSQL
                               │                                    │
                               └─► TransformationContext            └─► inject source/source_version
                                    ├─ Chain info (name, id, block range)
                                    ├─ Decoded data (events, calls)
                                    ├─ Historical queries (parquet)
                                    └─ Ad-hoc RPC calls
```

The transformation engine receives decoded data via channels from the decoders, invokes registered handlers, collects database operations, injects `source`/`source_version` columns automatically, and executes them transactionally.

### DAG Scheduler

The DAG-based execution scheduler lives in `src/transformations/scheduler/` (dag.rs, tracker.rs, loader.rs) and orchestrates handler execution respecting dependency ordering and concurrency limits.

**DagScheduler** takes a `CompletionTracker` and a concurrency limit, then executes work items in dependency order:

```
DagScheduler::new(tracker: CompletionTracker, concurrency: usize)
  └─ execute(items: Vec<WorkItem>, runner) -> Vec<WorkItemOutcome>
```

`execute(items, runner)` spawns one tokio task per `WorkItem`. Each task follows this flow:

1. **Await dependencies** via `CompletionTracker` -- block until all `dep_names` report completed for this range
2. **Acquire sequential semaphore** (if `sequential=true`) -- per-handler capacity-1 FIFO semaphore
3. **Acquire global concurrency permit** -- bounded by the scheduler's concurrency limit
4. **Run** the work item via the provided runner
5. **Mark completed or failed** in the `CompletionTracker`

Waits happen BEFORE permit acquisition to prevent permit-deadlock (holding a permit while waiting on a dependency that needs one).

Returns outcomes for ALL items, including those that were cascade-failed due to a dependency failure.

**WorkItem** struct:

| Field | Type | Description |
|-------|------|-------------|
| `handler_name` | `String` | Handler identity |
| `range_start` | `u64` | Block range start |
| `range_end` | `u64` | Block range end |
| `dep_names` | `Vec<String>` | Handler names that must complete first |
| `sequential` | `bool` | Whether to acquire the per-handler sequential semaphore |
| `payload` | generic | Data needed to execute the item |

**OutcomeStatus** enum:

| Variant | Description |
|---------|-------------|
| `Succeeded` | Handler completed successfully |
| `HandlerFailed` | Handler returned an error |
| `DepCascadeFailed` | A dependency failed, so this item was not attempted |
| `Panicked` | The handler task panicked |

**CompletionTracker** provides per-(handler_name, range_start) completion and failure tracking. Uses `tokio::sync::Notify` to wake blocked dependents. Supports `seed_completed()` to pre-mark ranges as done, enabling phase overlap (e.g., seeding from `_handler_progress` so catchup does not re-execute already-completed work).

**CatchupLoader** executes one `WorkItem` end-to-end for catchup mode: load parquet data, build `TransformationContext`, invoke the handler, execute the resulting database transaction, and record progress in `_handler_progress`.

### Handler Dependency Chains

Handlers declare ordering constraints via dependency specs on `EventHandler`.
The ergonomic entry point is `dep("HandlerName")`, which defaults to all chains:

```rust
use crate::transformations::traits::dep;

fn handler_dependency_specs(&self) -> Vec<HandlerDependencySpec> {
    vec![
        dep("V3CreateHandler"),
        dep("MigrationPoolCreateHandler").except([57073, 143]),
    ]
}
```

`handler_dependency_specs()` controls same-range dependencies.
`contiguous_handler_dependency_specs()` controls catchup-only contiguous dependencies.
If no selector is attached, the dependency applies to all chains. `.only([...])` and `.except([...])`
filter by `chain_id`.

For backward compatibility, the legacy `handler_dependencies()` / `contiguous_handler_dependencies()`
methods still exist and are wrapped into dependency specs automatically.

**Cascade failures:** If handler A fails on range R, all handlers that depend (directly or transitively) on A immediately cascade-fail for range R without being attempted.

**Startup validation** uses Kahn's algorithm to verify the dependency graph:

- **Missing names**: If a declared dependency does not resolve to a registered handler, the process panics at startup
- **Cycles**: If the dependency graph contains a cycle, the process panics with a diagnostic trace showing the cycle path
- **Dep type validation**: All declared dependencies must be event handlers

**Topological sort** of the resolved per-chain graph determines execution order for both catchup and live processing.

### Sequential Execution

Handlers can opt into strictly ordered execution across block ranges by implementing `requires_sequential() -> bool` (default `false`).

When a handler returns `true`:

- Its `WorkItem`s are created with `sequential = true`
- The DAG scheduler uses a per-handler capacity-1 FIFO semaphore
- Items are submitted in ascending `range_start` order and acquire permits in FIFO order

**Guarantee:** Within a handler that requires sequential execution, block ranges execute in strictly ascending order. This is useful for handlers that maintain cumulative state (e.g., running totals, position tracking) where processing range N depends on the result of range N-1.

### Multi-Trigger Handler Completion

Some handlers respond to multiple event triggers (e.g., a metrics handler that processes both Swap and Mint events).

- The registry tracks multi-trigger handler keys via `is_multi_trigger(handler_key)`
- In live mode, handler success is NOT persisted until all trigger batches for that block have been dispatched
- This is coordinated by the engine's batching logic and the `RangeFinalizer` to prevent marking a handler as complete before all of its trigger sources have been processed

### Catchup Execution with DAG

Catchup uses the DAG scheduler in a multi-phase approach:

**Phase 1 -- Initialization:**
- Collect handlers by kind (event, eth_call), sort by topological order
- Seed the `CompletionTracker` from `_handler_progress` so already-completed ranges are pre-marked

**Phase 2 -- Multi-pass item building loop:**
1. Build `WorkItem`s for each handler and block range
2. Skip ranges already completed (seeded in Phase 1)
3. Defer ranges where call-dependency files are unavailable (parquet not yet produced)
4. Cascade defer: if a handler's dependency was deferred, the handler is also deferred
5. Submit non-deferred items to the DAG scheduler
6. Repeat from step 1 until no deferrals remain

**Phase 3 -- Live processing:**
- Transition to live mode with pending event buffering for handlers whose call dependencies have not yet arrived

The engine delegates to sub-components:
- **HandlerExecutor** (`executor.rs`): Concurrent handler spawn-loop, context construction, source/version injection, optional snapshot capture
- **RangeFinalizer** (`finalizer.rs`): Range finalization, reorg cleanup, progress tracking
- **RetryProcessor** (`retry.rs`): Re-processing failed live blocks from bincode storage
- **LiveProcessingState** (`live_state.rs`): Pending event buffering for handlers with unmet call dependencies

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

SQL migrations are stored in the `migrations/` directory and run automatically on startup.

Global migrations (numbered files like `000_handler_progress.sql`, `001_active_versions.sql`) at the top level of `migrations/` are executed in alphabetical order. Each migration runs once and is tracked in a `_migrations` table. Handler-specific migration paths are also tracked in the same `_migrations` table using their full path as the key.

Handler migration paths are specified by each handler via `migration_paths()`. Each path can be:
- **A directory**: All `.sql` files in it are run in alphabetical order (scanned flat, no subdirectories)
- **A single `.sql` file**: Run directly

Examples:
```rust
fn migration_paths(&self) -> Vec<&'static str> {
    vec![
        "migrations/tables/tokens.sql",      // Single file
        "migrations/tables/pools.sql",       // Another single file
    ]
}

// Or using a directory:
fn migration_paths(&self) -> Vec<&'static str> {
    vec!["migrations/handlers/pools"]        // All .sql files in directory
}
```

## Active Versions

The `active_versions` table tracks which version of each source (handler) is currently active:

```sql
CREATE TABLE active_versions (
    source VARCHAR(255) PRIMARY KEY,
    active_version INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

When a handler starts for the first time, the engine automatically inserts a row with its current version. If the handler version changes (e.g., bumped from 1 to 2), the engine logs a warning but does **not** auto-upgrade — activation is manual:

```sql
UPDATE active_versions SET active_version = 2, updated_at = NOW() WHERE source = 'V4SwapHandler';
```

This allows multiple versions to coexist in the same table. Downstream queries and views should filter by `active_version`:

```sql
SELECT * FROM swaps s
JOIN active_versions av ON av.source = s.source AND av.active_version = s.source_version;
```

## Writing Handlers

### Handler Traits

All handlers implement the `TransformationHandler` trait:

```rust
#[async_trait]
pub trait TransformationHandler: Send + Sync + 'static {
    /// Unique name for this handler (used in logging and progress tracking).
    fn name(&self) -> &'static str;

    /// Version of this handler. Bump when the handler logic changes and you
    /// want to reprocess all data into a new versioned output table.
    fn version(&self) -> u32 { 1 }

    /// Computed identity key: `"{name}_v{version}"`.
    /// Used for progress tracking in the `_handler_progress` table.
    fn handler_key(&self) -> String {
        format!("{}_v{}", self.name(), self.version())
    }

    /// Migration paths for this handler's SQL files, relative to the project root.
    /// Each path can be either:
    /// - A directory: all `.sql` files in it are run in alphabetical order
    /// - A single `.sql` file: run directly
    ///
    /// Multiple paths are supported for handlers that write to multiple tables.
    /// Directories are scanned flat (no subdirectories).
    ///
    /// Examples:
    /// - `vec!["migrations/handlers/pools"]` — run all SQL in the `pools/` dir
    /// - `vec!["migrations/handlers/pools/create_table.sql"]` — run one file
    /// - `vec!["migrations/handlers/pools", "migrations/handlers/swaps"]` — multiple dirs
    fn migration_paths(&self) -> Vec<&'static str> { vec![] }

    /// Declare tables that should be rolled back during a chain reorganization.
    /// Used in live mode to clean up orphaned data when blocks are reverted.
    /// Returns table names that have reorg-sensitive data keyed by block_number.
    fn reorg_tables(&self) -> Vec<&'static str> { vec![] }

    /// Process decoded data for a block range.
    ///
    /// Called once per block range with all decoded events/calls matching
    /// this handler's triggers. Returns a list of database operations to
    /// execute transactionally.
    ///
    /// Do NOT include `source` or `source_version` columns — the engine injects them.
    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError>;

    /// Optional: Called once at startup for initialization.
    ///
    /// Can be used to create indexes, warm caches, etc.
    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        Ok(())
    }
}
```

Event handlers also implement `EventHandler`:

```rust
pub trait EventHandler: TransformationHandler {
    /// Event triggers this handler responds to.
    fn triggers(&self) -> Vec<EventTrigger>;

    /// Declare eth_call types this handler needs access to.
    /// Returns (source_name, function_name) pairs that must be available
    /// in decoded parquet files before this handler can process a range.
    fn call_dependencies(&self) -> Vec<(String, String)> { vec![] }

    /// Declarative same-range handler dependencies. Defaults to wrapping the
    /// legacy `handler_dependencies()` string API if present.
    fn handler_dependency_specs(&self) -> Vec<HandlerDependencySpec> { vec![] }

    /// Legacy string-only dependency API. Still supported for compatibility.
    fn handler_dependencies(&self) -> Vec<&'static str> { vec![] }
}
```

Call handlers implement `EthCallHandler`:

```rust
pub trait EthCallHandler: TransformationHandler {
    /// eth_call triggers this handler responds to.
    fn triggers(&self) -> Vec<EthCallTrigger>;
}
```

### Source/Version Injection

The engine automatically injects `source` (handler name) and `source_version` (handler version) into every `DbOperation` returned by handlers:

| Operation | Injection |
|-----------|-----------|
| `Upsert` | Appends to `columns`, `values`, and `conflict_columns` |
| `Insert` | Appends to `columns` and `values` |
| `Update` | Merges into `where_clause` (`Eq`/`And` only; `Raw` is skipped) |
| `Delete` | Merges into `where_clause` (`Eq`/`And` only; `Raw` is skipped) |
| `RawSql` | Skipped (handler must manage manually) |

Handlers should **not** include `source` or `source_version` in their operations — the engine handles this.

### Example: DERC20 Transfer Handler

A simple handler that processes ERC20 Transfer events:

```rust
// src/transformations/event/derc20_transfer.rs

use async_trait::async_trait;
use crate::db::{DbOperation, DbPool};
use crate::transformations::{
    TransformationHandler, EventHandler, EventTrigger,
    TransformationContext, TransformationError,
};

pub struct DERC20TransferHandler;

#[async_trait]
impl TransformationHandler for DERC20TransferHandler {
    fn name(&self) -> &'static str {
        "DERC20TransferHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/transfers.sql",
            "migrations/tables/users.sql",
        ]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("DERC20", "Transfer") {
            let from_address = event.get("from")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("from is not an address".to_string())
            })?;

            let to_address = event.get("to")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("to is not an address".to_string())
            })?;

            let value = event.get("value")?.as_uint256().ok_or_else(|| {
                TransformationError::TypeConversion("value is not a uint256".to_string())
            })?;

            // Build database operations...
            // source and source_version are injected automatically by the engine
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("DERC20TransferHandler initialized");
        Ok(())
    }
}

impl EventHandler for DERC20TransferHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "DERC20",
            "Transfer(address,address,uint256)"
        )]
    }
}
```

### Example: Handler with Call Dependencies

Handlers can declare eth_call dependencies that must be available before processing:

```rust
// src/transformations/event/v4/create.rs

pub struct V4CreateHandler;

#[async_trait]
impl TransformationHandler for V4CreateHandler {
    fn name(&self) -> &'static str {
        "V4CreateHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/tokens.sql",
            "migrations/tables/pools.sql",
            "migrations/tables/v4_pool_configs.sql",
        ]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for event in ctx.events_of_type("UniswapV4Initializer", "Create") {
            let hook = event.get("poolOrHook")?.as_address().ok_or_else(|| {
                TransformationError::TypeConversion("poolOrHook is not an address".to_string())
            })?;

            // Access call data that matches this event's block
            let hook_call = ctx.calls_for_address(hook)
                .filter(|call| call.function_name == "once")
                .next()
                .ok_or_else(|| {
                    TransformationError::MissingData(format!(
                        "No 'once' call found for hook {}",
                        alloy::primitives::Address::from(hook)
                    ))
                })?;

            // Extract data from the call result
            let pool_key_currency0 = hook_call.result.get("poolKey.currency0")
                .ok_or_else(|| TransformationError::MissingField("poolKey.currency0".into()))?
                .as_address()
                .ok_or_else(|| TransformationError::TypeConversion("currency0".into()))?;

            // Build database operations...
        }

        Ok(ops)
    }
}

impl EventHandler for V4CreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(
            "UniswapV4Initializer",
            "Create(address,address,address)"
        )]
    }

    // Declare that this handler needs these eth_call results to be decoded
    // before it can process events
    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![
            ("DERC20".to_string(), "once".to_string()),
            ("Numeraires".to_string(), "once".to_string()),
            ("DopplerV4Hook".to_string(), "once".to_string()),
        ]
    }
}
```

### Registering Handlers

Handlers are registered at compile-time via the `build_registry()` or `build_registry_for_chain()` functions in `src/transformations/registry.rs`. Each handler category has its own registration function.

**Event handlers** in `src/transformations/event/mod.rs` take `chain_id` (used by handlers that need chain-specific state like the `PoolMetadataCache`):

```rust
pub fn register_handlers(registry: &mut TransformationRegistry, chain_id: u64) {
    derc20_transfer::register_handlers(registry);
    v4::create::register_handlers(registry);
    // Handlers that share a PoolMetadataCache pass it down:
    let v3_cache = Arc::new(PoolMetadataCache::new());
    v3::create::register_handlers(registry, v3_cache.clone());
    v3::metrics::register_handlers(registry, chain_id, v3_cache);
}
```

**Eth_call handlers** in `src/transformations/eth_call/mod.rs`:

```rust
pub fn register_handlers(registry: &mut TransformationRegistry) {
    // oracle::register_handlers(registry);
}
```

Each handler module typically exports a `register_handlers` function:

```rust
// In src/transformations/event/v4/create.rs
pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(V4CreateHandler);
}
```

`build_registry(chain_id)` builds a global registry (used for running migrations at startup). In this mode, chain-filtered dependency specs are kept so migrations and startup validation see the full graph. `build_registry_for_chain(chain_id, contracts, factory_collections)` builds a registry filtered to handlers whose trigger sources are present in the chain's config and resolves dependency specs for that concrete `chain_id`.

## TransformationContext

The context provides handlers with all necessary data and utilities:

### Chain Information

```rust
ctx.chain_name      // "base", "mainnet", etc.
ctx.chain_id        // 8453, 1, etc.
ctx.blockrange_start
ctx.blockrange_end
```

### Decoded Data Access

```rust
// All events in current range (Arc<Vec<DecodedEvent>>)
ctx.events

// All calls in current range (Arc<Vec<DecodedCall>>)
ctx.calls

// Filter by type (automatically respects contract start_block configuration)
for event in ctx.events_of_type("Contract", "EventName") { ... }
for call in ctx.calls_of_type("Contract", "functionName") { ... }

// Filter by address (automatically respects contract start_block configuration)
for event in ctx.events_for_address(address) { ... }
for call in ctx.calls_for_address(address) { ... }

// Get contract start_block from configuration
if let Some(start_block) = ctx.get_contract_start_block("MyContract") {
    // start_block is the configured start block for this contract
}
```

**Note:** The filtering methods (`events_of_type`, `events_for_address`, etc.) automatically filter out events/calls that occur before a contract's configured `start_block`. This prevents processing events from before the contract was deployed.

### Transaction Address Lookup

Access transaction sender and recipient addresses from receipt data:

```rust
// Get the from_address for a transaction
if let Some(from) = ctx.tx_from(&event.transaction_hash) {
    // from is &[u8; 20]
}

// Get the to_address for a transaction (may be None for contract creation)
if let Some(to) = ctx.tx_to(&event.transaction_hash) {
    // to is &[u8; 20]
}
```

### Contract Configuration Helpers

Look up contract names and match addresses against configured contracts:

```rust
// Look up a contract name by its address
if let Some(name) = ctx.get_contract_name_by_address(address) {
    println!("Address belongs to contract: {}", name);
}

// Check if an address matches any of the specified contract names
let migration_type = ctx.match_contract_address(
    migrator_address,
    &["UniswapV4Migrator", "UniswapV2Migrator", "UniswapV3Migrator"],
).map(|contract_name| {
    match contract_name {
        "UniswapV4Migrator" => "v4",
        "UniswapV2Migrator" => "v2",
        "UniswapV3Migrator" => "v3",
        _ => "unknown",
    }
}).unwrap_or("unknown");
```

### Historical Queries

Query previously decoded data from Parquet files:

```rust
// Query events from past blocks
let past_events = ctx.query_events(HistoricalEventQuery {
    source: Some("UniswapV4PoolManager".into()),
    event_name: Some("Initialize".into()),
    from_block: 0,
    to_block: ctx.blockrange_end,  // Cannot exceed current range
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

**Important:** Historical queries cannot access future blocks. The `to_block` cannot exceed `blockrange_end` or the query will return a `FutureBlockAccess` error.

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
    NamedTuple(Vec<(String, DecodedValue)>),  // Tuple with named fields
    UnnamedTuple(Vec<DecodedValue>),           // Tuple without field names
    Array(Vec<DecodedValue>),
}

// Conversion methods
value.as_address()      // Option<[u8; 20]>
value.as_bytes32()      // Option<[u8; 32]>
value.as_uint256()      // Option<U256> (also converts from smaller uint types)
value.as_int256()       // Option<I256> (also converts from smaller int types)
value.as_u64()          // Option<u64>
value.as_i64()          // Option<i64>
value.as_i32()          // Option<i32>
value.as_u32()          // Option<u32>
value.as_u8()           // Option<u8>
value.as_bool()         // Option<bool>
value.as_string()       // Option<&str>
value.as_bytes()        // Option<&[u8]>
value.to_numeric_string() // Option<String> (for database storage)
value.get_field("name") // Option<&DecodedValue> (for named tuples)
```

## FieldExtractor Trait

Both `DecodedEvent` and `DecodedCall` implement the `FieldExtractor` trait, providing convenient typed extraction methods with contextual error messages:

```rust
pub trait FieldExtractor {
    fn extract_address(&self, field: &str) -> Result<[u8; 20], TransformationError>;
    fn extract_uint256(&self, field: &str) -> Result<U256, TransformationError>;
    fn extract_int256(&self, field: &str) -> Result<I256, TransformationError>;
    fn extract_u64(&self, field: &str) -> Result<u64, TransformationError>;
    fn extract_i64(&self, field: &str) -> Result<i64, TransformationError>;
    fn extract_u32(&self, field: &str) -> Result<u32, TransformationError>;
    fn extract_i32(&self, field: &str) -> Result<i32, TransformationError>;
    fn extract_u8(&self, field: &str) -> Result<u8, TransformationError>;
    fn extract_bool(&self, field: &str) -> Result<bool, TransformationError>;
    fn extract_string(&self, field: &str) -> Result<&str, TransformationError>;
    fn extract_bytes32(&self, field: &str) -> Result<[u8; 32], TransformationError>;
    fn extract_bytes(&self, field: &str) -> Result<&[u8], TransformationError>;
    fn extract_u64_flexible(&self, field: &str) -> Result<u64, TransformationError>;
    fn extract_i32_flexible(&self, field: &str) -> Result<i32, TransformationError>;
    fn extract_u32_flexible(&self, field: &str) -> Result<u32, TransformationError>;
}

// Usage example:
let from = event.extract_address("from")?;
let amount = event.extract_uint256("value")?;
let tick = call.extract_i32("tick")?;
```

The `_flexible` methods handle conversion from multiple numeric types and numeric strings, useful when the exact encoding varies:

- `extract_u64_flexible`: handles u64, i64, and numeric strings
- `extract_i32_flexible`: handles i32, u32, and numeric strings
- `extract_u32_flexible`: handles u32, i32, and numeric strings

## Database Operations

Handlers return `Vec<DbOperation>` which are executed transactionally. The engine automatically injects `source` and `source_version` into each operation — handlers should not include these columns.

### Upsert

```rust
DbOperation::Upsert {
    table: "swaps".into(),
    columns: vec!["id".into(), "amount".into()],
    values: vec![DbValue::Bytes32(id), DbValue::Numeric(amount)],
    conflict_columns: vec!["id".into()],
    update_columns: vec!["amount".into()],
    update_condition: None,  // Optional WHERE on the DO UPDATE (e.g. "EXCLUDED.block_number > swaps.block_number")
}
// Engine injects source/source_version into columns, values, and conflict_columns
// Generates: INSERT INTO swaps (id, amount, source, source_version) VALUES ($1, $2, $3, $4)
//            ON CONFLICT (id, source, source_version) DO UPDATE SET amount = EXCLUDED.amount
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
// Engine injects source/source_version into the where_clause
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
    query: "UPDATE pools SET total = total + $1 WHERE id = $2 AND source = $3 AND source_version = $4".into(),
    params: vec![DbValue::Numeric(amount), DbValue::Bytes32(id), DbValue::Text(source), DbValue::Int32(version)],
}
// Note: RawSql is NOT auto-injected — handler must include source/source_version manually
```

## DbValue Types

| Variant | Rust Type | PostgreSQL Type |
|---------|-----------|-----------------|
| `Null` | - | NULL |
| `Bool(bool)` | `bool` | BOOLEAN |
| `Int2(i16)` | `i16` | SMALLINT (INT2) |
| `Int32(i32)` | `i32` | INTEGER |
| `Int64(i64)` | `i64` | BIGINT |
| `Uint64(u64)` | `u64` | BIGINT |
| `Float64(f64)` | `f64` | DOUBLE PRECISION |
| `Text(String)` | `String` | TEXT |
| `VarChar(String)` | `String` | VARCHAR |
| `Bytes(Vec<u8>)` | `Vec<u8>` | BYTEA |
| `Address([u8; 20])` | `[u8; 20]` | BYTEA |
| `Bytes32([u8; 32])` | `[u8; 32]` | BYTEA |
| `Numeric(String)` | `String` | NUMERIC |
| `Timestamp(i64)` | `i64` | BIGINT |
| `Json(Value)` | `serde_json::Value` | JSON |
| `JsonB(Value)` | `serde_json::Value` | JSONB |

## Error Handling

Transformation errors are handled per-handler to maintain data integrity while allowing other handlers to continue:

```rust
pub enum TransformationError {
    HandlerError { handler_name: String, message: String },
    DatabaseError(DbError),
    MissingField(String),           // Required field missing from event/call params
    MissingData(String),            // Required data not found (e.g., no matching call)
    MissingColumn(String),          // Column missing in parquet file
    RpcError(String),
    FutureBlockAccess { requested: u64, current_max: u64 },
    DecodeError(String),
    IoError(std::io::Error),
    ParquetError(parquet::errors::ParquetError),
    ArrowError(arrow::error::ArrowError),
    TypeConversion(String),
    ConfigError(String),
    ChannelError(String),
    TransientBlocked(String),       // Call-dep blocking — range deferred, not failed
    IncludesPrecompileError(String), // Handler's asset or numeraire is a precompile address
    LiveStorage(StorageError),      // Live storage read/write failures
}

// Create a handler error with context
TransformationError::handler("MyHandler", "Failed to process swap event")
```

Notable variants:

- **`TransientBlocked(String)`**: Returned when a handler's call-dependency files are not yet available. During catchup, this causes the range to be deferred to a later pass rather than treated as a failure.
- **`IncludesPrecompileError(String)`**: Returned when a handler detects that an asset or numeraire address is a precompile (addresses 0x01-0x09). These handlers are skipped rather than errored.

When a handler returns an error:

1. The handler's transaction is rolled back (no partial data for that handler)
2. The error is logged with full context (handler key, block range)
3. Other handlers continue processing independently
4. In live mode, failed handler keys are persisted to the block's status file for retry

During catchup, a handler error stops catchup for that specific handler and propagates via cascade to all dependent handlers (see [Handler Dependency Chains](#handler-dependency-chains)). Independent handlers are unaffected.

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

## Live Mode Progress Tracking

In live mode, the transformation engine integrates with `LiveProgressTracker` to coordinate handler completion with the compaction service.

### How It Works

1. **Registration:** Before starting, all handlers are registered with the progress tracker:
   ```rust
   for handler in registry.all_handlers() {
       tracker.register_handler(&handler.handler_key());
   }
   ```

2. **Completion marking:** After processing a single-block range (live mode), the engine marks progress:
   ```rust
   // After record_completed_range_for_handler()
   if range_end - range_start == 1 {
       tracker.mark_complete(range_start, &handler_key).await?;
   }
   ```

3. **Compaction coordination:** The compaction service checks `is_block_complete(block)` before compacting. A block is complete only when ALL registered handlers have marked it complete.

### Why Single-Block Detection?

The engine uses `range_end - range_start == 1` to identify live mode ranges:
- Historical catchup: ranges like 1000-1999 (multi-block)
- Live mode: ranges like 100-101 (single block)

This heuristic allows the engine to use the same code path for both modes while only updating the live progress tracker for actual live blocks.

### Database Tables

| Table | Purpose |
|-------|---------|
| `_handler_progress` | Per-handler catchup progress (range-based) |
| `_live_progress` | Per-handler live block completion (block-based) |

During compaction, `_live_progress` entries are migrated to `_handler_progress` and deleted.

## File Structure

```
src/transformations/
├── mod.rs           # Module exports and re-exports
├── traits.rs        # TransformationHandler, EventHandler, EthCallHandler traits
├── context.rs       # TransformationContext, DecodedEvent, DecodedCall, FieldExtractor
├── engine.rs        # TransformationEngine (orchestration, catchup, live processing)
├── executor.rs      # HandlerExecutor — concurrent handler spawn-loop, source/version injection
├── finalizer.rs     # RangeFinalizer — range finalization, reorg cleanup, progress tracking
├── live_state.rs    # LiveProcessingState — pending event buffering, completion tracking
├── retry.rs         # RetryProcessor — live retry from bincode storage for failed blocks
├── registry.rs      # Handler registration and build_registry()
├── historical.rs    # HistoricalDataReader for parquet queries
├── error.rs         # TransformationError enum
├── scheduler/       # DAG-based execution scheduling
│   ├── mod.rs           # Public exports
│   ├── dag.rs           # DagScheduler, WorkItem, WorkItemOutcome
│   ├── tracker.rs       # CompletionTracker (dependency satisfaction)
│   └── loader.rs        # CatchupLoader (end-to-end WorkItem execution)
├── event/           # Event handlers
│   ├── mod.rs           # register_handlers() for all event handlers
│   ├── derc20_transfer.rs  # ERC20 Transfer handler
│   ├── airlock_migrate.rs  # Airlock migration handler
│   ├── v4/              # V4-specific handlers
│   │   ├── mod.rs
│   │   └── create.rs
│   ├── multicurve/      # Multicurve handlers
│   │   ├── mod.rs
│   │   └── create.rs
│   ├── scheduled_multicurve/  # Scheduled multicurve handlers
│   │   ├── mod.rs
│   │   └── create.rs
│   ├── decay_multicurve/  # Decay multicurve handlers
│   │   └── mod.rs
│   ├── dhook/           # DHook handlers
│   │   └── (scaffolded)
│   └── metrics/         # Shared metrics utilities
│       ├── mod.rs
│       ├── accumulator.rs    # BlockAccumulator
│       ├── swap_data.rs      # Swap processing
│       ├── v4_hook_extract.rs # V4 hook extraction
│       └── metrics.rs        # Metric calculations
├── eth_call/        # Call handlers
│   └── mod.rs           # register_handlers() for all call handlers
└── util/            # Shared utilities
    ├── mod.rs
    ├── metadata.rs      # Token metadata extraction utilities
    ├── sanitize.rs      # Address validation (precompile detection)
    └── db/              # Database operation helpers
        ├── mod.rs
        ├── pool.rs      # insert_pool helper
        ├── token.rs     # insert_token helper
        ├── users.rs     # upsert_user helper
        ├── transfers.rs # insert_transfer helper
        ├── v4_pool_configs.rs  # insert_pool_config helper
        ├── dhook_pool_configs.rs  # insert_dhook_pool_config helper
        ├── skipped_addresses.rs  # Skipped address filtering
        └── pool_metrics.rs       # Pool metrics queries

migrations/
├── 000_handler_progress.sql  # _handler_progress table
├── 001_active_versions.sql   # active_versions table
├── 002_live_progress.sql     # _live_progress table
├── tables/              # Table creation migrations
│   ├── tokens.sql
│   ├── pools.sql
│   ├── transfers.sql
│   ├── users.sql
│   ├── v4_pool_configs.sql
│   ├── swaps.sql
│   ├── pool_metrics.sql
│   ├── token_metrics.sql
│   ├── liquidity_delta.sql
│   ├── position.sql
│   ├── portfolios.sql
│   └── cumulated_fees.sql
└── views/               # View migrations
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
7. **Don't include source/source_version** - The engine injects these automatically into all operations (except `RawSql`)
8. **Use entity table names** - Use `pools`, `swaps` etc. instead of versioned names like `v3_pools_v1`
