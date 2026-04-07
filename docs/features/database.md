# Database Module

The database module provides a PostgreSQL connection pool, typed operation builder, migration system, and deadlock-resilient transaction execution. It is the sole interface through which the indexer persists transformed blockchain data.

## Scope

- Connection pool management via `deadpool_postgres`
- Typed SQL operation construction (insert, upsert, update, delete, raw SQL)
- Atomic transaction execution with automatic deadlock retry
- Snapshot reads within a transaction (pre-modification state)
- System and handler migration management
- Structured error types with detailed PostgreSQL error formatting

## Non-Scope

- Query planning or ORM functionality -- SQL is generated from typed operations, not a query DSL
- Schema design -- table schemas are defined in migration files, not in Rust types
- Connection encryption -- connections use `NoTls` (TLS is not configured)
- Read replicas or multi-database routing

## Module Structure

```
src/db/
├── mod.rs         # Public exports: DbError, DbPool, DbOperation, DbValue, WhereClause
├── pool.rs        # DbPool struct, transaction execution, SQL generation, deadlock retry
├── types.rs       # DbOperation, DbValue, WhereClause enums
├── error.rs       # DbError enum with detailed PostgreSQL formatting
└── migrations.rs  # System and handler migration runner

migrations/
├── 000_handler_progress.sql   # _handler_progress tracking table
├── 001_active_versions.sql    # _active_versions tracking table
└── 002_live_progress.sql      # _live_progress tracking table
```

## Data Flow

### Write Path

1. Transformation handlers produce `Vec<DbOperation>` from decoded blockchain data.
2. The transformation engine calls `DbPool::execute_transaction(operations)`.
3. `execute_transaction` delegates to `try_execute_transaction`, which:
   a. Acquires a connection from the pool.
   b. Opens a PostgreSQL transaction.
   c. For each `DbOperation`, calls `build_operation_sql()` to produce a SQL string and `Vec<SqlParam>`.
   d. Executes each statement within the transaction.
   e. Commits the transaction.
4. If step 3 fails with PostgreSQL error code `40P01` (deadlock detected), the entire transaction is retried up to `DEADLOCK_MAX_RETRIES` (3) times with exponential backoff starting at 50ms.

### Snapshot Read Path

1. The metrics/snapshot system calls `execute_transaction_with_snapshot_reads(snapshot_specs, operations)`.
2. Inside a single transaction:
   a. Snapshot queries run first, reading committed state before any modifications.
   b. Write operations execute, each returning an affected row count.
3. The transaction commits and returns `(Vec<Option<Vec<(String, DbValue)>>>, Vec<u64>)` -- snapshot results and per-operation affected row counts.
4. This method does not have deadlock retry (it is expected to be called from contexts where deadlocks are unlikely).

### Migration Path

1. On startup, `DbPool::run_migrations()` is called:
   a. Creates `_migrations` table if it does not exist (id, name, applied_at).
   b. Reads all `.sql` files from `migrations/` directory, sorted by filename.
   c. Skips already-applied migrations (tracked by name in `_migrations`).
   d. Each new migration runs in its own transaction and is recorded in `_migrations`.
2. Then `DbPool::run_handler_migrations(registry)` is called:
   a. Iterates all handlers in the registry, collecting `migration_paths()`.
   b. Deduplicates paths via `HashSet` to prevent double-execution when multiple handlers share migration files.
   c. Supports both individual `.sql` files and directories of `.sql` files (sorted by filename within each directory).
   d. Each migration runs in its own transaction and is recorded in `_migrations` using the full path as the name.
   e. Missing paths produce a warning log but do not fail.
3. All handler migrations run globally (once) before any per-chain engine spawning.

## DbPool

### Constructor

```rust
pub async fn new(database_url: &str) -> Result<Self, DbError>
```

- Parses the connection string into `tokio_postgres::Config`.
- Creates a `deadpool_postgres` pool with `RecyclingMethod::Fast`.
- Pool size: `DB_POOL_SIZE` env var, default 16.
- Validates the connection by acquiring and releasing one connection immediately.
- Fails fast on invalid connection strings with `DbError::InvalidConnectionString`.

### Key Methods

| Method | Description |
|--------|-------------|
| `execute_transaction(ops)` | Atomic multi-operation execution with deadlock retry |
| `execute_transaction_with_snapshot_reads(specs, ops)` | Reads pre-modification state inside transaction, returns snapshots and affected row counts |
| `query(sql, params)` | Raw parameterized query returning `Vec<Row>` |
| `query_row(table, key_columns)` | Single row lookup by key columns, returns `Option<Vec<(String, DbValue)>>` |
| `run_migrations()` | Runs system migrations from `migrations/` directory |
| `run_handler_migrations(registry)` | Runs handler-declared migrations from the transformation registry |
| `inner()` | Returns a reference to the underlying `deadpool_postgres::Pool` |

### Deadlock Retry

- Triggered only by PostgreSQL error code `40P01` (`T_R_DEADLOCK_DETECTED`).
- Maximum 3 retries (4 total attempts including the initial one).
- Exponential backoff: 50ms, 100ms, 200ms.
- The `is_deadlock()` helper inspects `DbError::PostgresError` variants for the specific SQL state.

## DbValue

Typed representation of values that can be stored in PostgreSQL. Each variant maps to a specific PostgreSQL type.

| Variant | Rust Type | PostgreSQL Type | SQL Placeholder |
|---------|-----------|----------------|-----------------|
| `Null` | -- | NULL | `$N` |
| `Bool(bool)` | `bool` | BOOLEAN | `$N` |
| `Int64(i64)` | `i64` | BIGINT | `$N` |
| `Int32(i32)` | `i32` | INTEGER | `$N` |
| `Int2(i16)` | `i16` | SMALLINT | `$N` |
| `Uint64(u64)` | `u64` | BIGINT (cast to i64) | `$N` |
| `Text(String)` | `String` | TEXT | `$N` |
| `VarChar(String)` | `String` | VARCHAR | `$N` |
| `Bytes(Vec<u8>)` | `Vec<u8>` | BYTEA | `$N` |
| `Address([u8; 20])` | `[u8; 20]` | BYTEA | `$N` |
| `Bytes32([u8; 32])` | `[u8; 32]` | BYTEA | `$N` |
| `Numeric(String)` | `String` | NUMERIC | `$N::text::numeric` |
| `Timestamp(i64)` | `i64` (unix seconds) | TIMESTAMP | `to_timestamp($N)` |
| `Json(Value)` | `serde_json::Value` | JSON | `$N` |
| `JsonB(Value)` | `serde_json::Value` | JSONB | `$N::jsonb` |
| `Float64(f64)` | `f64` | FLOAT8 | `$N` |

### Convenience Constructors

- `DbValue::jsonb<T: Serialize>(value)` -- serializes any `Serialize` type into a `JsonB` variant.
- `DbValue::json<T: Serialize>(value)` -- serializes any `Serialize` type into a `Json` variant.

### Value Conversion

`DbValue` is converted to `SqlParam` (an internal enum that implements `ToSql`) before being passed to `tokio_postgres`. Notable conversions:

- `Uint64(v)` is cast to `i64` (potential overflow for values > `i64::MAX`).
- `Address` and `Bytes32` are converted to `Vec<u8>`.
- `Numeric` is sent as text and cast by PostgreSQL via `::text::numeric`.
- `Timestamp` is sent as `f64` and wrapped in `to_timestamp()`.

### Row Extraction

When reading rows back from PostgreSQL (via `query_row` or snapshot reads), the `extract_db_value_from_row` function maps PostgreSQL column types back to `DbValue` variants:

- `BOOL` -> `Bool`
- `INT8` -> `Int64`
- `INT4` -> `Int32`
- `INT2` -> `Int2`
- `FLOAT8` -> `Float64`
- `TEXT`, `VARCHAR` -> `Text`
- `BYTEA` -> `Bytes`
- `NUMERIC` -> `Numeric` (via `rust_decimal::Decimal` with string fallback)
- `TIMESTAMP`, `TIMESTAMPTZ` -> `Timestamp` (unix seconds via `SystemTime`)
- `JSON`, `JSONB` -> `JsonB`
- Unknown types -> attempts `Bytes`, then `Text`, then `Null`

## DbOperation

Typed enum representing SQL write operations.

### Variants

**Upsert** -- `INSERT ... ON CONFLICT DO UPDATE`:
```rust
Upsert {
    table: String,
    columns: Vec<String>,
    values: Vec<DbValue>,
    conflict_columns: Vec<String>,    // Unique constraint columns
    update_columns: Vec<String>,      // Columns to update on conflict
    update_condition: Option<String>,  // Optional WHERE on DO UPDATE
}
```
- If `update_columns` is empty, generates `ON CONFLICT DO NOTHING`.
- If `update_condition` is provided, adds a `WHERE` clause to the `DO UPDATE SET` (e.g., conditional updates based on block number).

**Insert** -- simple `INSERT INTO`:
```rust
Insert {
    table: String,
    columns: Vec<String>,
    values: Vec<DbValue>,
}
```

**Update** -- `UPDATE ... SET ... WHERE`:
```rust
Update {
    table: String,
    set_columns: Vec<(String, DbValue)>,
    where_clause: WhereClause,
}
```

**Delete** -- `DELETE FROM ... WHERE`:
```rust
Delete {
    table: String,
    where_clause: WhereClause,
}
```

**RawSql** -- arbitrary parameterized SQL:
```rust
RawSql {
    query: String,
    params: Vec<DbValue>,
}
```

## WhereClause

Typed enum for building WHERE conditions.

| Variant | SQL Output |
|---------|-----------|
| `Eq(col, val)` | `"col" = $N` |
| `And(vec)` | `"col1" = $N AND "col2" = $M AND ...` |
| `Raw { condition, params }` | Raw SQL condition with parameterized values |

## SQL Generation

All SQL is generated by internal `build_*_sql` functions using a `QueryBuilder` struct that tracks parameter indices automatically.

Key properties:
- **All column names are double-quoted** via `quote_ident()` to handle PostgreSQL reserved keywords.
- **Parameter indices are 1-based** and managed automatically by `QueryBuilder`.
- **Type-specific placeholders** are generated by `placeholder_for()` (see the DbValue table above).
- Upsert uses `EXCLUDED.column` references in the `DO UPDATE SET` clause.

## DbError

Structured error type using `thiserror`.

| Variant | Source | Description |
|---------|--------|-------------|
| `PoolError` | `deadpool_postgres::PoolError` | Connection pool exhaustion or checkout failure |
| `PostgresError` | `tokio_postgres::Error` | SQL execution errors with detailed formatting |
| `BuildError` | `deadpool_postgres::BuildError` | Pool construction failure |
| `ConfigError` | `String` | Configuration errors |
| `MigrationError` | `String` | Migration execution or tracking errors |
| `IoError` | `std::io::Error` | File I/O errors (reading migration files) |
| `InvalidConnectionString` | `String` | Malformed database URL |

### PostgreSQL Error Formatting

`PostgresError` uses a custom `format_pg_error` function that extracts structured details from `tokio_postgres::Error`:
- Error code (e.g., `23505` for unique violation)
- Message
- Detail (if present)
- Hint (if present)
- Table name (if present)
- Column name (if present)
- Constraint name (if present)

Example output:
```
PostgreSQL error [23505]: duplicate key value violates unique constraint "pools_pkey"
  Detail: Key (chain_id, pool_address)=(1, \x1234...) already exists.
  Table: v3_pools_v1
  Constraint: pools_pkey
```

## Migration System

### System Migrations

Located in `migrations/` directory. Tracked in the `_migrations` table with columns:
- `id SERIAL PRIMARY KEY`
- `name VARCHAR(255) NOT NULL UNIQUE`
- `applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()`

Current system migrations:
- `000_handler_progress.sql` -- creates `_handler_progress` table for per-handler transformation progress tracking
- `001_active_versions.sql` -- creates `_active_versions` table for tracking active handler versions
- `002_live_progress.sql` -- creates `_live_progress` table for live mode progress tracking

### Handler Migrations

Declared by transformation handlers via `migration_paths()` method. Each handler can return paths to individual `.sql` files or directories containing `.sql` files.

Properties:
- Deduplication via `HashSet<PathBuf>` prevents the same migration from running twice when multiple handlers reference the same path.
- Directory entries are sorted by filename for deterministic ordering.
- Missing paths log a warning but do not cause failure.
- Migration names in `_migrations` use the full filesystem path (e.g., `migrations/handlers/v3/000_create_pools.sql`).
- All handler migrations execute globally at startup, not per-chain.

## Invariants and Constraints

1. **Column quoting**: All column names in generated SQL are double-quoted. This is unconditional and applies to all operations.
2. **Transaction atomicity**: All operations within a single `execute_transaction` call succeed or fail together.
3. **Deadlock retry scope**: Only PostgreSQL `40P01` errors trigger retry. All other errors propagate immediately.
4. **Snapshot consistency**: Snapshot reads in `execute_transaction_with_snapshot_reads` see committed state before any writes in the same transaction because reads execute first.
5. **Migration idempotency**: Migrations are tracked by name and skipped if already applied. Re-running migrations is safe.
6. **Migration deduplication**: Handler migration paths are deduplicated before execution. Two handlers declaring the same migration path will not cause double-execution.
7. **Pool validation**: `DbPool::new` acquires and releases a connection immediately, failing fast on unreachable databases or invalid credentials.
8. **Uint64 overflow**: `DbValue::Uint64` is cast to `i64` for PostgreSQL `BIGINT`. Values exceeding `i64::MAX` will overflow silently.
9. **No TLS**: Connections use `NoTls`. This is appropriate for localhost or VPC-internal databases but not for connections over the public internet.

## Dependencies

- `deadpool-postgres` -- async connection pool
- `tokio-postgres` -- async PostgreSQL driver
- `rust_decimal` -- NUMERIC column extraction
- `serde_json` -- JSON/JSONB value handling
- `thiserror` -- structured error derivation
- `bytes` -- `BytesMut` for `ToSql` implementation
