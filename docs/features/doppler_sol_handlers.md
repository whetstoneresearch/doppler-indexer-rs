# Doppler-Sol Solana Handler Implementation Brief

## Goal

Implement Rust transformation handlers that index events from the four Doppler Solana programs into PostgreSQL. This brief is self-contained — a fresh agent can implement everything from it without reading prior conversation.

## Codebase Locations

- **Indexer**: `/home/nymph/Code/whetstone/doppler-indexer-rs/solana-support/`
- **Contracts**: `/home/nymph/Code/whetstone/doppler-sol/`

---

## Handler Interface

All handlers live in `src/transformations/event/`. Each module must:

1. Define a handler struct
2. Implement `TransformationHandler` via `#[async_trait]`
3. Implement `EventHandler` (marker trait)
4. Expose `pub fn register_handlers(registry: &mut TransformationRegistry)`

Use `src/transformations/event/derc20_transfer.rs` as the canonical template.

Minimal skeleton:

```rust
use async_trait::async_trait;
use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

pub struct MyHandler;

#[async_trait]
impl TransformationHandler for MyHandler {
    fn name(&self) -> &'static str { "MyHandler" }
    fn version(&self) -> u32 { 1 }
    fn chain_type(&self) -> ChainType { ChainType::Solana }
    fn migration_paths(&self) -> Vec<&'static str> { vec!["migrations/tables/my_table.sql"] }
    fn reorg_tables(&self) -> Vec<&'static str> { vec!["my_table"] }

    async fn handle(&self, ctx: &TransformationContext) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();
        for event in ctx.events_of_type("DopplerCPMM", "PoolInitialized") {
            // extract fields, push DbOperation::Upsert
        }
        Ok(ops)
    }
}

impl EventHandler for MyHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new("DopplerCPMM", "PoolInitialized")]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(MyHandler);
}
```

---

## Critical Solana-Specific Rules

### chain_type()
Every Solana handler MUST override — the default is EVM:
```rust
fn chain_type(&self) -> ChainType { ChainType::Solana }
```

### Source names
The source string in `EventTrigger::new(source, event_name)` must match the key in the JSON config:

| Program | Source name string |
|---|---|
| CPMM (`9PSxVPoP...`) | `"DopplerCPMM"` |
| Initializer (`4h3Dqyo5...`) | `"DopplerInitializer"` |
| CPMM Migrator (`7WMUTNC4...`) | `"DopplerCpmmMigrator"` |
| Prediction Migrator (`HYHdyy7Q...`) | `"DopplerPredictionMigrator"` |

### Field extraction
- `Pubkey` → `event.extract_pubkey("field")?` → `[u8; 32]`
- `u64` → `event.extract_u64("amount")?`
- `u32` → `event.extract_u32("flags")?`
- `u16` → `event.extract_u32("fee_bps")? as u16` (no `extract_u16`; `u16` decodes as `Uint32`)
- `u8` → `event.extract_u8("direction")?`
- `u128` → `event.extract_uint256("shares")?` then `DbValue::Numeric(v.to_string())` (no `extract_u128`; `Uint128` widens to `U256` via `as_uint256()`)
- `[u8; 32]` raw bytes → `event.extract_bytes32("launch_id")?`
- `bool` → `event.extract_bool("is_winner")?`

### DbValue → Postgres type mapping
| Value | DbValue variant | Column type |
|---|---|---|
| Solana pubkey `[u8;32]` | `DbValue::Pubkey(arr)` | `BYTEA` |
| Solana tx sig `[u8;64]` | `DbValue::Bytes(sig.to_vec())` | `BYTEA` |
| `u64` amount | `DbValue::Uint64(v)` | `BIGINT` |
| `u128` / shares | `DbValue::Numeric(u256.to_string())` | `NUMERIC` |
| `u32` | `DbValue::Int32(v as i32)` | `INT` |
| `u16` / `u8` | `DbValue::Int2(v as i16)` | `SMALLINT` |
| `bool` | `DbValue::Bool(v)` | `BOOLEAN` |
| raw `[u8;32]` bytes | `DbValue::Bytes32(arr)` | `BYTEA` |
| timestamp | `DbValue::Timestamp(block_timestamp as i64)` | `TIMESTAMPTZ` |
| JSONB | `DbValue::JsonB(serde_json::json!({...}))` | `JSONB` |

### Accessing Solana event metadata
```rust
// 64-byte transaction signature:
let tx_sig = event.transaction_id.as_solana()
    .expect("Solana event has Solana tx id");

// Instruction-level log position (packed i64 ordinal):
let log_position = event.position.packed_ordinal_i64();

// Slot number ("block number" for Solana):
let slot = event.block_number;

// Block timestamp (unix seconds):
let ts = event.block_timestamp;
```

### source / source_version
The executor **auto-injects** `source` and `source_version` into every `Upsert`/`Insert`. Do NOT add them to `columns`/`values`. DO include them in `conflict_columns` — the executor appends them there automatically.

### DbOperation::Update and source scoping
For updates to shared tables (e.g. setting `migrated_at` on a `pools` row), the executor also injects `source`/`source_version` into the `WHERE` clause of `DbOperation::Update`. So always scope updates with `chain_id` + the natural key only:

```rust
DbOperation::Update {
    table: "pools".to_string(),
    set_columns: vec![("migrated_at".into(), DbValue::Timestamp(ts))],
    where_clause: WhereClause::And(vec![
        WhereClause::Eq("chain_id".into(), DbValue::Int64(ctx.chain_id as i64)),
        WhereClause::Eq("address".into(), DbValue::Pubkey(launch)),
    ]),
}
```

---

## Shared Table Schema Changes (already created)

Two ALTER TABLE migration files exist at:
- `migrations/tables/pools_solana_nullable.sql`
- `migrations/tables/swaps_solana_nullable.sql`

These make the following columns nullable (idempotent — safe to run on existing DBs):

**pools**: `integrator`, `initializer`, `fee`, `starting_time`, `ending_time`

**swaps**: `asset`, `fee0_delta`, `fee1_delta`, `tokens_sold_delta`, `graduation_balance_delta`, `max_threshold_delta`, `market_cap_usd_delta`, `liquidity_usd_delta`, `value_usd`

Every handler that writes to `pools` or `swaps` must include these files in `migration_paths()`.

---

## Program Events: exact field names and types

### CPMM — source: `"DopplerCPMM"`
```
PoolInitialized     { pool: Pubkey, token0_mint: Pubkey, token1_mint: Pubkey, vault0: Pubkey, vault1: Pubkey }
Swap                { pool: Pubkey, user: Pubkey, direction: u8, amount_in: u64, amount_out: u64, fee_total: u64, fee_dist: u64 }
AddLiquidity        { pool: Pubkey, owner: Pubkey, amount0: u64, amount1: u64, shares_out: u128 }
RemoveLiquidity     { pool: Pubkey, owner: Pubkey, amount0: u64, amount1: u64, shares_in: u128 }
PositionCreated     { pool: Pubkey, owner: Pubkey, position: Pubkey, position_id: u64 }
PositionClosed      { pool: Pubkey, owner: Pubkey, position: Pubkey }
```

### Initializer — source: `"DopplerInitializer"`
```
LaunchInitialized   { launch: Pubkey, namespace: Pubkey, launch_id: [u8;32], base_mint: Pubkey, quote_mint: Pubkey, base_total_supply: u64, curve_kind: u8, migrator_program: Pubkey, sentinel_program: Pubkey }
CurveSwap           { launch: Pubkey, user: Pubkey, direction: u8, amount_in: u64, amount_out: u64, fee_paid: u64, post_base_reserve: u64, post_quote_reserve: u64 }
LaunchMigrated      { launch: Pubkey, migrator_program: Pubkey, base_vault_amount: u64, quote_vault_amount: u64 }
```

### CPMM Migrator — source: `"DopplerCpmmMigrator"`
```
LaunchRegistered    { launch: Pubkey, state: Pubkey, cpmm_config: Pubkey, min_raise_quote: u64 }
LaunchMigrated      { launch: Pubkey, pool: Pubkey, quote_for_liquidity: u64, base_for_liquidity: u64 }
```

### Prediction Migrator — source: `"DopplerPredictionMigrator"`
```
MarketCreated       { market: Pubkey, oracle: Pubkey, quote_mint: Pubkey }
EntryRegistered     { market: Pubkey, oracle: Pubkey, entry_id: [u8;32], base_mint: Pubkey }
EntryMigrated       { market: Pubkey, oracle: Pubkey, entry_id: [u8;32], base_mint: Pubkey, contribution: u64, is_winner: bool }
MarketResolved      { market: Pubkey, oracle: Pubkey, winner_mint: Pubkey, claimable_supply: u64, total_pot: u64 }
RewardsClaimed      { market: Pubkey, claimer: Pubkey, burned_amount: u64, reward_amount: u64, total_burned: u64 }
```
*(Skip `AccumulatorUpdated` — internal bookkeeping)*

---

## Handler Plan

### Group 1: `src/transformations/event/solana/cpmm/`

```
mod.rs
pool_create.rs
swap.rs
metrics.rs
positions.rs
```

#### `mod.rs`
```rust
pub mod metrics;
pub mod pool_create;
pub mod positions;
pub mod swap;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    pool_create::register_handlers(registry);
    swap::register_handlers(registry);
    metrics::register_handlers(registry);
    positions::register_handlers(registry);
}
```

---

#### `pool_create.rs` — `CpmmPoolCreateHandler`

Writes to the shared **`pools`** table.

- **Trigger**: `("DopplerCPMM", "PoolInitialized")`
- **migration_paths**: `["migrations/tables/pools.sql", "migrations/tables/pools_solana_nullable.sql"]`
- **reorg_tables**: `["pools"]`
- **conflict_columns**: `["chain_id", "address"]`
- **update_columns**: `[]` (no update on conflict — pool is immutable once created)

Field mapping (all EVM-only columns set to `DbValue::Null`):

| `pools` column | value |
|---|---|
| `chain_id` | `ctx.chain_id as i64` |
| `block_height` | `event.block_number` |
| `created_at` | `event.block_timestamp` |
| `address` | `DbValue::Pubkey(pool)` |
| `base_token` | `DbValue::Pubkey(token0_mint)` |
| `quote_token` | `DbValue::Pubkey(token1_mint)` |
| `is_token_0` | `DbValue::Bool(true)` |
| `type` | `DbValue::VarChar("cpmm".into())` |
| `integrator` | `DbValue::Null` |
| `initializer` | `DbValue::Null` |
| `fee` | `DbValue::Null` |
| `min_threshold` | `DbValue::Null` |
| `max_threshold` | `DbValue::Null` |
| `migrator` | `DbValue::Null` |
| `migrated_at` | `DbValue::Null` |
| `migration_pool` | `DbValue::Null` |
| `migrated_from` | `DbValue::Null` |
| `migration_type` | `DbValue::Null` |
| `lock_duration` | `DbValue::Null` |
| `beneficiaries` | `DbValue::Null` |
| `pool_key` | `DbValue::JsonB(json!({"vault0": hex(vault0), "vault1": hex(vault1)}))` |
| `starting_time` | `DbValue::Null` |
| `ending_time` | `DbValue::Null` |

For `pool_key`, encode each pubkey as base58 string using `bs58::encode(pubkey).into_string()`. Add `bs58` as a dependency if not already present (check `Cargo.toml`).

---

#### `swap.rs` — `CpmmSwapHandler`

Writes to the shared **`swaps`** table.

- **Trigger**: `("DopplerCPMM", "Swap")`
- **migration_paths**: `["migrations/tables/swaps.sql", "migrations/tables/swaps_solana_nullable.sql"]`
- **reorg_tables**: `["swaps"]`
- **conflict_columns**: `["chain_id", "tx_id", "log_position"]`

Field mapping:

| `swaps` column | value |
|---|---|
| `chain_id` | `ctx.chain_id as i64` |
| `tx_id` | `DbValue::Bytes(tx_sig.to_vec())` — 64-byte Solana signature |
| `block_height` | `event.block_number` |
| `timestamp` | `event.block_timestamp` |
| `pool` | `DbValue::Pubkey(pool)` |
| `asset` | `DbValue::Null` |
| `amountIn` | `DbValue::Uint64(amount_in)` |
| `amountOut` | `DbValue::Uint64(amount_out)` |
| `is_buy` | `DbValue::Bool(direction == 0)` |
| `current_tick` | `DbValue::Null` |
| `graduation_tick` | `DbValue::Null` |
| `fee0_delta` | `DbValue::Uint64(fee_total)` |
| `fee1_delta` | `DbValue::Uint64(fee_dist)` |
| `tokens_sold_delta` | `DbValue::Null` |
| `graduation_balance_delta` | `DbValue::Null` |
| `max_threshold_delta` | `DbValue::Null` |
| `market_cap_usd_delta` | `DbValue::Null` |
| `liquidity_usd_delta` | `DbValue::Null` |
| `value_usd` | `DbValue::Null` |
| `log_position` | `event.position.packed_ordinal_i64()` |

---

#### `metrics.rs` — `CpmmMetricsHandler`

Writes OHLC + volume + liquidity snapshots to the shared **`pool_snapshots`** table, mirroring what EVM V3 metrics handlers do. No separate liquidity events table — liquidity changes are folded into `active_liquidity` on the snapshot.

- **Triggers**: `("DopplerCPMM", "Swap")` AND `("DopplerCPMM", "AddLiquidity")` AND `("DopplerCPMM", "RemoveLiquidity")`
- **migration_paths**: `["migrations/tables/pool_snapshots.sql", "migrations/tables/pool_snapshots_add_tvl.sql", "migrations/tables/pool_snapshots_add_volume_usd.sql"]`
- **reorg_tables**: `["pool_snapshots"]`
- **conflict_columns**: `["chain_id", "pool_id", "block_height"]`
- **update_columns**: `["price_open", "price_close", "price_high", "price_low", "active_liquidity", "volume0", "volume1", "swap_count"]`
- **requires_sequential**: `true` (accumulates per-block state across slots)

Processing logic (per block range, grouped by pool and slot):

For each **`Swap`** event:
- Derive price from `amount_in` / `amount_out` (direction=0: token1/token0, direction=1: token0/token1)
- Accumulate `volume0` / `volume1` and `swap_count`
- Track open/high/low/close price within each slot

For each **`AddLiquidity`** / **`RemoveLiquidity`** event:
- Add/subtract from `active_liquidity` accumulator for the pool+slot

Upsert one `pool_snapshots` row per (pool, slot) with the accumulated values. `pool_id` stores the pool pubkey as `DbValue::Pubkey`.

Note: `active_liquidity` in CPMM is shares-based not tick-based. Store raw share delta as a NUMERIC approximation — the column is already `NUMERIC NOT NULL`.

**handler_dependency**: none (reads only from events in `ctx`)

---

#### `positions.rs` — `CpmmPositionHandler`

Separate table from EVM positions. The EVM `positions` table has been renamed to `evm_cl_positions` (concentrated liquidity, with `tick_lower`/`tick_upper`/`liquidity`). CPMM positions are shares-based with no ticks — schemas are incompatible.

Any future EVM handler that writes to the positions table must declare `migration_paths`:
```rust
vec!["migrations/tables/position.sql", "migrations/tables/evm_cl_positions.sql"]
```
(in that order — the rename guard runs first, then the CREATE IF NOT EXISTS).

For the Solana CPMM position handler:

- **Triggers**: `("DopplerCPMM", "PositionCreated")` AND `("DopplerCPMM", "PositionClosed")`
- **migration_paths**: `["migrations/tables/sol_cpmm_positions.sql"]`
- **reorg_tables**: `["sol_cpmm_positions"]`
- **conflict_columns**: `["chain_id", "position"]`

On `PositionCreated`: Upsert with `closed_at = Null`, `update_columns: []`
On `PositionClosed`: Upsert with `closed_at = Timestamp(block_timestamp)`, `update_columns: ["closed_at"]`

Migration (`migrations/tables/sol_cpmm_positions.sql`):
```sql
CREATE TABLE IF NOT EXISTS sol_cpmm_positions (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    position BYTEA NOT NULL,
    pool BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    position_id BIGINT NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, position, source, source_version)
);
```

---

### Group 2: `src/transformations/event/solana/initializer/`

```
mod.rs
launch_create.rs
curve_swap.rs
launch_migrate.rs
```

#### `mod.rs`
```rust
pub mod curve_swap;
pub mod launch_create;
pub mod launch_migrate;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    launch_create::register_handlers(registry);
    curve_swap::register_handlers(registry);
    launch_migrate::register_handlers(registry);
}
```

---

#### `launch_create.rs` — `LaunchCreateHandler`

Writes to the shared **`pools`** table. The launch address maps to `pools.address`. Solana-specific fields (`namespace`, `launch_id`, `base_total_supply`, `curve_kind`, `sentinel_program`) are stored in `pool_key JSONB`.

- **Trigger**: `("DopplerInitializer", "LaunchInitialized")`
- **migration_paths**: `["migrations/tables/pools.sql", "migrations/tables/pools_solana_nullable.sql"]`
- **reorg_tables**: `["pools"]`
- **conflict_columns**: `["chain_id", "address"]`
- **update_columns**: `[]`

Field mapping:

| `pools` column | value |
|---|---|
| `chain_id` | `ctx.chain_id as i64` |
| `block_height` | `event.block_number` |
| `created_at` | `event.block_timestamp` |
| `address` | `DbValue::Pubkey(launch)` |
| `base_token` | `DbValue::Pubkey(base_mint)` |
| `quote_token` | `DbValue::Pubkey(quote_mint)` |
| `is_token_0` | `DbValue::Bool(true)` |
| `type` | `DbValue::VarChar(format!("curve_{}", curve_kind))` |
| `integrator` | `DbValue::Null` |
| `initializer` | `DbValue::Null` |
| `fee` | `DbValue::Null` |
| `min_threshold` | `DbValue::Null` |
| `max_threshold` | `DbValue::Null` |
| `migrator` | `DbValue::Pubkey(migrator_program)` |
| `migrated_at` | `DbValue::Null` |
| `migration_pool` | `DbValue::Null` |
| `migrated_from` | `DbValue::Null` |
| `migration_type` | `DbValue::Null` |
| `lock_duration` | `DbValue::Null` |
| `beneficiaries` | `DbValue::Null` |
| `pool_key` | `DbValue::JsonB(json!({"namespace": bs58(namespace), "launch_id": hex(launch_id), "base_total_supply": base_total_supply, "curve_kind": curve_kind, "sentinel_program": bs58(sentinel_program)}))` |
| `starting_time` | `DbValue::Null` |
| `ending_time` | `DbValue::Null` |

Use `bs58::encode(pubkey).into_string()` for pubkeys in JSON, `hex::encode(bytes)` for `launch_id`.

---

#### `curve_swap.rs` — `CurveSwapHandler`

Writes to the shared **`swaps`** table. The launch pubkey maps to `swaps.pool`.

- **Trigger**: `("DopplerInitializer", "CurveSwap")`
- **migration_paths**: `["migrations/tables/swaps.sql", "migrations/tables/swaps_solana_nullable.sql"]`
- **reorg_tables**: `["swaps"]`
- **conflict_columns**: `["chain_id", "tx_id", "log_position"]`

Field mapping:

| `swaps` column | value |
|---|---|
| `chain_id` | `ctx.chain_id as i64` |
| `tx_id` | `DbValue::Bytes(tx_sig.to_vec())` |
| `block_height` | `event.block_number` |
| `timestamp` | `event.block_timestamp` |
| `pool` | `DbValue::Pubkey(launch)` |
| `asset` | `DbValue::Null` |
| `amountIn` | `DbValue::Uint64(amount_in)` |
| `amountOut` | `DbValue::Uint64(amount_out)` |
| `is_buy` | `DbValue::Bool(direction == 0)` |
| `current_tick` | `DbValue::Null` |
| `graduation_tick` | `DbValue::Null` |
| `fee0_delta` | `DbValue::Uint64(fee_paid)` |
| `fee1_delta` | `DbValue::Null` |
| `tokens_sold_delta` | `DbValue::Null` |
| `graduation_balance_delta` | `DbValue::Null` |
| `max_threshold_delta` | `DbValue::Null` |
| `market_cap_usd_delta` | `DbValue::Null` |
| `liquidity_usd_delta` | `DbValue::Null` |
| `value_usd` | `DbValue::Null` |
| `log_position` | `event.position.packed_ordinal_i64()` |

---

#### `launch_migrate.rs` — `LaunchMigrateHandler`

Updates the `pools` row for the launch to set `migrated_at`.

- **Trigger**: `("DopplerInitializer", "LaunchMigrated")`
- **migration_paths**: `["migrations/tables/pools.sql", "migrations/tables/pools_solana_nullable.sql"]`
- **reorg_tables**: `[]` (update; reorg handled by slot-level rollback of the source row)
- **contiguous_handler_dependencies**: `["LaunchCreateHandler"]`

Operation:
```rust
DbOperation::Update {
    table: "pools".to_string(),
    set_columns: vec![
        ("migrated_at".into(), DbValue::Timestamp(event.block_timestamp as i64)),
    ],
    where_clause: WhereClause::And(vec![
        WhereClause::Eq("chain_id".into(), DbValue::Int64(ctx.chain_id as i64)),
        WhereClause::Eq("address".into(), DbValue::Pubkey(launch)),
    ]),
}
```

---

### Group 3: `src/transformations/event/solana/cpmm_migrator/`

```
mod.rs
handlers.rs
```

#### `mod.rs`
```rust
pub mod handlers;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    handlers::register_handlers(registry);
}
```

---

#### `handlers.rs` — `CpmmMigratorHandler`

Handles both migrator events. `LaunchRegistered` goes to a dedicated tracking table. `LaunchMigrated` updates the `pools` row for the launch with the final CPMM pool address.

- **Triggers**: `("DopplerCpmmMigrator", "LaunchRegistered")` AND `("DopplerCpmmMigrator", "LaunchMigrated")`
- **migration_paths**: `["migrations/tables/pools.sql", "migrations/tables/pools_solana_nullable.sql", "migrations/tables/sol_cpmm_migrator.sql"]`
- **reorg_tables**: `["sol_cpmm_migrator"]`

For `LaunchRegistered` → Upsert into `sol_cpmm_migrator`:
- **conflict_columns**: `["chain_id", "launch"]`
- **update_columns**: `[]`

For `LaunchMigrated` → two ops:
1. Upsert into `sol_cpmm_migrator` with `update_columns: ["pool", "quote_for_liquidity", "base_for_liquidity", "migrated"]`
2. `DbOperation::Update` on `pools` setting `migration_pool = pool, migrated_at = timestamp` WHERE `chain_id = ... AND address = launch`

Migration (`migrations/tables/sol_cpmm_migrator.sql`):
```sql
CREATE TABLE IF NOT EXISTS sol_cpmm_migrator (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    launch BYTEA NOT NULL,
    state BYTEA,
    cpmm_config BYTEA,
    min_raise_quote BIGINT,
    pool BYTEA,
    quote_for_liquidity BIGINT,
    base_for_liquidity BIGINT,
    migrated BOOLEAN NOT NULL DEFAULT false,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, launch, source, source_version)
);
```

---

### Group 4: `src/transformations/event/solana/prediction/`

```
mod.rs
market.rs
entries.rs
claims.rs
```

#### `mod.rs`
```rust
pub mod claims;
pub mod entries;
pub mod market;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    market::register_handlers(registry);
    entries::register_handlers(registry);
    claims::register_handlers(registry);
}
```

---

#### `market.rs` — `PredictionMarketHandler`

- **Triggers**: `("DopplerPredictionMigrator", "MarketCreated")` AND `("DopplerPredictionMigrator", "MarketResolved")`
- **migration_paths**: `["migrations/tables/sol_prediction_markets.sql"]`
- **reorg_tables**: `["sol_prediction_markets"]`
- **conflict_columns**: `["chain_id", "market"]`

On `MarketCreated`: Upsert, `update_columns: []`
On `MarketResolved`: Upsert, `update_columns: ["winner_mint", "claimable_supply", "total_pot", "resolved_at"]`

Migration (`migrations/tables/sol_prediction_markets.sql`):
```sql
CREATE TABLE IF NOT EXISTS sol_prediction_markets (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    market BYTEA NOT NULL,
    oracle BYTEA NOT NULL,
    quote_mint BYTEA NOT NULL,
    winner_mint BYTEA,
    claimable_supply BIGINT,
    total_pot BIGINT,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, market, source, source_version)
);
```

---

#### `entries.rs` — `PredictionEntryHandler`

- **Triggers**: `("DopplerPredictionMigrator", "EntryRegistered")` AND `("DopplerPredictionMigrator", "EntryMigrated")`
- **migration_paths**: `["migrations/tables/sol_prediction_entries.sql"]`
- **reorg_tables**: `["sol_prediction_entries"]`
- **conflict_columns**: `["chain_id", "market", "entry_id"]`
- `entry_id` is `[u8;32]` → `DbValue::Bytes32(v)`

On `EntryRegistered`: Upsert, `update_columns: []`
On `EntryMigrated`: Upsert, `update_columns: ["contribution", "is_winner"]`

Migration (`migrations/tables/sol_prediction_entries.sql`):
```sql
CREATE TABLE IF NOT EXISTS sol_prediction_entries (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    market BYTEA NOT NULL,
    oracle BYTEA NOT NULL,
    entry_id BYTEA NOT NULL,
    base_mint BYTEA NOT NULL,
    contribution BIGINT,
    is_winner BOOLEAN,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, market, entry_id, source, source_version)
);
```

---

#### `claims.rs` — `PredictionClaimHandler`

- **Trigger**: `("DopplerPredictionMigrator", "RewardsClaimed")`
- **migration_paths**: `["migrations/tables/sol_prediction_claims.sql"]`
- **reorg_tables**: `["sol_prediction_claims"]`
- **conflict_columns**: `["chain_id", "tx_id", "log_position"]`

Migration (`migrations/tables/sol_prediction_claims.sql`):
```sql
CREATE TABLE IF NOT EXISTS sol_prediction_claims (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    tx_id BYTEA NOT NULL,
    log_position BIGINT NOT NULL,
    market BYTEA NOT NULL,
    claimer BYTEA NOT NULL,
    burned_amount BIGINT NOT NULL,
    reward_amount BIGINT NOT NULL,
    total_burned BIGINT NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, tx_id, log_position, source, source_version)
);
```

---

## Handler Dependencies

`LaunchMigrateHandler` must declare:
```rust
impl EventHandler for LaunchMigrateHandler {
    fn triggers(&self) -> Vec<EventTrigger> { ... }
    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["LaunchCreateHandler"]
    }
}
```

All other handlers have no dependencies.

---

## Wiring: where to register handlers

**File**: `src/transformations/registry.rs`
**Function**: `build_registry_for_solana_chain()` (~line 1059)

Replace the placeholder comment with:
```rust
crate::transformations::event::solana::cpmm::register_handlers(&mut registry);
crate::transformations::event::solana::initializer::register_handlers(&mut registry);
crate::transformations::event::solana::cpmm_migrator::register_handlers(&mut registry);
crate::transformations::event::solana::prediction::register_handlers(&mut registry);
```

**File**: `src/transformations/event/mod.rs` — add:
```rust
pub mod solana;
```

**New file**: `src/transformations/event/solana/mod.rs`:
```rust
pub mod cpmm;
pub mod cpmm_migrator;
pub mod initializer;
pub mod prediction;
```

---

## Complete file checklist

**New/modified migration files:**
```
migrations/tables/pools_solana_nullable.sql    ← already exists
migrations/tables/swaps_solana_nullable.sql    ← already exists
migrations/tables/evm_cl_positions.sql         ← already exists (renamed from positions)
migrations/tables/position.sql                 ← already exists (now a rename guard)
migrations/tables/sol_cpmm_positions.sql       ← create
migrations/tables/sol_cpmm_migrator.sql        ← create
migrations/tables/sol_prediction_markets.sql   ← create
migrations/tables/sol_prediction_entries.sql   ← create
migrations/tables/sol_prediction_claims.sql    ← create
```

**New Rust files** (all to create):
```
src/transformations/event/solana/mod.rs
src/transformations/event/solana/cpmm/mod.rs
src/transformations/event/solana/cpmm/pool_create.rs
src/transformations/event/solana/cpmm/swap.rs
src/transformations/event/solana/cpmm/metrics.rs
src/transformations/event/solana/cpmm/positions.rs
src/transformations/event/solana/initializer/mod.rs
src/transformations/event/solana/initializer/launch_create.rs
src/transformations/event/solana/initializer/curve_swap.rs
src/transformations/event/solana/initializer/launch_migrate.rs
src/transformations/event/solana/cpmm_migrator/mod.rs
src/transformations/event/solana/cpmm_migrator/handlers.rs
src/transformations/event/solana/prediction/mod.rs
src/transformations/event/solana/prediction/market.rs
src/transformations/event/solana/prediction/entries.rs
src/transformations/event/solana/prediction/claims.rs
```

**Edits to existing files**:
1. `src/transformations/event/mod.rs` — add `pub mod solana;`
2. `src/transformations/registry.rs` — in `build_registry_for_solana_chain()`, replace placeholder comment with four `register_handlers` calls

---

## Sample JSON config (reference only — not part of the implementation)

```json
{
  "DopplerCPMM": {
    "program_id": "9PSxVPoPfnbZ8Q1uQhgS6ZxvBjFboZtebNsu34umxkgQ",
    "events": [
      { "name": "PoolInitialized" }, { "name": "Swap" },
      { "name": "AddLiquidity" }, { "name": "RemoveLiquidity" },
      { "name": "PositionCreated" }, { "name": "PositionClosed" }
    ]
  },
  "DopplerInitializer": {
    "program_id": "4h3Dqyo5qmteJoMxXt3tdtfXELDB6pdRTPU9mWruiKp1",
    "events": [
      { "name": "LaunchInitialized" }, { "name": "CurveSwap" }, { "name": "LaunchMigrated" }
    ]
  },
  "DopplerCpmmMigrator": {
    "program_id": "7WMUTNC41eMCo6eGH5Sy2xbgE3AycvLbFPo95AU9CSUd",
    "events": [{ "name": "LaunchRegistered" }, { "name": "LaunchMigrated" }]
  },
  "DopplerPredictionMigrator": {
    "program_id": "HYHdyy7QZg8Ucky9Z97xNtSCvrZxVNkeoney8xEPXjiZ",
    "events": [
      { "name": "MarketCreated" }, { "name": "EntryRegistered" },
      { "name": "EntryMigrated" }, { "name": "MarketResolved" }, { "name": "RewardsClaimed" }
    ]
  }
}
```
