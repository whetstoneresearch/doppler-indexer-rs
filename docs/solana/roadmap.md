# Solana Multi-Chain Support: Implementation Roadmap

## Context

Doppler Indexer is an EVM blockchain indexer. We're extending it to also index Solana chains. The codebase is deeply EVM-coupled at the data collection, RPC, and decoding layers, but the transformation engine (traits, DAG scheduler, executor) is already chain-agnostic. The goal is to add Solana support with minimal disruption to existing EVM code paths.

## Architecture: Parallel Implementation

Rather than abstracting everything behind traits, we use a **parallel implementation** approach:
- Solana-specific modules live in `src/solana/`, feature-gated behind `#[cfg(feature = "solana")]`
- Existing EVM code stays in place (not moved)
- The two pipelines converge at the transformation layer via shared `DecodedEvent`/`DecodedCall` types
- Target end-state: pipeline dispatch happens via `match chain_type` in `main.rs`

## Branch Status

Phases 1-7 are complete. The Solana historical pipeline is wired end-to-end: `chain_type: solana` in config activates signature-driven backfill, raw data collection, event/instruction decoding (via IDL or built-in decoders), and dispatches to the transformation engine when handlers exist. Live mode (Phase 8) is stubbed.

Implemented (Phases 1-2, from earlier work):
- `src/types/chain.rs` with `ChainAddress`, `TxId`, `LogPosition`, and `ChainType`
- `ChainConfig.chain_type` with a backward-compatible default of `evm`
- `TransformationHandler::chain_type()` defaulting to `ChainType::Evm`
- `DecodedAccountState`, `AccountStateTrigger`, and `AccountStateHandler`
- `TransformationContext.account_states` plus `extract_chain_address`, `extract_pubkey`, and account-state lookup helpers
- `DecodedValue::ChainAddress(ChainAddress)` and canonical Solana pubkey rendering via `ChainAddress::Display`
- `DbValue::Pubkey`, `LiveDbValue::Pubkey`
- Registry chain-type filtering and account-state handler indices
- Runtime / engine / executor / finalizer / live-state account-state plumbing
- `LogPosition::sort_key()` and `LogPosition::packed_ordinal_i64()` for BIGINT-backed storage
- `BIGINT` widening for persisted `log_index` columns
- `serde-big-array` support for serializing `[u8; 64]` in `TxId::Solana`
- UTF-8-safe RPC error truncation

Implemented (Phases 3-6):
- Solana config types: `SolanaProgramConfig`, `SolanaEventConfig`, `SolanaAccountReadConfig`, `SolanaDiscoveryConfig`, `SolanaCommitment`
- `SolanaRpcClient` with rate limiting, retry, batch operations
- `SolanaWsClient` with auto-reconnection and gap detection
- Raw data collection: slot/block fetching, event/instruction extraction, signature-driven backfill
- Address discovery: `DiscoveryManager` with bootstrap, event-driven discovery, persistence
- `ProgramDecoder` trait with `AnchorDecoder` (IDL-driven) and `SplTokenDecoder` (built-in)
- IDL parser supporting pre-0.30 and 0.30+ Anchor formats
- Dynamic Borsh deserializer for IDL type trees
- Event, instruction, and account decoder routers producing `DecodedEvent`/`DecodedAccountState`

Implemented (Phase 7):
- `RangeCompleteKind::Instructions` for independent instruction completion signaling
- `SolanaChainFeatures` detection from program config
- `SolanaChainRuntime` with RPC client, decoder construction, program ID mapping
- Decoder task functions (`decode_solana_events`, `decode_solana_instructions`) as async channel loops
- `process_solana_chain()` historical pipeline entry point
- `decode_only_solana_chain()` for re-decoding existing parquet
- `main.rs` chain-type dispatch routing Solana chains to the new pipeline
- `build_registry_for_solana_chain()` with source and chain-type filtering
- Decoded Solana storage paths (`decoded_solana_events_dir`, `decoded_solana_instructions_dir`)
- Solana RPC method variants in metrics

Implemented (Phase 8):
- `SolanaLiveCollector` for real-time slot processing via WebSocket `slotSubscribe`
- `SolanaReorgDetector` using parent-slot chain verification (commitment-aware depth)
- `SolanaLiveAccountReader` for event-triggered account state fetches
- `SolanaLiveStorage` with bincode persistence and JSON status tracking
- `SolanaCompactionService` for bincode-to-parquet merging
- `SolanaLiveCatchupService` for restart recovery of incomplete slots
- Pipeline wiring in `process_solana_chain()` and `process_solana_chain_live_only()`
- `main.rs` dispatch for Solana `--live-only` mode

Also implemented:
- `TransformationEngine` integration: RPC client and contracts made `Option` throughout the engine, executor, retry processor, and context. Solana pipelines pass `None` for EVM RPC. EVM-specific context methods (`eth_call`, `find_log_emitter`) return errors on non-EVM chains. Both historical and live Solana pipelines spawn the engine when handlers are registered.

Practical implication: `chain_type: solana` activates the full historical pipeline (backfill, decode, write parquet) and live mode (slot subscription, real-time extraction/decoding, bincode storage, compaction). Transformation handlers can be registered by implementing `TransformationHandler` with `chain_type() -> ChainType::Solana`. TransformationEngine integration is pending — the engine currently requires an EVM RPC client.

---

## Phase 1: Core Type Generalizations (Additive, No Breaking Changes)

### 1a. New chain-agnostic types in `src/types/chain.rs` (new file)

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChainAddress {
    Evm([u8; 20]),
    Solana([u8; 32]),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]  // Clone, NOT Copy
pub enum TxId {
    Evm([u8; 32]),
    Solana([u8; 64]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogPosition {
    Evm { log_index: u32 },
    Solana { instruction_index: u16, inner_instruction_index: Option<u16> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum ChainType { Evm, Solana }
```

`ChainAddress` and `LogPosition` are `Copy`, stack-allocated. `TxId` is `Clone` only — at 65 bytes for the Solana variant, implicit copies in hot loops are invisible overhead (see design.md §3.2).

### 1b. Extend `DecodedValue` for chain-aware addresses (`src/types/decoded.rs`)

`DecodedValue::ChainAddress(ChainAddress)` added alongside existing `Address([u8; 20])` to preserve bincode ordinals for legacy variants. Handlers gain:
- `as_chain_address() -> Option<ChainAddress>`
- `as_pubkey() -> Option<[u8; 32]>`
- `extract_chain_address(...)`
- `extract_pubkey(...)`

### 1c. Add `Pubkey([u8; 32])` to `DbValue` (`src/db/types.rs`)

Wire through `db_value_to_param` in `src/db/pool.rs` as 32-byte BYTEA. Same for `LiveDbValue` in `src/live/types.rs`.

### 1d. Add `extract_pubkey` to `FieldExtractor` (`src/transformations/context.rs`)

```rust
impl_field_extractor!(extract_pubkey, as_pubkey, [u8; 32], "a pubkey");
```

Also add `extract_chain_address` for generic address extraction.

### 1e. Add `chain_type()` to `TransformationHandler` trait (`src/transformations/traits.rs`)

Defaulted method so existing EVM handlers need no changes:
```rust
fn chain_type(&self) -> ChainType;
```
Registry validation uses this to reject handlers registered on the wrong chain type.

### 1f. Add `DecodedAccountState` type (`src/transformations/context.rs`)

New type for Solana account reads (distinct from `DecodedCall` — see design.md §5.2b):
```rust
pub struct DecodedAccountState {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub account_address: ChainAddress,
    pub owner_program: ChainAddress,
    pub source_name: String,
    pub account_type: String,
    pub fields: HashMap<String, DecodedValue>,
}
```

### 1g. Add `AccountStateTrigger` and `AccountStateHandler` trait (`src/transformations/traits.rs`)

New handler trait for Solana account state reads, following `EventHandler`/`EthCallHandler` pattern:
```rust
pub struct AccountStateTrigger { pub source: String, pub account_type: String }
pub trait AccountStateHandler: TransformationHandler { fn triggers(&self) -> Vec<AccountStateTrigger>; }
```

### 1h. Add `ChainServices` enum (`src/transformations/context.rs`)

Replace bare `rpc` + `contracts` fields on `TransformationContext` with chain-dispatched enum:
```rust
pub enum ChainServices {
    Evm { rpc: Arc<UnifiedRpcClient>, contracts: Arc<Contracts> },
    Solana { rpc: Arc<SolanaRpcClient>, programs: Arc<SolanaPrograms> },
}
```

Status: a placeholder `ChainServices::Evm` enum exists, but the context still uses `rpc` and `contracts` fields in production code.

**Files modified**: `src/types/decoded.rs`, `src/types/chain.rs` (new), `src/types/mod.rs`, `src/db/types.rs`, `src/db/pool.rs`, `src/live/types.rs`, `src/transformations/context.rs`, `src/transformations/traits.rs`

---

## Phase 2: Migrate DecodedEvent/DecodedCall to Chain-Agnostic Types

### 2a. Update `DecodedEvent` (`src/transformations/context.rs:142-157`)

```rust
pub struct DecodedEvent {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_id: TxId,           // was transaction_hash: [u8; 32]
    pub position: LogPosition,          // was log_index: u32
    pub contract_address: ChainAddress, // was [u8; 20]
    pub source_name: String,
    pub event_name: String,
    pub event_signature: String,        // Solidity sig on EVM, Anchor event name on Solana
    pub params: HashMap<String, DecodedValue>,
}
```

**`event_signature` semantics:** On EVM, this is the full Solidity signature (e.g., `"Swap(address,...)""`). On Solana, this is the Anchor event name (e.g., `"Traded"`). The discriminator is internal to the decoder — handlers subscribe by event name, the registry dispatches on `(source, event_signature)`, no registry changes needed. See design.md §5.1.

Add backward-compatible convenience methods for EVM handlers:
```rust
impl DecodedEvent {
    pub fn evm_address(&self) -> [u8; 20] { /* match Evm variant */ }
    pub fn log_index(&self) -> u32 { /* match Evm variant */ }
    pub fn evm_tx_hash(&self) -> [u8; 32] { /* match Evm variant */ }
}
```

### 2b. Update `DecodedCall` (EVM only)

`DecodedCall` remains EVM-specific (eth_call results). Solana account reads use `DecodedAccountState` (added in Phase 1f). Update addresses:
- `contract_address: [u8; 20]` → `ChainAddress`
- `trigger_log_index: Option<u32>` → `trigger_position: Option<LogPosition>`

### 2c. Update `TransformationContext` (`src/transformations/context.rs:299-327`)

- Replace `rpc: Arc<UnifiedRpcClient>` + `contracts: Arc<Contracts>` with `chain_services: ChainServices` (added in Phase 1h)
- Add `account_states: Arc<Vec<DecodedAccountState>>` (feature-gated)
- `tx_addresses: HashMap<TxId, TransactionAddresses>` (was `HashMap<[u8; 32], ...>`)
- `TransactionAddresses` fields become `ChainAddress`
- Keep backward-compatible `events_for_address([u8; 20])` that wraps into `ChainAddress::Evm`
- Add `events_for_chain_address(ChainAddress)` for Solana handlers
- Add `account_states_for_type(source, account_type)` for Solana handlers
- Update `HistoricalEventQuery.contract_address` to `Option<ChainAddress>`

### 2d. Update `HistoricalDataReader` (`src/transformations/historical.rs`)

- `get_address_column()` becomes size-aware: 20 bytes -> `ChainAddress::Evm`, 32 bytes -> `ChainAddress::Solana`
- Update `batch_to_events()` and `batch_to_calls()` to construct new field types

### 2e. Mechanical EVM handler updates

All existing handlers in `src/transformations/event/v3/`, `v4/`, etc.:
- `event.contract_address` -> `event.evm_address()`
- `event.log_index` -> `event.log_index()`
- `event.transaction_hash` -> `event.evm_tx_hash()`

### 2f. Update EVM decoders to produce new types

- `src/decoding/logs.rs` — `DecodedLogRecord` uses `ChainAddress::Evm(...)` and `LogPosition::Evm { log_index }`
- `src/decoding/types.rs` — `EthCallResult`, `OnceCallResult`, `EventCallResult` addresses become `ChainAddress::Evm`
- `src/live/types.rs` — `LiveDecodedEvent`, `LiveDecodedCall` updated

**Files modified**: `src/transformations/context.rs`, `src/transformations/historical.rs`, `src/transformations/executor.rs`, `src/transformations/event/**/*.rs`, `src/transformations/util/db/*.rs`, `src/decoding/logs.rs`, `src/decoding/types.rs`, `src/decoding/eth_calls/*.rs`, `src/live/types.rs`

---

## Phase 3: Config Layer

### 3a. Add `chain_type` to `ChainConfigRaw` (`src/types/config/chain.rs`)

```rust
pub struct ChainConfigRaw {
    pub chain_type: Option<ChainType>,  // defaults to Evm if absent (backward compat)
    // ... existing fields unchanged ...
    #[cfg(feature = "solana")]
    pub programs: Option<InlineOrPath<SolanaPrograms>>,
    #[cfg(feature = "solana")]
    pub commitment: Option<String>,  // "processed" | "confirmed" | "finalized" (all supported, reorg detection scales by level)
}
```

### 3b. Solana program config types (`src/types/config/solana.rs`, new file)

```rust
pub type SolanaPrograms = HashMap<String, SolanaProgramConfig>;

pub struct SolanaProgramConfig {
    pub program_id: String,           // base58 Pubkey
    pub idl_path: Option<String>,     // Anchor IDL JSON path
    pub events: Option<Vec<SolanaEventConfig>>,
    pub accounts: Option<Vec<SolanaAccountReadConfig>>,  // live-mode only
    pub discovery: Option<Vec<SolanaDiscoveryConfig>>,   // address discovery from events
    pub start_slot: Option<u64>,
}

pub struct SolanaEventConfig {
    pub name: String,
    pub discriminator: Option<String>, // 8-byte hex, computed from name if absent
}

pub struct SolanaAccountReadConfig {
    pub name: String,
    pub account_type: String,
    pub frequency: Frequency,         // reuse existing Frequency enum
}

pub struct SolanaDiscoveryConfig {
    pub event_name: String,           // e.g., "PoolInitialized"
    pub address_field: String,        // field in event containing new account address
    pub account_type: Option<String>, // IDL account type to associate with discovered address
}
```

**Files modified**: `src/types/config/chain.rs`, `src/types/config/solana.rs` (new), `src/types/config/mod.rs`

---

## Phase 4: Solana RPC Client

### 4a. `src/solana/rpc.rs` (new file, feature-gated)

Wraps `solana_client::nonblocking::rpc_client::RpcClient` with:
- Rate limiting via existing `SlidingWindowRateLimiter` (reused from `src/rpc/alchemy.rs`)
- Retry logic
- Methods: `get_slot`, `get_block`, `get_blocks_batch`, `get_transaction`, `get_signatures_for_address`, `get_account_info`, `get_multiple_accounts`, `get_program_accounts`
- Error type: `SolanaRpcError`

### 4b. Solana WebSocket client (`src/solana/ws.rs`)

- `slotSubscribe` for live mode
- `SolanaWsEvent::NewSlot { slot, parent }`, `Disconnected`, `Reconnected`
- Gap detection via HTTP `get_slot()` on reconnect

**New files**: `src/solana/mod.rs`, `src/solana/rpc.rs`, `src/solana/ws.rs`

---

## Phase 5: Solana Raw Data Collection

### 5a. Slot collector (`src/solana/raw_data/slots.rs`)

- Fetches blocks by slot via `getBlock(slot)`
- Handles skipped slots gracefully (Solana-specific: not all slots have blocks)
- Writes `SolanaSlotRecord` to parquet: `slot`, `block_time`, `parent_slot`, `blockhash` (32B), `transaction_count`, `transaction_signatures` (List of 64B)
- Sends `SlotData { slot, block }` downstream to extractor via `slot_tx` channel
- Skipped slots recorded in `skipped_slots.json` index (see design.md §8.5)

### 5b. Event + instruction extractor (`src/solana/raw_data/events.rs`, `instructions.rs`)

Single-pass extraction over block transactions. Failed transactions (`meta.err.is_some()`) are skipped entirely (see design.md §8.6).

**Events:**
- Parses `logMessages` from transaction metadata for `"Program data: <base64>"` entries
- Stack-based program ID tracking with instruction position tracking (matches "Program X invoke [N]" / "Program X success" patterns)
- `SolanaEventRecord` now includes `instruction_index` and `inner_instruction_index` alongside flat `log_index` (see design.md §8.2)
- Writes to parquet, sends `SolanaEventsReady` to `event_decoder_tx`

**Instructions** (same pass over transactions):
- Extracts from `transaction.message.instructions` (top-level) and `meta.innerInstructions` (CPI)
- Resolves account indices to pubkeys via `transaction.message.accountKeys`
- Filters by configured program IDs
- Writes `SolanaInstructionRecord` to parquet (see design.md §8.2b for full spec)
- Sends `SolanaInstructionsReady` to `instr_decoder_tx`

**CPI coverage**: Both `getSignaturesForAddress` and `getBlock` `logMessages` include CPI invocations because invoked programs must be in the transaction's `accountKeys`. See design.md §8.7.

### 5c. Address discovery (`src/solana/discovery.rs`)

Follows EVM factory collection timing pattern (see design.md §10.2b):
- **Bootstrap**: `getProgramAccounts` scan at startup (before backfill), persisted to `known_accounts.json`
- **During backfill**: event-driven discovery accumulates addresses (no feedback to collection)
- **During live**: incremental discovery triggers account reads via `discovery_addresses_tx`
- Catchup barrier (`discovery_catchup_done_tx`) ensures full address set before live account reads start

### 5d. Historical backfill strategy

**Signature-driven** (not slot-driven) for efficiency:
1. `getSignaturesForAddress(program_id, { limit: 1000, before: cursor })` — paginate backward
2. Group signatures by slot range (aligned to `parquet_block_range`)
3. Batch-fetch blocks for each range via `getBlock(slot)`
4. Extract events and instructions in single pass

This avoids iterating every slot (~2/sec = 170K/day) when most won't contain target program transactions. CPI transactions are captured because `getSignaturesForAddress` matches on `accountKeys` which includes CPI-invoked programs (see design.md §8.7).

### 5e. Skipped slots index

Range-aligned parquet files may contain fewer rows than the slot span due to skipped slots. A `skipped_slots.json` sidecar file per data directory tracks which slots were skipped, enabling the resume logic to distinguish complete ranges from interrupted ones. See design.md §8.5 for format and invariants.

### 5f. Channel topology

Solana uses a parallel channel topology: `slot_tx` → extractor → separate `event_decoder_tx` and `instr_decoder_tx` → both feed `transform_events_tx` → TransformationEngine. Discovery runs as a sidecar via `discovery_tx`. See design.md §10.5 for full topology, message types, and barrier semantics.

**New files**: `src/solana/raw_data/mod.rs`, `slots.rs`, `events.rs`, `instructions.rs`, `catchup.rs`, `src/solana/discovery.rs`

Note: Account reader (`src/solana/live/accounts.rs`) is live-mode-only — see Phase 8. Historical account reads are not supported (see design.md §8.3).

---

## Phase 6: Solana Decoding

### 6a. `ProgramDecoder` trait (`src/solana/decoding/traits.rs`)

Trait abstraction for decoding any Solana program — not just Anchor. Supports events, instructions, and account state. See design.md §14.2 for full trait definition and implementations table.

Implementations:
- `AnchorDecoder` — IDL-driven, Borsh encoding (most DeFi programs)
- `SplTokenDecoder` — hand-written, built-in (essential for any token indexing)
- Future: `ShankDecoder`, custom compile-time decoders

### 6a-ii. IDL parser (`src/solana/decoding/idl.rs`)

- Loads Anchor IDL JSON files with version detection (pre-0.30 vs 0.30+ format)
- Normalizes to internal `IdlType` / `IdlTypeDef` representation (see design.md §9.1b)
- Computes event discriminators: `sha256("event:<EventName>")[..8]`
- Computes account discriminators: `sha256("account:<AccountName>")[..8]`
- Computes instruction discriminators: `sha256("global:<instruction_name>")[..8]`
- Builds matchers: `HashMap<[u8; 8], (name, Vec<IdlField>)>` for events, instructions, accounts

### 6a-iii. Dynamic Borsh deserializer (`src/solana/decoding/borsh_dynamic.rs`)

The most complex new module. Recursive interpreter that walks IDL type tree at runtime:
- Handles all Borsh primitive types (bool, u8-u128, i8-i128, f32/f64, pubkey)
- Handles variable-length types (String, bytes, Vec, Option)
- Handles nested structs and enums via `Defined(name)` → type lookup → recursive descent
- Maps each IDL type to appropriate `DecodedValue` variant
- Full spec and code in design.md §9.1b
- Must be tested against real IDLs (Orca Whirlpool, Raydium CLMM) with known event/account data

### 6b. Event decoder (`src/solana/decoding/events.rs`)

- Routes raw events to the appropriate `ProgramDecoder` by program_id
- Decoder matches by discriminator (8 bytes), Borsh-deserializes via IDL schema
- Produces `DecodedEvent` with:
  - `ChainAddress::Solana(program_id)`, `LogPosition::Solana { ... }`, `TxId::Solana(signature)`
  - `event_signature` = event name (NOT discriminator hex — see design.md §5.1)
- Writes decoded parquet files under `data/{chain}/historical/decoded/events/{program}/{event_name}/`

### 6b-ii. Instruction decoder (`src/solana/decoding/instructions.rs`)

- Routes raw instructions to the appropriate `ProgramDecoder` by program_id
- Decoder matches by discriminator (first 8 bytes of instruction data), deserializes args
- Merges instruction args + named accounts into `DecodedEvent`:
  - `event_name` = instruction name, `event_signature` = instruction name
  - IDL-named accounts flattened into `params` as `DecodedValue::Pubkey`
- Writes decoded parquet under `data/{chain}/historical/decoded/instructions/{program}/{instruction_name}/`
- See design.md §14.3 for full specification

### 6c. Account state decoder (`src/solana/decoding/accounts.rs`) — live mode only

- Routes raw account data to the appropriate `ProgramDecoder` by owner program_id
- Decoder matches by discriminator (first 8 bytes), Borsh-deserializes remaining data
- Produces `DecodedAccountState` (NOT `DecodedCall` — see design.md §5.2b):
  - `account_address`, `owner_program`, `account_type`, `fields: HashMap`

### 6d. Extend `DecoderMessage` (`src/decoding/types.rs`)

Add Solana-specific variants:
```rust
#[cfg(feature = "solana")]
SolanaEventsReady { range_start, range_end, events: Vec<SolanaEventRecord>, live_mode: bool },
#[cfg(feature = "solana")]
SolanaInstructionsReady { range_start, range_end, instructions: Vec<SolanaInstructionRecord>, live_mode: bool },
#[cfg(feature = "solana")]
SolanaAccountsReady { slot: u64, source_name: String, results: Vec<AccountReadResult> },  // live-only
```

**New files**: `src/solana/decoding/mod.rs`, `traits.rs`, `idl.rs`, `borsh_dynamic.rs`, `events.rs`, `instructions.rs`, `accounts.rs`, `spl_token.rs`

---

## Phase 7: Pipeline Orchestration

### 7a. Solana pipeline entry points (`src/solana/pipeline.rs`)

Three functions mirroring EVM:
- `process_chain()` — historical + live
- `process_chain_live_only()` — live only
- `decode_only_chain()` — decode existing parquet

### 7b. Dispatch in `main.rs`

Not yet implemented.

```rust
match chain.chain_type() {
    ChainType::Evm => evm_pipeline::process_chain(...).await,
    #[cfg(feature = "solana")]
    ChainType::Solana => solana::pipeline::process_chain(...).await,
}
```

### 7c. Storage paths (`src/storage/paths.rs`)

Add Solana path builders: `raw_slots_dir`, `raw_solana_events_dir`, `raw_solana_instructions_dir`, `decoded_solana_events_dir`, `decoded_solana_instructions_dir`. Live-only paths: `live_raw_accounts_dir`, `live_decoded_accounts_dir`.

### 7d. Metrics (`src/metrics/rpc.rs`)

Add Solana RPC methods to `RpcMethod` enum: `GetSlot`, `GetBlock`, `GetTransaction`, `GetAccountInfo`, `GetProgramAccounts`, `GetSignaturesForAddress`.

---

## Phase 8: Solana Live Mode

### 8a. Live collector (`src/solana/live/collector.rs`)

- Subscribes via `slotSubscribe`
- Fetches full block via HTTP `getBlock(slot)` per new slot
- Handles skipped slots (mark complete immediately)
- Extracts events AND instructions in the same pass (reuses Phase 5b extractors)
- Writes bincode per slot, reuses compaction service

### 8a-ii. Reorg detection (`src/solana/live/reorg.rs`)

- `SolanaReorgDetector` using `parent_slot` chain verification (see design.md §14.4)
- Active for all commitment levels (`processed`, `confirmed`, `finalized`)
- Depth varies: 150 for `processed`, 32 for `confirmed`, 0 for `finalized`
- On detection: delete live storage for orphaned slots, rollback handler DB rows via `reorg_tables()`

### 8b. Live account reader (`src/solana/live/accounts.rs`)

Account reads are live-mode-only (see design.md §8.3). This module:
- Triggered by decoded events (e.g., after `Traded`, read the `Whirlpool` account)
- Uses addresses from discovery module (Phase 5c)
- Calls `get_multiple_accounts(pubkeys)` (batch up to 100)
- Writes raw account data to live bincode storage
- Feeds into account state decoder (Phase 6c) to produce `DecodedAccountState`

### 8c. Live storage (`src/solana/live/storage.rs`)

Parallel `SolanaLiveStorage` with Solana-specific bincode types (see design.md §14.5 for full type definitions):
- `LiveSlot`, `LiveTransaction`, `LiveSolanaEvent`, `LiveInstruction`, `LiveAccountRead`
- `LiveSlotStatus` with stage flags: `events_extracted`, `instructions_extracted`, `events_decoded`, `instructions_decoded`, `accounts_read`, `accounts_decoded`, `transformed`
- `LiveDecodedSolanaEvent`, `LiveDecodedInstruction` for decoded data

```
data/{chain}/live/
├── raw/slots/{slot}.bin
├── raw/transactions/{slot}.bin
├── raw/events/{slot}.bin
├── raw/instructions/{slot}.bin
├── raw/accounts/{slot}.bin         # live-only
├── decoded/events/{slot}/{program}/{event}.bin
├── decoded/instructions/{slot}/{program}/{instruction}.bin
├── decoded/accounts/{slot}/{program}/{type}.bin  # live-only
├── snapshots/{slot}.bin            # DB state for reorg rollback
├── status/{slot}.json
```

Shares atomic write helpers and status locking pattern with EVM `LiveStorage`, but is a separate struct with Solana-specific types. Types are best-effort — adjust during implementation.

---

## Dependencies (`Cargo.toml`)

```toml
[features]
default = []
solana = ["dep:solana-client", "dep:solana-sdk", "dep:solana-transaction-status", "dep:borsh", "dep:bs58"]

[dependencies]
solana-client = { version = "2.2", optional = true }
solana-sdk = { version = "2.2", optional = true }
solana-transaction-status = { version = "2.2", optional = true }
borsh = { version = "1.5", optional = true }
bs58 = { version = "0.5", optional = true }
```

Current branch note: `serde-big-array = "0.5"` has landed for `TxId::Solana`. The Solana feature flag and runtime crates above are planned work.

---

## Implementation Order

1. **Phase 1** — additive type changes, new traits, ChainServices (no breakage). Mostly complete: types, values, traits, registry, and runtime plumbing landed. `ChainServices` is placeholder only.
2. **Phase 2** — migrate `DecodedEvent`/`DecodedCall` (mechanical updates across ~40 files)
3. **Phase 3** — config extensions (+ discovery config)
4. **Phase 4** — Solana RPC client
5. **Phase 5** — raw data collectors + address discovery (no account reader — that's Phase 8)
6. **Phase 6** — Solana decoder (IDL parser + dynamic Borsh deserializer + event decoder)
7. **Phase 7** — pipeline wiring
8. **Phase 8** — live mode (collector + account reader + account decoder)

Phases 1-2 are prerequisites. Phases 4-6 can be developed in parallel after Phase 3. Phase 7 integrates everything. Phase 8 can follow independently.

**Recommended early smoke test:** After Phases 4-6, wire up a minimal end-to-end test: fetch one Orca Whirlpool block → extract events → decode one `Traded` event via IDL → verify `DecodedEvent` fields match on-chain data. This surfaces integration issues before the full pipeline is wired.

---

## Module Layout (Final)

```
src/
├── solana/                     # NEW - all feature-gated
│   ├── mod.rs
│   ├── rpc.rs                  # SolanaRpcClient
│   ├── ws.rs                   # WebSocket subscription
│   ├── pipeline.rs             # process_chain entry points
│   ├── discovery.rs            # Address discovery (event-driven + getProgramAccounts bootstrap)
│   ├── raw_data/
│   │   ├── mod.rs
│   │   ├── slots.rs            # slot/block collector
│   │   ├── events.rs           # program log event extractor
│   │   ├── instructions.rs     # instruction data extractor
│   │   └── catchup.rs          # historical backfill (signature-driven)
│   ├── decoding/
│   │   ├── mod.rs
│   │   ├── traits.rs           # ProgramDecoder trait (supports Anchor, Shank, custom)
│   │   ├── idl.rs              # Anchor/Shank IDL parser (version-aware)
│   │   ├── borsh_dynamic.rs    # Dynamic Borsh deserializer (recursive interpreter)
│   │   ├── events.rs           # Event decoder → DecodedEvent
│   │   ├── instructions.rs     # Instruction decoder → DecodedEvent (args + named accounts)
│   │   ├── accounts.rs         # Account state decoder → DecodedAccountState (live-only)
│   │   └── spl_token.rs        # Built-in SPL Token decoder (no IDL needed)
│   └── live/
│       ├── mod.rs
│       ├── collector.rs        # real-time slot processing
│       ├── reorg.rs            # SolanaReorgDetector (parent-slot chain verification)
│       ├── accounts.rs         # live account reader (event-triggered)
│       └── storage.rs          # SolanaLiveStorage with Solana-specific bincode types
├── types/
│   ├── chain.rs                # NEW - ChainAddress, TxId, LogPosition, ChainType
│   ├── decoded.rs              # MODIFIED - add Pubkey variant
│   └── config/
│       └── solana.rs           # NEW - Solana program + discovery config types
├── db/types.rs                 # MODIFIED - add Pubkey variant
├── transformations/
│   ├── context.rs              # MODIFIED - ChainServices, DecodedAccountState, generalized types
│   ├── traits.rs               # MODIFIED - chain_type(), AccountStateHandler
│   ├── historical.rs           # MODIFIED - size-aware parquet readers
│   └── registry.rs             # MODIFIED - account_state_handlers index, chain_type validation
├── decoding/types.rs           # MODIFIED - Solana decoder message variants
├── storage/paths.rs            # MODIFIED - Solana path builders
├── metrics/rpc.rs              # MODIFIED - Solana RPC method labels
└── main.rs                     # MODIFIED - chain type dispatch
```

## Verification

- **Compile check**: `cargo build` (no features) must produce identical EVM-only binary
- **Compile check**: `cargo build --features solana` must compile cleanly
- **EVM regression**: All existing handler tests pass unchanged
- **Unit tests**: IDL parser (both pre-0.30 and 0.30+ formats), dynamic Borsh deserializer (primitives, nested structs, enums, Option, Vec), event discriminator computation, log parser (including adversarial log messages)
- **Integration test**: Index a known Solana program (e.g., Orca Whirlpool) for a small slot range, verify decoded parquet output
- **Cross-chain**: Run EVM + Solana chains simultaneously in same config, verify no handler/trigger collisions
