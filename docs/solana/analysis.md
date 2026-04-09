# Solana Support: Codebase Analysis

## Update From Branch Commits

This analysis predated the current `feat/solana-phase1-traits` implementation work. The branch has now landed some of the proposed groundwork:

- `ChainType` is part of chain config and feeds chain-filtered handler registration.
- `TransformationHandler::chain_type()`, `DecodedAccountState`, `AccountStateHandler`, and registry account-state indices are implemented.
- The transformation runtime now has account-state message/context plumbing.
- `DecodedValue` did not gain a dedicated `Pubkey` variant; the branch uses `DecodedValue::ChainAddress(ChainAddress)` instead.

Still not landed:

- `DbValue::Pubkey` / `LiveDbValue::Pubkey`
- `ChainServices` wiring in `TransformationContext`
- generalized `DecodedEvent` / `DecodedCall`
- `main.rs` dispatch by chain type
- Solana RPC / raw-data / decoding / live modules

Read the tables below as target analysis plus branch notes, not a literal inventory of what is already implemented.

## Solana vs EVM: Fundamental Differences

| Concept | EVM | Solana |
|---------|-----|--------|
| **Address** | 20-byte (hex) | 32-byte Ed25519 pubkey (base58) |
| **Block** | Block with txs, uncles, gas, receipts | Slot with transactions containing instructions |
| **Events** | Logs with topics + data (ABI-encoded) | Program logs (text) + Anchor events (Borsh-encoded) |
| **State reads** | `eth_call` against contract | `getAccountInfo`, `getProgramAccounts` |
| **Encoding** | Solidity ABI (32-byte words) | Borsh / custom binary |
| **Contract discovery** | Factory events emit new addresses | Program-derived addresses (PDAs) |
| **Reorgs** | Parent hash chain, variable depth | Slot confirmation levels (processed/confirmed/finalized) |
| **WebSocket** | `eth_subscribe("newHeads")` | `slotSubscribe`, `programSubscribe`, `logsSubscribe` |
| **Transaction model** | Account-based, single `to` + `data` | Instructions with program_id + accounts[] + data |
| **Hashing** | Keccak-256 | SHA-256 (for most things) |

---

## Subsystem-by-Subsystem Breakdown

### 1. Configuration (`src/types/config/`)

**Deeply EVM-coupled.** Almost every config struct assumes EVM.

| What changes | Details |
|---|---|
| `ChainConfigRaw` | `block_receipts_method`, `contracts`, `factory_collections` are all EVM-only. Solana needs `program_ids`, `idl_paths`, account subscription config |
| `ContractConfig` | `address: AddressOrAddresses` uses `alloy::Address` (20-byte). Solana needs 32-byte `Pubkey`. Events/calls/factories are all EVM concepts |
| `EthCallConfig` | Entire file is EVM-specific — function signatures, `EvmType` enum, `ParamConfig`, `CallTarget`. Solana equivalent: account reads with Borsh schemas |
| `EventConfig` | `signature` field holds Solidity ABI signatures. Solana would need Anchor event discriminators or program log patterns |
| `EvmType` enum | 1050+ lines of Solidity type system. Solana needs Borsh type descriptors or IDL-based type definitions |
| `FieldsConfig` | `receipt_fields`, `log_fields` — no equivalents on Solana. Replace with `instruction_fields`, `program_log_fields` |
| `BlockReceiptsMethod` | Entirely EVM RPC methods. No Solana equivalent |
| `FactoryConfig` / `FactoryEventConfig` | Factory pattern doesn't exist on Solana. PDA derivation is deterministic, not event-driven |

**Approach:** Introduce a chain-type discriminator (enum `ChainType { Evm, Solana }`) in `ChainConfigRaw`. Make contract/event/call configs chain-type-specific variants. The `EvmType` system stays for EVM; add a parallel `SolanaType` or IDL-based type system.

---

### 2. RPC Layer (`src/rpc/`)

**Entirely EVM-coupled.** Every method is an Ethereum JSON-RPC call.

| Component | EVM-specific | Solana equivalent |
|---|---|---|
| `RpcProvider` trait | `get_block`, `get_logs`, `eth_call`, `get_block_receipts` | `getBlock`, `getTransaction`, `getProgramAccounts`, `getAccountInfo` |
| `RpcClient` | `RootProvider<Ethereum>` from alloy | Solana RPC client (e.g., `solana-client` crate) |
| `AlchemyClient` | Alchemy CU costs per EVM method | Different CU costs for Solana methods |
| `UnifiedRpcClient` | Routes to Standard/Alchemy (both EVM) | Needs Solana variant |
| `WsClient` | `eth_subscribe("newHeads")` | `slotSubscribe`, `programSubscribe` |
| `BlockHeader` | `hash`, `parent_hash` as `B256` | Slot number, blockhash, parent slot |

**Approach:** The rate limiter (`SlidingWindowRateLimiter`) and retry logic are reusable. Need a `trait ChainRpcClient` with EVM and Solana implementations. The `UnifiedRpcClient` enum gets a `Solana(SolanaRpcClient)` variant.

---

### 3. Raw Data Collection (`src/raw_data/`)

**The most heavily EVM-coupled subsystem.** Every collector assumes EVM data shapes.

| Collector | EVM assumption | Solana equivalent |
|---|---|---|
| **Blocks** (`blocks.rs`) | `FullBlockRecord` with 25+ EVM fields (miner, uncles, gas, bloom, etc.) | Slot/block with leader, rewards, transactions with instructions |
| **Receipts** (`receipts.rs`) | Transaction receipts with `from`, `to`, `status`, `gas_used`, logs array | No receipts — transactions have `meta` with `err`, `fee`, `innerInstructions`, `logMessages` |
| **Logs** (`logs.rs`) | `address` + `topics[]` + `data` | Program logs (text strings), Anchor events (discriminator + Borsh data) |
| **Factories** (`factories.rs`) | Event-driven address discovery via topic0 matching | PDA derivation (deterministic from seeds + program_id), or parse `InitializeAccount` instructions |
| **Eth Calls** (`eth_calls/`) | `eth_call` with ABI-encoded params/results, Multicall3 batching | `getAccountInfo` / `getMultipleAccounts` with Borsh deserialization |
| **Event triggers** (`event_triggers.rs`) | Extract params from log topics/data (32-byte words) | Parse instruction data or program log entries |
| **Parquet schemas** | `FixedSizeBinary(20)` for addresses, `FixedSizeBinary(32)` for hashes | `FixedSizeBinary(32)` for pubkeys, `Utf8` for base58 signatures |

**Approach:** The channel-based pipeline pattern (block → receipts → logs → factories → eth_calls) is sound. Need parallel Solana collector implementations:
- `SolanaBlockCollector` (slots + transactions)
- `SolanaInstructionCollector` (replaces receipts+logs)
- `SolanaAccountReader` (replaces eth_calls)
- `SolanaPdaDiscovery` (replaces factories)

---

### 4. Decoding (`src/decoding/`)

**Completely EVM-specific.** The entire subsystem assumes Solidity ABI encoding.

| Component | EVM assumption | Solana equivalent |
|---|---|---|
| `event_parsing.rs` | Parses Solidity event signatures, computes keccak256 topic0 | Parse Anchor IDL event definitions, compute 8-byte discriminators (sha256 prefix) |
| `logs.rs` | Matches by topic0 + address, decodes ABI data | Match by program_id + discriminator, decode Borsh data |
| `eth_calls/decode.rs` | `DynSolType.abi_decode_params()` | Borsh deserialization with IDL-derived schemas |
| `DecodedValue` enum | `Address([u8; 20])`, `Uint256(U256)`, alloy types | `Pubkey([u8; 32])`, `u64` (Solana uses u64 not u256), Borsh types |
| `EvmType` → Arrow mapping | 20-byte addresses, 256-bit integers | 32-byte pubkeys, 64-bit integers, Borsh structs |

**Approach:** Need a `SolanaDecoder` that:
- Loads Anchor IDLs instead of ABI signatures
- Computes 8-byte event discriminators (`sha256("event:<EventName>")[..8]`)
- Deserializes Borsh-encoded data instead of ABI
- Maps Solana types (Pubkey, u64, i64, String, Vec<u8>) to Arrow/parquet

---

### 5. Transformations (`src/transformations/`)

**Mixed — the framework is generic, the data types and handlers are EVM-specific.**

| Component | Chain-agnostic? | Changes needed |
|---|---|---|
| `TransformationHandler` trait | **Yes** | `chain_type()` has now landed; it defaults to `Evm` and is used for registry filtering |
| `EventHandler` / `EthCallHandler` traits | **Yes** | Solana event handlers implement `EventHandler`. New `AccountStateHandler` trait for Solana account reads (not shoehorned into `EthCallHandler`) |
| `TransformationRegistry` | **Yes** | `account_state_handlers` and chain-type validation have now landed |
| `engine.rs` | **Yes** | Branch now includes account-state channel/message plumbing; Solana producers are still missing |
| `scheduler/dag.rs` | **Yes** | No changes needed |
| `TransformationContext` | **Partially** | Branch adds `account_states` plus account-state extractors/helpers. `ChainServices` is still only a placeholder; `rpc` and `contracts` remain in use |
| `DecodedValue` enum | **No** | Branch adds `ChainAddress(ChainAddress)` rather than a dedicated `Pubkey([u8; 32])` variant |
| Handler implementations (v3/, v4/) | **No** | These stay as EVM handlers. Write new Solana-specific handlers |
| `util/` (tick_math, sanitize, etc.) | **No** | EVM/Uniswap-specific. Solana handlers get their own utils |

**Approach:** The trait system and DAG scheduler remain the shared core. This branch has already landed `DecodedAccountState`, `AccountStateHandler`, and account-state engine plumbing. `DecodedEvent` and `DecodedCall` have not yet been generalized with `ChainAddress`/`TxId`/`LogPosition`; that is still future work.

---

### 6. Live Mode (`src/live/`)

**Heavily EVM-coupled in data types, generic in infrastructure.**

| Component | Changes needed |
|---|---|
| `LiveBlock` | Hash/parent_hash are `[u8; 32]` (works for Solana blockhash), but `tx_hashes` structure differs. Solana transactions have signatures (64 bytes), not 32-byte hashes |
| `LiveReceipt` / `LiveLog` | No equivalent on Solana. Replace with `LiveTransaction` containing instructions + program logs |
| `LiveEthCall` | Replace with `LiveAccountRead` (live-mode-only — no historical account reads) |
| `LiveBlockStatus` | `receipts_collected`, `logs_collected`, `eth_calls_collected` flags are EVM-specific. Solana needs `events_collected`, `accounts_read` |
| `ReorgDetector` | At `confirmed` commitment, skip reorg detection (Solana has never reorged a confirmed block). Reject `processed` commitment in config validation |
| `WsClient` | `eth_subscribe("newHeads")` → `slotSubscribe` or `blockSubscribe` |
| `CompactionService` | Generic — works for any block-ranged data |
| `LiveProgressTracker` | Generic — no changes needed |

---

### 7. Storage (`src/storage/`)

**Mostly chain-agnostic.** Minor path convention changes.

| Component | Changes needed |
|---|---|
| `StorageBackend` trait | None — fully generic |
| `StorageManager` | None |
| Path helpers | Currently hardcodes `receipts`, `logs`, `eth_calls`, `factories`. Add Solana equivalents (`instructions`, `accounts`, `program_logs`) |
| `parquet_readers.rs` | Reads assume `FixedSizeBinary(20)` addresses. Solana readers need `FixedSizeBinary(32)` for pubkeys |

---

### 8. Database (`src/db/`)

**Mostly chain-agnostic, with two EVM-specific types.**

| Component | Changes needed |
|---|---|
| `DbValue::Address([u8; 20])` | Add `DbValue::Pubkey([u8; 32])` — distinct type, not `Vec<u8>`, maps to 32-byte BYTEA |
| `DbValue::Bytes32([u8; 32])` | Works for Solana blockhashes. Solana signatures are 64 bytes — add `DbValue::Signature([u8; 64])` if needed |
| `DbPool`, `DbOperation`, `WhereClause` | Fully generic — no changes |
| Migration system | Generic — Solana handlers bring their own migrations |
| Schema tables (pools, tokens, etc.) | **Already cross-chain-ready** — see note below |

**Cross-chain table compatibility:** The existing schema is more portable than it appears. Every table has `chain_id`, all address columns use variable-length `BYTEA` (not `BYTEA(20)`), and `tx_hash` columns are also `BYTEA`. A Solana handler can write 32-byte pubkeys to the same `tokens.address` column that holds 20-byte EVM addresses. Cross-chain queries like `SELECT * FROM tokens WHERE symbol = 'USDC'` return results from both chains.

Protocol-specific columns (`is_derc20`, `graduation_tick`, `migration_pool`, etc.) are already nullable in most cases and would simply be NULL for Solana rows. For `log_index INT` columns (e.g., `liquidity_deltas`), Solana stores the packed ordinal from `LogPosition::ordinal()`.

Solana handlers should write to the **same canonical tables** (tokens, pools, swaps, pool_state, etc.) with appropriate `chain_id`. Protocol-specific extensions (e.g., Orca-specific pool metadata) go in separate handler-owned tables that JOIN to the canonical ones. This enables unified cross-chain querying from day one.

---

### 9. Metrics (`src/metrics/`)

**Mostly generic, labels are EVM-flavored.**

| Component | Changes needed |
|---|---|
| `RpcMethod` enum | Add Solana methods: `GetSlot`, `GetBlock`, `GetTransaction`, `GetAccountInfo`, `GetProgramAccounts` |
| Collection/live metrics | Labels like `data_type` currently emit `receipts`, `logs`. Add `instructions`, `accounts` |
| RAII guards | Fully generic |

---

### 10. Dependencies (`Cargo.toml`)

| Current | Change |
|---|---|
| `alloy` + `alloy-primitives` | Keep for EVM. Add behind `evm` feature flag |
| None for Solana | Add `solana-sdk`, `solana-client`, `solana-transaction-status`, `anchor-lang` (for IDL parsing), `borsh` |
| Feature flags | Add `features = ["evm", "solana"]` with `default = ["evm", "solana"]` |

---

## Recommended Architecture

Rather than trying to abstract everything behind traits (which would be a massive refactor with diminishing returns), a **parallel implementation** approach is recommended. EVM code stays in place (not moved into `src/evm/`), Solana code lives in `src/solana/`, feature-gated:

```
src/
├── rpc/               # Current EVM code, unchanged
├── raw_data/          # Current EVM code, unchanged
├── decoding/          # Current EVM code, minor additions (DecoderMessage variants)
├── live/              # Current EVM code, unchanged
├── solana/            # NEW - all feature-gated behind #[cfg(feature = "solana")]
│   ├── rpc.rs         # SolanaRpcClient
│   ├── ws.rs          # WebSocket (slotSubscribe)
│   ├── discovery.rs   # Address discovery (event-driven + getProgramAccounts)
│   ├── raw_data/      # Slot collector, event extractor, historical backfill
│   ├── decoding/      # IDL parser, dynamic Borsh deserializer, event/account decoders
│   └── live/          # Live collector, account reader (live-only)
├── transformations/   # Stays shared — traits (+ chain_type(), AccountStateHandler),
│                      #   engine, DAG, registry (+ account_state_handlers, chain validation),
│                      #   context (+ ChainServices enum, DecodedAccountState)
├── db/                # Stays shared (add Pubkey variant to DbValue)
├── storage/           # Stays shared (add Solana path helpers)
├── metrics/           # Stays shared (add Solana RPC methods)
├── types/             # chain.rs (new), config/solana.rs (new), decoded.rs (Pubkey variant)
└── main.rs            # Dispatch by chain type
```

**What stays shared:** transformation engine + traits + DAG scheduler, database layer, storage backends, metrics infrastructure, compaction service, progress tracking.

**What gets parallel implementations:** RPC client, raw data collectors, decoders, live collector, config types, WebSocket subscription, address discovery.

**Key design decisions:**
- `DecodedCall` stays EVM-only; Solana account reads use new `DecodedAccountState` type with `AccountStateHandler` trait
- `TransformationContext` uses `ChainServices` enum instead of bare EVM-specific fields
- `TransformationHandler` gains `chain_type()` for startup validation
- Account reads are live-mode-only (historical account state is not available via RPC)
- `TxId` is `Clone` not `Copy` (65 bytes for Solana variant)

**Scope of changes:**
- ~60% of the codebase is EVM-specific and needs parallel Solana implementations
- ~30% is infrastructure that stays shared with minor additions
- ~10% needs generalization (address types in `DecodedEvent`/`DecodedCall`, `DbValue` variants, context services)
