# Solana Support: Detailed Design Document

## 1. Motivation

Doppler Indexer currently supports EVM chains exclusively. Adding Solana support requires addressing fundamental data model differences while preserving the existing EVM pipeline unchanged. This document specifies the exact types, interfaces, data flows, and module designs required.

## 1.1 Branch Delta

This document is still the intended end-state design. The current `feat/solana-phase1-traits` branch only implements a subset of it.

What has landed:
- `src/types/chain.rs` with `ChainAddress`, `TxId`, `LogPosition`, and `ChainType`
- `chain_type` in chain config, with default `evm`
- `TransformationHandler::chain_type()`, `DecodedAccountState`, `AccountStateHandler`, and registry account-state indices
- transformation runtime / engine / live-state plumbing for account-state messages
- UTF-8-safe RPC error truncation and a follow-up fix to keep EVM stuck-event warnings working after the new completion plumbing

Where the branch currently differs from the design below:
- `DecodedValue` uses `ChainAddress(ChainAddress)` rather than a dedicated `Pubkey([u8; 32])` variant
- `TransformationContext` still uses the existing `rpc` and `contracts` fields in production code; `ChainServices` exists only as a placeholder enum
- `main.rs` does not yet dispatch to a Solana pipeline by `chain_type`
- no Solana RPC, raw-data, decoding, or live modules have landed yet

Use the rest of this file as the target architecture, not a claim that every section is already implemented on this branch.

---

## 2. Solana vs EVM: Data Model Mapping

| Concept | EVM | Solana | Design Impact |
|---------|-----|--------|---------------|
| **Address** | 20-byte hex | 32-byte Ed25519 pubkey (base58) | New `ChainAddress` enum |
| **Tx identifier** | 32-byte keccak hash | 64-byte Ed25519 signature | New `TxId` enum |
| **Block** | Block with txs, uncles, gas, receipts | Slot with transactions containing instructions | Separate collector |
| **Events** | Logs: topics[] + data (ABI-encoded) | Program logs: `"Program data: <base64>"` with 8-byte discriminator + Borsh | Separate decoder |
| **State reads** | `eth_call` with ABI-encoded params/results | `getAccountInfo` with Borsh-encoded account data | Separate reader |
| **Encoding** | Solidity ABI (32-byte words, big-endian) | Borsh (variable-width, little-endian) | Separate codec |
| **Contract discovery** | Factory events emit new addresses | PDAs (deterministic from seeds + program_id) | Config-driven |
| **Finality** | Parent hash chain, variable reorg depth | Confirmation levels: processed/confirmed/finalized | Simplified reorg handling |
| **WebSocket** | `eth_subscribe("newHeads")` | `slotSubscribe`, `logsSubscribe`, `programSubscribe` | Separate WS client |
| **Transaction model** | Single `to` + `data` per tx | Multiple instructions per tx, each with program_id + accounts[] + data | Richer event positioning |
| **Hash function** | Keccak-256 | SHA-256 | Different discriminator computation |
| **Numeric types** | uint256/int256 dominant | u64/i64 dominant, u128 for prices/liquidity | `DecodedValue` already covers these |

---

## 3. Core Type Abstractions

### 3.1 ChainAddress

The most critical abstraction. Used in `DecodedEvent`, `DecodedCall`, `TransformationContext`, `DbValue`, query filters, and handler utilities.

```rust
// src/types/chain.rs

/// A blockchain address, sized appropriately for the source chain.
/// Stack-allocated, Copy, and usable as HashMap key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChainAddress {
    /// EVM address: 20 bytes (160-bit, hex-encoded)
    Evm([u8; 20]),
    /// Solana pubkey: 32 bytes (Ed25519 public key, base58-encoded)
    Solana([u8; 32]),
}

impl ChainAddress {
    /// Raw bytes regardless of chain type
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            ChainAddress::Evm(a) => a,
            ChainAddress::Solana(p) => p,
        }
    }

    /// Byte length of the address (20 for EVM, 32 for Solana)
    pub fn len(&self) -> usize {
        match self {
            ChainAddress::Evm(_) => 20,
            ChainAddress::Solana(_) => 32,
        }
    }

    /// Hex-encoded string (0x-prefixed for EVM, plain hex for Solana)
    pub fn to_hex(&self) -> String {
        match self {
            ChainAddress::Evm(a) => format!("0x{}", hex::encode(a)),
            ChainAddress::Solana(p) => hex::encode(p),
        }
    }

    /// Display-friendly string (hex for EVM, base58 for Solana)
    pub fn to_display(&self) -> String {
        match self {
            ChainAddress::Evm(a) => format!("0x{}", hex::encode(a)),
            ChainAddress::Solana(p) => bs58::encode(p).into_string(),
        }
    }
}
```

**Why not alternatives:**
- `Vec<u8>`: Heap-allocated. Addresses appear in every `DecodedEvent` in every block — this is a hot path. Unacceptable allocation overhead.
- `[u8; 32]` (pad EVM): Loses type information. Cannot distinguish Solana pubkey from EVM address. Breaks `Address::from(event.contract_address)` pattern used in all handlers.
- Generic `DecodedEvent<A: AddressType>`: Infects the entire trait hierarchy. `TransformationHandler` becomes `TransformationHandler<A>`, breaking `Arc<dyn TransformationHandler>` dynamic dispatch. Massive disruption.
- Enum: 33 bytes on stack (1 discriminant + 32 max payload). Compiler likely pads to 40 bytes. `Copy`, zero heap allocation, branch-predictable (EVM handlers always hit `Evm` variant).

### 3.2 TxId

Transaction identifiers differ in size: EVM uses 32-byte keccak hashes, Solana uses 64-byte Ed25519 signatures.

```rust
/// A transaction identifier, sized for the source chain.
/// Clone but NOT Copy — at 65 bytes for the Solana variant, implicit copies
/// in hot loops (thousands of events per block) add up. Explicit `.clone()`
/// makes performance-sensitive code reviewable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxId {
    /// EVM transaction hash: 32 bytes (keccak-256)
    Evm([u8; 32]),
    /// Solana transaction signature: 64 bytes (Ed25519)
    Solana([u8; 64]),
}

impl TxId {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            TxId::Evm(h) => h,
            TxId::Solana(s) => s,
        }
    }
}
```

**Why Clone and not Copy:** `ChainAddress` at 33 bytes is small enough for `Copy` — comparable to common `Copy` types. `TxId::Solana` at 65 bytes crosses the threshold where implicit copies become invisible overhead. When iterating thousands of `DecodedEvent`s per block, each `let id = event.transaction_id` is a 65-byte memcpy. With `Clone`, these copies are explicit (`.clone()`), making hot-path code reviewable. The EVM variant at 33 bytes pays the minor cost of `.clone()` syntax for consistency.

### 3.3 LogPosition

EVM events have a flat `log_index` within a block. Solana has a two-level hierarchy: instruction index within a transaction, and inner instruction index within an instruction (for CPI calls).

```rust
/// Position of an event within its transaction/block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogPosition {
    /// EVM: global log index within the block
    Evm { log_index: u32 },
    /// Solana: instruction index + optional inner instruction index (CPI)
    Solana {
        instruction_index: u16,
        inner_instruction_index: Option<u16>,
    },
}

/// Maximum inner instructions per outer instruction for ordinal packing.
/// Solana's runtime limits CPI depth to 4, but a single instruction can
/// emit many inner instructions. 10,000 provides ample headroom.
const MAX_INNER_INSTRUCTIONS: u64 = 10_000;

impl LogPosition {
    /// Ordering within a transaction for sorting events
    pub fn ordinal(&self) -> u64 {
        match self {
            LogPosition::Evm { log_index } => *log_index as u64,
            LogPosition::Solana { instruction_index, inner_instruction_index } => {
                (*instruction_index as u64) * MAX_INNER_INSTRUCTIONS
                    + inner_instruction_index.unwrap_or(0) as u64
            }
        }
    }
}
```

### 3.4 ChainType

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainType {
    Evm,
    Solana,
}

impl Default for ChainType {
    fn default() -> Self { Self::Evm }
}
```

---

## 4. DecodedValue Extensions

### 4.1 New Pubkey Variant

```rust
// src/types/decoded.rs — additions to existing enum

pub enum DecodedValue {
    // ... existing variants unchanged ...
    Address([u8; 20]),     // EVM address (kept for backward compat)
    Pubkey([u8; 32]),      // NEW: Solana pubkey
    // ...
}

impl DecodedValue {
    // NEW methods
    pub fn as_pubkey(&self) -> Option<[u8; 32]> {
        match self {
            DecodedValue::Pubkey(p) => Some(*p),
            _ => None,
        }
    }

    pub fn as_chain_address(&self) -> Option<ChainAddress> {
        match self {
            DecodedValue::Address(a) => Some(ChainAddress::Evm(*a)),
            DecodedValue::Pubkey(p) => Some(ChainAddress::Solana(*p)),
            _ => None,
        }
    }

    // Update existing as_bytes to include Pubkey
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            DecodedValue::Bytes(b) => Some(b),
            DecodedValue::Bytes32(b) => Some(b),
            DecodedValue::Address(a) => Some(a),
            DecodedValue::Pubkey(p) => Some(p),  // NEW
            _ => None,
        }
    }
}
```

**Why `Pubkey` instead of reusing `Bytes32`:** Semantic distinction matters. A Solana pubkey is not arbitrary bytes — handler authors need `extract_pubkey()` alongside `extract_address()`. Database mapping differs: pubkeys map to `DbValue::Pubkey` with different display/indexing semantics than `DbValue::Bytes32`.

### 4.2 DbValue Extension

```rust
// src/db/types.rs — addition

pub enum DbValue {
    // ... existing variants unchanged ...
    Address([u8; 20]),    // EVM: 20-byte BYTEA
    Pubkey([u8; 32]),     // NEW: Solana 32-byte BYTEA
    Bytes32([u8; 32]),    // kept: generic 32-byte hash
    // ...
}
```

In `src/db/pool.rs`, wire `DbValue::Pubkey(v)` to `$N::BYTEA` parameter binding (same as `Address` but 32 bytes).

### 4.3 LiveDbValue Extension

```rust
// src/live/types.rs — addition to LiveDbValue enum

pub enum LiveDbValue {
    // ... existing variants unchanged ...
    Pubkey([u8; 32]),  // NEW
    // ...
}
```

Wire bidirectional conversion with `DbValue::Pubkey` for snapshot serialization.

---

## 5. Transformation Layer Convergence

### 5.1 Updated DecodedEvent

```rust
// src/transformations/context.rs

pub struct DecodedEvent {
    pub block_number: u64,              // slot number on Solana
    pub block_timestamp: u64,           // block_time on Solana
    pub transaction_id: TxId,           // was: transaction_hash: [u8; 32]
    pub position: LogPosition,          // was: log_index: u32
    pub contract_address: ChainAddress, // was: [u8; 20]
    pub source_name: String,            // program name on Solana
    pub event_name: String,             // Anchor event name on Solana
    pub event_signature: String,        // Anchor event name on Solana (see note below)
    pub params: HashMap<String, DecodedValue>,
}
```

**`event_signature` semantics by chain:**

The `event_signature` field is the trigger matching key — the `TransformationRegistry` indexes handlers by `(source_name, event_signature)`. Its semantics differ by chain but its role is the same: identify the event type for handler dispatch.

| Chain | `event_signature` value | Example |
|-------|------------------------|---------|
| EVM | Full Solidity event signature | `"Swap(address,address,int256,int256,uint160,uint128,int24)"` |
| Solana | Anchor event name | `"Traded"` |

**Reason:** The discriminator (8-byte hash) is an internal detail of how the Solana decoder matches raw log entries. It's not the handler author's concern. Handlers subscribe by event name, which is human-readable and unique within a program. This means zero changes to `TransformationRegistry` — it already dispatches on `(String, String)`.

The Solana decoder sets `event_signature = event_name` when constructing `DecodedEvent`. The discriminator is used only inside the decoder to match raw log bytes to IDL event definitions.

```rust
// Handler registration — uses event name, not discriminator
EventTrigger::new("orca_whirlpool", "Traded")

// Decoder output — event_signature == event_name
DecodedEvent {
    source_name: "orca_whirlpool".into(),
    event_name: "Traded".into(),
    event_signature: "Traded".into(),  // matches the trigger
    ..
}
```

**Backward-compatible EVM convenience methods:**

```rust
impl DecodedEvent {
    /// EVM convenience: extract 20-byte address. Panics on non-EVM events.
    pub fn evm_address(&self) -> [u8; 20] {
        match self.contract_address {
            ChainAddress::Evm(a) => a,
            _ => panic!("evm_address() called on non-EVM event"),
        }
    }

    /// EVM convenience: extract log index. Panics on non-EVM events.
    pub fn log_index(&self) -> u32 {
        match self.position {
            LogPosition::Evm { log_index } => log_index,
            _ => panic!("log_index() called on non-EVM event"),
        }
    }

    /// EVM convenience: extract 32-byte tx hash. Panics on non-EVM events.
    pub fn evm_tx_hash(&self) -> [u8; 32] {
        match self.transaction_id {
            TxId::Evm(h) => h,
            _ => panic!("evm_tx_hash() called on non-EVM event"),
        }
    }
}
```

These are safe to use in EVM handlers because the trigger-based dispatch guarantees EVM handlers only receive EVM events. The panics serve as a development-time safety net.

### 5.2 Updated DecodedCall

`DecodedCall` remains EVM eth_call specific. Solana account reads get their own type (see 5.2b).

```rust
pub struct DecodedCall {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: ChainAddress,         // was: [u8; 20]
    pub source_name: String,
    pub function_name: String,
    pub trigger_position: Option<LogPosition>,  // was: trigger_log_index: Option<u32>
    pub result: HashMap<String, DecodedValue>,
    pub is_reverted: bool,
    pub revert_reason: Option<String>,
}
```

### 5.2b DecodedAccountState (new — Solana account reads)

Solana account reads are semantically different from EVM `eth_call`: they read stored state rather than invoking a function. Shoehorning them into `DecodedCall` would leave `is_reverted`, `revert_reason`, and `trigger_position` as permanently dead fields, confusing Solana handler authors and misrepresenting the data model.

```rust
/// Decoded account state from a Solana `getAccountInfo` read.
/// Analogous to DecodedCall for EVM, but models state reads rather than function invocations.
#[derive(Debug, Clone)]
pub struct DecodedAccountState {
    pub block_number: u64,               // slot
    pub block_timestamp: u64,            // block_time
    pub account_address: ChainAddress,   // the account that was read
    pub owner_program: ChainAddress,     // the program that owns this account
    pub source_name: String,             // program config name (e.g., "orca_whirlpool")
    pub account_type: String,            // IDL account type name (e.g., "Whirlpool")
    pub fields: HashMap<String, DecodedValue>,
}

impl FieldExtractor for DecodedAccountState {
    fn field_values(&self) -> &HashMap<String, DecodedValue> {
        &self.fields
    }

    fn context_info(&self) -> String {
        format!(
            "account_state {}:{} at slot {} address {}",
            self.source_name, self.account_type, self.block_number,
            self.account_address.to_display()
        )
    }
}
```

**Corresponding handler trait:**

```rust
/// Trigger for account-state-based handlers (Solana).
#[derive(Debug, Clone)]
pub struct AccountStateTrigger {
    /// Program config name (e.g., "orca_whirlpool")
    pub source: String,
    /// IDL account type name (e.g., "Whirlpool")
    pub account_type: String,
}

/// Handler trait for Solana account state reads.
/// Follows the same pattern as EventHandler and EthCallHandler.
#[cfg(feature = "solana")]
pub trait AccountStateHandler: TransformationHandler {
    fn triggers(&self) -> Vec<AccountStateTrigger>;
}
```

**Reason:** The transformation engine and DAG scheduler operate on `Arc<dyn TransformationHandler>` — they don't care which sub-trait a handler implements. The registry adds a third index map `account_state_handlers: HashMap<(String, String), Vec<Arc<dyn AccountStateHandler>>>` following the existing pattern. This keeps the type system honest: each data kind has its own type, handler authors see only fields that are meaningful for their data, and the registry dispatch is uniform.

### 5.3 Updated TransformationContext

```rust
/// Chain-specific services available to handlers via the context.
/// Avoids accumulating Option<SolanaX> fields as chains are added.
pub enum ChainServices {
    Evm {
        rpc: Arc<UnifiedRpcClient>,
        contracts: Arc<Contracts>,
    },
    #[cfg(feature = "solana")]
    Solana {
        rpc: Arc<SolanaRpcClient>,
        programs: Arc<SolanaPrograms>,
    },
}

pub struct TransformationContext {
    pub chain_name: String,
    pub chain_id: u64,
    pub blockrange_start: u64,
    pub blockrange_end: u64,
    pub events: Arc<Vec<DecodedEvent>>,
    pub calls: Arc<Vec<DecodedCall>>,
    #[cfg(feature = "solana")]
    pub account_states: Arc<Vec<DecodedAccountState>>,
    tx_addresses: HashMap<TxId, TransactionAddresses>,  // was: HashMap<[u8; 32], ...>
    pub(crate) historical: Arc<HistoricalDataReader>,
    pub(crate) chain_services: ChainServices,            // was: rpc + contracts
}

pub struct TransactionAddresses {
    pub from_address: ChainAddress,           // was: [u8; 20]
    pub to_address: Option<ChainAddress>,     // was: Option<[u8; 20]>
}
```

**Reason for `ChainServices` enum:** The previous design had `rpc: Arc<UnifiedRpcClient>` and `contracts: Arc<Contracts>` — both EVM-only. Adding Solana would require `solana_rpc: Option<Arc<SolanaRpcClient>>` and `solana_programs: Option<Arc<SolanaPrograms>>`, with every new chain adding more `Option` fields. The enum dispatches cleanly: handlers know their chain type and access the right variant. The context constructor matches the chain config to build the appropriate variant.

**Backward-compatible address methods:**

```rust
impl TransformationContext {
    // Keep existing signature — wraps [u8; 20] into ChainAddress::Evm internally
    pub fn events_for_address(&self, address: [u8; 20]) -> impl Iterator<Item = &DecodedEvent> + '_ {
        self.events_for_chain_address(ChainAddress::Evm(address))
    }

    // New generic version for any chain
    pub fn events_for_chain_address(&self, address: ChainAddress) -> impl Iterator<Item = &DecodedEvent> + '_ {
        self.events.iter().filter(move |e| {
            if e.contract_address != address { return false; }
            let start_block = self.get_contract_start_block(&e.source_name);
            start_block.is_none_or(|sb| e.block_number >= sb)
        })
    }

    // Same pattern for calls_for_address / calls_for_chain_address
    pub fn calls_for_address(&self, address: [u8; 20]) -> impl Iterator<Item = &DecodedCall> + '_ {
        self.calls_for_chain_address(ChainAddress::Evm(address))
    }

    pub fn calls_for_chain_address(&self, address: ChainAddress) -> impl Iterator<Item = &DecodedCall> + '_ {
        self.calls.iter().filter(move |c| {
            if c.contract_address != address { return false; }
            let start_block = self.get_contract_start_block(&c.source_name);
            start_block.is_none_or(|sb| c.block_number >= sb)
        })
    }

    // Account state lookups (Solana)
    #[cfg(feature = "solana")]
    pub fn account_states_for_program(&self, address: ChainAddress) -> impl Iterator<Item = &DecodedAccountState> + '_ {
        self.account_states.iter().filter(move |a| a.owner_program == address)
    }

    #[cfg(feature = "solana")]
    pub fn account_states_for_type(&self, source: &str, account_type: &str) -> impl Iterator<Item = &DecodedAccountState> + '_ {
        self.account_states.iter().filter(move |a| {
            a.source_name == source && a.account_type == account_type
        })
    }

    // Transaction address lookups use TxId
    pub fn tx_from(&self, tx_id: &TxId) -> Option<&ChainAddress> {
        self.tx_addresses.get(tx_id).map(|a| &a.from_address)
    }

    pub fn tx_to(&self, tx_id: &TxId) -> Option<&ChainAddress> {
        self.tx_addresses.get(tx_id).and_then(|a| a.to_address.as_ref())
    }

    // Chain service accessors
    pub fn evm_rpc(&self) -> &Arc<UnifiedRpcClient> {
        match &self.chain_services {
            ChainServices::Evm { rpc, .. } => rpc,
            _ => panic!("evm_rpc() called on non-EVM context"),
        }
    }

    pub fn evm_contracts(&self) -> &Arc<Contracts> {
        match &self.chain_services {
            ChainServices::Evm { contracts, .. } => contracts,
            _ => panic!("evm_contracts() called on non-EVM context"),
        }
    }

    #[cfg(feature = "solana")]
    pub fn solana_rpc(&self) -> &Arc<SolanaRpcClient> {
        match &self.chain_services {
            ChainServices::Solana { rpc, .. } => rpc,
            _ => panic!("solana_rpc() called on non-Solana context"),
        }
    }
}
```

### 5.3b `chain_type()` on TransformationHandler

Add a `chain_type()` method to the base handler trait:

```rust
pub trait TransformationHandler: Send + Sync + 'static {
    fn chain_type(&self) -> ChainType;
    fn name(&self) -> &'static str;
    fn version(&self) -> u32 { 1 }
    // ... existing methods unchanged ...
}
```

The registry validates at registration time:

```rust
impl TransformationRegistry {
    fn register_event_handler(&mut self, handler: Arc<dyn EventHandler>, chain_type: ChainType) {
        if handler.chain_type() != chain_type {
            panic!(
                "Handler '{}' declares chain_type {:?} but is being registered on a {:?} chain",
                handler.name(), handler.chain_type(), chain_type
            );
        }
        // ... existing registration logic ...
    }
}
```

**Reason:** Without this, a Solana handler accidentally registered on an EVM chain (or vice versa) would receive events it can't process, producing panics from `evm_address()` or garbage data from wrong field extraction. This catches config mistakes at startup — fail fast, not at runtime in the middle of a block range. It's one line per handler implementation and one check in the registry.

### 5.4 Updated Query Types

```rust
pub struct HistoricalEventQuery {
    pub source: Option<String>,
    pub event_name: Option<String>,
    pub contract_address: Option<ChainAddress>,  // was: Option<[u8; 20]>
    pub from_block: u64,
    pub to_block: u64,
    pub limit: Option<usize>,
}

pub struct HistoricalCallQuery {
    pub source: Option<String>,
    pub function_name: Option<String>,
    pub contract_address: Option<ChainAddress>,  // was: Option<[u8; 20]>
    pub from_block: u64,
    pub to_block: u64,
    pub limit: Option<usize>,
}
```

### 5.5 FieldExtractor Additions

```rust
// In FieldExtractor trait
impl_field_extractor!(extract_pubkey, as_pubkey, [u8; 32], "a pubkey");

fn extract_chain_address(&self, name: &str) -> Result<ChainAddress, TransformationError> {
    let val = self.get_field(name)?;
    val.as_chain_address().ok_or_else(|| {
        TransformationError::TypeConversion(format!(
            "'{}' is not an address or pubkey in {}",
            name,
            self.context_info()
        ))
    })
}
```

### 5.6 HistoricalDataReader Parquet Changes

The `get_address_column()` function in `src/transformations/historical.rs` currently expects exactly 20-byte `FixedSizeBinary`. It becomes size-aware:

```rust
fn get_chain_address_column(
    batch: &RecordBatch,
    name: &str,
) -> Result<Vec<ChainAddress>, TransformationError> {
    let col = batch.column_by_name(name)
        .ok_or_else(|| TransformationError::MissingColumn(name.to_string()))?;
    let arr = col.as_any().downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TransformationError::TypeConversion(format!("{} is not FixedSizeBinary", name)))?;

    let mut result = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        let bytes = arr.value(i);
        let addr = match bytes.len() {
            20 => ChainAddress::Evm(bytes.try_into().unwrap()),
            32 => ChainAddress::Solana(bytes.try_into().unwrap()),
            n => return Err(TransformationError::TypeConversion(
                format!("{} has unexpected address size {}", name, n)
            )),
        };
        result.push(addr);
    }
    Ok(result)
}
```

---

## 6. Configuration Layer

### 6.1 ChainConfigRaw Extension

```rust
// src/types/config/chain.rs — additions to existing struct

pub struct ChainConfigRaw {
    // NEW field, defaults to Evm for backward compatibility
    #[serde(default)]
    pub chain_type: ChainType,

    // Existing fields unchanged
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env_var: String,
    pub ws_url_env_var: Option<String>,
    pub start_block: Option<U256>,
    pub contracts: Option<InlineOrPath<Contracts>>,
    pub block_receipts_method: Option<BlockReceiptsMethod>,
    pub factory_collections: Option<InlineOrPath<FactoryCollections>>,
    pub rpc: RpcConfig,

    // NEW Solana-specific fields (feature-gated)
    #[cfg(feature = "solana")]
    pub programs: Option<InlineOrPath<SolanaPrograms>>,
    #[cfg(feature = "solana")]
    pub commitment: Option<String>,  // "processed" | "confirmed" | "finalized", default "confirmed"
}
```

### 6.2 Solana Program Config

```rust
// src/types/config/solana.rs (new file)

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use super::eth_call::Frequency;

pub type SolanaPrograms = HashMap<String, SolanaProgramConfig>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolanaProgramConfig {
    /// Base58-encoded program ID
    pub program_id: String,
    /// Path to IDL JSON file (relative to config dir). Format detected from idl_format.
    pub idl_path: Option<String>,
    /// IDL format: "anchor" (default) | "shank". Ignored if `decoder` is set.
    pub idl_format: Option<String>,
    /// Built-in decoder name (e.g., "spl_token"). Takes precedence over idl_path.
    pub decoder: Option<String>,
    /// Events to index (None = all events from IDL)
    pub events: Option<Vec<SolanaEventConfig>>,
    /// Account types to read in live mode (triggered by events)
    pub accounts: Option<Vec<SolanaAccountReadConfig>>,
    /// Address discovery rules — extract new account addresses from events (see 10.2b)
    pub discovery: Option<Vec<SolanaDiscoveryConfig>>,
    /// First slot to start indexing from
    pub start_slot: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolanaEventConfig {
    /// Anchor event name (e.g., "Traded", "PoolInitialized")
    pub name: String,
    /// 8-byte discriminator as hex string. If absent, computed as sha256("event:<name>")[..8]
    pub discriminator: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolanaAccountReadConfig {
    /// Human-readable name for this account type
    pub name: String,
    /// Account type name from the IDL (e.g., "Whirlpool", "PoolState")
    pub account_type: String,
    /// How often to read account state
    pub frequency: Frequency,
    /// Specific account addresses to read (if not using PDA discovery)
    pub addresses: Option<Vec<String>>,
}
```

### 6.3 Example Solana Config

```json
{
  "chains": [
    {
      "name": "solana-mainnet",
      "chain_type": "solana",
      "chain_id": -1,
      "rpc_url_env_var": "SOLANA_RPC_URL",
      "ws_url_env_var": "SOLANA_WS_URL",
      "start_block": 250000000,
      "commitment": "confirmed",
      "programs": {
        "orca_whirlpool": {
          "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
          "idl_path": "idl/whirlpool.json",
          "events": [
            { "name": "Traded" },
            { "name": "PoolInitialized" },
            { "name": "LiquidityIncreased" },
            { "name": "LiquidityDecreased" }
          ],
          "discovery": [
            {
              "event_name": "PoolInitialized",
              "address_field": "whirlpool",
              "account_type": "Whirlpool"
            }
          ],
          "accounts": [
            {
              "name": "whirlpool_state",
              "account_type": "Whirlpool",
              "frequency": "on_events"
            }
          ]
        }
      },
      "rpc": {
        "concurrency": 50,
        "batch_size": 100
      }
    }
  ]
}
```

---

## 7. Solana RPC Client

### 7.1 SolanaRpcClient (`src/solana/rpc.rs`)

```rust
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{
    EncodedConfirmedBlock, EncodedConfirmedTransactionWithStatusMeta,
    UiTransactionEncoding, TransactionDetails,
};

pub struct SolanaRpcClient {
    client: RpcClient,
    rate_limiter: Arc<SlidingWindowRateLimiter>,
    commitment: CommitmentConfig,
    max_batch_size: usize,
}

impl SolanaRpcClient {
    pub fn new(
        url: &str,
        rate_limiter: Arc<SlidingWindowRateLimiter>,
        commitment: CommitmentLevel,
    ) -> Result<Self, SolanaRpcError>;

    // ===== Slot/Block operations =====

    /// Get current slot height
    pub async fn get_slot(&self) -> Result<u64, SolanaRpcError>;

    /// Fetch a full block by slot. Returns None for skipped slots.
    pub async fn get_block(&self, slot: u64) -> Result<Option<EncodedConfirmedBlock>, SolanaRpcError>;

    /// Batch-fetch blocks for multiple slots
    pub async fn get_blocks_batch(
        &self, slots: &[u64],
    ) -> Result<Vec<(u64, Option<EncodedConfirmedBlock>)>, SolanaRpcError>;

    // ===== Transaction operations =====

    /// Fetch a single transaction by signature
    pub async fn get_transaction(
        &self, signature: &Signature,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, SolanaRpcError>;

    /// Paginated transaction history for an address (newest first, max 1000 per call)
    pub async fn get_signatures_for_address(
        &self,
        address: &Pubkey,
        before: Option<Signature>,
        limit: usize,
    ) -> Result<Vec<SignatureInfo>, SolanaRpcError>;

    // ===== Account operations =====

    /// Read account data at current (or specified min) slot
    pub async fn get_account_info(
        &self, pubkey: &Pubkey,
    ) -> Result<Option<AccountData>, SolanaRpcError>;

    /// Batch account reads (max 100 per call)
    pub async fn get_multiple_accounts(
        &self, pubkeys: &[Pubkey],
    ) -> Result<Vec<Option<AccountData>>, SolanaRpcError>;

    /// Find all accounts owned by a program, with optional filters
    pub async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        filters: Vec<RpcFilterType>,
    ) -> Result<Vec<(Pubkey, AccountData)>, SolanaRpcError>;
}

pub struct AccountData {
    pub data: Vec<u8>,
    pub owner: Pubkey,
    pub lamports: u64,
    pub executable: bool,
}

pub struct SignatureInfo {
    pub signature: Signature,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub err: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SolanaRpcError {
    #[error("Solana RPC client error: {0}")]
    Client(String),
    #[error("Slot {0} was skipped (no block produced)")]
    SlotSkipped(u64),
    #[error("Account not found: {0}")]
    AccountNotFound(String),
    #[error("Rate limit exceeded")]
    RateLimited,
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}
```

### 7.2 Rate Limiting

Reuse `SlidingWindowRateLimiter` from `src/rpc/alchemy.rs`. Solana RPC providers (Helius, QuickNode, Triton) have their own rate limit models. Define Solana-specific CU costs:

```rust
pub struct SolanaCuCost;
impl SolanaCuCost {
    pub const GET_SLOT: u32 = 1;
    pub const GET_BLOCK: u32 = 50;       // expensive: full block with txs
    pub const GET_TRANSACTION: u32 = 10;
    pub const GET_SIGNATURES: u32 = 20;
    pub const GET_ACCOUNT_INFO: u32 = 5;
    pub const GET_MULTIPLE_ACCOUNTS: u32 = 20;
    pub const GET_PROGRAM_ACCOUNTS: u32 = 100;  // very expensive
}
```

### 7.3 Solana WebSocket Client (`src/solana/ws.rs`)

```rust
pub struct SolanaWsClient {
    url: Url,
    http_client: Arc<SolanaRpcClient>,
}

pub enum SolanaWsEvent {
    NewSlot { slot: u64, parent: u64, root: u64 },
    Disconnected { last_slot: Option<u64> },
    Reconnected { missed_from: u64, missed_to: u64 },
}
```

Subscription message:
```json
{ "jsonrpc": "2.0", "id": 1, "method": "slotSubscribe" }
```

Response notifications:
```json
{ "result": { "slot": 123456, "parent": 123455, "root": 123400 } }
```

Reconnection and gap detection follow the same pattern as the EVM `WsClient` in `src/rpc/websocket.rs`: exponential backoff, HTTP `get_slot()` to detect missed range, emit `Reconnected` event.

---

## 8. Solana Raw Data Collection

### 8.1 Slot Collector (`src/solana/raw_data/slots.rs`)

**Input**: Slot range `[start, end)` from config or catchup resume point.

**Process**:
1. Iterate slots in range
2. Call `get_block(slot)` — returns `None` for skipped slots
3. For non-skipped slots, extract `SolanaSlotRecord`
4. Write to parquet in range files: `data/{chain}/historical/raw/slots/slots_{start}-{end}.parquet`
5. Send `(slot, block_time, Vec<Signature>)` downstream

**Handling skipped slots**: Solana produces ~2 slots/sec but some are skipped (leader failed to produce a block). The collector logs skipped slots at debug level and continues. Parquet files may contain fewer rows than the slot range span. Downstream consumers must not assume `row_count == range_size`.

**Parquet schema**:

| Column | Arrow Type | Nullable |
|--------|-----------|----------|
| `slot` | UInt64 | false |
| `block_time` | Int64 | true |
| `block_height` | UInt64 | true |
| `parent_slot` | UInt64 | false |
| `blockhash` | FixedSizeBinary(32) | false |
| `previous_blockhash` | FixedSizeBinary(32) | false |
| `transaction_count` | UInt32 | false |
| `transaction_signatures` | List(FixedSizeBinary(64)) | false |

### 8.2 Event Extractor (`src/solana/raw_data/events.rs`)

**Input**: Block data from slot collector (or directly from `getBlock` response).

**Process**: For each transaction in the block:
1. Parse `logMessages` array using a stack-based program tracker
2. Track program invocations via `"Program <pubkey> invoke [depth]"` entries
3. Extract events from `"Program data: <base64>"` entries
4. Attribute each event to the currently active program (top of stack)
5. Filter by configured program IDs

**Stack-based log parser**:

```rust
struct ProgramLogParser {
    program_stack: Vec<Pubkey>,
    events: Vec<SolanaEventRecord>,
}

impl ProgramLogParser {
    /// Parse a single log line, maintaining the program invocation stack.
    ///
    /// Log format is stable across Solana runtime versions:
    /// - "Program <base58_pubkey> invoke [<depth>]"  — push to stack
    /// - "Program data: <base64>"                     — event data from current program
    /// - "Program <base58_pubkey> success"             — pop from stack
    /// - "Program <base58_pubkey> failed: <reason>"    — pop from stack
    ///
    /// Note: programs can emit arbitrary log messages via `msg!()`. The parser must
    /// match exact prefixes/suffixes to avoid false positives from adversarial logs.
    fn parse_log_line(&mut self, line: &str, slot: u64, block_time: Option<i64>, tx_sig: &[u8; 64]) {
        // Match "Program <pubkey> invoke [N]" — handles all CPI depths
        if let Some(rest) = line.strip_prefix("Program ") {
            if let Some(pubkey_str) = rest.strip_suffix(|c: char| c == ']')
                .and_then(|s| s.rsplit_once(" invoke ["))
                .map(|(pk, _depth)| pk)
            {
                if let Ok(pubkey) = Pubkey::from_str(pubkey_str) {
                    self.program_stack.push(pubkey);
                }
                return;
            }

            // Match "Program <pubkey> success" — exact suffix
            if let Some(pubkey_str) = rest.strip_suffix(" success") {
                if Pubkey::from_str(pubkey_str).is_ok() {
                    self.program_stack.pop();
                }
                return;
            }

            // Match "Program <pubkey> failed: <reason>" — exact pattern
            if let Some((pubkey_str, _reason)) = rest.split_once(" failed: ") {
                if Pubkey::from_str(pubkey_str).is_ok() {
                    self.program_stack.pop();
                }
                return;
            }
        }

        // Match "Program data: <base64>" — event payload
        if let Some(b64) = line.strip_prefix("Program data: ") {
            if let Ok(data) = base64::decode(b64) {
                if data.len() >= 8 {
                    let discriminator: [u8; 8] = data[..8].try_into().unwrap();
                    let event_data = data[8..].to_vec();
                    let program_id = self.program_stack.last().copied().unwrap_or_default();
                    self.events.push(SolanaEventRecord {
                        slot, block_time,
                        transaction_signature: *tx_sig,
                        program_id: program_id.to_bytes(),
                        event_discriminator: discriminator,
                        event_data,
                        log_index: self.events.len() as u32,
                    });
                }
            }
        }
    }
}
```

**Parquet schema**:

| Column | Arrow Type | Nullable |
|--------|-----------|----------|
| `slot` | UInt64 | false |
| `block_time` | Int64 | true |
| `transaction_signature` | FixedSizeBinary(64) | false |
| `program_id` | FixedSizeBinary(32) | false |
| `event_discriminator` | FixedSizeBinary(8) | false |
| `event_data` | Binary | false |
| `log_index` | UInt32 | false |

### 8.3 Account Reader (`src/solana/live/accounts.rs`) — Live Mode Only

Account reads are **excluded from the historical pipeline**. Most Solana RPC providers do not support `getAccountInfo` at a specific historical slot — only the current (or near-current) state is available. Including account reads in historical backfill would be a promise the infrastructure can't keep.

**Design decision:** For historical data, handlers must reconstruct state from the event stream (e.g., derive pool state from `Traded` and `LiquidityChanged` events). Account reads are available only in live mode, where they read current state triggered by new events.

**Input**: Configured account addresses (or discovered addresses from PDA discovery) + event triggers.

**Process** (live mode only):
1. On event trigger (e.g., after `Traded` event), collect relevant account addresses
2. Call `get_multiple_accounts(pubkeys)` (max 100 per call)
3. Write raw account data to live bincode storage
4. Decode via account state decoder (see 9.3), produce `DecodedAccountState`

**Parquet schema** (for compacted live data):

| Column | Arrow Type | Nullable |
|--------|-----------|----------|
| `slot` | UInt64 | false |
| `block_time` | Int64 | true |
| `account_address` | FixedSizeBinary(32) | false |
| `owner` | FixedSizeBinary(32) | false |
| `data` | Binary | false |
| `lamports` | UInt64 | false |
| `executable` | Boolean | false |

**Reason for live-only:** This simplifies the historical pipeline significantly — no account reader task, no `SolanaAccountsReady` decoder message for historical mode, no impossible-to-fulfill historical `getAccountInfo` calls. The historical pipeline becomes: slot collector → event extractor → event decoder → transformations. Clean and achievable.

### 8.4 Historical Backfill Strategy

**Signature-driven backfill** is orders of magnitude more efficient than slot iteration for Solana:

```
Slot iteration:   ~170K slots/day * 365 days = 62M slots to scan
Signature-driven: Program with 1M transactions = 1K pagination calls
```

**Process**:
1. `getSignaturesForAddress(program_id, { limit: 1000 })` — newest first
2. Paginate backward using `before: last_signature` until reaching `start_slot`
3. Group signatures by slot range (matching `parquet_block_range` config)
4. For each range, fetch full blocks via `getBlock(slot)` for all slots that had transactions
5. Extract events and instructions
6. Write to parquet files organized by slot range

**Catchup resume**: Scan existing parquet files in `data/{chain}/historical/raw/events/` to find the latest processed slot. Resume from there.

---

## 9. Solana Decoding

### 9.1 IDL Parser (`src/solana/decoding/idl.rs`)

Anchor IDL JSON files define the program's interface. The parser loads them at startup and builds event/account matchers.

**IDL structure** (simplified):

```json
{
  "address": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
  "metadata": { "name": "whirlpool", "version": "0.3.0" },
  "instructions": [...],
  "accounts": [
    {
      "name": "Whirlpool",
      "discriminator": [63, 149, 209, 12, 225, 128, 99, 9],
      "type": {
        "kind": "struct",
        "fields": [
          { "name": "whirlpoolsConfig", "type": "pubkey" },
          { "name": "tickSpacing", "type": "u16" },
          { "name": "liquidity", "type": "u128" },
          { "name": "sqrtPrice", "type": "u128" },
          { "name": "tickCurrentIndex", "type": "i32" },
          { "name": "tokenMintA", "type": "pubkey" },
          { "name": "tokenMintB", "type": "pubkey" }
        ]
      }
    }
  ],
  "events": [
    {
      "name": "Traded",
      "discriminator": [225, 207, 42, 174, 148, 75, 25, 78],
      "fields": [
        { "name": "whirlpool", "type": "pubkey" },
        { "name": "aToB", "type": "bool" },
        { "name": "preSqrtPrice", "type": "u128" },
        { "name": "postSqrtPrice", "type": "u128" },
        { "name": "inputAmount", "type": "u64" },
        { "name": "outputAmount", "type": "u64" }
      ]
    }
  ],
  "types": [...]
}
```

**Discriminator computation**: If not provided in the IDL, compute as:
- Events: `sha256("event:<EventName>")[..8]`
- Accounts: `sha256("account:<AccountName>")[..8]`
- Instructions: `sha256("global:<instruction_name>")[..8]`

**IDL type to DecodedValue mapping**:

| IDL Type | Borsh Encoding | DecodedValue Variant |
|----------|---------------|---------------------|
| `bool` | 1 byte (0/1) | `Bool(bool)` |
| `u8` | 1 byte LE | `Uint8(u8)` |
| `u16` | 2 bytes LE | `Uint32(u32)` |
| `u32` | 4 bytes LE | `Uint32(u32)` |
| `u64` | 8 bytes LE | `Uint64(u64)` |
| `u128` | 16 bytes LE | `Uint128(u128)` |
| `i8` | 1 byte LE | `Int8(i8)` |
| `i16` | 2 bytes LE | `Int32(i32)` |
| `i32` | 4 bytes LE | `Int32(i32)` |
| `i64` | 8 bytes LE | `Int64(i64)` |
| `i128` | 16 bytes LE | `Int128(i128)` |
| `pubkey` | 32 bytes raw | `Pubkey([u8; 32])` |
| `string` | 4-byte len + UTF-8 | `String(String)` |
| `bytes` | 4-byte len + raw | `Bytes(Vec<u8>)` |
| `Option<T>` | 1-byte tag + T | `Null` or decoded T |
| `Vec<T>` | 4-byte len + T[] | `Array(Vec<DecodedValue>)` |
| Struct | fields in order | `NamedTuple(Vec<(String, DecodedValue)>)` |
| Enum | 1-byte variant + data | `NamedTuple` with variant name |

### 9.1b Dynamic Borsh Deserializer (`src/solana/decoding/borsh_dynamic.rs`)

The `borsh` crate's derive macros are compile-time — they generate code for known structs. We need **runtime** deserialization driven by IDL type definitions loaded at startup. This is the most complex new module in the Solana support and requires a recursive interpreter that walks the IDL type tree.

**IDL version handling**: Anchor IDL format has changed across versions:
- **Pre-0.30** (legacy): `types` array with `kind: "struct"/"enum"`, field types as strings (`"u64"`, `"publicKey"`)
- **0.30+** (current): `types` array with `kind: "struct"/"enum"`, field types as objects or strings, `accounts` and `events` have top-level `discriminator` arrays

The IDL parser must detect the version (presence of `metadata.spec` field or `discriminator` arrays) and normalize to an internal representation before the deserializer sees it.

**Internal type representation**:

```rust
/// Normalized IDL type, version-independent.
#[derive(Debug, Clone)]
pub enum IdlType {
    Bool,
    U8, U16, U32, U64, U128,
    I8, I16, I32, I64, I128,
    F32, F64,
    Pubkey,
    String,
    Bytes,
    Option(Box<IdlType>),
    Vec(Box<IdlType>),
    Array(Box<IdlType>, usize),       // fixed-size array [T; N]
    Defined(String),                   // reference to a named type in the IDL
}

/// Normalized struct/enum definition from the IDL.
#[derive(Debug, Clone)]
pub enum IdlTypeDef {
    Struct { fields: Vec<(String, IdlType)> },
    Enum { variants: Vec<IdlEnumVariant> },
}

#[derive(Debug, Clone)]
pub struct IdlEnumVariant {
    pub name: String,
    pub fields: Option<Vec<(String, IdlType)>>,  // None for unit variants
}
```

**Recursive deserializer**:

```rust
/// Deserialize a value from a Borsh byte stream according to an IDL type definition.
/// Advances the cursor past the consumed bytes.
pub fn deserialize_value(
    cursor: &mut &[u8],
    idl_type: &IdlType,
    defined_types: &HashMap<String, IdlTypeDef>,
) -> Result<DecodedValue, BorshDecodeError> {
    match idl_type {
        // === Primitives: fixed-width, little-endian ===
        IdlType::Bool => {
            let b = read_u8(cursor)?;
            Ok(DecodedValue::Bool(b != 0))
        }
        IdlType::U8 => Ok(DecodedValue::Uint8(read_u8(cursor)?)),
        IdlType::U16 => Ok(DecodedValue::Uint32(read_le::<u16>(cursor)? as u32)),
        IdlType::U32 => Ok(DecodedValue::Uint32(read_le::<u32>(cursor)?)),
        IdlType::U64 => Ok(DecodedValue::Uint64(read_le::<u64>(cursor)?)),
        IdlType::U128 => Ok(DecodedValue::Uint128(read_le::<u128>(cursor)?)),
        IdlType::I8 => Ok(DecodedValue::Int8(read_i8(cursor)?)),
        IdlType::I16 => Ok(DecodedValue::Int32(read_le::<i16>(cursor)? as i32)),
        IdlType::I32 => Ok(DecodedValue::Int32(read_le::<i32>(cursor)?)),
        IdlType::I64 => Ok(DecodedValue::Int64(read_le::<i64>(cursor)?)),
        IdlType::I128 => Ok(DecodedValue::Int128(read_le::<i128>(cursor)?)),
        IdlType::F32 => Ok(DecodedValue::Float(read_le::<f32>(cursor)? as f64)),
        IdlType::F64 => Ok(DecodedValue::Float(read_le::<f64>(cursor)?)),
        IdlType::Pubkey => {
            let bytes: [u8; 32] = read_bytes(cursor, 32)?.try_into().unwrap();
            Ok(DecodedValue::Pubkey(bytes))
        }

        // === Variable-length: 4-byte LE length prefix ===
        IdlType::String => {
            let len = read_le::<u32>(cursor)? as usize;
            let bytes = read_bytes(cursor, len)?;
            Ok(DecodedValue::String(String::from_utf8_lossy(bytes).into_owned()))
        }
        IdlType::Bytes => {
            let len = read_le::<u32>(cursor)? as usize;
            let bytes = read_bytes(cursor, len)?;
            Ok(DecodedValue::Bytes(bytes.to_vec()))
        }

        // === Compound types: recursive ===
        IdlType::Option(inner) => {
            let tag = read_u8(cursor)?;
            if tag == 0 {
                Ok(DecodedValue::Null)
            } else {
                deserialize_value(cursor, inner, defined_types)
            }
        }
        IdlType::Vec(inner) => {
            let len = read_le::<u32>(cursor)? as usize;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(deserialize_value(cursor, inner, defined_types)?);
            }
            Ok(DecodedValue::Array(items))
        }
        IdlType::Array(inner, size) => {
            let mut items = Vec::with_capacity(*size);
            for _ in 0..*size {
                items.push(deserialize_value(cursor, inner, defined_types)?);
            }
            Ok(DecodedValue::Array(items))
        }
        IdlType::Defined(name) => {
            let type_def = defined_types.get(name)
                .ok_or_else(|| BorshDecodeError::UnknownType(name.clone()))?;
            match type_def {
                IdlTypeDef::Struct { fields } => {
                    let mut decoded = Vec::with_capacity(fields.len());
                    for (field_name, field_type) in fields {
                        let value = deserialize_value(cursor, field_type, defined_types)?;
                        decoded.push((field_name.clone(), value));
                    }
                    Ok(DecodedValue::NamedTuple(decoded))
                }
                IdlTypeDef::Enum { variants } => {
                    let variant_idx = read_u8(cursor)? as usize;
                    let variant = variants.get(variant_idx)
                        .ok_or(BorshDecodeError::InvalidEnumVariant(variant_idx))?;
                    match &variant.fields {
                        None => Ok(DecodedValue::String(variant.name.clone())),
                        Some(fields) => {
                            let mut decoded = vec![
                                ("variant".to_string(), DecodedValue::String(variant.name.clone()))
                            ];
                            for (field_name, field_type) in fields {
                                let value = deserialize_value(cursor, field_type, defined_types)?;
                                decoded.push((field_name.clone(), value));
                            }
                            Ok(DecodedValue::NamedTuple(decoded))
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BorshDecodeError {
    #[error("Unexpected end of data: need {needed} bytes, have {available}")]
    UnexpectedEof { needed: usize, available: usize },
    #[error("Unknown defined type: {0}")]
    UnknownType(String),
    #[error("Invalid enum variant index: {0}")]
    InvalidEnumVariant(usize),
    #[error("Invalid UTF-8 in string field")]
    InvalidUtf8,
}
```

**Testing strategy**: The deserializer must be tested against known IDLs with known account/event data:
1. **Orca Whirlpool IDL**: Deserialize a known `Traded` event and `Whirlpool` account, verify field values match on-chain data
2. **Raydium CLMM IDL**: Different field types, verify cross-program compatibility
3. **Edge cases**: Empty `Vec`, `None` options, nested structs, enum variants with data, maximum-depth recursion
4. **Fuzz testing**: Random bytes against IDL schemas to verify no panics (only `Err` returns)

### 9.2 Anchor Event Decoder (`src/solana/decoding/events.rs`)

**Input**: Raw `SolanaEventRecord` from event extractor.

**Process**:
1. Match by `(program_id, event_discriminator)` against loaded IDL matchers
2. Borsh-deserialize `event_data` using the dynamic deserializer (9.1b) with IDL field definitions
3. Construct `DecodedEvent` with:
   - `contract_address: ChainAddress::Solana(program_id)`
   - `position: LogPosition::Solana { instruction_index, inner_instruction_index }`
   - `transaction_id: TxId::Solana(signature)`
   - `event_signature: event_name` (the Anchor event name, NOT discriminator hex — see section 5.1)
   - `params: HashMap` of field_name → `DecodedValue`
4. Write decoded parquet under `data/{chain}/historical/decoded/events/{program}/{event_name}/`

**Decoded event parquet schema**: Same pattern as EVM decoded logs — fixed columns (`slot`, `block_time`, `transaction_signature`, `program_id`, `log_index`) plus dynamic columns per IDL field.

### 9.3 Account State Decoder (`src/solana/decoding/accounts.rs`) — Live Mode Only

**Input**: Raw `AccountReadResult` from live account reader.

**Process**:
1. Skip first 8 bytes (account discriminator)
2. Borsh-deserialize remaining bytes using the dynamic deserializer (9.1b) with IDL account type definition
3. Construct `DecodedAccountState` (NOT `DecodedCall`) with:
   - `account_address: ChainAddress::Solana(account_pubkey)`
   - `owner_program: ChainAddress::Solana(owner_program_id)`
   - `account_type`: IDL account type name (e.g., "Whirlpool")
   - `fields: HashMap` of field_name → `DecodedValue`

### 9.4 DecoderMessage Extensions

```rust
// src/decoding/types.rs — new variants (feature-gated)

pub enum DecoderMessage {
    // ... existing EVM variants unchanged ...

    /// Solana anchor events ready for Borsh decoding (historical + live)
    #[cfg(feature = "solana")]
    SolanaEventsReady {
        range_start: u64,
        range_end: u64,
        events: Vec<SolanaEventRecord>,
        live_mode: bool,
    },

    /// Solana account data ready for Borsh decoding (live mode only)
    #[cfg(feature = "solana")]
    SolanaAccountsReady {
        slot: u64,
        source_name: String,
        results: Vec<AccountReadResult>,
    },
}
```

Note: `SolanaAccountsReady` has no `range_start`/`range_end` or `live_mode` flag — it is always live-only, always for a single slot.

---

## 10. Pipeline Orchestration

### 10.1 Dispatch in `main.rs`

```rust
// In the chain_tasks loop
for chain_config in &config.chains {
    let config = config.clone();
    chain_tasks.spawn(async move {
        match chain_config.chain_type {
            ChainType::Evm => {
                // existing EVM pipeline (unchanged)
                if live_only { process_chain_live_only(&config, &chain_config, ...).await }
                else if decode_only { decode_only_chain(&config, &chain_config).await }
                else { process_chain(&config, &chain_config, ...).await }
            }
            #[cfg(feature = "solana")]
            ChainType::Solana => {
                solana::pipeline::run(&config, &chain_config, ...).await
            }
        }
    });
}
```

### 10.2 Solana Pipeline Flow

```
Historical Mode (events only — no account reads):
┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────┐
│ Slot Collector│────►│ Event Extractor  │────►│ Event Decoder   │────►│Transformations│──► PostgreSQL
│  (getBlock)   │     │ (parse logs)     │     │ (Borsh + IDL)   │     │    Engine     │
└──────────────┘     └──────────────────┘     └─────────────────┘     └──────────────┘
       │
       └──── Address Discovery (from creation events, see 10.2b)

Live Mode (events + account reads):
                    slotSubscribe (WebSocket)
                              │
                              ▼
                ┌───────────────────────────┐
                │   SolanaLiveCollector     │
                │   (slot notifications)    │
                └───────────────────────────┘
                              │
                              ▼
                ┌───────────────────────────┐
                │ HTTP: getBlock(slot)       │
                │ + event extract + decode   │
                └───────────────────────────┘
                              │
                    ┌─────────┼──────────┐
                    │                    │
                    ▼                    ▼
        ┌───────────────────┐  ┌────��──────────────────┐
        │ Decoded events    │  │ Account reads         │
        │ → handlers        │  │ (triggered by events) │
        └───────────────────┘  │ → decode → handlers   │
                    │          └───────────────────────┘
                    │                    │
                    ▼                    ▼
        ┌─────────────────────────────────────────────┐
        │  LiveStorage (bincode) + Transformations     │
        └─────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────────┐
        │              CompactionService               │
        │         (bincode → parquet at range_size)    │
        └─────────────────────────────────────────────┘
```

Note: Account reads are live-mode-only (see section 8.3). Historical handlers reconstruct state from events.

### 10.2b Address Discovery (`src/solana/discovery.rs`)

The Solana equivalent of EVM factory events. On EVM, factory contracts emit events containing newly deployed contract addresses. On Solana, accounts are discovered via:

**Event-driven discovery** (preferred): Parse creation events to discover account addresses. Configured per-program:

```rust
/// Config for discovering new accounts from program events.
/// Analogous to EVM FactoryConfig.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolanaDiscoveryConfig {
    /// Event that signals a new account was created (e.g., "PoolInitialized")
    pub event_name: String,
    /// Field in the event containing the new account address (e.g., "whirlpool")
    pub address_field: String,
    /// Account type from the IDL to read for this discovered address
    pub account_type: Option<String>,
}
```

Example config:
```json
{
  "orca_whirlpool": {
    "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "discovery": [
      {
        "event_name": "PoolInitialized",
        "address_field": "whirlpool",
        "account_type": "Whirlpool"
      }
    ]
  }
}
```

When a `PoolInitialized` event is decoded, the discovery module extracts the `whirlpool` pubkey field and registers it as a known account. In live mode, this address is then eligible for event-triggered account reads.

**`getProgramAccounts` bootstrap** (fallback): One-time scan with discriminator filter to discover all existing accounts of a type. Expensive but necessary for historical data when you need the full set of accounts that existed before your `start_slot`:

```rust
/// Bootstrap: discover all existing accounts of a type via getProgramAccounts.
/// Run once at startup, then switch to event-driven discovery.
pub async fn bootstrap_accounts(
    rpc: &SolanaRpcClient,
    program_id: &Pubkey,
    discriminator: &[u8; 8],
) -> Result<Vec<Pubkey>, SolanaRpcError> {
    let filter = RpcFilterType::Memcmp(Memcmp::new_raw_bytes(0, discriminator.to_vec()));
    let accounts = rpc.get_program_accounts(program_id, vec![filter]).await?;
    Ok(accounts.into_iter().map(|(pk, _)| pk).collect())
}
```

**Reason:** Without address discovery, account reads would require manually listing every account address in config — impractical for protocols with thousands of pools. Event-driven discovery mirrors how EVM factory collection works and scales naturally.

### 10.3 Storage Path Layout

```
data/{chain}/
├── historical/
│   ├── decoded/events/{program}/{event_name}/
│   └── raw/
│       ├── slots/
│       └── events/
└── live/
    ├── raw/slots/{slot}.bin
    ├── raw/events/{slot}.bin
    ├── raw/accounts/{slot}.bin          # live-only
    ├── decoded/events/{slot}/{program}/{event}.bin
    ├── decoded/accounts/{slot}/{program}/{type}.bin  # live-only
    └── status/{slot}.json
```

Note: `raw/accounts/` and `decoded/accounts/` exist only under `live/`. There is no `historical/raw/accounts/` or `historical/decoded/accounts/` — see section 8.3 for rationale.

### 10.4 Solana Live Mode Specifics

**Reorg handling**: All commitment levels are supported with `SolanaReorgDetector` (see §14.4). Parent-slot chain verification runs at every level; depth varies: 150 for `processed`, 32 for `confirmed`, 0 for `finalized`. On reorg detection, orphaned slots are rolled back using the same mechanism as EVM (delete live storage, rollback handler DB rows via `reorg_tables()`).

**Skipped slots in live mode**: When `slotSubscribe` notifies of a new slot, the collector calls `getBlock(slot)`. If the slot was skipped, the collector marks it complete in the status file and moves on. No gap backfill needed for skipped slots — they simply don't have data.

**Gap detection on reconnect**: On WebSocket reconnection, compare `last_processed_slot` with `get_slot()` to find missed range. Backfill by iterating the missed slots.

---

## 11. Solana DeFi: Target Programs

Initial target programs for handler development:

### 11.1 Orca Whirlpool

**Program ID**: `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc`

Key events (directly analogous to Uniswap V3):
- `Traded` — swap event (maps to V3 `Swap`)
- `PoolInitialized` — pool creation (maps to V3 `PoolCreated`)
- `LiquidityIncreased` / `LiquidityDecreased` — liquidity changes (maps to V3 `Mint`/`Burn`)
- `PositionOpened` / `PositionClosed` — position lifecycle

Key account types:
- `Whirlpool` — pool state (tick, sqrt_price, liquidity, token mints)
- `Position` — user position (tick range, liquidity)
- `TickArray` — tick data

### 11.2 Raydium CLMM

**Program ID**: `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK`

Key events:
- `SwapEvent` — swap with pool_state, amounts, sqrt_price, tick
- `LiquidityChangeEvent` — liquidity add/remove

Key account types:
- `PoolState` — pool parameters (tick, sqrt_price_x64, liquidity, token mints)

---

## 11b. Cross-Chain Database Schema

The existing database tables are **already cross-chain-ready** and Solana handlers should write to the same canonical tables rather than creating parallel ones. This enables queries like `SELECT * FROM tokens WHERE symbol = 'USDC'` to return results from both EVM and Solana chains.

**Why this works out of the box:**

| Property | Current schema | Solana compatibility |
|----------|---------------|---------------------|
| `chain_id BIGINT` | Every table has it | Solana mainnet = -1, devnet = -2 (negative to avoid EVM collision, see §14.7) |
| `address BYTEA` | Variable-length, not `BYTEA(20)` | 32-byte Solana pubkeys fit natively |
| `tx_hash BYTEA` | Variable-length, not `BYTEA(32)` | 64-byte Solana signatures fit natively |
| `log_index INT` | Used in uniqueness constraints | Store `LogPosition::ordinal()` for Solana |
| `source VARCHAR` | Handler provenance | `"orca_whirlpool"`, `"raydium_clmm"`, etc. |

**Shared tables** (Solana handlers write to these):
- `tokens` — chain_id, address, name, symbol, decimals. Protocol-specific columns (`is_derc20`, `creator_coin_pool`) are nullable and NULL for Solana rows.
- `pools` — chain_id, address, base_token, quote_token, fee. Protocol-specific columns (`integrator`, `migration_pool`, `graduation_tick`) are NULL for Solana rows.
- `swaps` — chain_id, tx_hash, pool, asset, amountIn, amountOut, is_buy. Protocol-specific columns (`graduation_balance_delta`, etc.) are NULL for Solana rows.
- `pool_state` — chain_id, pool_id, tick, sqrt_price, price, active_liquidity. Already generic.
- `pool_snapshots` — chain_id, pool_id, OHLC prices, volume, swap_count. Already generic.
- `prices`, `users`, `portfolios`, `transfers` — all chain_id-qualified, all BYTEA addresses.

**Protocol-specific tables** (new, per-protocol):
- `orca_whirlpool_configs` — Orca-specific pool metadata (fee_rate, protocol_fee_rate, reward configs)
- `raydium_pool_configs` — Raydium-specific metadata

These JOIN to canonical `pools` via `(chain_id, address)`.

**Example: Solana token handler writing to shared `tokens` table:**
```rust
// Orca handler produces a token insert identical in shape to EVM handlers
DbOperation::Upsert {
    table: "tokens".to_string(),
    values: vec![
        ("chain_id".into(), DbValue::BigInt(-1)),  // Solana mainnet
        ("address".into(), DbValue::Pubkey(mint_pubkey)),  // 32 bytes
        ("name".into(), DbValue::Text(name)),
        ("symbol".into(), DbValue::Text(symbol)),
        ("decimals".into(), DbValue::SmallInt(decimals)),
        // Protocol-specific columns omitted (NULL by default)
    ],
    conflict_columns: vec!["chain_id", "address", "source", "source_version"],
}
```

---

## 12. Dependencies

```toml
[features]
default = []
solana = [
    "dep:solana-client",
    "dep:solana-sdk",
    "dep:solana-transaction-status",
    "dep:borsh",
    "dep:bs58",
]

[dependencies]
# Solana (optional, feature-gated)
solana-client = { version = "2.2", optional = true }
solana-sdk = { version = "2.2", optional = true }
solana-transaction-status = { version = "2.2", optional = true }
borsh = { version = "1.5", optional = true }
bs58 = { version = "0.5", optional = true }
# Note: anchor-lang is NOT a dependency. IDL parsing is done from JSON directly.
# This keeps the dependency tree lean and avoids pulling in the entire Anchor framework.
```

**Compile-time isolation**: All Solana code is behind `#[cfg(feature = "solana")]`. Building without `--features solana` produces a binary identical in behavior to today's EVM-only indexer with zero additional dependencies.

---

## 13. Performance Considerations

- **ChainAddress is Copy**: 33 bytes on stack (1 discriminant + 32 data). Slightly larger than `[u8; 20]` but avoids all heap allocation. The compiler likely pads to 40 bytes for alignment.
- **TxId is Clone (not Copy)**: 65 bytes on stack. Avoids heap allocation but requires explicit `.clone()` to make copies visible in hot paths. See section 3.2 for rationale.
- **Branch prediction**: EVM handlers always hit the `Evm` variant in pattern matches. Modern CPUs handle this with near-zero overhead since the branch is perfectly predictable.
- **Solana RPC latency**: Solana `getBlock` returns full blocks with all transactions — no separate receipt/log fetch needed. This simplifies the pipeline (fewer RPC calls per slot) but each call returns more data.
- **Historical backfill**: Signature-driven backfill avoids scanning millions of empty slots. For a program with 1M transactions, pagination requires only ~1K RPC calls vs. scanning ~60M slots.
- **`solana-sdk` compile time**: The `solana-sdk` crate pulls hundreds of transitive dependencies. Expect a significant compile time increase when building with `--features solana`. This is a known issue in the Solana ecosystem. Feature-gating ensures EVM-only builds are unaffected.

---

## 14. Design Decisions (Resolved)

### 14.1 Error type unification

**Decision:** Wrap in existing types.

```rust
pub enum TransformationError {
    // ... existing variants ...
    #[cfg(feature = "solana")]
    SolanaRpc(SolanaRpcError),
    #[cfg(feature = "solana")]
    BorshDecode(BorshDecodeError),
}
```

The pipeline already dispatches by chain type, so error handling is chain-specific. No unified `ChainError` needed.

### 14.2 Non-Anchor programs — Decoder trait abstraction

**Decision:** Anchor-only is NOT sufficient. The decoder layer must support multiple program frameworks through a `ProgramDecoder` trait.

Programs that don't use Anchor are common and important:
- **SPL Token / Token-2022**: No Anchor IDL, well-known instruction layouts, essential for any DeFi indexer
- **Shank programs** (e.g., Metaplex): Shank IDL format differs from Anchor
- **Raw programs**: Custom binary encoding, hand-written decoders needed

**Trait abstraction:**

```rust
/// Trait for decoding a Solana program's events, instructions, and account state.
/// Implemented by AnchorDecoder (IDL-driven), and hand-written decoders for
/// well-known programs like SPL Token.
pub trait ProgramDecoder: Send + Sync {
    /// Program ID this decoder handles
    fn program_id(&self) -> [u8; 32];

    /// Human-readable name for logging
    fn program_name(&self) -> &str;

    /// Attempt to decode an event from raw "Program data:" log entry.
    /// Returns None if the discriminator doesn't match any known event.
    fn decode_event(
        &self,
        discriminator: &[u8],
        data: &[u8],
    ) -> Result<Option<DecodedEventFields>, BorshDecodeError>;

    /// Attempt to decode instruction data + account keys.
    /// Returns None if the discriminator doesn't match any known instruction.
    fn decode_instruction(
        &self,
        data: &[u8],
        accounts: &[[u8; 32]],
    ) -> Result<Option<DecodedInstructionFields>, BorshDecodeError>;

    /// Attempt to decode account state from raw account data.
    /// Returns None if the discriminator doesn't match any known account type.
    fn decode_account(
        &self,
        data: &[u8],
    ) -> Result<Option<DecodedAccountFields>, BorshDecodeError>;

    /// Event types this decoder can produce (for trigger registration).
    fn event_types(&self) -> Vec<String>;

    /// Instruction types this decoder can produce.
    fn instruction_types(&self) -> Vec<String>;

    /// Account types this decoder can produce.
    fn account_types(&self) -> Vec<String>;
}

pub struct DecodedEventFields {
    pub event_name: String,
    pub params: HashMap<String, DecodedValue>,
}

pub struct DecodedInstructionFields {
    pub instruction_name: String,
    pub args: HashMap<String, DecodedValue>,
    /// Named account references from the IDL (e.g., "pool", "tokenAccountA")
    pub named_accounts: HashMap<String, [u8; 32]>,
}

pub struct DecodedAccountFields {
    pub account_type: String,
    pub fields: HashMap<String, DecodedValue>,
}
```

**Implementations:**

| Decoder | Source | Encoding | How it's loaded |
|---------|--------|----------|-----------------|
| `AnchorDecoder` | Anchor IDL JSON | Borsh | Config: `idl_path: "idl/whirlpool.json"` |
| `ShankDecoder` | Shank IDL JSON | Borsh | Config: `idl_path: "idl/metaplex.json"`, `idl_format: "shank"` |
| `SplTokenDecoder` | Built-in | Custom binary | Config: `decoder: "spl_token"` (no IDL path needed) |
| `CustomDecoder` | User-provided Rust code | Any | Registered in handler code at compile time |

The `AnchorDecoder` wraps the IDL parser (§9.1) and dynamic Borsh deserializer (§9.1b). The `SplTokenDecoder` is hand-written with knowledge of SPL Token instruction layouts. The trait ensures all decoders produce the same output types.

**Config extension:**

```rust
pub struct SolanaProgramConfig {
    pub program_id: String,
    pub idl_path: Option<String>,
    pub idl_format: Option<String>,         // "anchor" (default) | "shank"
    pub decoder: Option<String>,            // built-in decoder name, e.g. "spl_token"
    // ... existing fields ...
}
```

If `decoder` is set, it takes precedence over `idl_path`. If neither is set, events/instructions for that program are stored raw but not decoded.

### 14.3 Instruction data decoding

**Decision:** Design the pipeline to support instruction decoding from the start.

Instruction data is captured alongside events in the raw data extraction phase. Both flow through the same `ProgramDecoder` trait and produce decoded data for handlers.

**Raw data: add `SolanaInstructionRecord`**

```rust
/// Raw instruction data extracted from a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaInstructionRecord {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub transaction_signature: [u8; 64],
    pub program_id: [u8; 32],
    pub data: Vec<u8>,                         // raw instruction data (includes discriminator)
    pub accounts: Vec<[u8; 32]>,               // account keys referenced by this instruction
    pub instruction_index: u16,
    pub inner_instruction_index: Option<u16>,  // None for top-level, Some for CPI
}
```

**Parquet schema** (stored alongside events in `raw/instructions/`):

| Column | Arrow Type | Nullable |
|--------|-----------|----------|
| `slot` | UInt64 | false |
| `block_time` | Int64 | true |
| `transaction_signature` | FixedSizeBinary(64) | false |
| `program_id` | FixedSizeBinary(32) | false |
| `data` | Binary | false |
| `accounts` | List(FixedSizeBinary(32)) | false |
| `instruction_index` | UInt16 | false |
| `inner_instruction_index` | UInt16 | true |

**Decoder output:** Decoded instructions produce `DecodedEvent` with:
- `event_name` = instruction name (e.g., `"swap"`, `"transfer"`)
- `event_signature` = instruction name (same — triggers match on this)
- `params` = instruction arguments + named accounts merged into one HashMap
- `position` = `LogPosition::Solana { instruction_index, inner_instruction_index }`

The `named_accounts` from `DecodedInstructionFields` are flattened into params as `DecodedValue::Pubkey` entries. For example, a swap instruction with accounts `[pool, tokenAccountA, tokenAccountB]` and IDL-named accounts would produce:

```rust
params: {
    "pool": Pubkey([...]),
    "tokenAccountA": Pubkey([...]),
    "tokenAccountB": Pubkey([...]),
    "amountIn": Uint64(1000000),
    "amountOut": Uint64(950000),
}
```

Handlers don't need to know whether data came from an event log or instruction — they subscribe by name, get params. The decoder is what makes the distinction.

**Extraction in `ProgramLogParser`:** The event extractor (§8.2) already iterates transactions. Instruction extraction runs in the same pass — for each transaction, iterate `transaction.message.instructions` and `meta.inner_instructions`, produce `SolanaInstructionRecord` for programs matching config. Both events and instructions flow through the decoder, both produce `DecodedEvent`, both reach handlers via the same trigger dispatch.

### 14.4 Reorg detection — support all commitment levels

**Decision:** Support `processed`, `confirmed`, and `finalized`. Add reorg detection for all levels.

Solana's confirmation model:
- `processed` — optimistically confirmed by the leader, CAN reorg
- `confirmed` — voted on by supermajority, has never reorged but theoretically could
- `finalized` — rooted, cannot reorg

Rather than rejecting `processed`, implement lightweight reorg detection using `parent_slot` chain verification — the same pattern as EVM's `parent_hash` checking. The Solana `getBlock` response includes `parentSlot` and `previousBlockhash`, which is sufficient.

```rust
/// Solana reorg detector using parent-slot chain.
/// Same principle as EVM ReorgDetector but using slot numbers instead of block hashes.
pub struct SolanaReorgDetector {
    /// Recent slots: slot_number -> (parent_slot, blockhash)
    recent_slots: BTreeMap<u64, (u64, [u8; 32])>,
    max_depth: usize,
}

impl SolanaReorgDetector {
    /// Check if a new slot is consistent with the chain we've seen.
    /// Returns Err with the fork point if a reorg is detected.
    pub fn check_slot(
        &mut self,
        slot: u64,
        parent_slot: u64,
        blockhash: [u8; 32],
    ) -> Result<(), ReorgDetected> {
        if let Some((expected_parent, _)) = self.recent_slots.get(&slot) {
            if *expected_parent != parent_slot {
                // Same slot number, different parent — reorg detected
                return Err(ReorgDetected { fork_slot: slot });
            }
        }
        // Verify parent chain continuity
        if let Some(parent) = self.recent_slots.get(&parent_slot) {
            // Parent exists in our history — chain is consistent
        } else if !self.recent_slots.is_empty() {
            // Gap or unknown parent — potential reorg or missed slots
            // For skipped slots, parent_slot may not be slot-1
            // Only flag as reorg if parent_slot < our oldest tracked slot
        }
        self.recent_slots.insert(slot, (parent_slot, blockhash));
        self.trim();
        Ok(())
    }
}
```

The `max_depth` varies by commitment level:
- `finalized`: 0 (no reorg possible, skip detection entirely)
- `confirmed`: 32 (defensive — keep short history, warn on mismatch)
- `processed`: 150 (Solana's slot confirmation window)

On reorg detection, the same rollback mechanism as EVM applies: delete live storage for orphaned slots, rollback handler DB rows via `reorg_tables()`.

### 14.5 Live mode bincode types

**Decision:** Parallel `SolanaLiveStorage` with Solana-specific types. Share atomic write/status helpers via functions, not generics.

The types below are best-effort based on the EVM live mode patterns (`LiveBlock` → `LiveSlot`, `LiveReceipt`+`LiveLog` → `LiveTransaction`, etc.). These should be adjusted during implementation by someone with hands-on Solana experience.

```rust
/// Mirrors LiveBlock — slot header data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSlot {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub parent_slot: u64,
    pub blockhash: [u8; 32],
    pub previous_blockhash: [u8; 32],
    pub transaction_count: u32,
}

/// Mirrors LiveReceipt — Solana has no separate receipt concept,
/// so this combines transaction metadata with its log output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTransaction {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: [u8; 64],
    pub is_err: bool,
    pub err_msg: Option<String>,
    pub fee: u64,
    pub compute_units_consumed: Option<u64>,
    pub log_messages: Vec<String>,
    /// All account keys in the transaction (static + writable + readonly)
    pub account_keys: Vec<[u8; 32]>,
}

/// Mirrors LiveLog — extracted from logMessages via ProgramLogParser.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSolanaEvent {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: [u8; 64],
    pub program_id: [u8; 32],
    pub discriminator: [u8; 8],
    pub data: Vec<u8>,
    pub log_index: u32,
}

/// Raw instruction data — for instruction decoding pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveInstruction {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: [u8; 64],
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
    pub accounts: Vec<[u8; 32]>,
    pub instruction_index: u16,
    pub inner_instruction_index: Option<u16>,
}

/// Mirrors LiveEthCall — account reads triggered by events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveAccountRead {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub account_address: [u8; 32],
    pub owner: [u8; 32],
    pub data: Vec<u8>,
    pub lamports: u64,
    pub executable: bool,
}

/// Mirrors LiveBlockStatus — pipeline progress for a single slot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveSlotStatus {
    pub collected: bool,                    // slotSubscribe notification received
    pub block_fetched: bool,                // getBlock completed
    pub events_extracted: bool,             // logMessages parsed into events
    pub instructions_extracted: bool,       // instruction data extracted
    pub events_decoded: bool,               // events run through ProgramDecoder
    pub instructions_decoded: bool,         // instructions run through ProgramDecoder
    pub accounts_read: bool,                // event-triggered account reads completed
    pub accounts_decoded: bool,             // account data run through ProgramDecoder
    pub transformed: bool,                  // all transformation handlers complete
    #[serde(default)]
    pub completed_handlers: HashSet<String>,
    #[serde(default)]
    pub failed_handlers: HashSet<String>,
}

/// Mirrors LiveDecodedLog — decoded event ready for transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDecodedSolanaEvent {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: [u8; 64],
    pub program_id: [u8; 32],
    pub log_index: u32,
    pub event_name: String,
    pub decoded_values: Vec<DecodedValue>,
}

/// Decoded instruction ready for transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDecodedInstruction {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: [u8; 64],
    pub program_id: [u8; 32],
    pub instruction_index: u16,
    pub inner_instruction_index: Option<u16>,
    pub instruction_name: String,
    pub decoded_values: Vec<DecodedValue>,
    pub named_accounts: Vec<(String, [u8; 32])>,
}
```

**Storage layout:**
```
data/{chain}/live/
├── raw/slots/{slot}.bin            # LiveSlot (bincode)
├── raw/transactions/{slot}.bin     # Vec<LiveTransaction> (bincode)
├── raw/events/{slot}.bin           # Vec<LiveSolanaEvent> (bincode)
├── raw/instructions/{slot}.bin     # Vec<LiveInstruction> (bincode)
├── raw/accounts/{slot}.bin         # Vec<LiveAccountRead> (bincode)
├── decoded/events/{slot}/{program}/{event}.bin
├── decoded/instructions/{slot}/{program}/{instruction}.bin
├── decoded/accounts/{slot}/{program}/{type}.bin
├── snapshots/{slot}.bin            # DB state for reorg rollback
└── status/{slot}.json              # LiveSlotStatus (JSON, debuggable)
```

**Shared infrastructure:** `SolanaLiveStorage` reuses the same patterns from EVM `LiveStorage`:
- Atomic write: temp file + rename (via shared `atomic_write_with()` function)
- Status locking: exclusive `.lock` file for read-modify-write
- Bincode for data, JSON for status
- `durable_writes` flag for `sync_all()` on status writes

### 14.6 Database table architecture

**Decision:** Canonical core tables + protocol extension tables. The core infrastructure provides the canonical tables. Handler implementers design their own extension tables.

Canonical tables (`tokens`, `pools`, `swaps`, `pool_state`, `pool_snapshots`, `prices`, `users`, `portfolios`, `transfers`) are shared across chains — Solana handlers write to them with `chain_id` and 32-byte BYTEA addresses. See §11b.

Protocol-specific extensions (e.g., `orca_whirlpool_configs`, `raydium_pool_configs`) are owned by handlers and JOIN to canonical tables via `(chain_id, address)`. The design of these tables is left to handler implementers — the infrastructure provides the canonical schema and the `DbOperation` machinery.

### 14.7 Solana `chain_id` convention

**Decision:** Use negative IDs to guarantee no collision with EVM chain IDs.

| Network | `chain_id` |
|---------|-----------|
| Solana Mainnet | `-1` |
| Solana Devnet | `-2` |
| Solana Testnet | `-3` |
| Solana Localnet | `-4` |

These are defaults in the config. Users can override `chain_id` to any value they choose, but the defaults use negative numbers since all EVM chain IDs are positive. This avoids any possibility of collision when querying across chains.

```json
{
  "name": "solana-mainnet",
  "chain_type": "solana",
  "chain_id": -1,
  ...
}
```

The `chain_id` column in PostgreSQL is `BIGINT` (signed), so negative values work natively.

### 14.8 `getProgramAccounts` provider compatibility

`getProgramAccounts` is expensive and some providers disable it entirely. The address discovery bootstrap (§10.2b) depends on it.

**Required RPC methods** (all providers must support):
- `getSlot`, `getBlock`, `getTransaction`, `getSignaturesForAddress`
- `getAccountInfo`, `getMultipleAccounts`

**Optional RPC methods** (graceful degradation):
- `getProgramAccounts` — used for bootstrap discovery. If unavailable:
  - Config flag: `bootstrap_discovery: false` to skip
  - Fall back to event-driven discovery only (starts from `start_slot`, misses pre-existing accounts)
  - Cache bootstrap results to disk to avoid re-scanning on restart

```rust
pub struct SolanaProgramConfig {
    // ... existing fields ...
    /// Whether to run getProgramAccounts bootstrap on startup.
    /// Set to false if your RPC provider doesn't support it.
    /// Default: true
    pub bootstrap_discovery: Option<bool>,
}
```

## 15. Open Design Questions

The following questions remain unresolved and will be decided during implementation:

### 15.1 Shank IDL parser

The `ProgramDecoder` trait (§14.2) supports Shank via `idl_format: "shank"`, but the actual Shank IDL parsing logic is unspecified. Shank IDLs have a different JSON structure than Anchor. This can be implemented when a Shank program is first targeted — the trait abstraction ensures it plugs in cleanly.

### 15.2 Instruction decoding trigger semantics

When both an event AND an instruction fire for the same state change (common in Anchor programs), should handlers receive both? Options:
- **a)** Deduplicate: prefer event, fall back to instruction if no event emitted
- **b)** Deliver both: handlers subscribe to event OR instruction triggers independently
- **c)** Merge: combine event params + instruction accounts into a single enriched event

Leaning toward (b) — handlers explicitly choose their data source. But this needs validation against real protocols.

### 15.3 `LiveTransaction` scope

The `LiveTransaction` type (§14.5) stores all `log_messages` and `account_keys` for a transaction. For blocks with hundreds of transactions where only a few are relevant, this is wasteful. Should we filter transactions by program_id before storage, or store everything and filter at decode time? Filtering saves storage but requires knowing program IDs at collection time (before decoding).
