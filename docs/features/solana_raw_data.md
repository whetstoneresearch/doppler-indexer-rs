# Solana Raw Data Extraction

## Scope

Event and instruction extraction from Solana blocks. All code is behind `#[cfg(feature = "solana")]`.

- Event extraction from transaction `logMessages` via `ProgramLogParser`
- Instruction extraction from the transaction instruction tree (top-level and CPI)
- Combined single-pass extraction from a full `UiConfirmedBlock`
- Program-ID-based filtering against a configured set

## Non-scope

- Slot collection and historical backfill (see `slots.rs`, `catchup.rs`)
- Parquet serialization (see `parquet.rs`)
- Address discovery (see `discovery.rs`)
- WebSocket subscriptions for live blocks

## Data Flow

```
UiConfirmedBlock (from RPC)
        |
        v
extract_events_and_instructions()          [extraction.rs]
        |
        +---> for each transaction:
        |        |
        |        +---> meta.err check (skip failed)
        |        |
        |        +---> extract_events_from_logs()    [events.rs]
        |        |        |
        |        |        +---> ProgramLogParser
        |        |                 parse "Program X invoke [N]"
        |        |                 parse "Program data: <base64>"
        |        |                 parse "Program X success/failed"
        |        |                 -> Vec<SolanaEventRecord>
        |        |
        |        +---> extract_instructions_from_transaction()  [instructions.rs]
        |                 |
        |                 +---> EncodedTransaction::decode() -> VersionedTransaction
        |                 +---> static_account_keys + loaded_addresses (ALT)
        |                 +---> top-level instructions from message
        |                 +---> inner instructions from meta.inner_instructions
        |                 -> Vec<SolanaInstructionRecord>
        |
        v
(Vec<SolanaEventRecord>, Vec<SolanaInstructionRecord>)
```

## Files and Key Exports

| File | Role | Key Exports |
|------|------|-------------|
| `src/solana/raw_data/events.rs` | Log-based event extraction | `ProgramLogParser`, `extract_events_from_logs()` |
| `src/solana/raw_data/instructions.rs` | Instruction tree extraction | `extract_instructions_from_transaction()` |
| `src/solana/raw_data/extraction.rs` | Combined block extraction | `extract_events_and_instructions()` |
| `src/solana/raw_data/types.rs` | Record types | `SolanaEventRecord`, `SolanaInstructionRecord`, `SolanaSlotRecord` |

## Invariants and Constraints

- Failed transactions (meta.err is Some) are always skipped
- Events require at least 8 bytes of decoded data (discriminator); shorter data is silently dropped
- Event attribution goes to the program at the top of the invocation stack when "Program data:" is emitted
- Instruction data from binary-encoded transactions is raw bytes; from JSON-encoded transactions it is bs58-decoded
- For v0 transactions, account keys include loaded addresses (ALT) from `meta.loaded_addresses` appended after static keys
- `ProgramLogParser.is_truncated()` detects when the Solana 10,000-line log limit caused truncation (program stack non-empty after parsing)
- Trust assumption: programs can emit arbitrary `msg!()` lines including fake "Program data:" lines. The program-ID filter is the only defense.
- The `configured_programs` filter is applied post-extraction as a `HashSet<[u8; 32]>` lookup
