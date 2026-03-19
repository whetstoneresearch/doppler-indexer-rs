# Factory Collection

The factory collection module tracks contracts that are dynamically created by other contracts. This is useful for indexing token deployments, liquidity pool creations, and other patterns where contract addresses aren't known in advance.

## Overview

Factory collection works by:
1. Monitoring logs from configured factory contracts
2. Matching logs against specified event signatures
3. Extracting the created contract address from event parameters
4. Writing discovered addresses to parquet files
5. Notifying the logs and eth_calls collectors about new addresses

## Configuration

Factories are configured within the `factories` array of a contract:

```json
{
    "Airlock": {
        "address": "0x660eAaEdEBc968f8f3694354FA8EC0b4c5Ba8D12",
        "factories": [
            {
                "collection": "DERC20",
                "factory_events": {
                    "name": "Create",
                    "topics_signature": "address",
                    "data_signature": "address,address,address",
                    "factory_parameters": "data[0]"
                },
                "calls": [
                    {"function": "totalSupply()", "output_type": "uint256"}
                ]
            }
        ]
    }
}
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `collection` | string | Yes | Identifier for this group of factory-created contracts (alias: `collection_name` for backward compatibility) |
| `factory_events` | object or array | Yes | Event signature information for matching (can be single object or array of objects) |
| `calls` | array | No | eth_call configs to execute on factory-created contracts (merged with collection type if defined) |
| `events` | array | No | Event configs to decode from factory-created contracts (merged with collection type if defined) |

## Factory Collection Types (Shared Configuration)

When multiple contracts create the same type of factory contracts (e.g., multiple initializers creating DERC20 tokens), you can define shared configuration at the chain level to avoid duplication.

### Chain-Level Configuration

Add a `factory_collections` field to your chain config, either inline or as a path to a JSON file:

```json
{
    "name": "base",
    "chain_id": 8453,
    "factory_collections": "factory_collections/base.json",
    "contracts": "contracts/"
}
```

### Factory Collections File

Define shared calls and events for each collection type:

```json
{
    "DERC20": {
        "calls": [
            {"function": "name()", "output_type": "string", "frequency": "once"},
            {"function": "symbol()", "output_type": "string", "frequency": "once"},
            {"function": "totalSupply()", "output_type": "uint256"}
        ],
        "events": [
            { "signature": "Transfer(address indexed from, address indexed to, uint256 value)" }
        ]
    },
    "UniswapV2Pairs": {
        "calls": [
            {"function": "getReserves()", "output_type": "(uint112,uint112,uint32)"}
        ],
        "events": [
            { "signature": "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)" }
        ]
    }
}
```

### Contract References Collection

Contracts then reference the collection by name, providing only the factory-specific detection config:

```json
{
    "Airlock": {
        "address": "0x660eAaEdEBc968f8f3694354FA8EC0b4c5Ba8D12",
        "factories": [{
            "collection": "DERC20",
            "factory_events": {
                "name": "Create",
                "topics_signature": "address",
                "data_signature": "address,address,address",
                "factory_parameters": "data[0]"
            }
        }]
    },
    "AnotherInitializer": {
        "address": "0x...",
        "factories": [{
            "collection": "DERC20",
            "factory_events": {
                "name": "TokenDeployed",
                "topics_signature": "address",
                "factory_parameters": "topics[1]"
            }
        }]
    }
}
```

Both contracts feed into the same `DERC20` collection, sharing the calls and events config.

### Merge Behavior

When a factory config references a collection type:

1. **Calls are merged**: Collection type calls + inline calls (extended, not replaced). Deduplication by function signature happens when `get_factory_call_configs` merges across multiple contracts contributing to the same collection.
2. **Events are merged**: Collection type events + inline events (extended, not replaced)
3. **Inline overrides**: If you specify calls/events inline, they extend (not replace) the collection type config

```json
{
    "collection": "DERC20",
    "factory_events": {...},
    "calls": [
        {"function": "customCall()", "output_type": "uint256"}
    ]
}
```

This would include all DERC20 collection calls PLUS the custom `customCall()`.

### Benefits

- **No duplication**: Define events/calls once, use across multiple contracts
- **Correct merging**: Multiple contracts contributing to the same collection have their discovered addresses merged correctly
- **Easy maintenance**: Update shared config in one place

### Factory Events

The `factory_events` field can be a single object or an array of objects, allowing a single factory config to match multiple event types. Each object defines how to match a factory creation event:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Event name (e.g., "Create", "PairCreated", "PoolCreated") |
| `topics_signature` | string | Comma-separated types of indexed parameters (after topic0) |
| `data_signature` | string | Comma-separated types of non-indexed parameters in event data (optional) |
| `factory_parameters` | string | Which parameter contains the created contract address |

**Multiple Events Example:**

```json
{
    "factory_events": [
        {
            "name": "Create",
            "topics_signature": "address,address,address",
            "factory_parameters": "topics[1]"
        },
        {
            "name": "TokenDeployed",
            "topics_signature": "address",
            "data_signature": "uint256",
            "factory_parameters": "topics[1]"
        }
    ]
}
```

**Event Signature Computation:**

The event topic0 is computed as `keccak256("{name}({topics_signature},{data_signature}")`:

```
Event: Create(address indexed token, address pool, address hook, address asset)
topics_signature: "address"
data_signature: "address,address,address"
topic0: keccak256("Create(address,address,address,address)")
```

### Factory Parameters

The `factory_parameters` field (inside `factory_events`) specifies where to extract the created contract address:

| Format | Description |
|--------|-------------|
| `topics[0]` | Event signature (topic0) - rarely useful |
| `topics[1]` | First indexed parameter |
| `topics[2]` | Second indexed parameter |
| `data[0]` | First non-indexed parameter |
| `data[1]` | Second non-indexed parameter |
| `data[0][1]` | Nested tuple access - second element of first tuple |
| etc. | Continue pattern as needed |

**Example:** For a `PairCreated(address indexed token0, address indexed token1, address pair, uint)` event:
- `topics[1]` = token0 address
- `topics[2]` = token1 address
- `data[0]` = pair address (the created contract)

**Nested Tuple Example:** For an event like `Created((address,uint256,address) params)` where the address is in a tuple:
- `data[0][0]` = first address in the tuple
- `data[0][2]` = third address in the tuple

When using nested access, the `data_signature` should include the tuple type:
```json
{
    "factory_events": {
        "name": "Created",
        "topics_signature": "",
        "data_signature": "(address,uint256,address)",
        "factory_parameters": "data[0][0]"
    }
}
```

## Architecture

Factory collection uses a two-phase architecture to maximize throughput and support resumability:

### Phase 1: Catchup (`catchup/factories.rs`)

Processes existing data to ensure consistency before streaming new data:

1. Loads existing factory parquet files and sends addresses to downstream collectors
2. Scans for log parquet files missing corresponding factory files
3. Processes gaps concurrently using semaphore-bounded parallelism
4. Signals completion via `factory_catchup_done_tx` oneshot channel

```rust
pub async fn collect_factories(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    logs_factory_tx: &Option<Sender<FactoryAddressData>>,
    log_decoder_tx: &Option<Sender<DecoderMessage>>,
    recollect_tx: &Option<Sender<RecollectRequest>>,
    factory_catchup_done_tx: Option<oneshot::Sender<()>>,
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<FactoryCatchupState, FactoryCollectionError>
```

Note: `eth_calls_factory_tx` and `call_decoder_tx` are intentionally omitted from the catchup signature. Both consumers load factory addresses from parquet during their own catchup phases. Sending during factory catchup would deadlock because those channels are not consumed until after factory catchup completes.

Returns `FactoryCatchupState` containing:
- `matchers`: Arc-wrapped factory matchers for reuse
- `existing_files`: Arc-wrapped set of existing parquet file paths
- `output_dir`: Arc-wrapped output directory path
- `s3_manifest`: Optional S3 manifest for remote storage
- `storage_manager`: Optional storage manager for S3 uploads
- `chain_name`: Chain name string

### Phase 2: Current/Streaming (`current/factories.rs`)

Processes new logs from the receipt collector channel:

```rust
pub async fn collect_factories(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    log_rx: Receiver<LogMessage>,
    logs_factory_tx: Option<Sender<FactoryAddressData>>,
    eth_calls_factory_tx: Option<Sender<FactoryMessage>>,
    log_decoder_tx: Option<Sender<DecoderMessage>>,
    call_decoder_tx: Option<Sender<DecoderMessage>>,
    matchers: Arc<Vec<FactoryMatcher>>,
    existing_files: Arc<HashSet<String>>,
    output_dir: Arc<PathBuf>,
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<(), FactoryCollectionError>
```

### Parameters

| Parameter | Type | Phase | Description |
|-----------|------|-------|-------------|
| `chain` | `&ChainConfig` | Both | Chain configuration with contracts |
| `raw_data_config` | `&RawDataCollectionConfig` | Both | Parquet range size and concurrency configuration |
| `log_rx` | `Receiver<LogMessage>` | Current | Channel receiving log messages from receipt collector |
| `logs_factory_tx` | `Option<Sender<FactoryAddressData>>` | Both | Channel to send addresses to logs collector |
| `eth_calls_factory_tx` | `Option<Sender<FactoryMessage>>` | Current | Channel to send factory messages to eth_calls collector |
| `log_decoder_tx` | `Option<Sender<DecoderMessage>>` | Both | Channel to send addresses to log decoder |
| `call_decoder_tx` | `Option<Sender<DecoderMessage>>` | Current | Channel to send addresses to call decoder |
| `recollect_tx` | `Option<Sender<RecollectRequest>>` | Catchup | Channel to request recollection of corrupted log files |
| `factory_catchup_done_tx` | `Option<oneshot::Sender<()>>` | Catchup | Signal when catchup completes |
| `matchers` | `Arc<Vec<FactoryMatcher>>` | Current | Pre-built factory matchers from catchup |
| `existing_files` | `Arc<HashSet<String>>` | Current | Pre-scanned existing files from catchup |
| `output_dir` | `Arc<PathBuf>` | Current | Output directory from catchup |
| `s3_manifest` | `Option<S3Manifest>` | Both | S3 manifest for checking remote file existence and skipping already-uploaded ranges |
| `storage_manager` | `Option<Arc<StorageManager>>` | Both | Storage manager for uploading parquet files to S3 |

### LogMessage Protocol

The factory collector receives messages via the `LogMessage` enum:

| Variant | Description |
|---------|-------------|
| `Logs(Vec<LogData>)` | Batch of log entries to process |
| `RangeComplete { range_start, range_end }` | Signals a block range is complete, triggers parquet writing |
| `AllRangesComplete` | Signals all ranges are done, collector can shut down |

### FactoryMessage Protocol

The factory collector sends messages to eth_calls via the `FactoryMessage` enum:

| Variant | Description |
|---------|-------------|
| `IncrementalAddresses(FactoryAddressData)` | Batch of discovered factory addresses |
| `RangeComplete { range_start, range_end }` | Signals a block range is complete, triggers factory eth_call processing |
| `AllComplete` | Signals all processing is complete |

This incremental forwarding allows eth_calls to begin RPC fetching for factory contracts before the full range is complete.

## Output

### Parquet Files

Factory addresses are written to:
- **Historical mode**: `data/{chain}/historical/factories/{collection_name}/`
- **Live mode**: `data/{chain}/live/factories/{collection_name}/`

With the naming convention:

```
{start_block}-{end_block}.parquet
```

Example: `data/base/historical/factories/DERC20/0-999.parquet`

### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | UInt64 | Block where the contract was created |
| `block_timestamp` | UInt64 | Unix timestamp of the creation block |
| `factory_address` | FixedSizeBinary(20) | Address of the created contract |
| `collection_name` | Utf8 | Name of the factory collection |

### FactoryAddressData Message

Data sent to downstream collectors:

```rust
pub struct FactoryAddressData {
    pub range_start: u64,
    pub range_end: u64,
    pub addresses_by_block: HashMap<u64, Vec<(u64, Address, String)>>,
}
```

The `addresses_by_block` map contains `(timestamp, address, collection_name)` tuples grouped by block number.

## Data Flow

```
                                     CATCHUP PHASE
                    ┌─────────────────────────────────────────────────┐
                    │                                                 │
┌──────────────────┐│  ┌─────────────────────────────────────────┐   │
│ existing logs    ││  │     catchup/factories.rs                │   │
│ parquet files    │├─▶│                                         │   │
│ (gaps only)      ││  │  • loads existing factory parquet       │   │
└──────────────────┘│  │  • processes gaps concurrently          │   │
                    │  │  • signals factory_catchup_done_tx      │   │
                    │  └──────────────────┬──────────────────────┘   │
                    │                     │                          │
                    │                     ▼ FactoryCatchupState      │
                    │                     │ (matchers, existing_files│
                    │                     │  output_dir, s3_manifest,│
                    │                     │  storage_manager,        │
                    │                     │  chain_name)             │
                    └─────────────────────┼──────────────────────────┘
                                          │
                                          ▼
                                    CURRENT PHASE
                    ┌─────────────────────────────────────────────────┐
                    │                                                 │
┌──────────────────┐│  ┌─────────────────────────────────────────┐   │
│   receipts.rs    ││  │     current/factories.rs                │   │
│                  │├─▶│                                         │   │
│  extracts logs   ││  │  • receives LogMessage from channel     │   │
│  from receipts   ││  │  • matches factory events               │   │
└──────────────────┘│  │  • extracts addresses                   │   │
      LogMessage    │  └──────────────────┬──────────────────────┘   │
                    │                     │                          │
                    └─────────────────────┼──────────────────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┬────────────────────┐
              │                           │                           │                    │
              ▼                           ▼                           ▼                    ▼
    ┌─────────────────┐         ┌──────────────────┐       ┌─────────────────┐   ┌─────────────────┐
    │    logs.rs      │         │  eth_calls.rs    │       │  log_decoder    │   │  call_decoder   │
    │                 │         │                  │       │                 │   │                 │
    │  filters logs   │         │ receives         │       │ decodes logs    │   │ decodes calls   │
    │  to include     │         │ FactoryMessage:  │       │ from factory    │   │ from factory    │
    │  factory        │         │ • Incremental    │       │ contracts       │   │ contracts       │
    │  contracts      │         │   Addresses      │       │                 │   │                 │
    │                 │         │ • RangeComplete  │       │                 │   │                 │
    │ FactoryAddress  │         │ • AllComplete    │       │ DecoderMessage  │   │ DecoderMessage  │
    │ Data            │         │                  │       │ ::FactoryAddr   │   │ ::FactoryAddr   │
    └─────────────────┘         └──────────────────┘       └─────────────────┘   └─────────────────┘
              │                           │
              │                           ▼
              │                 ┌──────────────────┐
              │                 │    parquet       │
              │                 │                  │
              └────────────────▶│ data/{chain}/    │
                                │ historical/      │
                                │ factories/       │
                                └──────────────────┘
```

## Integration with Other Collectors

### Logs Collector

When `contract_logs_only: true` is configured:
1. The logs collector waits for factory addresses before filtering each range
2. Logs from factory-created contracts are included in the filtered output
3. This allows tracking events from dynamically created contracts

### Eth Calls Collector

When factories have `calls` configured:
1. Regular eth_calls execute immediately (don't wait for factory data)
2. Factory eth_calls receive addresses via `FactoryMessage::IncrementalAddresses` as they're discovered
3. Processing triggers on `FactoryMessage::RangeComplete` after all addresses for a range are sent
4. Factory call results use the collection name in the directory path (e.g., `eth_calls/DERC20/totalSupply/0-9999.parquet`)
5. Calls with `frequency: "once"` are made at the discovery block and stored in `eth_calls/{collection}/once/`
6. Calls with block-based or duration-based frequency are filtered accordingly

See [eth_call Collection](./ETH_CALL_COLLECTION.md) for detailed frequency documentation.

### Decoders

Factory addresses are also sent to the log and call decoders via `DecoderMessage::FactoryAddresses`. This allows:
1. Log decoder to decode events from factory-created contracts using their ABIs
2. Call decoder to decode eth_call results from factory contracts

The decoder messages include the block range and a map of collection names to addresses discovered in that range:

```rust
DecoderMessage::FactoryAddresses {
    range_start: u64,
    range_end: u64,
    /// collection_name -> addresses discovered in this range
    addresses: HashMap<String, Vec<Address>>,
}
```

## Helper Functions

The module exports helper functions for working with factory configurations:

```rust
// Get eth_call configs for each factory collection
// Merges collection type config with inline overrides, deduplicates by function signature
pub fn get_factory_call_configs(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> HashMap<String, Vec<EthCallConfig>>

// Get list of all factory collection names
// Includes names from both contracts and factory_collections
pub fn get_factory_collection_names(
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> Vec<String>

// Resolve a factory config by merging collection type with inline overrides
pub fn resolve_factory_config(
    factory: &FactoryConfig,
    collection_types: &FactoryCollections,
) -> ResolvedFactoryConfig
```

These are useful for:
- Building the eth_calls collector configuration
- Checking which collections exist before processing
- Getting the resolved (merged) configuration for a factory

### Internal Functions

The shared `factories.rs` module provides internal functions used by both catchup and current phases:

```rust
// Build factory matchers from contract configuration
// Handles both Single and Multiple address configs, creating matchers for each address
pub fn build_factory_matchers(contracts: &Contracts) -> Vec<FactoryMatcher>

// Scan existing factory parquet files, returns relative paths like "collection/start-end.parquet"
pub(crate) fn scan_existing_parquet_files(dir: &Path) -> HashSet<String>

// Load factory addresses from existing parquet files
pub(crate) fn load_factory_addresses_from_parquet(dir: &Path) -> Result<Vec<FactoryAddressData>, ...>

// Get existing log file ranges for catchup gap detection
// Also includes ranges from S3 manifest that aren't present locally
pub(crate) fn get_existing_log_ranges(chain_name: &str, s3_manifest: Option<&S3Manifest>) -> Vec<ExistingLogRange>

// Read log batches from parquet with column projection (block_number, block_timestamp, address, topics, data)
pub(crate) fn read_log_batches_from_parquet(file_path: &Path) -> Result<Vec<RecordBatch>, ...>

// Process log batches using Arrow-native column access (efficient for catchup)
pub(crate) async fn process_range_batches(
    range_start: u64,
    range_end: u64,
    batches: Vec<RecordBatch>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
    chain_name: &str,
) -> Result<FactoryAddressData, ...>

// Process logs from Vec<LogData> (used by current/streaming phase)
pub(crate) async fn process_range(
    range_start: u64,
    range_end: u64,
    logs: Vec<LogData>,
    matchers: &[FactoryMatcher],
    output_dir: &Path,
    existing_files: &HashSet<String>,
    s3_manifest: Option<&S3Manifest>,
    storage_manager: Option<&Arc<StorageManager>>,
    chain_name: &str,
) -> Result<FactoryAddressData, ...>
```

## Example Use Cases

### Token Factory

Track all tokens deployed by a token factory:

```json
{
    "TokenFactory": {
        "address": "0x...",
        "factories": [
            {
                "collection": "FactoryTokens",
                "factory_events": {
                    "name": "TokenCreated",
                    "topics_signature": "",
                    "data_signature": "address,string,string",
                    "factory_parameters": "data[0]"
                },
                "calls": [
                    {"function": "totalSupply()", "output_type": "uint256"},
                    {"function": "name()", "output_type": "string", "frequency": "once"},
                    {"function": "symbol()", "output_type": "string", "frequency": "once"},
                    {"function": "decimals()", "output_type": "uint8", "frequency": "once"}
                ]
            }
        ]
    }
}
```

**Note:** Using `frequency: "once"` for immutable fields like `name`, `symbol`, and `decimals` significantly reduces RPC usage. These calls are made once when the contract is first discovered.

### Uniswap V2 Pairs

Track all pairs created by Uniswap V2 factory:

```json
{
    "UniswapV2Factory": {
        "address": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        "factories": [
            {
                "collection": "UniswapV2Pairs",
                "factory_events": {
                    "name": "PairCreated",
                    "topics_signature": "address,address",
                    "data_signature": "address,uint256",
                    "factory_parameters": "data[0]"
                },
                "calls": [
                    {"function": "getReserves()", "output_type": "(uint112,uint112,uint32)"}
                ]
            }
        ]
    }
}
```

### Multiple Factories Per Contract

A single contract can have multiple factory configurations:

```json
{
    "MultiFactory": {
        "address": "0x...",
        "factories": [
            {
                "collection": "TypeA",
                "factory_events": {"name": "TypeACreated", "factory_parameters": "data[0]", ...}
            },
            {
                "collection": "TypeB",
                "factory_events": {"name": "TypeBCreated", "factory_parameters": "topics[1]", ...}
            }
        ]
    }
}
```

### Multiple Contracts, Same Collection

Multiple contracts can contribute to the same collection using shared configuration:

```json
// In factory_collections/base.json
{
    "DERC20": {
        "calls": [
            {"function": "name()", "output_type": "string", "frequency": "once"},
            {"function": "symbol()", "output_type": "string", "frequency": "once"}
        ],
        "events": [
            { "signature": "Transfer(address indexed from, address indexed to, uint256 value)" }
        ]
    }
}

// In contracts/airlock.json
{
    "Airlock": {
        "address": "0x...",
        "factories": [{
            "collection": "DERC20",
            "factory_events": {"name": "Create", "factory_parameters": "data[0]", ...}
        }]
    }
}

// In contracts/other_initializer.json
{
    "OtherInitializer": {
        "address": "0x...",
        "factories": [{
            "collection": "DERC20",
            "factory_events": {"name": "TokenDeployed", "factory_parameters": "topics[1]", ...}
        }]
    }
}
```

Both contracts contribute discovered addresses to the same `DERC20` collection, sharing the calls and events config.

## Error Handling

| Error | Cause |
|-------|-------|
| `Io` | File system error when writing parquet |
| `Parquet` | Parquet write error |
| `Arrow` | Arrow array construction error |
| `ChannelSend` | Failed to send data to downstream collector |
| `AbiDecode` | Failed to decode event data (logged as warning, doesn't stop collection) |
| `JoinError` | Tokio task join error during blocking parquet writes |

ABI decode errors are logged but don't stop collection. This allows the indexer to continue even if some events have unexpected data formats.

## Querying Factory Data

Example using DuckDB to query discovered factory contracts:

```sql
-- Find all tokens created in a time range (historical data)
SELECT
    block_number,
    block_timestamp,
    encode(factory_address, 'hex') as token_address,
    collection_name
FROM read_parquet('data/base/historical/factories/**/*.parquet')
WHERE collection_name = 'DERC20'
  AND block_timestamp BETWEEN 1700000000 AND 1700100000
ORDER BY block_number;

-- Count contracts per collection (combine historical and live data)
SELECT
    collection_name,
    COUNT(*) as contract_count
FROM (
    SELECT * FROM read_parquet('data/base/historical/factories/**/*.parquet')
    UNION ALL
    SELECT * FROM read_parquet('data/base/live/factories/**/*.parquet')
)
GROUP BY collection_name;
```

## Runtime Configuration

Factory collection behavior is controlled by `RawDataCollectionConfig`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `parquet_block_range` | u32 | 1000 | Number of blocks per parquet file |
| `factory_concurrency` | usize | 4 | Number of concurrent tasks for catchup processing |
| `factory_channel_capacity` | usize | 1000 | Capacity for factory-related channels |

Example configuration:

```json
{
    "raw_data": {
        "parquet_block_range": 1000,
        "factory_concurrency": 8,
        "factory_channel_capacity": 2000
    }
}
```

## Performance Considerations

- Factory matching is O(n x m) where n is logs and m is matchers
- ABI decoding for data extraction adds overhead
- Large numbers of factory contracts increase eth_call RPC costs
- Consider using `parquet_block_range` to balance file count vs. size
- When no factory matchers are configured, the collector simply forwards empty `FactoryAddressData` to downstream collectors without writing any files
- **Catchup concurrency**: Controlled by `factory_concurrency` config (default: 4). Higher values improve catchup throughput but use more memory
- **Arrow-native processing**: Catchup uses `process_range_batches` which operates directly on Arrow arrays instead of materializing `LogData` structs, reducing memory allocations
- **Incremental forwarding**: Factory addresses are sent to eth_calls via `FactoryMessage::IncrementalAddresses` as they're discovered, allowing RPC fetching to begin before the full range completes

## Resumability

Collection is fully resumable with multiple safeguards:

### Factory Parquet Skipping

Existing parquet files are scanned on startup and their ranges are skipped during collection. When S3 storage is configured, the S3 manifest is also checked, so ranges already uploaded to S3 are skipped even if the local files have been cleaned up.

### Catchup Phase

On startup, before processing new logs from the channel, the factory collector performs a catchup phase:

1. **Load existing factory data**: Reads all existing factory parquet files and sends addresses to downstream collectors (logs, decoders)
2. **Scan for gaps**: Scans `data/{chain}/historical/raw/logs/` for existing log parquet files (and S3 manifest ranges not present locally)
3. **Check completeness**: For each log file, checks if corresponding factory files exist for all configured collections (checking both local files and S3 manifest)
4. **Process gaps concurrently**: If any factory files are missing, reads logs from the existing parquet file (downloading from S3 if needed) and processes them using semaphore-bounded concurrency (controlled by `factory_concurrency` config)
5. **Upload to S3**: If storage manager is configured, newly written factory parquet files are uploaded to S3
6. **Signal completion**: Sends `factory_catchup_done_tx` oneshot to unblock dependent catchup phases (e.g., eth_calls)

This is particularly useful when:
- Factory collection was interrupted mid-run
- New factory configurations are added to existing contracts
- Factory files were manually deleted for re-processing

### Synchronization with eth_calls

The eth_calls catchup phase waits for `factory_catchup_done_rx` before processing factory-dependent calls. This ensures factory addresses are available before eth_calls attempts to fetch data from factory-created contracts.

### Corrupted File Recovery

During catchup, if a log parquet file is corrupted (fails to parse), the factory collector automatically handles recovery:

1. **Detection**: When `read_log_batches_from_parquet` fails with a parse error
2. **Deletion**: The corrupted file is deleted from `data/{chain}/historical/raw/logs/`
3. **Recollection Request**: A `RecollectRequest` is sent via the `recollect_tx` channel to the receipt collector
4. **Re-fetching**: The receipt collector reads block info from the corresponding block file, re-fetches receipts via RPC, and sends logs through the normal `factory_log_tx` channel
5. **Processing**: The factory collector receives the re-fetched logs through its normal channel and processes them

#### RecollectRequest Structure

```rust
pub struct RecollectRequest {
    pub range_start: u64,
    pub range_end: u64,
    pub _file_path: PathBuf,
}
```

#### Data Flow for Corrupted File Recovery

```
Factory Catchup                           Receipt Collector
     │                                           │
     │ (attempts to read corrupted file)         │
     │                                           │
     ▼                                           │
  Parse error detected                           │
     │                                           │
     ▼                                           │
  Delete corrupted file                          │
  (data/{chain}/historical/raw/logs/X-Y.parquet) │
     │                                           │
     │─────── RecollectRequest ─────────────────►│
     │        {range_start, range_end}           │
     │                                           ▼
     │                                  Read block info from
     │                                  blocks_X-Y.parquet
     │                                           │
     │                                           ▼
     │                                  Fetch receipts via RPC
     │                                           │
     │◄──────────── LogMessage ──────────────────│
     │          (via factory_log_tx)             │
     │                                           │
     ▼                                           │
  Process logs normally                          │
  Write factory parquet                          │
```

This automatic recovery ensures data integrity without manual intervention, even when parquet files become corrupted due to crashes, disk errors, or other issues.

### Empty Range Handling

For each block range processed, the factory collector writes a parquet file for **every configured collection**, even if no factory events were found for that collection in the range. This ensures:

1. The receipt collector's catchup logic can verify factory processing is complete
2. Ranges with no factory activity aren't re-processed on subsequent runs
3. Downstream collectors know the range was fully processed

### Downstream Collector Notification

On startup, factory addresses from existing parquet files are read and sent to the logs collector and log decoder. The eth_calls collector and call decoder are intentionally not notified during catchup -- they load factory addresses from parquet during their own catchup phases. This avoids deadlock since those channels are not consumed until after factory catchup completes.

### Catchup Coordination

The receipt collector checks for factory files during its catchup phase. If block files exist but factory files are missing for any configured collection, the receipt collector will re-process that range, which triggers factory collection.

This means if you have:
- Block parquet files for ranges 0-999, 1000-1999, 2000-2999
- Factory parquet files for ranges 0-999, 1000-1999 (missing 2000-2999)

On the next run, the receipt collector will detect the missing factory files and re-process range 2000-2999, ensuring factory collection completes.

### Manual Re-collection

To re-collect a factory range, delete the corresponding file from `data/{chain}/historical/factories/{collection}/` (or `data/{chain}/live/factories/{collection}/` for live data). Note that you may also need to delete the corresponding receipts file to trigger re-processing.

## S3 Storage Integration

When a `StorageManager` is configured, factory collection integrates with S3 for remote storage:

### Upload

After writing a factory parquet file locally, it is uploaded to S3 using the data type path `factories/{collection_name}`. This applies to both non-empty and empty (placeholder) factory parquet files.

### Skip Logic

Before writing a factory parquet file, the collector checks both:
- The local `existing_files` set (scanned on startup)
- The `S3Manifest` (via `has_factories()`)

If the range already exists in either location, writing is skipped.

### Download on Demand

During catchup, if a log parquet file is referenced in the S3 manifest but not present locally, the collector uses `DataLoader::ensure_local()` to download it from S3 before processing.

### Gap Detection

`get_existing_log_ranges()` merges local log file ranges with ranges from the S3 manifest, so catchup can detect and process gaps even when some data only exists remotely.
