# S3 Storage Support

The indexer supports S3-compatible object storage for durable, distributed data storage. This enables running separate services on different machines that share data via S3.

## Overview

### Use Cases

- **Durable storage**: Persist parquet files to S3 for disaster recovery
- **Distributed deployment**: Run collector, decoder, and transformer on separate machines
- **Cost optimization**: Use cheaper storage tiers for historical data
- **Multi-region**: Replicate data across regions for low-latency access

### Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Collector  │────▶│     S3      │◀────│  Decoder    │
│  (writes)   │     │   Bucket    │     │  (reads)    │
└─────────────┘     └─────────────┘     └─────────────┘
                           ▲
                           │
                    ┌──────┴──────┐
                    │ Transformer │
                    │  (reads)    │
                    └─────────────┘
```

### Data Layout

```
s3://{bucket}/
  {chain}/
    historical/
      raw/
        blocks/
        logs/
        receipts/
        eth_calls/
      decoded/
        logs/{source}/{event}/
        eth_calls/{source}/{function}/
      factories/{collection}/
    markers/
      raw/blocks/{start}_{end}.marker
      raw/logs/{start}_{end}.marker
      raw/receipts/{start}_{end}.marker
      raw/eth_calls/{contract}/{function}/{start}_{end}.marker
      raw/eth_calls/{contract}/{function}/on_events/{start}_{end}.marker
      decoded/logs/{source}/{event}/{start}_{end}.marker
      decoded/eth_calls/{source}/{function}/{start}_{end}.marker
      factories/{collection}/{start}_{end}.marker
    manifest.json
```

## Configuration

Add a `storage` section to your `config.json`:

```json
{
  "chains": [...],
  "raw_data_collection": {...},
  "storage": {
    "s3": {
      "endpoint_env_var": "S3_ENDPOINT",
      "access_key_env_var": "S3_ACCESS_KEY_ID",
      "secret_key_env_var": "S3_SECRET_ACCESS_KEY",
      "bucket_env_var": "S3_BUCKET",
      "region": "us-east-1"
    },
    "cache": {
      "max_size_gb": 100,
      "pinned_prefixes": ["factories", "decoded"],
      "eviction_threshold": 0.8
    },
    "sync": {
      "marker_freshness_ranges": 10,
      "manifest_refresh_secs": 60,
      "retry_interval_secs": 30,
      "max_retries": 10
    }
  }
}
```

### S3 Configuration

| Field | Description |
|-------|-------------|
| `endpoint_env_var` | Environment variable containing S3 endpoint URL |
| `access_key_env_var` | Environment variable containing access key ID |
| `secret_key_env_var` | Environment variable containing secret access key |
| `bucket_env_var` | Environment variable containing bucket name |
| `region` | AWS region (default: `us-east-1`) |

### Cache Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `max_size_gb` | 100 | Maximum local cache size in GB |
| `pinned_prefixes` | `["factories", "decoded"]` | Prefixes that are never evicted |
| `eviction_threshold` | 0.8 | Eviction triggers at this fraction of max_size |

### Sync Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `marker_freshness_ranges` | 10 | Recent ranges to check markers directly |
| `manifest_refresh_secs` | 60 | How often to refresh manifest from S3 |
| `retry_interval_secs` | 30 | Retry interval for failed uploads |
| `max_retries` | 10 | Max retry attempts before giving up |

## Environment Variables

Set these environment variables (names are configurable in the config):

```bash
# AWS S3
export S3_ENDPOINT="https://s3.us-east-1.amazonaws.com"
export S3_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export S3_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export S3_BUCKET="my-indexer-bucket"

# MinIO (local development)
export S3_ENDPOINT="http://localhost:9000"
export S3_ACCESS_KEY_ID="minioadmin"
export S3_SECRET_ACCESS_KEY="minioadmin"
export S3_BUCKET="indexer-data"

# Cloudflare R2
export S3_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
export S3_ACCESS_KEY_ID="<r2-access-key>"
export S3_SECRET_ACCESS_KEY="<r2-secret-key>"
export S3_BUCKET="indexer-data"
```

## Caching Strategy

The indexer uses a tiered caching strategy:

| Data Type | Local Policy | S3 Sync |
|-----------|--------------|---------|
| Live bincode | Always local | Never (ephemeral) |
| Factories | **Pinned** (never evict) | After compaction |
| Decoded logs/calls | **Pinned** (never evict) | After compaction |
| Raw historical | LRU cache | After write |

### Pinned Prefixes

Data matching pinned prefixes is never evicted from local cache. The match is a substring check (via `contains`), not a strict prefix match, so a key like `chain/decoded/logs/event.parquet` matches the `decoded` prefix. This ensures transformation dependencies are always available locally:

- `factories` - Factory addresses needed for log filtering
- `decoded` - Decoded events/calls needed for transformations

### LRU Eviction

When cache exceeds `max_size_gb * eviction_threshold`:

1. Non-pinned entries are sorted by last access time
2. Oldest entries are evicted until cache is at 80% of threshold
3. Evicted data can be re-fetched from S3 on demand

### Cache Index Persistence

The cache index is persisted to `data/.cache_index.json` for crash recovery:

- Index is saved after each eviction cycle
- On restart, the index is loaded to restore cache metadata
- Access times are approximated from timestamps (slight drift is acceptable for LRU)
- Atomic writes via tmp file + rename ensure crash safety

## Distributed Coordination

### Marker Files

After each successful S3 upload, a marker file is written:

```json
{
  "uploaded_at": 1709567890,
  "uploader": "collector-01",
  "file_path": "ethereum/historical/raw/blocks/blocks_0-999.parquet"
}
```

Markers enable:
- Other services to discover newly available data
- Atomic signaling (marker written after data is fully uploaded)
- Distributed service coordination without external dependencies

### Manifest

The manifest aggregates all marker information for fast queries:

```json
{
  "generated_at": 1709567890,
  "raw_blocks": [[0, 999], [1000, 1999], ...],
  "raw_logs": [[0, 999], [1000, 1999], ...],
  "raw_receipts": [[0, 999], [1000, 1999], ...],
  "raw_eth_calls": {
    "contract/function": [[0, 999], [1000, 1999], ...]
  },
  "decoded_logs": {
    "v3/Swap": [[0, 999], [1000, 1999], ...]
  },
  "decoded_eth_calls": {
    "v3/slot0": [[0, 999], [1000, 1999], ...]
  },
  "factories": {
    "v3_pools": [[0, 999], [1000, 1999], ...]
  }
}
```

The manifest is stored per-chain at `{chain}/manifest.json`. Services check the manifest on startup to avoid re-collecting existing data. The `ManifestManager` supports multi-chain deployments via `register_chain()` and per-chain manifest access via `manifest_for(chain)`.

## Graceful Degradation

If S3 is not configured, the indexer operates in local-only mode:

- All data stored on local filesystem
- No manifest or marker files
- Full backward compatibility with existing deployments

To run without S3, simply omit the `storage` section from config.

## Module Overview

The S3 storage system is composed of several modules in `src/storage/`:

| Module | Description |
|--------|-------------|
| `mod.rs` | `StorageBackend` trait, `StorageManager` high-level manager |
| `s3.rs` | `S3Backend` using the `object_store` crate |
| `local.rs` | `LocalBackend` for filesystem operations (atomic writes via tmp+rename) |
| `cached.rs` | `CachedBackend` write-through cache combining local and S3 with LRU eviction |
| `manifest.rs` | `ManifestManager`, `S3Manifest`, and `Marker` types for distributed coordination |
| `initial_sync.rs` | `InitialSyncService` for uploading existing local data to S3 |
| `retry.rs` | `RetryQueue` with exponential backoff for failed uploads |
| `upload.rs` | `upload_parquet_to_s3` helper for historical collectors |
| `data_loader.rs` | `DataLoader` for on-demand S3 download with local caching |
| `error.rs` | `StorageError` enum (Io, ObjectStore, NotFound, Config, Serialization, Cache, Manifest) |

### Contract Index Tracking

The storage system maintains per-directory `contract_index.json` files to track which factory-discovered contracts were included in each processed range. This enables reprocessing when configuration changes add new contracts.

**Types** (`src/storage/contract_index.rs`):
```rust
pub type ContractIndex = HashMap<String, HashMap<String, Vec<String>>>
// range_key -> source_name -> [contract_addresses]

pub type ExpectedContracts = HashMap<String, Vec<String>>
// source_name -> [contract_addresses]
```

**Key functions**:
- `range_key(start, end)` - generates range key string
- `build_expected_factory_contracts(contracts)` - computes expected contracts from config
- `get_missing_contracts(index, range_key, expected)` - finds contracts not yet in index
- `update_contract_index(index, range_key, contracts)` - adds contracts to index
- `read_contract_index(dir)` / `write_contract_index(dir, index)` - atomic file I/O
- `detect_contracts_in_log_parquet(path, address_maps)` - scans parquet for contract presence

**Migration helpers**: When the contract index is first introduced, existing data is migrated from the factory layer to downstream indices (eth_calls, decoded logs) without requiring a full reprocess.

### Atomic Parquet Writes

All parquet file writes use `atomic_write_parquet()` (`src/storage/parquet_writer.rs`):

1. Write to a temporary file (`.tmp` suffix)
2. Apply Snappy compression
3. Call `sync_all()` to ensure data is flushed to disk
4. Atomic `rename()` to final path

This prevents corrupt parquet files from partial writes or crashes. Parquet files have their metadata footer written last, so a partial write produces an unreadable file. The atomic rename guarantees readers always see a complete file.

### DataLoader

The `DataLoader` provides transparent on-demand fetching from S3. When a file is needed locally but missing, it downloads from S3 and writes it to the local filesystem atomically (via tmp file + rename). This is used by catchup collectors to fetch data that may have been produced by another service.

```rust
let loader = DataLoader::new(Some(storage_manager), "ethereum", local_base);
// Returns true if file exists locally or was downloaded from S3
let available = loader.ensure_local(&path).await?;
```

## Initial Sync

When S3 storage is first configured on a node that already has local data, the indexer automatically uploads existing data to S3.

### How It Works

On startup, if S3 is enabled:

1. Scans all configured prefixes (e.g., `{chain}/historical/`, `{chain}/live/`)
2. Identifies `.parquet` files (skips bincode live data and `.tmp` files)
3. Checks if each file exists in S3
4. Uploads missing files with bounded concurrency (10 parallel uploads)
5. Writes marker files for each uploaded file (when a manifest manager is provided)
6. Logs a summary when complete

The service accepts a shutdown signal and will stop early if the process is shutting down.

### What Gets Synced

| Data Type | Synced to S3 |
|-----------|--------------|
| Historical parquet files | Yes |
| Compacted live parquet files | Yes |
| Live bincode files (`.bin`) | No (ephemeral) |
| Temporary files (`.tmp`) | No |

### Progress Logging

The service logs progress on completion:

```
Initial sync complete: scanned=1234, already_synced=1000, uploaded=234 (5.67 GB), failed=0
```

### No Configuration Required

The initial sync service starts automatically when S3 is configured. It runs in the background and does not block indexer startup.

## Failure Handling

### Automatic Retry Service

When S3 is enabled, the indexer automatically starts a background retry service on startup:

1. Loads any pending uploads from `data/.upload_queue.json`
2. Processes failed uploads with exponential backoff
3. Saves queue state after each retry cycle (crash-safe)
4. Runs until process exit

The service requires no configuration - it starts automatically when S3 storage is configured.

### Upload Failures

Failed uploads are queued for retry:

1. Data is written locally first (always succeeds)
2. S3 upload is attempted
3. On failure, upload is queued with exponential backoff (`base * 2^attempts`, capped at 1 hour)
4. Queue is persisted to `data/.upload_queue.json` for crash recovery (atomic write via tmp+rename)
5. Background retry service processes the queue automatically
6. Uploads exceeding `max_retries` are dropped with an error log

### Network Partitions

During S3 unavailability:

- Local operations continue uninterrupted
- Uploads queue for later retry
- Reads fall back to local cache (if data exists)
- Missing data triggers S3 fetch when connectivity returns

## Local Development with MinIO

Start MinIO for local S3 testing:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Create a bucket:

```bash
# Using mc (MinIO client)
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/indexer-data
```

Configure environment:

```bash
export S3_ENDPOINT="http://localhost:9000"
export S3_ACCESS_KEY_ID="minioadmin"
export S3_SECRET_ACCESS_KEY="minioadmin"
export S3_BUCKET="indexer-data"
```

## Monitoring

Key metrics to monitor:

- **Cache hit rate**: High hit rate indicates good cache sizing
- **Eviction frequency**: Frequent evictions may indicate undersized cache
- **Retry queue depth**: Growing queue indicates S3 connectivity issues
- **Manifest staleness**: Stale manifest may cause duplicate collection

## Troubleshooting

### "Environment variable not set"

Ensure all required environment variables are set:

```bash
env | grep S3_
```

### "Failed to build S3 client"

Check endpoint URL format:
- Must include protocol (`http://` or `https://`)
- HTTP is automatically allowed when the endpoint starts with `http://`
- Path-style requests are always used (forced for MinIO/R2 compatibility)

### "Upload failed, queued for retry"

Check S3 connectivity:
- Verify endpoint is reachable
- Check credentials are valid
- Ensure bucket exists and is writable

### Data not visible to other services

1. Check marker files exist in S3
2. Refresh manifest: restart service or wait for refresh interval
3. Verify services are reading from same bucket
