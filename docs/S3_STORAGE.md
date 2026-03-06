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
      ...
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

Data matching pinned prefixes is never evicted from local cache. This ensures transformation dependencies are always available locally:

- `factories/` - Factory addresses needed for log filtering
- `decoded/` - Decoded events/calls needed for transformations

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
  "decoded_logs": {
    "v3/Swap": [[0, 999], [1000, 1999], ...]
  },
  "factories": {
    "v3_pools": [[0, 999], [1000, 1999], ...]
  }
}
```

Services check the manifest on startup to avoid re-collecting existing data.

## Graceful Degradation

If S3 is not configured, the indexer operates in local-only mode:

- All data stored on local filesystem
- No manifest or marker files
- Full backward compatibility with existing deployments

To run without S3, simply omit the `storage` section from config.

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
2. S3 upload is attempted synchronously
3. On failure, upload is queued with exponential backoff
4. Queue is persisted to `data/.upload_queue.json` for crash recovery
5. Background retry service processes the queue automatically

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
- For MinIO, ensure `http://` is used for local development

### "Upload failed, queued for retry"

Check S3 connectivity:
- Verify endpoint is reachable
- Check credentials are valid
- Ensure bucket exists and is writable

### Data not visible to other services

1. Check marker files exist in S3
2. Refresh manifest: restart service or wait for refresh interval
3. Verify services are reading from same bucket
