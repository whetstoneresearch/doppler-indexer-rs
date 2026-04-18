# API Design: REST + GraphQL for Doppler Indexer

## Overview

This document specifies a read-only REST and GraphQL API over the doppler indexer database. The API serves paginated, filtered data derived from the indexer schema. All mutations flow through the indexer — the API is query-only.

The design is language-agnostic. A Rust reference implementation sketch using `axum`, `tower`, `sqlx`, and `async-graphql` is provided in the final section.

---

## Conventions

### Addresses

Addresses are stored as `BYTEA` in the database. The API surface uses the `ChainAddress` wire format, which is chain-aware:

- **EVM addresses** (20 bytes): lowercase `0x`-prefixed hex — `"0xd8da6bf26964af9d7eed9e03e53415d37aa96045"`
- **Solana pubkeys** (32 bytes): base58 — `"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"`

The same convention applies to path segments, query parameters, and response JSON. The API layer converts to `&[u8]` before binding to SQL `BYTEA` parameters, and formats from `BYTEA` on the way out. The format is self-identifying: `0x` prefix → EVM, no prefix → Solana base58. The server returns `400` if the string is not parseable for its expected chain type.

Transaction IDs follow the same rule: EVM tx hash is `0x{64 hex chars}`, Solana signature is base58 of the 64-byte signature.

### Numeric Types

Token amounts and prices are returned as **decimal strings** to preserve arbitrary precision (`NUMERIC` columns). Integer fields (`BIGINT`, `INT`) are returned as JSON numbers where they fit in 53-bit integers, strings otherwise. API documentation specifies per-field.

### Timestamps

All timestamps are returned as **unix seconds (`i64`)** in responses. The database stores timestamps as `TIMESTAMPTZ` (required for TimescaleDB hypertables); the API layer converts via `DateTime<Utc>` → `.timestamp()`.

### Chain IDs

`chain_id` is a required query parameter for all chain-scoped resources. Multi-chain clients must issue per-chain requests.

### Source Versions

All source-versioned tables are filtered by joining against the `active_versions` table, which is updated manually when an indexer version is promoted to production. Queries never filter by `MAX(source_version)` directly — the join against `active_versions` is the single source of truth for which data is live. Clients cannot override this; `source` and `source_version` are not exposed as query parameters.

### Authentication

The API inherits database credentials from `DATABASE_URL` in the process environment (`.env`). No additional per-request authentication layer is defined — access control is at the network/infrastructure level.

### Error Responses

All errors return a JSON body regardless of HTTP status:

```json
{
  "error": {
    "code": "INVALID_ADDRESS",
    "message": "parameter 'pool': missing 0x prefix",
    "request_id": "01HXYZ4ABCDEF123456789"
  }
}
```

`request_id` is a ULID injected by tower middleware on every request, present in both error and success responses (as a response header `X-Request-Id`). Error codes:

| Code              | HTTP status | Meaning                                  |
|-------------------|-------------|------------------------------------------|
| `INVALID_ADDRESS` | 400         | Malformed address or tx hash parameter   |
| `INVALID_CURSOR`  | 400         | Cursor is expired, tampered, or malformed|
| `INVALID_PARAM`   | 400         | Other bad query parameter                |
| `NOT_FOUND`       | 404         | Resource does not exist                  |
| `INTERNAL`        | 500         | Unexpected server error                  |

---

## Pagination

Two distinct pagination contracts exist, chosen by the endpoint — not by query parameters.

### Feed Pagination (immutable sort key)

Used by endpoints whose sort key never changes after a row is inserted (`block_number`, `id`). Examples: `GET /v1/pools/new`, `GET /v1/swaps`.

New arrivals always land at the "top" (DESC) or strictly after the current position (ASC), so they never inject into the middle of a paginated window. This makes it safe to carry a **head cursor** pointing at the newest item seen and poll for new arrivals on every request.

**Request parameters**

| Parameter      | Type    | Default | Description                                      |
|----------------|---------|---------|--------------------------------------------------|
| `cursor`       | string  | —       | Tail cursor — fetch items older than this        |
| `head_cursor`  | string  | —       | Optional — if provided, bundle new arrivals too  |
| `limit`        | integer | 50      | Max items per direction (max 500)                |
| `known_reorg_epoch` | integer | — | Client's last known reorg epoch (see Reorg Safety) |

**Response envelope**

```json
{
  "new_since_head": [...],
  "data": [...],
  "pagination": {
    "next_cursor": "eyJibG9jayI6MTIzNDU2LCJpZCI6NDJ9",
    "head_cursor": "eyJibG9jayI6MTMwMDAwMCwiaWQiOjEwMH0",
    "has_more": true,
    "has_more_new": false
  },
  "chain": {
    "confirmed_tip": 13000000,
    "reorg_epoch": 42,
    "invalidated_from_block": null
  }
}
```

- `new_since_head` — items newer than `head_cursor`, ordered newest-first. Omitted (not null) if `head_cursor` was not sent. Capped at 50; if `has_more_new` is true the client should refresh from the top rather than attempt to paginate forward.
- `head_cursor` — updated to the newest item now known. Client persists this and sends it on every subsequent request.
- `chain.invalidated_from_block` — non-null when `reorg_epoch` advanced since `known_reorg_epoch`. Client discards all cached items with `block_number >= invalidated_from_block` and resets `head_cursor` to before that block.

**Cursor encoding** — `{ "block": <bigint>, "id": <bigint> }`. Keyset condition: `WHERE (block_number, id) < ($block, $id)` for DESC (tail), `> ($block, $id)` for ASC (head). Tiebreaker on `id` handles multiple rows in the same block.

---

### Leaderboard Pagination (mutable sort key)

Used by endpoints sorted by a value that changes as the indexer processes new blocks (`active_liquidity_usd`, `tvl_usd`, etc.). Examples: `GET /v1/pools`.

Sort key drift — a pool moving from rank 50 to rank 3 mid-session — is unavoidable when ranking directly against the live, UPSERTed `pool_state` table. The solution is **versioned ranking snapshots**: the indexer periodically materializes a point-in-time ranking into `pool_leaderboard_snapshot`, keyed by a monotonic `snapshot_id`. The first page captures `MAX(snapshot_id)` into the cursor and every subsequent page reads the same snapshot. Keyset seeks run against a composite index `(chain_id, sort_key, snapshot_id, sort_val DESC, id DESC)` — every page is `O(log N + limit)` regardless of depth, and rankings are perfectly stable for the life of the session.

Old snapshots are garbage-collected after a retention window (default 1 hour). If a client's cursor references an expired `snapshot_id` the server returns `INVALID_CURSOR` and the client restarts the session. To get fresh rankings the client starts a new session (no cursor); the response includes `data_as_of_block` (the block height the snapshot was taken at) so clients can display a staleness indicator.

**Snapshot table**

```sql
CREATE SEQUENCE pool_leaderboard_snapshot_id_seq;

CREATE TABLE pool_leaderboard_snapshot (
    snapshot_id   BIGINT      NOT NULL,
    chain_id      BIGINT      NOT NULL,
    sort_key      TEXT        NOT NULL,   -- 'active_liquidity_usd', 'tvl_usd', ...
    pool_id       BYTEA       NOT NULL,
    id            BIGINT      NOT NULL,   -- pools.id, used as keyset tiebreaker
    sort_val      NUMERIC,                -- nullable: pools with no state sort last
    block_height  BIGINT      NOT NULL,   -- the pool_state block this row reflects
    taken_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_id, chain_id, sort_key, pool_id)
);

CREATE INDEX idx_plsnap_keyset
    ON pool_leaderboard_snapshot (chain_id, sort_key, snapshot_id, sort_val DESC NULLS LAST, id DESC);
CREATE INDEX idx_plsnap_reorg ON pool_leaderboard_snapshot (chain_id, block_height);
CREATE INDEX idx_plsnap_gc    ON pool_leaderboard_snapshot (taken_at);
```

**Indexer write path.** After every N processed blocks (or every T seconds — configurable; defaults 100 blocks / 30 s) the indexer materializes one new snapshot covering every supported `sort_key`. In the implementation the snapshot id is pulled via `SELECT nextval(...)` and then passed as a bound parameter to one `INSERT ... SELECT` per sort key, all inside a single transaction; the shape below is the equivalent single-statement form:

```sql
WITH sid AS (SELECT nextval('pool_leaderboard_snapshot_id_seq') AS snapshot_id)
INSERT INTO pool_leaderboard_snapshot
    (snapshot_id, chain_id, sort_key, pool_id, id, sort_val, block_height)
SELECT sid.snapshot_id, ps.chain_id, k.sort_key, ps.pool_id, p.id,
       CASE k.sort_key
           WHEN 'active_liquidity_usd' THEN ps.active_liquidity_usd
           WHEN 'tvl_usd'              THEN ps.tvl_usd
           WHEN 'volume_24h_usd'       THEN ps.volume_24h_usd
           WHEN 'market_cap_usd'       THEN ps.market_cap_usd
       END,
       ps.block_height
FROM   pool_state ps
INNER JOIN active_versions av
    ON av.source = ps.source AND av.active_version = ps.source_version
INNER JOIN pools p
    ON p.chain_id = ps.chain_id
   AND p.address  = ps.pool_id
   AND p.source   = ps.source
   AND p.source_version = ps.source_version
CROSS JOIN sid
CROSS JOIN (VALUES ('active_liquidity_usd'), ('tvl_usd'),
                   ('volume_24h_usd'),       ('market_cap_usd')) AS k(sort_key);
```

A separate background job runs `DELETE FROM pool_leaderboard_snapshot WHERE taken_at < now() - INTERVAL '1 hour'`. Snapshot cadence and retention are per-deployment knobs; shorter cadence means fresher new-session rankings at the cost of write amplification.

**Reorg handling.** If a reorg invalidates blocks at or after a snapshot's `block_height`, the indexer deletes that snapshot on reorg processing — any still-active cursor pointing at it falls back to `INVALID_CURSOR` on the next request, forcing the client to restart with current data. This keeps the snapshot table aligned with the canonical chain without needing per-row reconciliation.

**Request parameters**

| Parameter      | Type    | Default | Description                        |
|----------------|---------|---------|-------------------------------------|
| `cursor`       | string  | —       | Opaque keyset cursor                |
| `limit`        | integer | 50      | Max items (max 500)                 |
| `known_reorg_epoch` | integer | — | See Reorg Safety                  |

**Response envelope**

```json
{
  "data": [...],
  "pagination": {
    "next_cursor": "...",
    "has_more": true
  },
  "chain": {
    "confirmed_tip": 13000000,
    "data_as_of_block": 12999990,
    "reorg_epoch": 42,
    "invalidated_from_block": null
  }
}
```

**Cursor encoding** — `{ "val": "<decimal_string>", "id": <bigint>, "snapshot_id": <bigint> }`. Decimal string preserves `NUMERIC` precision. `snapshot_id` pins all subsequent page reads to the same materialized ranking; if it has been GC'd the server returns `INVALID_CURSOR`.

---

### Reorg Safety

**When reorgs matter**

A reorg only threatens cursors when the cursor's anchor block is in the reorged zone. The failure mode is not a crash — the keyset SQL still runs — but silent inconsistency across page boundaries: rows from the old canonical chain disappear, and if replacement rows have higher BIGSERIAL ids they fall outside the current cursor window and are silently skipped.

For data more than a few blocks behind the tip this risk is negligible. It is only material for open-ended recency queries paginating from the tip backwards.

**Confirmed horizon (default for feed endpoints)**

Feed endpoints bound results to `block_number <= tip - CONFIRMATION_DEPTH`:

| Chain | Depth | Block time | Lag |
|-------|-------|-----------|-----|
| EVM (post-Merge) | 6 | ~12s | ~72s |
| Solana (`finalized`) | 32 | ~400ms | ~13s |

`chain.confirmed_tip` tells clients where the stable zone ends.

**Opt-out**

Pass `unconfirmed=true` on feed endpoints to bypass the horizon. Response includes `X-Reorg-Risk: true`. Use the reorg epoch mechanism below to detect invalidation.

**Historical range queries**

Requests with explicit `from_block`/`to_block` are client-owned. No horizon is applied.

**Reorg epoch**

Every response includes `chain.reorg_epoch` — a monotonically increasing counter, incremented by the indexer on every processed reorg. When the client sends `known_reorg_epoch=X`, the server queries:

```sql
SELECT MIN(from_block) FROM reorg_log
WHERE chain_id = $chain_id AND id > $known_epoch
```

If any reorgs occurred since `X`, `chain.invalidated_from_block` is set to `MIN(from_block)`. The client:
1. Discards any cached items with `block_number >= invalidated_from_block`
2. Resets `head_cursor` to a point before `invalidated_from_block`
3. Updates its stored `reorg_epoch` to the new value

If `known_reorg_epoch` is omitted, `invalidated_from_block` is always `null`.

**Live sub-10s feeds**

Cursor-paginated REST is the wrong transport for a live trade feed. A future SSE/WebSocket endpoint should push new events and emit `{ type: "reorg", invalidated_from_block: N }` messages. Out of scope for this design.

---

## REST API

Base path: `/v1`

### Tokens

#### `GET /v1/tokens`

List tokens, optionally filtered and sorted.

**Query Parameters**

| Name             | Type    | Description                                  |
|------------------|---------|----------------------------------------------|
| `chain_id`       | integer | **Required**                                 |
| `is_derc20`      | boolean | Filter by derc20 status                      |
| `is_creator_coin`| boolean | Filter                                       |
| `creator`        | string  | Filter by creator address (hex)              |
| `integrator`     | string  | Filter by integrator address (hex)           |
| `sort_by`        | enum    | `created_at` (default), `symbol`             |
| `order`          | enum    | `desc` (default), `asc`                      |
| `cursor`, `limit`|         | See Pagination                               |

**Response** `200 OK`

```json
{
  "data": [
    {
      "id": 1,
      "chain_id": 8453,
      "block_number": 12345678,
      "created_at": 1710504000,
      "address": "0xabc...",
      "creator_address": "0xdef...",
      "integrator": "0x111...",
      "pool": "0x222...",
      "name": "Example Token",
      "symbol": "EXT",
      "decimals": 18,
      "total_supply": "1000000000000000000000000",
      "is_derc20": true,
      "is_creator_coin": false,
      "is_content_coin": false
    }
  ],
  "pagination": { "next_cursor": "...", "has_more": true }
}
```

#### `GET /v1/tokens/:address`

Fetch a single token by address.

**Query Parameters**: `chain_id` (required)

**Response** `200 OK` — single token object (no envelope)

---

### Pools

#### `GET /v1/pools/new`

Temporal feed of newly created pools, sorted by `(block_number DESC, id DESC)`. Uses **feed pagination** — supports `head_cursor` and `new_since_head`. See Pagination section.

**Query Parameters**

| Name                | Type    | Description                                                   |
|---------------------|---------|---------------------------------------------------------------|
| `chain_id`          | integer | **Required**                                                  |
| `type`              | string  | Filter by pool type (`v4`, `v3`, `dhook`)                     |
| `integrator`        | string  | Filter by integrator address                                  |
| `migrated`          | boolean | Filter: has `migrated_at` set                                 |
| `unconfirmed`       | boolean | Bypass confirmed horizon — returns `X-Reorg-Risk: true`       |
| `cursor`            | string  | Tail cursor (older items)                                     |
| `head_cursor`       | string  | Optional — bundles new arrivals in `new_since_head`           |
| `limit`             | integer | Default 50, max 500                                           |
| `known_reorg_epoch` | integer | See Reorg Safety                                              |

**Response** uses the feed envelope (`new_since_head`, `data`, `pagination.head_cursor`, `chain.confirmed_tip`).

---

#### `GET /v1/pools`

Ranked pool leaderboard. Sort keys are mutable; uses **leaderboard pagination** backed by the `pool_leaderboard_snapshot` table. No `head_cursor` — start a new session (no cursor) to pick up the latest snapshot.

All four sort modes share one query shape: the read is always keyset seek on `(chain_id, sort_key, snapshot_id, sort_val, id)`, where `sort_key` is the string-literal column name (`'active_liquidity_usd'`, `'tvl_usd'`, `'volume_24h_usd'`, `'market_cap_usd'`). The composite index `idx_plsnap_keyset` covers every mode equally — there is no per-sort-mode query.

The first page resolves `snapshot_id = (SELECT MAX(snapshot_id) FROM pool_leaderboard_snapshot WHERE chain_id = $1 AND sort_key = $2)` and joins to `pools` / `pool_state` for the full response object. Subsequent pages reuse the `snapshot_id` carried in the cursor. Pools whose `sort_val` is null in the snapshot sort last under `order=desc` and first under `order=asc` — the index is declared `DESC NULLS LAST`, and Postgres scans it backwards with flipped null ordering for ASC.

**Direction**. For `order=desc` (default), the keyset predicate is `(sort_val, id) < ($cursor_val, $cursor_id)` with `ORDER BY sort_val DESC NULLS LAST, id DESC`. For `order=asc`, the predicate flips to `(sort_val, id) > ($cursor_val, $cursor_id)` with `ORDER BY sort_val ASC NULLS FIRST, id ASC`. Both shapes use `idx_plsnap_keyset` — the ASC case via a backward index scan — so neither introduces a sort.

**Filter interaction**. `base_token`, `quote_token`, `type`, and `migrated` are columns on `pools`, not on the snapshot. The query applies them in the join to `pools` *after* the keyset seek on the snapshot. For highly selective filters (e.g. a `quote_token` matching 1% of pools) the server may need to scan many snapshot rows before assembling a full `limit`-sized page; the server caps this post-filter scan at an internal multiplier of `limit` to bound worst-case latency, returning `has_more=true` whenever the cap is hit so the cursor still advances. Clients with highly selective filters should expect more pages with fewer rows rather than a single full page.

**Query Parameters**

| Name                | Type    | Description                                                                           |
|---------------------|---------|---------------------------------------------------------------------------------------|
| `chain_id`          | integer | **Required**                                                                          |
| `base_token`        | string  | Filter by base token address                                                          |
| `quote_token`       | string  | Filter by quote token address                                                         |
| `type`              | string  | Filter by pool type                                                                   |
| `migrated`          | boolean | Filter: has `migrated_at` set                                                         |
| `sort_by`           | enum    | `active_liquidity_usd` (default), `tvl_usd`, `volume_24h_usd`, `market_cap_usd`      |
| `order`             | enum    | `desc` (default), `asc`                                                               |
| `cursor`            | string  | Opaque leaderboard cursor (encodes sort value + id + snapshot_id)                     |
| `limit`             | integer | Default 50, max 500                                                                   |
| `known_reorg_epoch` | integer | See Reorg Safety                                                                      |

Sort semantics (all read from `pool_leaderboard_snapshot.sort_val` where `sort_key = '<column>'`):
- `active_liquidity_usd`: in-range liquidity only. For constant-product pools this equals TVL; for v4/CLMM pools it reflects only liquidity currently earning fees — a meaningfully different signal.
- `tvl_usd`: total value locked in the pool, at snapshot capture time.
- `volume_24h_usd`: trailing 24-hour swap volume in USD.
- `market_cap_usd`: token market cap at snapshot capture time.

`sort_val` reflects the `pool_state` column value at the block the snapshot was taken (`chain.data_as_of_block`), not the live value. Within one session all rankings are frozen to that block.

**Response** uses the leaderboard envelope (`data`, `pagination.next_cursor`, `chain.data_as_of_block`).

**Pool Object**

```json
{
  "id": 42,
  "chain_id": 8453,
  "block_number": 12000000,
  "created_at": 1704067200,
  "address": "0xpool...",
  "base_token": "0xbase...",
  "quote_token": "0xquote...",
  "is_token_0": true,
  "type": "v4",
  "integrator": "0xintegrator...",
  "initializer": "0xinit...",
  "fee": 3000,
  "min_threshold": "0",
  "max_threshold": "1000000000",
  "migrator": null,
  "migrated_at": null,
  "migration_pool": null,
  "migrated_from": null,
  "migration_type": null,
  "lock_duration": 0,
  "beneficiaries": [
    { "address": "0xbeneficiary...", "amount": "500000000000000000000000" }
  ],
  "pool_key": {
    "currency0": "0xcurrency0...",
    "currency1": "0xcurrency1...",
    "fee": 3000,
    "tick_spacing": 60,
    "hooks": "0xhooks..."
  },
  "starting_time": 1704067200,
  "ending_time": 1735689600
}
```

`beneficiaries[].amount` is a decimal string (`uint96` → `NUMERIC`). `pool_key` fields `currency0`, `currency1`, `hooks` use chain-aware address format.

#### `GET /v1/pools/:address`

Single pool by address. **Query**: `chain_id`.

#### `GET /v1/pools/:address/state`

Current state for a pool (latest `pool_state` row).

**Query**: `chain_id`

**Response**

```json
{
  "chain_id": 8453,
  "pool_id": "0xpool...",
  "block_number": 13000000,
  "block_timestamp": 1717200000,
  "tick": -887272,
  "sqrt_price": "79228162514264337593543950336",
  "price": "1.000000",
  "active_liquidity": "500000000000",
  "volume_24h_usd": "120000.50",
  "price_change_1h": "0.025",
  "price_change_24h": "-0.03",
  "swap_count_24h": 482,
  "amount0": "5000000000",
  "amount1": "5000000000",
  "tvl_usd": "50000.00",
  "market_cap_usd": "2000000.00",
  "total_supply": "1000000000000000000000000",
  "active_liquidity_usd": "45000.00"
}
```

#### `GET /v1/pools/:address/snapshots`

OHLCV snapshots for a pool, block-ordered.

**Query Parameters**

| Name           | Type    | Description              |
|----------------|---------|--------------------------|
| `chain_id`     | integer | **Required**             |
| `from_block`   | integer | Inclusive lower bound    |
| `to_block`     | integer | Inclusive upper bound    |
| `order`        | enum    | `desc` (default), `asc`  |
| `cursor`, `limit` |      | See Pagination           |

**Snapshot Object**

```json
{
  "chain_id": 8453,
  "pool_id": "0xpool...",
  "block_number": 12500000,
  "block_timestamp": 1711929600,
  "price_open": "1.05",
  "price_close": "1.08",
  "price_high": "1.12",
  "price_low": "1.03",
  "active_liquidity": "400000000000",
  "volume0": "100000000",
  "volume1": "100000000",
  "swap_count": 22,
  "amount0": "4000000000",
  "amount1": "4000000000",
  "tvl_usd": "48000.00",
  "market_cap_usd": "1900000.00",
  "active_liquidity_usd": "43000.00",
  "volume_usd": "5000.00"
}
```

#### `GET /v1/pools/:address/swaps`

Swaps for a pool.

**Query**: `chain_id`, `is_buy` (boolean filter), `cursor`, `limit`, `order`

**Swap Object**

```json
{
  "id": 1001,
  "chain_id": 8453,
  "tx_id": "0xtx...",
  "block_number": 12600000,
  "timestamp": 1713170200,
  "pool": "0xpool...",
  "asset": "0xasset...",
  "amount_in": "1000000000000000000",
  "amount_out": "980000000",
  "is_buy": true,
  "current_tick": -887100,
  "graduation_tick": -887272,
  "value_usd": "1000"
}
```

#### `GET /v1/pools/:address/metrics`

Historical pool metrics.

**Query**: `chain_id`, `from` (ISO timestamp), `to` (ISO timestamp), `cursor`, `limit`, `order`

**Pool Metrics Object**

```json
{
  "id": 5000,
  "chain_id": 8453,
  "block_number": 12700000,
  "timestamp": 1714521600,
  "last_swap_timestamp": 1714521300,
  "address": "0xpool...",
  "total_fee0": "10000000",
  "total_fee1": "10000000",
  "total_proceeds": "500000000",
  "total_tokens_sold": "100000000000",
  "graduation_balance": "900000000",
  "graduation_percentage": 0.45,
  "graduation_tick": -887272,
  "reserves0": "5000000000",
  "reserves1": "5000000000",
  "market_cap_usd": 2000000,
  "liquidity_usd": 50000,
  "volume_usd": 5000,
  "tick": -887100
}
```

#### `GET /v1/pools/:address/positions`

Open positions for a pool.

**Query**: `chain_id`, `owner` (address filter), `cursor`, `limit`

**Position Object**

```json
{
  "id": 77,
  "chain_id": 8453,
  "pool": "0xpool...",
  "owner": "0xowner...",
  "tick_lower": -887272,
  "tick_upper": 887272,
  "liquidity": "1000000000000",
  "created_at": 1705276800
}
```

#### `GET /v1/pools/:address/fees`

Cumulated fees per beneficiary.

**Query**: `chain_id`, `beneficiary` (optional filter)

```json
{
  "data": [
    {
      "pool_id": "0xpool...",
      "beneficiary": "0xbeneficiary...",
      "token0_fees": "500000000",
      "token1_fees": "500000000"
    }
  ]
}
```

---

### Swaps

#### `GET /v1/swaps`

Cross-pool swap feed.

**Query Parameters**: `chain_id`, `pool` (address), `asset` (address), `is_buy`, `from` (timestamp), `to` (timestamp), `cursor`, `limit`, `order`

---

### Prices

#### `GET /v1/prices`

Token price history.

**Query Parameters**

| Name          | Type    | Description                     |
|---------------|---------|---------------------------------|
| `chain_id`    | integer | **Required**                    |
| `token`       | string  | **Required** — token address    |
| `quote_token` | string  | Optional quote token filter     |
| `from`        | string  | ISO timestamp lower bound       |
| `to`          | string  | ISO timestamp upper bound       |
| `cursor`, `limit` |     | See Pagination                  |

**Price Object**

```json
{
  "id": 9000,
  "timestamp": 1717200000,
  "block_number": 13000000,
  "chain_id": 8453,
  "token": "0xtoken...",
  "quote_token": "0xusdc...",
  "price": "1.0005"
}
```

---

### Users

#### `GET /v1/users/:address`

Fetch a user record.

**Query**: `chain_id`

```json
{
  "id": 200,
  "chain_id": 8453,
  "address": "0xuser...",
  "first_seen": 1704067200,
  "last_seen": 1717200000
}
```

#### `GET /v1/users/:address/portfolio`

Portfolio balances for a user.

**Query**: `chain_id`, `cursor`, `limit`

```json
{
  "data": [
    {
      "id": 500,
      "chain_id": 8453,
      "token": "0xtoken...",
      "balance": "5000000000000000000",
      "first_interacted": 1706745600,
      "last_interacted": 1717200000
    }
  ],
  "pagination": { "next_cursor": null, "has_more": false }
}
```

---

### Token Metrics

#### `GET /v1/tokens/:address/metrics`

Historical holder and market metrics for a token.

**Query**: `chain_id`, `from`, `to`, `cursor`, `limit`, `order`

```json
{
  "data": [
    {
      "id": 300,
      "chain_id": 8453,
      "block_number": 13000000,
      "timestamp": 1717200000,
      "address": "0xtoken...",
      "holder_count": 1234,
      "market_cap_usd": 2000000,
      "liquidity_usd": 50000,
      "volume_usd": 5000
    }
  ]
}
```

---

## GraphQL API

Endpoint: `POST /graphql`  
Playground (GET): `/graphql`

The GraphQL schema mirrors the REST types. It adds relationship traversal (e.g. `pool { token { ... } }`) and enables clients to fetch exactly the fields they need.

### Schema

```graphql
scalar Address   # 0x-prefixed hex string
scalar BigDecimal # arbitrary-precision decimal string
scalar DateTime  # ISO 8601 UTC

enum SortOrder { ASC DESC }

type PageInfo {
  nextCursor: String
  hasMore: Boolean!
}

# ── Types ───────────────────────────────────────────────────────────────────

type Token {
  id: Int!
  chainId: Int!
  blockNumber: Int!
  createdAt: DateTime!
  address: Address!
  creatorAddress: Address
  integrator: Address
  pool: Address
  name: String!
  symbol: String!
  decimals: Int!
  totalSupply: BigDecimal!
  isDerc20: Boolean!
  isCreatorCoin: Boolean!
  isContentCoin: Boolean!
  # resolved relationships
  poolState: PoolState
  metrics(limit: Int, cursor: String, order: SortOrder): TokenMetricsPage!
}

type Pool {
  id: Int!
  chainId: Int!
  blockNumber: Int!
  createdAt: DateTime!
  address: Address!
  baseToken: Address!
  quoteToken: Address!
  isToken0: Boolean!
  type: String!
  fee: Int
  minThreshold: BigDecimal
  maxThreshold: BigDecimal
  migratedAt: DateTime
  startingTime: DateTime
  endingTime: DateTime
  # resolved relationships
  state: PoolState
  snapshots(fromBlock: Int, toBlock: Int, limit: Int, cursor: String, order: SortOrder): PoolSnapshotPage!
  swaps(isBuy: Boolean, limit: Int, cursor: String, order: SortOrder): SwapPage!
  metrics(from: DateTime, to: DateTime, limit: Int, cursor: String, order: SortOrder): PoolMetricsPage!
  positions(owner: Address, limit: Int, cursor: String): PositionPage!
  fees: [CumulatedFees!]!
}

type PoolState {
  chainId: Int!
  poolId: Address!
  blockNumber: Int!
  blockTimestamp: DateTime!
  tick: Int
  sqrtPrice: BigDecimal
  price: BigDecimal
  activeLiquidity: BigDecimal
  volume24hUsd: BigDecimal
  priceChange1h: BigDecimal
  priceChange24h: BigDecimal
  swapCount24h: Int
  tvlUsd: BigDecimal
  marketCapUsd: BigDecimal
  totalSupply: BigDecimal
  activeLiquidityUsd: BigDecimal
}

type PoolSnapshot {
  chainId: Int!
  poolId: Address!
  blockNumber: Int!
  blockTimestamp: DateTime!
  priceOpen: BigDecimal!
  priceClose: BigDecimal!
  priceHigh: BigDecimal!
  priceLow: BigDecimal!
  activeLiquidity: BigDecimal
  volume0: BigDecimal
  volume1: BigDecimal
  swapCount: Int
  tvlUsd: BigDecimal
  marketCapUsd: BigDecimal
  activeLiquidityUsd: BigDecimal
  volumeUsd: BigDecimal
}

type Swap {
  id: Int!
  chainId: Int!
  txId: String!
  blockNumber: Int!
  timestamp: DateTime!
  pool: Address!
  asset: Address!
  amountIn: BigDecimal!
  amountOut: BigDecimal!
  isBuy: Boolean!
  currentTick: Int
  valueUsd: BigDecimal
}

type Position {
  id: Int!
  chainId: Int!
  pool: Address!
  owner: Address!
  tickLower: Int!
  tickUpper: Int!
  liquidity: BigDecimal!
  createdAt: DateTime!
}

type Price {
  id: Int!
  timestamp: DateTime!
  blockNumber: Int!
  chainId: Int!
  token: Address!
  quoteToken: Address!
  price: BigDecimal!
}

type CumulatedFees {
  chainId: Int!
  poolId: Address!
  beneficiary: Address!
  token0Fees: BigDecimal!
  token1Fees: BigDecimal!
}

type PoolMetrics {
  id: Int!
  chainId: Int!
  blockNumber: Int!
  timestamp: DateTime!
  address: Address!
  totalFee0: BigDecimal
  totalFee1: BigDecimal
  totalProceeds: BigDecimal
  totalTokensSold: BigDecimal
  graduationBalance: BigDecimal
  graduationPercentage: Float
  marketCapUsd: BigDecimal
  liquidityUsd: BigDecimal
  volumeUsd: BigDecimal
  tick: Int
}

type TokenMetrics {
  id: Int!
  chainId: Int!
  blockNumber: Int!
  timestamp: DateTime!
  address: Address!
  holderCount: Int
  marketCapUsd: BigDecimal
  liquidityUsd: BigDecimal
  volumeUsd: BigDecimal
}

# ── Page wrappers ────────────────────────────────────────────────────────────

type TokenPage       { data: [Token!]!        pageInfo: PageInfo! }
type PoolPage        { data: [Pool!]!          pageInfo: PageInfo! }
type PoolSnapshotPage{ data: [PoolSnapshot!]!  pageInfo: PageInfo! }
type SwapPage        { data: [Swap!]!          pageInfo: PageInfo! }
type PositionPage    { data: [Position!]!      pageInfo: PageInfo! }
type PricePage       { data: [Price!]!         pageInfo: PageInfo! }
type PoolMetricsPage { data: [PoolMetrics!]!   pageInfo: PageInfo! }
type TokenMetricsPage{ data: [TokenMetrics!]!  pageInfo: PageInfo! }

# ── Root queries ─────────────────────────────────────────────────────────────

type Query {
  token(chainId: Int!, address: Address!): Token
  tokens(
    chainId: Int!
    isDerc20: Boolean
    isCreatorCoin: Boolean
    creator: Address
    integrator: Address
    sortBy: String
    order: SortOrder
    cursor: String
    limit: Int
  ): TokenPage!

  pool(chainId: Int!, address: Address!): Pool
  pools(
    chainId: Int!
    baseToken: Address
    quoteToken: Address
    type: String
    migrated: Boolean
    order: SortOrder
    cursor: String
    limit: Int
  ): PoolPage!

  swaps(
    chainId: Int!
    pool: Address
    asset: Address
    isBuy: Boolean
    from: DateTime
    to: DateTime
    order: SortOrder
    cursor: String
    limit: Int
  ): SwapPage!

  prices(
    chainId: Int!
    token: Address!
    quoteToken: Address
    from: DateTime
    to: DateTime
    cursor: String
    limit: Int
  ): PricePage!
}
```

### Example Query

```graphql
query PoolOHLCV($chainId: Int!, $pool: Address!, $fromBlock: Int!) {
  pool(chainId: $chainId, address: $pool) {
    address
    state {
      price
      tvlUsd
      volume24hUsd
      priceChange24h
    }
    snapshots(fromBlock: $fromBlock, limit: 100, order: ASC) {
      pageInfo { nextCursor hasMore }
      data {
        blockTimestamp
        priceOpen priceClose priceHigh priceLow
        volumeUsd
        swapCount
      }
    }
  }
}
```

---

## Rust Reference Implementation

The API is a second binary in the same crate, gated behind an `api` feature flag. All existing dependencies (`tokio-postgres`, `deadpool-postgres`, `rust_decimal`, `serde`, `chrono`, `hex`, `bs58`, `anyhow`, `tracing`) are already present — only the HTTP/GraphQL stack is new.

**`Cargo.toml` additions:**

```toml
[features]
# existing features unchanged …
api = [
    "dep:axum",
    "dep:tower",
    "dep:tower-http",
    "dep:async-graphql",
    "dep:async-graphql-axum",
]

[dependencies]
# existing deps unchanged, except add with-chrono-0_4 to tokio-postgres:
tokio-postgres = { version = "0.7", features = ["with-serde_json-1", "with-chrono-0_4"] }

# new, api-only:
axum               = { version = "0.8", optional = true }
tower              = { version = "0.5", optional = true }
tower-http         = { version = "0.6", features = ["cors", "compression-gzip", "trace"], optional = true }
async-graphql      = { version = "7", optional = true }
async-graphql-axum = { version = "7", optional = true }

[[bin]]
name = "doppler-api"
path = "src/bin/api.rs"
required-features = ["api"]
```

Build: `cargo run --bin doppler-api --features api`

### Types (`src/api/types.rs`)

```rust
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio_postgres::Row;
use crate::types::chain::{ChainAddress, TxId};

// ── Address API wrappers ──────────────────────────────────────────────────
//
// ChainAddress and TxId from src/types/chain.rs are the canonical in-memory
// types. The API layer adds serde impls for JSON serialization.
//
// Wire format (matches ChainAddress::Display):
//   EVM address  (20 bytes) → "0x{40 hex chars}"
//   Solana pubkey (32 bytes) → base58
//   EVM tx hash  (32 bytes) → "0x{64 hex chars}"
//   Solana sig   (64 bytes) → base58
//
// Disambiguation: 0x prefix → EVM, no prefix → Solana.

// tokio_postgres reads BYTEA as Vec<u8>; we convert via parse_chain_address
// in TryFrom<&Row> impls.

pub fn parse_chain_address(bytes: Vec<u8>) -> Result<ChainAddress, String> {
    match bytes.len() {
        20 => Ok(ChainAddress::Evm(bytes.try_into().unwrap())),
        32 => Ok(ChainAddress::Solana(bytes.try_into().unwrap())),
        n  => Err(format!("expected 20 or 32 bytes, got {n}")),
    }
}

pub fn parse_tx_id(bytes: Vec<u8>) -> Result<TxId, String> {
    match bytes.len() {
        32 => Ok(TxId::Evm(bytes.try_into().unwrap())),
        64 => Ok(TxId::Solana(bytes.try_into().unwrap())),
        n  => Err(format!("expected 32 or 64 bytes, got {n}")),
    }
}

// Serde wrappers for API serialization (ChainAddress::Display handles both formats)
pub mod serde_chain_address {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(addr: &ChainAddress, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&addr.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ChainAddress, D::Error> {
        let raw = String::deserialize(d)?;
        if let Some(hex) = raw.strip_prefix("0x") {
            let bytes = hex::decode(hex).map_err(serde::de::Error::custom)?;
            parse_chain_address(bytes).map_err(serde::de::Error::custom)
        } else {
            // base58 Solana pubkey
            let bytes = bs58::decode(&raw).into_vec().map_err(serde::de::Error::custom)?;
            parse_chain_address(bytes).map_err(serde::de::Error::custom)
        }
    }
}

pub mod serde_tx_id {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(tx: &TxId, s: S) -> Result<S::Ok, S::Error> {
        let encoded = match tx {
            TxId::Evm(h)  => format!("0x{}", hex::encode(h)),
            TxId::Solana(sig) => bs58::encode(sig).into_string(),
        };
        s.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<TxId, D::Error> {
        let raw = String::deserialize(d)?;
        if let Some(hex) = raw.strip_prefix("0x") {
            let bytes = hex::decode(hex).map_err(serde::de::Error::custom)?;
            parse_tx_id(bytes).map_err(serde::de::Error::custom)
        } else {
            let bytes = bs58::decode(&raw).into_vec().map_err(serde::de::Error::custom)?;
            parse_tx_id(bytes).map_err(serde::de::Error::custom)
        }
    }
}

// Helper: serialize DateTime<Utc> as unix seconds i64
pub mod serde_unix {
    use chrono::{DateTime, Utc};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(dt: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_i64(dt.timestamp())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        use chrono::TimeZone;
        let ts = i64::deserialize(d)?;
        Utc.timestamp_opt(ts, 0)
            .single()
            .ok_or_else(|| serde::de::Error::custom("invalid unix timestamp"))
    }
}

// ── JSONB inner types ─────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Beneficiary {
    #[serde(with = "serde_chain_address")]
    pub address: ChainAddress,
    pub amount: Decimal,          // uint96 stored as NUMERIC
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PoolKey {
    #[serde(with = "serde_chain_address")]
    pub currency0: ChainAddress,
    #[serde(with = "serde_chain_address")]
    pub currency1: ChainAddress,
    pub fee: u32,                 // uint24
    pub tick_spacing: i32,        // int24
    #[serde(with = "serde_chain_address")]
    pub hooks: ChainAddress,
}

// ── REST response types ───────────────────────────────────────────────────

/// Feed pagination (immutable sort key — /pools/new, /swaps).
#[derive(Debug, Serialize)]
pub struct FeedPage<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_since_head: Option<Vec<T>>,
    pub data: Vec<T>,
    pub pagination: FeedPageInfo,
    pub chain: ChainMeta,
}

#[derive(Debug, Serialize)]
pub struct FeedPageInfo {
    pub next_cursor: Option<String>,
    pub head_cursor: Option<String>,
    pub has_more: bool,
    pub has_more_new: bool,
}

/// Leaderboard pagination (mutable sort key — /pools).
#[derive(Debug, Serialize)]
pub struct LeaderboardPage<T> {
    pub data: Vec<T>,
    pub pagination: LeaderboardPageInfo,
    pub chain: ChainMeta,
}

#[derive(Debug, Serialize)]
pub struct LeaderboardPageInfo {
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Serialize)]
pub struct ChainMeta {
    pub confirmed_tip: i64,
    pub reorg_epoch: i64,
    /// Non-null when reorg_epoch advanced since the client's known_reorg_epoch.
    pub invalidated_from_block: Option<i64>,
    /// Leaderboard only: block_number the active ranking snapshot was captured at.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_as_of_block: Option<i64>,
}

// ── DB row types ─────────────────────────────────────────────────────────
//
// tokio_postgres reads BYTEA as Vec<u8> and TIMESTAMPTZ as DateTime<Utc>
// (requires the `with-chrono-0_4` feature). ChainAddress/TxId are built
// via parse_chain_address/parse_tx_id in TryFrom<&Row> impls.
// serde_unix serializes DateTime<Utc> as unix seconds in responses.

// Helper: map a row decode error into tokio_postgres::Error
fn addr_err(e: String) -> tokio_postgres::Error {
    // tokio_postgres doesn't expose a public constructor for custom decode
    // errors; use a conversion through Box<dyn Error>.
    panic!("address decode error: {e}")
    // In production code, use a custom error type that wraps this rather
    // than panicking — shown simplified here for brevity.
}

#[derive(Debug, Serialize)]
pub struct TokenRow {
    pub id: i64,
    pub chain_id: i64,
    pub block_number: i64,
    #[serde(serialize_with = "serde_unix::serialize")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "serde_chain_address")]
    pub address: ChainAddress,
    #[serde(with = "serde_chain_address", skip_serializing_if = "Option::is_none")]
    pub creator_address: Option<ChainAddress>,
    #[serde(with = "serde_chain_address", skip_serializing_if = "Option::is_none")]
    pub integrator: Option<ChainAddress>,
    #[serde(with = "serde_chain_address", skip_serializing_if = "Option::is_none")]
    pub pool: Option<ChainAddress>,
    pub name: String,
    pub symbol: String,
    pub decimals: i16,
    pub total_supply: Decimal,
    pub is_derc20: bool,
    pub is_creator_coin: bool,
    pub is_content_coin: bool,
}

impl TryFrom<&Row> for TokenRow {
    type Error = tokio_postgres::Error;
    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let addr = |col: &str| -> Result<ChainAddress, tokio_postgres::Error> {
            parse_chain_address(row.try_get::<_, Vec<u8>>(col)?).map_err(addr_err)
        };
        let addr_opt = |col: &str| -> Result<Option<ChainAddress>, tokio_postgres::Error> {
            row.try_get::<_, Option<Vec<u8>>>(col)?
               .map(|b| parse_chain_address(b).map_err(addr_err))
               .transpose()
        };
        Ok(Self {
            id:              row.try_get("id")?,
            chain_id:        row.try_get("chain_id")?,
            block_number:    row.try_get("block_number")?,
            created_at:      row.try_get("created_at")?,
            address:         addr("address")?,
            creator_address: addr_opt("creator_address")?,
            integrator:      addr_opt("integrator")?,
            pool:            addr_opt("pool")?,
            name:            row.try_get("name")?,
            symbol:          row.try_get("symbol")?,
            decimals:        row.try_get("decimals")?,
            total_supply:    row.try_get("total_supply")?,
            is_derc20:       row.try_get("is_derc20")?,
            is_creator_coin: row.try_get("is_creator_coin")?,
            is_content_coin: row.try_get("is_content_coin")?,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct PoolStateRow {
    pub chain_id: i64,
    #[serde(with = "serde_chain_address")]
    pub pool_id: ChainAddress,
    pub block_number: i64,
    #[serde(serialize_with = "serde_unix::serialize")]
    pub block_timestamp: DateTime<Utc>,
    pub tick: Option<i32>,
    pub sqrt_price: Option<Decimal>,
    pub price: Option<Decimal>,
    pub active_liquidity: Option<Decimal>,
    pub volume_24h_usd: Option<Decimal>,
    pub price_change_1h: Option<Decimal>,
    pub price_change_24h: Option<Decimal>,
    pub swap_count_24h: Option<i32>,
    pub amount0: Option<Decimal>,
    pub amount1: Option<Decimal>,
    pub tvl_usd: Option<Decimal>,
    pub market_cap_usd: Option<Decimal>,
    pub total_supply: Option<Decimal>,
    pub active_liquidity_usd: Option<Decimal>,
}

#[derive(Debug, Serialize)]
pub struct PoolSnapshotRow {
    pub chain_id: i64,
    #[serde(with = "serde_chain_address")]
    pub pool_id: ChainAddress,
    pub block_number: i64,
    #[serde(serialize_with = "serde_unix::serialize")]
    pub block_timestamp: DateTime<Utc>,
    pub price_open: Decimal,
    pub price_close: Decimal,
    pub price_high: Decimal,
    pub price_low: Decimal,
    pub active_liquidity: Option<Decimal>,
    pub volume0: Decimal,
    pub volume1: Decimal,
    pub swap_count: i32,
    pub amount0: Option<Decimal>,
    pub amount1: Option<Decimal>,
    pub tvl_usd: Option<Decimal>,
    pub market_cap_usd: Option<Decimal>,
    pub active_liquidity_usd: Option<Decimal>,
    pub volume_usd: Option<Decimal>,
}

impl TryFrom<&Row> for PoolSnapshotRow {
    type Error = tokio_postgres::Error;
    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id:             row.try_get("chain_id")?,
            pool_id:              parse_chain_address(row.try_get::<_, Vec<u8>>("pool_id")?)
                                      .map_err(addr_err)?,
            block_number:         row.try_get("block_number")?,
            block_timestamp:      row.try_get("block_timestamp")?,
            price_open:           row.try_get("price_open")?,
            price_close:          row.try_get("price_close")?,
            price_high:           row.try_get("price_high")?,
            price_low:            row.try_get("price_low")?,
            active_liquidity:     row.try_get("active_liquidity")?,
            volume0:              row.try_get("volume0")?,
            volume1:              row.try_get("volume1")?,
            swap_count:           row.try_get("swap_count")?,
            amount0:              row.try_get("amount0")?,
            amount1:              row.try_get("amount1")?,
            tvl_usd:              row.try_get("tvl_usd")?,
            market_cap_usd:       row.try_get("market_cap_usd")?,
            active_liquidity_usd: row.try_get("active_liquidity_usd")?,
            volume_usd:           row.try_get("volume_usd")?,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct SwapRow {
    pub id: i64,
    pub chain_id: i64,
    #[serde(with = "serde_tx_id")]
    pub tx_id: TxId,
    pub block_number: i64,
    #[serde(serialize_with = "serde_unix::serialize")]
    pub timestamp: DateTime<Utc>,
    #[serde(with = "serde_chain_address")]
    pub pool: ChainAddress,
    #[serde(with = "serde_chain_address")]
    pub asset: ChainAddress,
    pub amount_in: i64,
    pub amount_out: i64,
    pub is_buy: bool,
    pub current_tick: Option<i32>,
    pub value_usd: Option<i64>,
}

impl TryFrom<&Row> for SwapRow {
    type Error = tokio_postgres::Error;
    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id:           row.try_get("id")?,
            chain_id:     row.try_get("chain_id")?,
            tx_id:        parse_tx_id(row.try_get::<_, Vec<u8>>("tx_id")?)
                              .map_err(addr_err)?,
            block_number: row.try_get("block_number")?,
            timestamp:    row.try_get("timestamp")?,
            pool:         parse_chain_address(row.try_get::<_, Vec<u8>>("pool")?)
                              .map_err(addr_err)?,
            asset:        parse_chain_address(row.try_get::<_, Vec<u8>>("asset")?)
                              .map_err(addr_err)?,
            amount_in:    row.try_get("amountIn")?,
            amount_out:   row.try_get("amountOut")?,
            is_buy:       row.try_get("is_buy")?,
            current_tick: row.try_get("current_tick")?,
            value_usd:    row.try_get("value_usd")?,
        })
    }
}

// ── Cursor for keyset pagination ──────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Cursor {
    /// Feed endpoints: sort by (block_number, id) — immutable.
    BlockId { block: i64, id: i64 },
    /// Time-ordered endpoints (swaps, prices).
    TimestampId { ts: i64, id: i64 },
    /// Leaderboard endpoints: sort by mutable value column.
    /// snapshot_id pins reads to a single row in pool_leaderboard_snapshot
    /// for session consistency; expires with the snapshot's retention window.
    ValId { val: Decimal, id: i64, snapshot_id: i64 },
}

impl Cursor {
    pub fn encode(&self) -> String {
        let json = serde_json::to_string(self).unwrap();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json.as_bytes())
    }

    pub fn decode(s: &str) -> anyhow::Result<Self> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s)?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}
```

Also add `bs58` to dependencies:

```toml
bs58 = "0.5"
```

### Query Layer (`src/api/queries.rs`)

`ChainAddress::as_slice()` returns `&[u8]`, which `tokio_postgres` binds as `BYTEA`. Every query runs inside a `REPEATABLE READ` read-only transaction to prevent mid-request inconsistency if a reorg is being processed concurrently. `active_versions` is joined on every source-versioned table.

```rust
use deadpool_postgres::Pool;
use tokio_postgres::IsolationLevel;
use crate::types::chain::ChainAddress;
use crate::api::types::{Cursor, PoolSnapshotRow, SwapRow};

/// Run `f` inside a REPEATABLE READ read-only transaction.
async fn with_snapshot<F, T>(pool: &Pool, f: F) -> anyhow::Result<T>
where
    F: for<'c> AsyncFnOnce(&'c tokio_postgres::Transaction<'c>) -> anyhow::Result<T>,
{
    let mut client = pool.get().await?;
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let result = f(&tx).await?;
    tx.commit().await?;
    Ok(result)
}

pub async fn list_pool_snapshots(
    pool: &Pool,
    chain_id: i64,
    pool_id: &ChainAddress,
    from_block: Option<i64>,
    to_block: Option<i64>,
    cursor: Option<Cursor>,
    limit: i64,
) -> anyhow::Result<Vec<PoolSnapshotRow>> {
    let cursor_block: Option<i64> = match &cursor {
        Some(Cursor::BlockId { block, .. }) => Some(*block),
        _ => None,
    };

    with_snapshot(pool, async |tx| {
        let rows = tx.query(
            "SELECT DISTINCT ON (ps.block_number)
                 ps.chain_id, ps.pool_id, ps.block_number, ps.block_timestamp,
                 ps.price_open, ps.price_close, ps.price_high, ps.price_low,
                 ps.active_liquidity, ps.volume0, ps.volume1, ps.swap_count,
                 ps.amount0, ps.amount1, ps.tvl_usd, ps.market_cap_usd,
                 ps.active_liquidity_usd, ps.volume_usd
             FROM pool_snapshots ps
             INNER JOIN active_versions av USING (source, source_version)
             WHERE ps.chain_id = $1
               AND ps.pool_id = $2
               AND ($3::BIGINT IS NULL OR ps.block_number >= $3)
               AND ($4::BIGINT IS NULL OR ps.block_number <= $4)
               AND ($5::BIGINT IS NULL OR ps.block_number < $5)
             ORDER BY ps.block_number DESC
             LIMIT $6",
            &[
                &chain_id,
                &pool_id.as_slice(),   // &[u8] → BYTEA
                &from_block,
                &to_block,
                &cursor_block,
                &(limit + 1),
            ],
        ).await?;
        rows.iter().map(PoolSnapshotRow::try_from).collect::<Result<_, _>>()
           .map_err(Into::into)
    }).await
}

pub async fn list_swaps(
    pool: &Pool,
    chain_id: i64,
    pool_addr: Option<&ChainAddress>,
    is_buy: Option<bool>,
    cursor: Option<Cursor>,
    limit: i64,
) -> anyhow::Result<Vec<SwapRow>> {
    let (cursor_ts, cursor_id): (Option<i64>, Option<i64>) = match cursor {
        Some(Cursor::TimestampId { ts, id }) => (Some(ts), Some(id)),
        _ => (None, None),
    };
    // Bind Option<&[u8]>: None → SQL NULL, Some → BYTEA
    let pool_bytes: Option<&[u8]> = pool_addr.map(|a| a.as_slice());

    with_snapshot(pool, async |tx| {
        let rows = tx.query(
            "SELECT s.id, s.chain_id, s.tx_id, s.block_number, s.timestamp,
                    s.pool, s.asset, s.\"amountIn\", s.\"amountOut\", s.is_buy,
                    s.current_tick, s.value_usd
             FROM swaps s
             INNER JOIN active_versions av USING (source, source_version)
             WHERE s.chain_id = $1
               AND ($2::BYTEA IS NULL OR s.pool = $2)
               AND ($3::BOOLEAN IS NULL OR s.is_buy = $3)
               AND (
                 $4::BIGINT IS NULL
                 OR (EXTRACT(EPOCH FROM s.timestamp)::BIGINT, s.id) < ($4, $5)
               )
             ORDER BY s.timestamp DESC, s.id DESC
             LIMIT $6",
            &[
                &chain_id,
                &pool_bytes,           // Option<&[u8]> — None binds NULL
                &is_buy,
                &cursor_ts,
                &cursor_id,
                &(limit + 1),
            ],
        ).await?;
        rows.iter().map(SwapRow::try_from).collect::<Result<_, _>>()
           .map_err(Into::into)
    }).await
}
```

### Handlers (`src/api/handlers.rs`)

Path segments arrive as plain `String` and are parsed with `parse_chain_address(hex::decode(...))`. Query parameters that are addresses (e.g. `?pool=0x...`) use a `ChainAddressParam` newtype with a custom `Deserialize` that calls `parse_chain_address`.

```rust
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use crate::api::{
    queries,
    types::{parse_chain_address, serde_chain_address, Cursor, FeedPage, FeedPageInfo, LeaderboardPage, LeaderboardPageInfo, PoolSnapshotRow},
    AppState,
};

// Newtype for query params — serde_chain_address::deserialize handles 0x/base58
#[derive(Deserialize)]
struct ChainAddressParam(
    #[serde(deserialize_with = "serde_chain_address::deserialize")]
    crate::types::chain::ChainAddress
);

// ── Error type ────────────────────────────────────────────────────────────

pub enum ApiError {
    BadAddress(String),
    BadCursor,
    Db(anyhow::Error),
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self { ApiError::Db(e) }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            ApiError::BadAddress(s) => (StatusCode::BAD_REQUEST, s),
            ApiError::BadCursor     => (StatusCode::BAD_REQUEST, "invalid cursor".into()),
            ApiError::Db(e)         => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        (status, msg).into_response()
    }
}

// ── Query params ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SnapshotQuery {
    pub chain_id: i64,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    pub order: Option<String>,
}

#[derive(Deserialize)]
pub struct SwapQuery {
    pub chain_id: i64,
    pub pool: Option<ChainAddressParam>,  // "0x..." or base58 deserialized automatically
    pub is_buy: Option<bool>,
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 { 50 }

// ── Handlers ──────────────────────────────────────────────────────────────

pub async fn pool_snapshots(
    State(state): State<AppState>,
    Path(address): Path<String>,   // path param arrives as raw string
    Query(q): Query<SnapshotQuery>,
) -> Result<Json<FeedPage<PoolSnapshotRow>>, ApiError> {
    // Convert path segment "0x{hex}" or base58 → ChainAddress
    let addr_bytes = if address.starts_with("0x") {
        hex::decode(&address[2..]).map_err(|e| ApiError::BadAddress(e.to_string()))?
    } else {
        bs58::decode(&address).into_vec().map_err(|e| ApiError::BadAddress(e.to_string()))?
    };
    let pool_id = parse_chain_address(addr_bytes)
        .map_err(ApiError::BadAddress)?;

    let cursor = q.cursor.as_deref()
        .map(Cursor::decode)
        .transpose()
        .map_err(|_| ApiError::BadCursor)?;

    let desc = q.order.as_deref() != Some("asc");
    let limit = q.limit.min(500);

    let mut rows = queries::list_pool_snapshots(
        &state.db,
        q.chain_id,
        &pool_id,
        q.from_block,
        q.to_block,
        cursor,
        limit,
        desc,
    )
    .await
    .map_err(anyhow::Error::from)?;

    let has_more = rows.len() as i64 > limit;
    if has_more { rows.pop(); }

    let next_cursor = if has_more {
        rows.last().map(|r| Cursor::BlockId { block: r.block_number, id: r.block_number }.encode())
    } else {
        None
    };

    Ok(Json(FeedPage {
        new_since_head: None,
        data: rows,
        pagination: FeedPageInfo { next_cursor, head_cursor: None, has_more, has_more_new: false },
        chain: state.chain_meta(q.chain_id, None).await?,
    }))
}

pub async fn list_swaps(
    State(state): State<AppState>,
    Query(q): Query<SwapQuery>,    // pool: Option<Address> auto-deserialized from "0x..."
) -> Result<Json<FeedPage<SwapRow>>, ApiError> {
    let cursor = q.cursor.as_deref()
        .map(Cursor::decode)
        .transpose()
        .map_err(|_| ApiError::BadCursor)?;

    let limit = q.limit.min(500);

    let mut rows = queries::list_swaps(
        &state.db,
        q.chain_id,
        q.pool.as_ref().map(|p| &p.0),  // Option<&ChainAddress> — None → NULL in SQL
        q.is_buy,
        cursor,
        limit,
    )
    .await
    .map_err(anyhow::Error::from)?;

    let has_more = rows.len() as i64 > limit;
    if has_more { rows.pop(); }

    let next_cursor = if has_more {
        rows.last().map(|r| Cursor::TimestampId { ts: r.timestamp, id: r.id }.encode())
    } else {
        None
    };

    Ok(Json(FeedPage {
        new_since_head: None,
        data: rows,
        pagination: FeedPageInfo { next_cursor, head_cursor: None, has_more, has_more_new: false },
        chain: state.chain_meta(q.chain_id, None).await?,
    }))
}
```

### GraphQL Address Scalar (`src/api/graphql/scalars.rs`)

The `Address` scalar wraps `ChainAddress`. `ChainAddress::Display` already produces the correct wire format for both variants, and the parser uses the `0x`-prefix heuristic to disambiguate.

```rust
use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use crate::types::chain::ChainAddress;
use crate::api::types::parse_chain_address;

pub struct GqlAddress(pub ChainAddress);

#[Scalar(name = "Address")]
impl ScalarType for GqlAddress {
    fn parse(value: Value) -> InputValueResult<Self> {
        let Value::String(s) = value else {
            return Err(InputValueError::expected_type(value));
        };
        let bytes = if let Some(hex) = s.strip_prefix("0x") {
            hex::decode(hex).map_err(|e| InputValueError::custom(e.to_string()))?
        } else {
            bs58::decode(&s).into_vec().map_err(|e| InputValueError::custom(e.to_string()))?
        };
        parse_chain_address(bytes)
            .map(GqlAddress)
            .map_err(InputValueError::custom)
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())   // ChainAddress::Display
    }
}
```

Usage in resolvers: accept `GqlAddress` as argument type, pass `.0` (the inner `ChainAddress`) to query functions.

### Router (`src/api/router.rs`)

```rust
use axum::{Router, routing::get};
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    trace::TraceLayer,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use crate::api::{handlers, graphql::build_schema, AppState};

pub fn build_router(state: AppState) -> Router {
    let schema = build_schema(state.db.clone());

    let api = Router::new()
        .route("/tokens",                     get(handlers::list_tokens))
        .route("/tokens/:address",            get(handlers::get_token))
        .route("/tokens/:address/metrics",    get(handlers::token_metrics))
        .route("/pools/new",                  get(handlers::new_pools_feed))
        .route("/pools",                      get(handlers::list_pools))
        .route("/pools/:address",             get(handlers::get_pool))
        .route("/pools/:address/state",       get(handlers::pool_state))
        .route("/pools/:address/snapshots",   get(handlers::pool_snapshots))
        .route("/pools/:address/swaps",       get(handlers::pool_swaps))
        .route("/pools/:address/metrics",     get(handlers::pool_metrics))
        .route("/pools/:address/positions",   get(handlers::pool_positions))
        .route("/pools/:address/fees",        get(handlers::pool_fees))
        .route("/swaps",                      get(handlers::list_swaps))
        .route("/prices",                     get(handlers::list_prices))
        .route("/users/:address",             get(handlers::get_user))
        .route("/users/:address/portfolio",   get(handlers::user_portfolio));

    Router::new()
        .nest("/v1", api)
        .route(
            "/graphql",
            get(graphql_playground).post(
                |req: GraphQLRequest| async move {
                    GraphQLResponse::from(schema.execute(req.into_inner()).await)
                },
            ),
        )
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new())
                .layer(CorsLayer::permissive()),
        )
        .with_state(state)
}

async fn graphql_playground() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}
```

### Entry Point (`src/bin/api.rs`)

```rust
use deadpool_postgres::{Config as PgConfig, ManagerConfig, RecyclingMethod, Runtime};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct AppState {
    pub db: deadpool_postgres::Pool,
}

impl AppState {
    /// Fetch confirmed tip, reorg_epoch, and optional invalidated_from_block
    /// for a given chain in a single query.
    pub async fn chain_meta(
        &self,
        chain_id: i64,
        known_reorg_epoch: Option<i64>,
    ) -> anyhow::Result<crate::api::types::ChainMeta> {
        let client = self.db.get().await?;
        let row = client.query_one(
            "SELECT confirmed_tip, reorg_epoch,
                    CASE WHEN $2::BIGINT IS NOT NULL AND reorg_epoch > $2
                         THEN (SELECT MIN(from_block) FROM reorg_log
                               WHERE chain_id = $1 AND reorg_epoch > $2)
                         ELSE NULL
                    END AS invalidated_from_block
             FROM chain_tips WHERE chain_id = $1",
            &[&chain_id, &known_reorg_epoch],
        ).await?;
        Ok(crate::api::types::ChainMeta {
            confirmed_tip: row.try_get("confirmed_tip")?,
            reorg_epoch:   row.try_get("reorg_epoch")?,
            invalidated_from_block: row.try_get("invalidated_from_block")?,
            data_as_of_block: None,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let db_url = std::env::var("DATABASE_URL")?;

    let mut cfg = PgConfig::new();
    cfg.url = Some(db_url);
    cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });

    let tls = MakeTlsConnector::new(TlsConnector::builder().build()?);
    let db = cfg.create_pool(Some(Runtime::Tokio1), tls)?;

    let state = AppState { db };
    let app = crate::api::router::build_router(state);

    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
```

---

## Solana-Specific Endpoints

These endpoints expose data from Solana-only tables (`sol_cpmm_positions`, `sol_cpmm_migrator`, `sol_prediction_markets`, `sol_prediction_entries`, `sol_prediction_claims`). All address fields are Solana pubkeys serialized as base58 strings. `block_height` is the Solana slot number (equivalent to `block_number` on EVM chains).

All amounts stored as `BIGINT` (u64 token quantities) are returned as **decimal strings** to avoid 53-bit JSON integer overflow.

---

### CPMM Positions

#### `GET /v1/cpmm/positions`

List CPMM liquidity positions, filterable by pool or owner.

**Query Parameters**

| Name         | Type    | Description                                   |
|--------------|---------|-----------------------------------------------|
| `chain_id`   | integer | **Required**                                  |
| `pool`       | string  | Filter by pool pubkey (base58)                |
| `owner`      | string  | Filter by owner pubkey (base58)               |
| `open_only`  | boolean | If `true`, exclude positions with `closed_at` set |
| `cursor`, `limit` |    | See Pagination                                |
| `order`      | enum    | `desc` (default), `asc` — ordered by `(block_height, id)` |

Uses **feed pagination**.

**Response** `200 OK`

```json
{
  "data": [
    {
      "id": 1,
      "chain_id": 1399811149,
      "block_height": 320000000,
      "created_at": 1710504000,
      "closed_at": null,
      "position": "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
      "pool": "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe8bT",
      "owner": "FXqTHkFkPkX4bJkMdwRNMKSUVV7UF2rDHqfSFj5LLWD",
      "position_id": 42
    }
  ],
  "pagination": { "next_cursor": "...", "has_more": false }
}
```

#### `GET /v1/cpmm/positions/:position`

Single CPMM position by position pubkey.

**Query Parameters**: `chain_id` (required)

**Response** `200 OK` — single position object (no envelope)

---

### CPMM Migrator

#### `GET /v1/cpmm/migrators`

List CPMM migrator launch records.

**Query Parameters**

| Name         | Type    | Description                              |
|--------------|---------|------------------------------------------|
| `chain_id`   | integer | **Required**                             |
| `migrated`   | boolean | Filter by migration status               |
| `cursor`, `limit` |    | See Pagination                           |
| `order`      | enum    | `desc` (default), `asc` — by `(block_height, id)` |

Uses **feed pagination**.

**Response** `200 OK`

```json
{
  "data": [
    {
      "id": 10,
      "chain_id": 1399811149,
      "block_height": 320000001,
      "created_at": 1710504100,
      "launch": "LaunchAcctPubkey11111111111111111111111111",
      "state": "StateAcctPubkey1111111111111111111111111111",
      "cpmm_config": "CpmmConfigPubkey111111111111111111111111111",
      "min_raise_quote": "1000000000",
      "pool": null,
      "quote_for_liquidity": null,
      "base_for_liquidity": null,
      "migrated": false
    }
  ],
  "pagination": { "next_cursor": null, "has_more": false }
}
```

`min_raise_quote`, `quote_for_liquidity`, `base_for_liquidity` are decimal strings (u64 token amounts).

#### `GET /v1/cpmm/migrators/:launch`

Single migrator record by launch pubkey.

**Query Parameters**: `chain_id` (required)

**Response** `200 OK` — single migrator object (no envelope)

---

### Prediction Markets

#### `GET /v1/prediction/markets`

List prediction markets.

**Query Parameters**

| Name         | Type    | Description                                     |
|--------------|---------|------------------------------------------------ |
| `chain_id`   | integer | **Required**                                    |
| `oracle`     | string  | Filter by oracle pubkey (base58)                |
| `quote_mint` | string  | Filter by quote mint pubkey (base58)            |
| `resolved`   | boolean | Filter: has `resolved_at` set                   |
| `cursor`, `limit` |    | See Pagination                                  |
| `order`      | enum    | `desc` (default), `asc` — by `(block_height, id)` |

Uses **feed pagination**.

**Response** `200 OK`

```json
{
  "data": [
    {
      "id": 5,
      "chain_id": 1399811149,
      "block_height": 320000010,
      "created_at": 1710505000,
      "resolved_at": null,
      "market": "MarketPubkey1111111111111111111111111111111",
      "oracle": "OraclePubkey111111111111111111111111111111",
      "quote_mint": "QuoteMintPubkey1111111111111111111111111111",
      "winner_mint": null,
      "claimable_supply": null,
      "total_pot": null
    }
  ],
  "pagination": { "next_cursor": null, "has_more": false }
}
```

`claimable_supply` and `total_pot` are decimal strings when present (u64 token amounts).

#### `GET /v1/prediction/markets/:market`

Single prediction market by market pubkey.

**Query Parameters**: `chain_id` (required)

**Response** `200 OK` — single market object (no envelope)

#### `GET /v1/prediction/markets/:market/entries`

Entries registered for a prediction market.

**Query Parameters**

| Name         | Type    | Description                              |
|--------------|---------|------------------------------------------|
| `chain_id`   | integer | **Required**                             |
| `is_winner`  | boolean | Filter by winner status                  |
| `cursor`, `limit` |    | See Pagination                           |

**Entry Object**

```json
{
  "id": 20,
  "chain_id": 1399811149,
  "block_height": 320000020,
  "created_at": 1710506000,
  "market": "MarketPubkey1111111111111111111111111111111",
  "oracle": "OraclePubkey111111111111111111111111111111",
  "entry_id": "EntryIdPubkey11111111111111111111111111111",
  "base_mint": "BaseMintPubkey1111111111111111111111111111",
  "contribution": "500000000",
  "is_winner": null
}
```

`contribution` is a decimal string (u64 token amount); `is_winner` is `null` until the market resolves.

#### `GET /v1/prediction/markets/:market/claims`

Reward claims against a prediction market, sorted by `(block_height DESC, id DESC)`.

**Query Parameters**

| Name         | Type    | Description                              |
|--------------|---------|------------------------------------------|
| `chain_id`   | integer | **Required**                             |
| `claimer`    | string  | Filter by claimer pubkey (base58)        |
| `cursor`, `limit` |    | See Pagination                           |

Uses **feed pagination**.

**Claim Object**

```json
{
  "id": 100,
  "chain_id": 1399811149,
  "block_height": 320000100,
  "created_at": 1710510000,
  "tx_id": "5KJp8Xv2mNrQHs3oT6vLuB9cJkFdEoR7pWyZ4nM1qSt...",
  "log_position": 0,
  "market": "MarketPubkey1111111111111111111111111111111",
  "claimer": "ClaimerPubkey11111111111111111111111111111",
  "burned_amount": "250000000",
  "reward_amount": "312500000",
  "total_burned": "500000000"
}
```

All amount fields are decimal strings (u64). `tx_id` is the base58-encoded 64-byte Solana transaction signature.

---

### GraphQL additions

The following types extend the GraphQL schema for Solana-specific data. They follow the same address conventions as existing types — all pubkeys serialize as base58 `Address` scalars.

```graphql
type CpmmPosition {
  id: Int!
  chainId: Int!
  blockHeight: Int!
  createdAt: DateTime!
  closedAt: DateTime
  position: Address!
  pool: Address!
  owner: Address!
  positionId: Int!
}

type CpmmMigrator {
  id: Int!
  chainId: Int!
  blockHeight: Int!
  createdAt: DateTime!
  launch: Address!
  state: Address
  cpmmConfig: Address
  minRaiseQuote: BigDecimal
  pool: Address
  quoteForLiquidity: BigDecimal
  baseForLiquidity: BigDecimal
  migrated: Boolean!
}

type PredictionMarket {
  id: Int!
  chainId: Int!
  blockHeight: Int!
  createdAt: DateTime!
  resolvedAt: DateTime
  market: Address!
  oracle: Address!
  quoteMint: Address!
  winnerMint: Address
  claimableSupply: BigDecimal
  totalPot: BigDecimal
  # resolved relationships
  entries(isWinner: Boolean, limit: Int, cursor: String): PredictionEntryPage!
  claims(claimer: Address, limit: Int, cursor: String): PredictionClaimPage!
}

type PredictionEntry {
  id: Int!
  chainId: Int!
  blockHeight: Int!
  createdAt: DateTime!
  market: Address!
  oracle: Address!
  entryId: Address!
  baseMint: Address!
  contribution: BigDecimal
  isWinner: Boolean
}

type PredictionClaim {
  id: Int!
  chainId: Int!
  blockHeight: Int!
  createdAt: DateTime!
  txId: String!
  logPosition: Int!
  market: Address!
  claimer: Address!
  burnedAmount: BigDecimal!
  rewardAmount: BigDecimal!
  totalBurned: BigDecimal!
}

type CpmmPositionPage   { data: [CpmmPosition!]!    pageInfo: PageInfo! }
type CpmmMigratorPage   { data: [CpmmMigrator!]!    pageInfo: PageInfo! }
type PredictionMarketPage{ data: [PredictionMarket!]! pageInfo: PageInfo! }
type PredictionEntryPage { data: [PredictionEntry!]! pageInfo: PageInfo! }
type PredictionClaimPage { data: [PredictionClaim!]! pageInfo: PageInfo! }
```

**Root query additions:**

```graphql
type Query {
  # ... existing queries ...

  cpmmPosition(chainId: Int!, position: Address!): CpmmPosition
  cpmmPositions(
    chainId: Int!
    pool: Address
    owner: Address
    openOnly: Boolean
    order: SortOrder
    cursor: String
    limit: Int
  ): CpmmPositionPage!

  cpmmMigrator(chainId: Int!, launch: Address!): CpmmMigrator
  cpmmMigrators(
    chainId: Int!
    migrated: Boolean
    order: SortOrder
    cursor: String
    limit: Int
  ): CpmmMigratorPage!

  predictionMarket(chainId: Int!, market: Address!): PredictionMarket
  predictionMarkets(
    chainId: Int!
    oracle: Address
    quoteMint: Address
    resolved: Boolean
    order: SortOrder
    cursor: String
    limit: Int
  ): PredictionMarketPage!
}
```

---

## Query Patterns Reference

| Use case                          | Endpoint                                          | Pagination    | Key parameters                                        |
|-----------------------------------|---------------------------------------------------|---------------|-------------------------------------------------------|
| New pools discovery feed          | `GET /v1/pools/new`                              | Feed          | `head_cursor` for live updates; `cursor` for history  |
| Pool leaderboard by active liq    | `GET /v1/pools`                                  | Leaderboard   | `sort_by=active_liquidity_usd`                        |
| Pool leaderboard by TVL           | `GET /v1/pools`                                  | Leaderboard   | `sort_by=tvl_usd`                                     |
| Token OHLCV chart                 | `GET /v1/pools/:addr/snapshots`                  | Feed          | `from_block`, `to_block`, `order=asc`                 |
| Recent swaps feed                 | `GET /v1/swaps`                                  | Feed          | `chain_id`, `pool`, `head_cursor` for live updates    |
| Portfolio holdings                | `GET /v1/users/:addr/portfolio`                  | Leaderboard   | `chain_id`                                            |
| Price history for a token         | `GET /v1/prices`                                 | Feed          | `token`, `quote_token`, `from`/`to`                   |
| Graduation progress               | `GET /v1/pools/:addr/metrics`                    | Feed          | latest row, `graduation_percentage`                   |
| All positions in a pool           | `GET /v1/pools/:addr/positions`                  | Leaderboard   | `owner` optional                                      |
| Fee accumulation per LP           | `GET /v1/pools/:addr/fees`                       | —             | `beneficiary` optional                                |
| Multi-field pool + state in one   | `POST /graphql`                                  | —             | nested `pool { state snapshots { ... } }`             |
| CPMM positions by owner           | `GET /v1/cpmm/positions`                         | Feed          | `owner`, `pool`, `open_only`                          |
| CPMM migration status             | `GET /v1/cpmm/migrators/:launch`                 | —             | `chain_id`                                            |
| Open prediction markets           | `GET /v1/prediction/markets`                     | Feed          | `resolved=false`, `oracle`, `quote_mint`              |
| Market entries + winner flag      | `GET /v1/prediction/markets/:market/entries`     | Feed          | `is_winner` filter after resolution                   |
| Reward claims for a market        | `GET /v1/prediction/markets/:market/claims`      | Feed          | `claimer` optional                                    |
| Full market resolution snapshot   | `POST /graphql`                                  | —             | nested `predictionMarket { entries claims { ... } }`  |
