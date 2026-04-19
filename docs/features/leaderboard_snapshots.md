# Leaderboard Snapshots

Periodic materialization of pool rankings into `pool_leaderboard_snapshot` so the
API layer can paginate a leaderboard on mutable sort keys with stable,
O(log N + limit) keyset seeks.

## Scope

- Indexer-side: writer service that periodically materializes one row per
  `(sort_key, pool)` under a new monotonic `snapshot_id`.
- Indexer-side: range-delete of stale snapshots on reorg.
- Schema: `pool_leaderboard_snapshot` table, its keyset/reorg/GC indexes, and
  the `pool_leaderboard_snapshot_id_seq` sequence.

## Non-scope

- The API query layer that reads from this table. A separate feature (`docs/api_design/design.md`) covers the REST/GraphQL surface.
- Backfill of historical snapshots — only forward-writing from indexer start.
- Per-chain config overrides via JSON. V1 uses a single global default; add
  per-chain knobs only when a deployment needs them.

## Data / control flow

```
┌────────────────────────────────────────────────────────────┐
│  spawn_live_mode (EVM)  /  process_solana_chain (Solana)   │
│                                                            │
│    LeaderboardSnapshotService::run                         │
│      │                                                     │
│      │ sleep(interval_secs)                                │
│      ▼                                                     │
│    run_cycle:                                              │
│      1. SELECT MAX(block_height) FROM pool_state ⋈ av      │
│      2. if advanced >= interval_blocks:                    │
│           SELECT nextval(seq)  →  snapshot_id              │
│           INSERT (one per sort_key) in a single txn        │
│           remember snapshot_id / head_block                │
│      3. DELETE rows with taken_at < now() - retention_secs │
└────────────────────────────────────────────────────────────┘

API reads                         Indexer reorg path
──────────                        ──────────────────
1st page: pick MAX(snapshot_id)   RangeFinalizer::process_reorg
          store in cursor         Phase 4:
Nth page: reuse cursor's id         DELETE FROM pool_leaderboard_snapshot
          keyset seek via             WHERE chain_id = X
          idx_plsnap_keyset             AND block_height >= MIN(orphaned)
Expired/GC'd snapshot →           Client holding cursor → INVALID_CURSOR
  INVALID_CURSOR                    on next request, restarts session
```

## Invariants

- `snapshot_id` is monotonically increasing and unique across all chains; the
  sequence is global to the database.
- All rows sharing the same `snapshot_id` share the same `block_height`
  (captured as `MAX(pool_state.block_height)` at cycle start).
- Rows per snapshot = (number of active pools) × (number of configured sort_keys).
- Keyset reads always filter on `(chain_id, sort_key, snapshot_id)` before the
  `(sort_val, id)` range predicate — this matches `idx_plsnap_keyset`.
- Reorg invalidation is range-based (`block_height >= min_orphaned`), not
  set-based (`block_height IN (orphaned)`), because a snapshot taken *after*
  an orphaned block still captures orphaned pool_state.
- `pool_leaderboard_snapshot` is never declared in any handler's `reorg_tables()` — the Phase 4 hook in `RangeFinalizer` is the single rollback path.

## Related files

| Path | Role |
|------|------|
| `migrations/007_pool_leaderboard_snapshot.sql` | Table, sequence, indexes |
| `src/live/leaderboard_snapshot.rs` | `LeaderboardSnapshotService` — the writer + GC |
| `src/live/types.rs` | `LeaderboardSnapshotConfig` + `LiveModeConfig.leaderboard_snapshot` |
| `src/live/mod.rs` | Re-exports |
| `src/main.rs` (`spawn_live_mode`) | Spawns the writer for EVM live chains |
| `src/solana/pipeline.rs` (near `SolanaCompactionService`) | Spawns the writer for Solana live chains |
| `src/transformations/finalizer.rs` (`invalidate_leaderboard_snapshots`) | Phase 4 reorg delete |
| `src/types/config/defaults.rs` (`raw_data` module) | Default cadence/retention constants |
| `docs/api_design/design.md` | API consumer contract (cursor encoding, INVALID_CURSOR semantics) |

## Configuration

`LeaderboardSnapshotConfig` lives on `LiveModeConfig.leaderboard_snapshot`.
`None` disables the writer for that chain.

| Field | Default | Meaning |
|-------|---------|---------|
| `interval_blocks` | 100 | Skip the write if head hasn't advanced by at least this many blocks since the previous snapshot. |
| `interval_secs` | 30 | Wall-clock cadence of the `sleep`/`run_cycle` loop. Also the maximum wait for a snapshot when blocks are coming in quickly. |
| `retention_secs` | 3600 | GC any snapshot whose `taken_at` is older than this. |
| `sort_keys` | `[active_liquidity_usd, tvl_usd, volume_24h_usd, market_cap_usd]` | Allowlisted columns on `pool_state` to materialize. Validated before SQL interpolation. |
