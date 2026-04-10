# Pool Metrics: Tables, State & Math

## SQL Tables

All tables include `chain_id`, `source`, and `source_version` columns for multi-chain and multi-version isolation. These are injected automatically by the executor — handler code does not set them.

### pool_state — one row per (chain, pool, source version); updated on every swap

```sql
CREATE TABLE IF NOT EXISTS pool_state (
    chain_id             BIGINT NOT NULL,
    pool_id              BYTEA NOT NULL,
    block_number         BIGINT NOT NULL,
    block_timestamp      BIGINT NOT NULL,       -- unix seconds
    tick                 INTEGER NOT NULL,
    sqrt_price_x96       NUMERIC NOT NULL,
    price                NUMERIC NOT NULL,      -- quote tokens per base token, decimal-adjusted
    active_liquidity     NUMERIC NOT NULL,      -- raw Uniswap v3 L at current tick (not a dollar amount)
    volume_24h_usd       NUMERIC,               -- rolling 24h (phase 6)
    price_change_1h      NUMERIC,               -- (phase 6)
    price_change_24h     NUMERIC,               -- (phase 6)
    swap_count_24h       INTEGER,               -- (phase 6)
    amount0              NUMERIC,               -- raw token0 across all positions (phase 7)
    amount1              NUMERIC,               -- raw token1 across all positions (phase 7)
    tvl_usd              NUMERIC,               -- dollar value of (amount0, amount1) (phase 7)
    active_liquidity_usd NUMERIC,               -- dollar value of straddling-position amounts (phase 7)
    market_cap_usd       NUMERIC,               -- price_close × total_supply × usd_per_quote (phase 7)
    total_supply         NUMERIC,               -- base token total supply, raw (phase 7)
    source               VARCHAR(255) NOT NULL,
    source_version       INT NOT NULL,
    UNIQUE (chain_id, pool_id, source, source_version)
);

CREATE INDEX IF NOT EXISTS idx_pool_state_price ON pool_state (price DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_pool_state_reorg ON pool_state (chain_id, block_number);
```

**Upsert semantics**: `ON CONFLICT DO UPDATE SET ... WHERE EXCLUDED.block_number > pool_state.block_number`. Writes from stale blocks (e.g., retried historical ranges) are silently dropped. This conditional update is the basis for correct reorg snapshot capture — see [Reorg Invariants](#reorg-invariants).

**Phase 7 TVL writes**: the TVL handler UPDATEs the Phase 7 columns (`amount0`, `amount1`, `tvl_usd`, `active_liquidity_usd`, `market_cap_usd`, `total_supply`) via `DbOperation::RawSql`, gated on `pool_state.block_number = $block_number` to avoid overwriting a newer range's state with stale TVL. The WHERE clause also matches the swap handler's `source` / `source_version` (passed as params) since the TVL handler owns a different handler name but needs to enrich rows written by the swap handler.

### pool_snapshots — one row per (chain, pool, block) with swap activity

```sql
CREATE TABLE IF NOT EXISTS pool_snapshots (
    chain_id             BIGINT NOT NULL,
    pool_id              BYTEA NOT NULL,
    block_number         BIGINT NOT NULL,
    block_timestamp      BIGINT NOT NULL,       -- unix seconds
    price_open           NUMERIC NOT NULL,      -- price at first swap in block
    price_close          NUMERIC NOT NULL,      -- price at last swap in block
    price_high           NUMERIC NOT NULL,
    price_low            NUMERIC NOT NULL,
    active_liquidity     NUMERIC NOT NULL,      -- raw Uniswap v3 L at last swap in block
    volume0              NUMERIC NOT NULL DEFAULT 0,   -- sum of |amount0| across all swaps
    volume1              NUMERIC NOT NULL DEFAULT 0,   -- sum of |amount1| across all swaps
    swap_count           INT NOT NULL DEFAULT 0,
    volume_usd           NUMERIC,               -- quote-side volume × usd_per_quote (phase 6)
    amount0              NUMERIC,               -- raw token0 across all positions (phase 7)
    amount1              NUMERIC,               -- raw token1 across all positions (phase 7)
    tvl_usd              NUMERIC,               -- dollar value of (amount0, amount1) (phase 7)
    active_liquidity_usd NUMERIC,               -- dollar value of straddling-position amounts (phase 7)
    market_cap_usd       NUMERIC,               -- price_close × total_supply × usd_per_quote (phase 7)
    source               VARCHAR(255) NOT NULL,
    source_version       INT NOT NULL,
    UNIQUE (chain_id, pool_id, block_number, source, source_version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_time ON pool_snapshots (pool_id, block_timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshots_reorg ON pool_snapshots (chain_id, block_number);
```

**Upsert semantics**: `ON CONFLICT DO UPDATE SET ...` (unconditional — updates all columns). Idempotent across retries for the same block. Swap handlers write the OHLC, volume, and volume_usd columns (phases 2–6). The TVL handler (phase 7) populates the TVL columns via a follow-up `DbOperation::RawSql` UPDATE targeting `source = <swap handler name>` in the WHERE clause; TVL writes are explicitly isolated from the idempotent swap upsert.

### liquidity_deltas — append-only recovery log for Mint/Burn events

```sql
CREATE TABLE IF NOT EXISTS liquidity_deltas (
    chain_id        BIGINT NOT NULL,
    pool_id         BYTEA NOT NULL,
    block_number    BIGINT NOT NULL,
    log_index       INT NOT NULL,
    tick_lower      INT NOT NULL,
    tick_upper      INT NOT NULL,
    liquidity_delta NUMERIC NOT NULL,       -- positive = mint, negative = burn
    source          VARCHAR(255) NOT NULL,
    source_version  INT NOT NULL,
    UNIQUE (chain_id, pool_id, block_number, log_index, source, source_version)
);

CREATE INDEX IF NOT EXISTS idx_liq_deltas_pool ON liquidity_deltas (pool_id);
CREATE INDEX IF NOT EXISTS idx_liq_deltas_reorg ON liquidity_deltas (chain_id, block_number);
```

**Insert semantics**: `ON CONFLICT DO NOTHING`. Pure append log — one row per Mint/Burn event. Used to rebuild in-memory tick maps for future TVL computation (phase 3+).

---

## In-Memory State

```python
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
import math

# Tick map: pool_id -> {(tick_lower, tick_upper): net_liquidity}
tick_maps: dict[bytes, dict[tuple[int, int], int]] = defaultdict(dict)

@dataclass
class BlockAccumulator:
    """Accumulates events within a single block for one pool."""
    price_open: Decimal | None = None
    price_close: Decimal | None = None
    price_high: Decimal | None = None
    price_low: Decimal | None = None
    volume0: Decimal = Decimal(0)
    volume1: Decimal = Decimal(0)
    swap_count: int = 0
    # latest values (updated by each event in order)
    tick: int = 0
    sqrt_price_x96: int = 0
    active_liquidity: int = 0

    def update_price(self, price: Decimal):
        if self.price_open is None:
            self.price_open = price
        self.price_close = price
        self.price_high = max(self.price_high or price, price)
        self.price_low = min(self.price_low or price, price)
```

---

## Math

### 1. Spot Price

```python
def compute_price(sqrt_price_x96: int, decimals0: int, decimals1: int) -> Decimal:
    """
    sqrtPriceX96 = sqrt(price) * 2^96
    where price = token1 / token0 in raw (non-decimal-adjusted) terms

    To get human-readable price (decimal-adjusted):
        price = (sqrtPriceX96 / 2^96)^2 * 10^(decimals0 - decimals1)
    """
    Q96 = Decimal(2 ** 96)
    sqrt_price = Decimal(sqrt_price_x96) / Q96
    raw_price = sqrt_price ** 2
    adjusted_price = raw_price * Decimal(10 ** (decimals0 - decimals1))
    return adjusted_price
```

### 2. Active Liquidity (raw L)

Just the `liquidity` value from the pool's slot0/state. Your handler grabs this
directly from the event or call result — no math needed. Stored as-is in the
`active_liquidity` column. **Not a dollar amount** — it's the Uniswap v3
liquidity parameter `L`, useful for price-impact math but meaningless without
context. Use `active_liquidity_usd` (§3b) for dollar-valued liquidity.

### 3. TVL (Token Amounts)

```python
def compute_tvl(
    tick_map: dict[tuple[int, int], int],
    current_tick: int,
    sqrt_price_x96: int,
    decimals0: int,
    decimals1: int,
) -> tuple[Decimal, Decimal]:
    """
    For each active position in the tick map, compute token amounts
    based on where the position sits relative to the current tick.

    Returns (amount0, amount1) in human-readable (decimal-adjusted) units.

    Core formulas (in real-value terms):
        sqrt_price = sqrt(1.0001^tick)

        If position is entirely below current tick (tick_upper <= current_tick):
            All token1:
            amount1 = L * (sqrt_upper - sqrt_lower)

        If position is entirely above current tick (tick_lower > current_tick):
            All token0:
            amount0 = L * (1/sqrt_lower - 1/sqrt_upper)

        If position straddles current tick:
            amount0 = L * (1/sqrt_current - 1/sqrt_upper)
            amount1 = L * (sqrt_current - sqrt_lower)
    """
    Q96 = 2 ** 96
    total_amount0_raw = Decimal(0)
    total_amount1_raw = Decimal(0)

    sqrt_current = Decimal(sqrt_price_x96) / Decimal(Q96)

    for (tick_lower, tick_upper), liquidity in tick_map.items():
        if liquidity <= 0:
            continue

        L = Decimal(liquidity)
        sqrt_lower = Decimal(math.sqrt(1.0001 ** tick_lower))
        sqrt_upper = Decimal(math.sqrt(1.0001 ** tick_upper))

        if tick_upper <= current_tick:
            # entirely in token1
            total_amount1_raw += L * (sqrt_upper - sqrt_lower)

        elif tick_lower > current_tick:
            # entirely in token0
            total_amount0_raw += L * (1 / sqrt_lower - 1 / sqrt_upper)

        else:
            # straddles current tick
            total_amount0_raw += L * (1 / sqrt_current - 1 / sqrt_upper)
            total_amount1_raw += L * (sqrt_current - sqrt_lower)

    amount0 = total_amount0_raw / Decimal(10 ** decimals0)
    amount1 = total_amount1_raw / Decimal(10 ** decimals1)

    return amount0, amount1
```

### 3b. Active Liquidity Token Amounts

Same walk as §3, but only positions whose range straddles the current tick
(Uniswap v3 convention: `tick_lower ≤ current_tick < tick_upper`). These
positions are the subset that can absorb a swap at the current price — their
token amounts represent "liquidity available right now". Non-straddling
positions still hold tokens (and contribute to total TVL) but those tokens are
not price-accessible until price moves into their range.

```python
def compute_active_liquidity_amounts(
    tick_map: dict[tuple[int, int], int],
    current_tick: int,
    sqrt_price_x96: int,
    decimals0: int,
    decimals1: int,
) -> tuple[Decimal, Decimal]:
    """
    Returns (amount0, amount1) in human-readable units, restricted to
    positions straddling current_tick. Only the "straddles current tick"
    branch from compute_tvl is reachable here; the other two branches
    are skipped by the filter.
    """
    Q96 = 2 ** 96
    total_amount0_raw = Decimal(0)
    total_amount1_raw = Decimal(0)
    sqrt_current = Decimal(sqrt_price_x96) / Decimal(Q96)

    for (tick_lower, tick_upper), liquidity in tick_map.items():
        if liquidity <= 0:
            continue
        # Filter: only straddling positions
        if tick_lower > current_tick or tick_upper <= current_tick:
            continue

        L = Decimal(liquidity)
        sqrt_lower = Decimal(math.sqrt(1.0001 ** tick_lower))
        sqrt_upper = Decimal(math.sqrt(1.0001 ** tick_upper))
        total_amount0_raw += L * (1 / sqrt_current - 1 / sqrt_upper)
        total_amount1_raw += L * (sqrt_current - sqrt_lower)

    amount0 = total_amount0_raw / Decimal(10 ** decimals0)
    amount1 = total_amount1_raw / Decimal(10 ** decimals1)
    return amount0, amount1
```

### 4. TVL in USD (shared conversion for both TVL and active liquidity)

```python
def compute_tvl_usd(
    amount0: Decimal,
    amount1: Decimal,
    price0_usd: Decimal,
    price1_usd: Decimal,
) -> Decimal:
    """
    Simple sum of token values.
    Price resolution depends on your protocol:
      - If token1 is a stablecoin: price1_usd = 1.0, price0_usd = pool price
      - If neither is stable: chain through a reference pool
        e.g., TOKEN/WETH pool price * WETH/USDC pool price

    The same function is applied twice per block:
      - Once with (amount0_total, amount1_total) from compute_tvl → tvl_usd
      - Once with (amount0_active, amount1_active) from
        compute_active_liquidity_amounts → active_liquidity_usd
    """
    return amount0 * price0_usd + amount1 * price1_usd
```

### 5. Market Cap

```python
def compute_market_cap(
    price_in_usd: Decimal,
    total_supply: Decimal,
    token_decimals: int,
) -> Decimal:
    """
    market_cap = token_price_usd * circulating_supply

    total_supply is raw (from the contract), divide by 10^decimals
    for human-readable units.

    For a launchpad protocol, total_supply may equal max supply
    if fully minted at launch.
    """
    supply = total_supply / Decimal(10 ** token_decimals)
    return price_in_usd * supply
```

### 6. Price Change

```python
def compute_price_change(
    current_price: Decimal,
    historical_price: Decimal,
) -> Decimal:
    """
    Returns percentage change as a decimal (e.g., 0.05 = 5%).

    For 1h change: look up the price_close from the snapshot nearest to
    (current_block_timestamp - 1 hour).
    For 24h: same, minus 24 hours.

    Query:
        SELECT price_close FROM pool_snapshots
        WHERE pool_id = $1
          AND block_timestamp <= $2
        ORDER BY block_timestamp DESC
        LIMIT 1
    """
    if historical_price == 0:
        return Decimal(0)
    return (current_price - historical_price) / historical_price
```

### 7. Volume (per block)

```python
def accumulate_swap_volume(accumulator: BlockAccumulator, swap_event):
    """
    From each Swap event, extract the absolute amounts swapped.

    In V4, the Swap event gives you:
      - amount0: int256 (positive = token0 in, negative = token0 out)
      - amount1: int256 (same convention)

    Take absolute values for volume:
    """
    accumulator.volume0 += abs(Decimal(swap_event.amount0))
    accumulator.volume1 += abs(Decimal(swap_event.amount1))
    accumulator.swap_count += 1
```

### 8. 24h Rolling Volume

```python
def compute_24h_volume(pool_id: bytes, current_timestamp) -> Decimal:
    """
    Sum from snapshots:

        SELECT COALESCE(SUM(volume_usd), 0)
        FROM pool_snapshots
        WHERE pool_id = $1
          AND block_timestamp > $2 - INTERVAL '24 hours'
    """
    ...
```

---

## Reorg Invariants

The metrics handlers have specific requirements for correct reorg rollback. Violating these causes orphaned rows or incorrect state restoration.

### pool_state (stateful — needs snapshot restore)

`pool_state` holds one row per pool updated in place. On reorg, the pre-block value must be **restored**, not deleted. The executor captures a rollback snapshot (the row's value before the write) for every upsert where `affected_rows > 0`. The conditional `WHERE EXCLUDED.block_number > pool_state.block_number` ensures:

- No-op writes (stale block) produce `affected_rows = 0` → **no snapshot recorded** → correct: nothing to roll back
- Real writes produce `affected_rows = 1` → snapshot records the previous value → reorg restores it

### pool_snapshots (block-keyed — needs delete on reorg)

`pool_snapshots` has one row per (pool, block). On reorg, the row must be **deleted** (no previous state to restore — the row simply shouldn't exist). The snapshot capture records `previous_row = None` on first insert, which triggers a keyed DELETE during reorg cleanup.

### liquidity_deltas (append-only — fallback DELETE is correct)

`liquidity_deltas` uses `ON CONFLICT DO NOTHING` (`update_columns = []`). The snapshot capture loop skips ops with empty `update_columns`, so no rollback snapshot is ever written for this table. The reorg cleanup's fallback `DELETE ... WHERE chain_id = X AND block_number IN (...)` is the correct and complete reorg action.

### Multi-trigger handler constraint (see issue #95)

The liquidity metrics handlers (`V3LiquidityMetricsHandler`, `LockableV3LiquidityMetricsHandler`) are multi-trigger (Mint + Burn). Multi-trigger handlers run with `DbExecMode::Direct` (no snapshot capture). This is safe **only because** `liquidity_deltas` uses `update_columns = []` — snapshot capture would be a no-op anyway. If a future multi-trigger handler writes to a stateful table (non-empty `update_columns`), it would silently lose reorg protection and fall back to DELETE instead of restore.

### Retry safety

The retry path always uses snapshot capture. Before retrying a handler for a block, the retry loop checks whether the handler is already recorded as complete for that range. If so, it skips both `handle()` and the snapshot write — preventing duplicate snapshots that would conflict with the original write's snapshot and restore orphaned rows during reorg.

---

## V4 Base Cumulative Counters

`DopplerV4Hook` emits `Swap(int24 currentTick, uint256 totalProceeds, uint256 totalTokensSold)` where `totalProceeds` and `totalTokensSold` are **cumulative since pool creation**, not per-swap deltas. To feed the shared `process_swaps()` pipeline (which expects per-swap `amount0`/`amount1`), the `V4BaseMetricsHandler` computes deltas by subtracting the previous cumulative values for each pool.

### State storage

**Durable**: `v4_base_proceeds_state` — one row per `(chain_id, pool_id, source, source_version)` storing the most recent cumulative totals. Upserted inside the same transaction as the `pool_snapshots`/`pool_state` writes.

```sql
CREATE TABLE IF NOT EXISTS v4_base_proceeds_state (
    chain_id           BIGINT NOT NULL,
    pool_id            BYTEA NOT NULL,
    total_proceeds     NUMERIC NOT NULL DEFAULT '0',
    total_tokens_sold  NUMERIC NOT NULL DEFAULT '0',
    source             VARCHAR(255) NOT NULL,
    source_version     INT NOT NULL,
    UNIQUE (chain_id, pool_id, source, source_version)
);
```

**In-memory**: `RwLock<HashMap<[u8;32], (U256, U256)>>` on the handler. Loaded from `v4_base_proceeds_state` at `initialize()`, refilled on miss (one scoped `pool_id = ANY($4)` query for just the missing IDs), and updated optimistically in `handle()` after computing deltas.

### Amount sign convention

The protocol sells base tokens for quote tokens. For each delta:

```
proceeds_delta  = quote received by pool (inflow)
tokens_delta    = base sold out of pool  (outflow)
```

| is_token_0 | amount0              | amount1              |
|------------|----------------------|----------------------|
| `true`     | `-(tokens_delta)`    | `+proceeds_delta`    |
| `false`    | `+proceeds_delta`    | `-(tokens_delta)`    |

### Sequential processing

This handler sets `requires_sequential() = true`. The catchup engine creates a capacity-1 `Semaphore` for this handler only, and Tokio's FIFO permit ordering guarantees ranges run in ascending block order so the "prev cumulative" used for each delta is always the immediately preceding range's committed state.

### Retry & recovery

The handler uses three trait hooks (`on_commit_success`, `on_commit_failure`, `on_reorg`) to keep the in-memory `proceeds_state` cache consistent with the committed DB state across failures and reorgs:

- **Optimistic cache snapshot**: before the optimistic cache update, `handle()` stashes the current (pre-update) cumulative per touched pool into `in_flight_pre`.
- **`on_commit_success(range)`** (called by executor/engine/retry after a successful tx): drains `in_flight_pre` and removes `range` from `failed_ranges`.
- **`on_commit_failure(range)`** (called after a failed tx): restores the cache from `in_flight_pre` (reverting the optimistic update) and records `range` in `failed_ranges`.
- **Gate in `handle()`**: if `failed_ranges` contains any range smaller than the current one, the handler returns `TransformationError::TransientBlocked`, which the retry machinery surfaces as a retryable failure. This blocks subsequent ranges from advancing the cumulative past a stuck block. The retry of the failed range itself is always admitted (`current == min_failed`), so the handler converges as soon as the underlying error clears.
- **`on_reorg(orphaned)`**: called by the finalizer after DB rollback. Clears `proceeds_state`, `in_flight_pre`, and `failed_ranges` entirely; the next `handle()` lazily reloads the cache from `v4_base_proceeds_state`.

Catchup has its own guardrail: a commit failure halts catchup via the engine's `handler_errored` flag, so restart still rebuilds the cache correctly for historical ranges.

### pool_id lookup

The V4 base Swap event has no `poolId` field. The handler maps `event.contract_address` (the hook address) to the 32-byte `pool_id` via `v4_pool_configs`. The map is loaded at init and refreshed on cache miss; `handler_dependencies = ["V4CreateHandler"]` guarantees the config row exists before metrics run for a range.

### active_liquidity

V4 base Swap events do not emit liquidity, and Doppler auction "active liquidity" isn't directly analogous to a v3 pool's. The handler writes `active_liquidity = 0` for V4 base snapshots.

---

## Handler Flow

```python
def handle_block(block_number: int, block_timestamp, events: list, call_results: dict):
    accumulators: dict[bytes, BlockAccumulator] = {}
    modified_pools: set[bytes] = set()
    delta_rows = []

    # --- Phase 1: process events in order ---
    for event in sorted(events, key=lambda e: e.log_index):
        pool_id = event.pool_id

        if pool_id not in accumulators:
            accumulators[pool_id] = BlockAccumulator()
        acc = accumulators[pool_id]

        if event.type == "ModifyLiquidity":
            # update in-memory tick map
            key = (event.tick_lower, event.tick_upper)
            tick_maps[pool_id][key] = tick_maps[pool_id].get(key, 0) + event.liquidity_delta
            if tick_maps[pool_id][key] <= 0:
                del tick_maps[pool_id][key]

            # record delta for recovery log
            delta_rows.append((pool_id, block_number, event.log_index,
                               event.tick_lower, event.tick_upper, event.liquidity_delta))

            modified_pools.add(pool_id)

        elif event.type == "Swap":
            # update accumulator with post-swap state
            pool_meta = get_pool_meta(pool_id)  # decimals, tick_spacing, etc.
            price = compute_price(event.sqrt_price_x96, pool_meta.decimals0, pool_meta.decimals1)

            acc.update_price(price)
            accumulate_swap_volume(acc, event)
            acc.tick = event.tick
            acc.sqrt_price_x96 = event.sqrt_price_x96
            acc.active_liquidity = event.liquidity  # post-swap active L

            modified_pools.add(pool_id)

    # --- Phase 2: compute final metrics & write ---
    snapshot_rows = []
    state_upserts = []

    for pool_id in modified_pools:
        acc = accumulators[pool_id]
        meta = get_pool_meta(pool_id)

        # compute total TVL token amounts from in-memory tick map
        amount0, amount1 = compute_tvl(
            tick_maps.get(pool_id, {}),
            acc.tick, acc.sqrt_price_x96,
            meta.decimals0, meta.decimals1,
        )

        # compute active-liquidity token amounts (straddling positions only)
        amount0_active, amount1_active = compute_active_liquidity_amounts(
            tick_maps.get(pool_id, {}),
            acc.tick, acc.sqrt_price_x96,
            meta.decimals0, meta.decimals1,
        )

        # resolve USD prices (protocol-specific)
        price0_usd, price1_usd = resolve_usd_prices(pool_id, acc.price_close)
        tvl_usd = compute_tvl_usd(amount0, amount1, price0_usd, price1_usd)
        active_liquidity_usd = compute_tvl_usd(
            amount0_active, amount1_active, price0_usd, price1_usd
        )

        # market cap
        market_cap = compute_market_cap(price0_usd, meta.total_supply, meta.decimals0)

        # volume in USD
        volume_usd = (acc.volume0 / Decimal(10**meta.decimals0)) * price0_usd \
                    + (acc.volume1 / Decimal(10**meta.decimals1)) * price1_usd

        # price changes (query recent snapshots)
        price_1h_ago = query_price_at(pool_id, block_timestamp - timedelta(hours=1))
        price_24h_ago = query_price_at(pool_id, block_timestamp - timedelta(hours=24))
        price_change_1h = compute_price_change(acc.price_close, price_1h_ago)
        price_change_24h = compute_price_change(acc.price_close, price_24h_ago)

        # 24h rolling volume
        volume_24h = query_24h_volume(pool_id, block_timestamp) + volume_usd

        snapshot_rows.append((
            pool_id, block_number, block_timestamp,
            acc.price_open, acc.price_close, acc.price_high, acc.price_low,
            acc.active_liquidity,  # raw Uniswap v3 L
            amount0, amount1, tvl_usd, active_liquidity_usd, market_cap,
            acc.volume0, acc.volume1, volume_usd, acc.swap_count,
        ))

        state_upserts.append((
            pool_id, meta.token0, meta.token1,
            meta.decimals0, meta.decimals1, meta.tick_spacing,
            block_number, block_timestamp,
            acc.tick, acc.sqrt_price_x96,
            acc.price_close,
            acc.active_liquidity,       # raw L
            active_liquidity_usd,       # dollar value (phase 7)
            amount0, amount1, tvl_usd,
            meta.total_supply, market_cap,
            volume_24h, price_change_1h, price_change_24h,
            acc.swap_count,  # this is just this block, accumulate differently for 24h
        ))

    # --- Phase 3: batch write ---
    batch_insert("liquidity_deltas", delta_rows)
    batch_insert("pool_snapshots", snapshot_rows)
    batch_upsert("pool_state", state_upserts)
```

---

## Startup / Recovery

```python
def rebuild_tick_maps():
    """
    One query to rebuild all in-memory state from the recovery log.
    Run on process start before processing new blocks.
    """
    rows = query("""
        SELECT pool_id, tick_lower, tick_upper, SUM(liquidity_delta) AS net_liquidity
        FROM liquidity_deltas
        GROUP BY pool_id, tick_lower, tick_upper
        HAVING SUM(liquidity_delta) > 0
    """)
    for row in rows:
        tick_maps[row.pool_id][(row.tick_lower, row.tick_upper)] = row.net_liquidity
```

---

## Query Patterns

### Paginated list (dashboard)

```sql
-- Top pools by market cap, page 2
-- active_liquidity is raw Uniswap v3 L (price-impact math);
-- active_liquidity_usd is the dollar-valued "liquidity at current tick".
SELECT pool_id, price, market_cap_usd,
       active_liquidity, active_liquidity_usd,
       tvl_usd, volume_24h_usd, price_change_24h
FROM pool_state
ORDER BY market_cap_usd DESC NULLS LAST
LIMIT 30 OFFSET 30;
```

### Single pool detail

```sql
SELECT * FROM pool_state WHERE pool_id = $1;
```

### Price chart (24h, block-level)

```sql
SELECT block_timestamp, price_open, price_close, price_high, price_low, volume_usd
FROM pool_snapshots
WHERE pool_id = $1
  AND block_timestamp > now() - INTERVAL '24 hours'
ORDER BY block_number;
```

### Price chart (30d, hourly candles)

```sql
SELECT
    date_trunc('hour', block_timestamp) AS hour,
    (array_agg(price_open ORDER BY block_number))[1] AS open,
    (array_agg(price_close ORDER BY block_number DESC))[1] AS close,
    max(price_high) AS high,
    min(price_low) AS low,
    sum(volume_usd) AS volume,
    (array_agg(market_cap_usd ORDER BY block_number DESC))[1] AS market_cap,
    (array_agg(active_liquidity ORDER BY block_number DESC))[1] AS active_liquidity,
    (array_agg(tvl_usd ORDER BY block_number DESC))[1] AS tvl
FROM pool_snapshots
WHERE pool_id = $1
  AND block_timestamp > now() - INTERVAL '30 days'
GROUP BY date_trunc('hour', block_timestamp)
ORDER BY hour;
```

### TVL and active liquidity history for a pool

```sql
SELECT block_timestamp, tvl_usd, active_liquidity_usd
FROM pool_snapshots
WHERE pool_id = $1
  AND block_timestamp > now() - INTERVAL '7 days'
ORDER BY block_number;
```

### Protocol-wide TVL and active liquidity (sum across all pools)

```sql
SELECT
    COALESCE(SUM(tvl_usd), 0) AS protocol_tvl,
    COALESCE(SUM(active_liquidity_usd), 0) AS protocol_active_liquidity
FROM pool_state;
```