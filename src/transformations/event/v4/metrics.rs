//! V4 base (DopplerV4Hook) pool metrics handler.
//!
//! Processes `Swap(int24 currentTick, uint256 totalProceeds, uint256 totalTokensSold)` events
//! from DopplerV4Hook factory contracts into pool_state and pool_snapshots.
//!
//! ## Design
//!
//! The V4 base swap event emits **cumulative** counters rather than per-swap deltas, so this
//! handler must compute deltas by subtracting the previous cumulative values.
//!
//! Previous values live in an in-memory `RwLock<HashMap>`. The cache is:
//!   - Populated from `v4_base_proceeds_state` at `initialize()`.
//!   - Refilled on cache miss (only for the specific missing pool IDs).
//!   - Updated optimistically inside `handle()` after computing deltas.
//!
//! Each `handle()` returns an upsert to `v4_base_proceeds_state` for every pool it touched,
//! so the checkpoint table is always durable. On restart the cache is rebuilt from the
//! checkpoint.
//!
//! ## Sequential processing
//!
//! `requires_sequential()` returns `true`. The catchup engine enforces one-at-a-time range
//! execution via a capacity-1 FIFO semaphore, so in-memory cache updates serialize naturally
//! and the DB checkpoint is always up-to-date relative to what any pending range will read.
//!
//! ## Pool ID lookup
//!
//! The swap event has no pool ID field. Pool IDs are resolved by mapping the event's
//! `contract_address` (the hook address) to the pool ID via `v4_pool_configs`. The map
//! is loaded at `initialize()` and refreshed on cache miss.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Once, OnceLock, RwLock};

use alloy_primitives::{I256, U256};
use async_trait::async_trait;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbPool, DbValue};
use crate::transformations::context::{DecodedEvent, FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::event::metrics::swap_data::{
    process_swaps, refresh_cache_if_needed, SwapInput,
};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::pool_metadata::PoolMetadataCache;
use crate::transformations::util::tick_math::tick_to_sqrt_price_x96;
use crate::transformations::util::usd_price::{
    build_usd_price_context_with_paths, chainlink_latest_answer_dependency, OraclePriceCache,
};

const SOURCE: &str = "DopplerV4Hook";

/// pool_id → (total_proceeds, total_tokens_sold) cumulative state.
type ProceedsState = HashMap<[u8; 32], (U256, U256)>;

// ─── Handler ─────────────────────────────────────────────────────────────────

pub struct V4BaseMetricsHandler {
    metadata_cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
    decimals_init: Once,
    chain_id: u64,
    db_pool: OnceLock<Pool>,
    /// hook_address → pool_id, loaded from v4_pool_configs at init, refreshed on miss.
    hook_pool_map: RwLock<HashMap<[u8; 20], [u8; 32]>>,
    /// pool_id → (total_proceeds, total_tokens_sold) — loaded from v4_base_proceeds_state
    /// at init, refreshed on cache miss, updated optimistically in handle().
    proceeds_state: RwLock<ProceedsState>,
    /// Pre-update cumulatives for the currently-in-flight batch, used by
    /// `on_commit_failure` to revert the optimistic cache update. Populated
    /// by `handle()` just before the optimistic write; drained by either
    /// commit hook. Always `None` between batches in normal operation.
    in_flight_pre: RwLock<Option<ProceedsState>>,
    /// Ranges whose commit failed. `handle()` refuses any range strictly
    /// greater than the minimum entry so delta computation never advances
    /// past a stuck block. Retries drain this set via `on_commit_success`.
    failed_ranges: RwLock<BTreeSet<(u64, u64)>>,
}

#[async_trait]
impl TransformationHandler for V4BaseMetricsHandler {
    fn name(&self) -> &'static str {
        "V4BaseMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_state.sql",
            "migrations/tables/pool_state_add_tvl.sql",
            "migrations/tables/pool_snapshots.sql",
            "migrations/tables/pool_snapshots_add_tvl.sql",
            "migrations/tables/pool_snapshots_add_volume_usd.sql",
            "migrations/tables/v4_base_proceeds_state.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pool_state", "pool_snapshots"]
    }

    fn requires_sequential(&self) -> bool {
        true
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        // Gate: if an earlier range's commit is still outstanding, refuse any
        // range that is strictly greater. This keeps delta computation from
        // advancing past a stuck block — the failed range must retry and drain
        // `failed_ranges` before later ranges can proceed.
        {
            let failed = self.failed_ranges.read().unwrap();
            if let Some(&min_failed) = failed.iter().next() {
                let current = (ctx.blockrange_start, ctx.blockrange_end);
                if current > min_failed {
                    return Err(TransformationError::TransientBlocked(format!(
                        "V4BaseMetricsHandler: waiting on failed range {:?} before processing {:?}",
                        min_failed, current
                    )));
                }
            }
        }

        self.decimals_init.call_once(|| {
            self.metadata_cache.resolve_quote_decimals(&ctx.contracts);
        });

        let events: Vec<&DecodedEvent> = ctx.events_of_type(SOURCE, "Swap").collect();
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // Ensure all hook addresses in this batch are in the map.
        self.refresh_hook_pool_map_for_events(&events).await?;

        // Collect unique pool IDs — block scope ensures the guard is dropped before any await.
        let pool_id_vecs: Vec<Vec<u8>> = {
            let hook_pool_map = self.hook_pool_map.read().unwrap();
            let mut seen: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
            events
                .iter()
                .filter_map(|e| hook_pool_map.get(&e.contract_address).copied())
                .filter(|id| seen.insert(*id))
                .map(|id| id.to_vec())
                .collect()
            // hook_pool_map guard dropped here
        };

        if pool_id_vecs.is_empty() {
            return Ok(Vec::new());
        }

        // Refresh pool metadata cache so decimals / is_token_0 are available.
        refresh_cache_if_needed(
            pool_id_vecs.iter(),
            &self.metadata_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            self.name(),
            SOURCE,
        )
        .await?;

        // Convert to fixed-size arrays for cache lookups.
        let pool_ids: Vec<[u8; 32]> = pool_id_vecs
            .iter()
            .filter_map(|v| {
                if v.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(v);
                    Some(arr)
                } else {
                    None
                }
            })
            .collect();

        // Fetch any pools missing from the in-memory cache.
        self.refresh_proceeds_state_for_pools(&pool_ids).await?;

        // Snapshot the current in-memory cumulative state for the pools in this batch.
        let prev_state: ProceedsState = {
            let state = self.proceeds_state.read().unwrap();
            pool_ids
                .iter()
                .filter_map(|id| state.get(id).map(|&v| (*id, v)))
                .collect()
        };

        // Extract swap inputs + determine new cumulative totals.
        let (swaps, new_cumulative) = self.extract_swaps(&events, &prev_state)?;

        if swaps.is_empty() {
            return Ok(Vec::new());
        }

        let (usd_ctx, price_ops) = build_usd_price_context_with_paths(
            ctx,
            &self.oracle_cache,
            &self.db_pool,
            self.chain_id,
            &ctx.contracts,
            &self.metadata_cache,
        )
        .await;
        let mut ops = process_swaps(
            &swaps,
            &self.metadata_cache,
            self.chain_id,
            self.name(),
            SOURCE,
            Some(&usd_ctx),
            self.version(),
        );
        ops.extend(price_ops);

        // Durably checkpoint the new cumulative totals.
        for (pool_id, (total_proceeds, total_tokens_sold)) in &new_cumulative {
            ops.push(upsert_proceeds_state(
                self.chain_id,
                pool_id,
                total_proceeds,
                total_tokens_sold,
                SOURCE,
                self.version(),
            ));
        }

        // Stash pre-update cumulatives so `on_commit_failure` can revert the
        // optimistic cache write if the transaction fails.
        let pre: ProceedsState = {
            let state = self.proceeds_state.read().unwrap();
            new_cumulative
                .keys()
                .map(|id| {
                    let prev = state.get(id).copied().unwrap_or((U256::ZERO, U256::ZERO));
                    (*id, prev)
                })
                .collect()
        };
        *self.in_flight_pre.write().unwrap() = Some(pre);

        // Update the in-memory cache optimistically. `on_commit_success` drains
        // `in_flight_pre` after the transaction commits; `on_commit_failure`
        // restores these entries and records the range so subsequent ranges
        // block until the retry succeeds.
        {
            let mut state = self.proceeds_state.write().unwrap();
            for (pool_id, totals) in new_cumulative {
                state.insert(pool_id, totals);
            }
        }

        Ok(ops)
    }

    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        self.db_pool.set(db_pool.inner().clone()).ok();
        self.oracle_cache
            .load_from_db_once(db_pool.inner(), self.chain_id)
            .await?;
        self.metadata_cache
            .load_into(db_pool, self.chain_id)
            .await?;
        self.load_hook_pool_map(db_pool).await?;
        self.load_all_proceeds_state(db_pool).await?;
        tracing::info!("V4BaseMetricsHandler initialized");
        Ok(())
    }

    async fn on_reorg(&self, _orphaned: &[u64]) -> Result<(), TransformationError> {
        // The DB has just been rolled back to its pre-reorg state by the
        // finalizer; clear all in-memory state so subsequent handle() calls
        // lazily reload from the (now-restored) DB.
        self.proceeds_state.write().unwrap().clear();
        *self.in_flight_pre.write().unwrap() = None;
        self.failed_ranges.write().unwrap().clear();
        tracing::info!("V4BaseMetricsHandler: cleared in-memory state after reorg");
        Ok(())
    }

    async fn on_commit_success(&self, range: (u64, u64)) -> Result<(), TransformationError> {
        *self.in_flight_pre.write().unwrap() = None;
        self.failed_ranges.write().unwrap().remove(&range);
        Ok(())
    }

    async fn on_commit_failure(&self, range: (u64, u64)) -> Result<(), TransformationError> {
        // Revert the optimistic cache update using the pre-values stashed in
        // handle(). If no pre-values are stashed (e.g., handle() returned
        // empty ops or failed before the stash), there is nothing to revert.
        if let Some(pre) = self.in_flight_pre.write().unwrap().take() {
            let mut state = self.proceeds_state.write().unwrap();
            for (pool_id, prev) in pre {
                state.insert(pool_id, prev);
            }
        }
        self.failed_ranges.write().unwrap().insert(range);
        tracing::warn!(
            "V4BaseMetricsHandler: commit failed for range {:?}, blocking later ranges until retry succeeds",
            range
        );
        Ok(())
    }
}

impl EventHandler for V4BaseMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![EventTrigger::new(SOURCE, "Swap(int24,uint256,uint256)")]
    }

    fn call_dependencies(&self) -> Vec<(String, String)> {
        vec![chainlink_latest_answer_dependency()]
    }

    fn contiguous_handler_dependencies(&self) -> Vec<&'static str> {
        vec!["V4CreateHandler"]
    }
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

impl V4BaseMetricsHandler {
    /// Load hook_address → pool_id map from v4_pool_configs for this chain.
    async fn load_hook_pool_map(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        let client = db_pool
            .inner()
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows = client
            .query(
                "SELECT pool_id, hook_address FROM v4_pool_configs WHERE chain_id = $1",
                &[&(self.chain_id as i64)],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let mut map = self.hook_pool_map.write().unwrap();
        for row in &rows {
            let pool_id_bytes: Vec<u8> = row.get("pool_id");
            let hook_addr_bytes: Vec<u8> = row.get("hook_address");
            if pool_id_bytes.len() == 32 && hook_addr_bytes.len() == 20 {
                let mut pool_id = [0u8; 32];
                let mut hook_addr = [0u8; 20];
                pool_id.copy_from_slice(&pool_id_bytes);
                hook_addr.copy_from_slice(&hook_addr_bytes);
                map.entry(hook_addr).or_insert(pool_id);
            }
        }

        tracing::debug!(
            "V4BaseMetricsHandler: loaded {} hook→pool entries",
            map.len()
        );
        Ok(())
    }

    /// Re-query v4_pool_configs if any event's hook address is missing from the map.
    async fn refresh_hook_pool_map_for_events(
        &self,
        events: &[&DecodedEvent],
    ) -> Result<(), TransformationError> {
        let missing = {
            let map = self.hook_pool_map.read().unwrap();
            events
                .iter()
                .any(|e| !map.contains_key(&e.contract_address))
        };

        if !missing {
            return Ok(());
        }

        let Some(pool) = self.db_pool.get() else {
            return Ok(());
        };

        let client = pool
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows = client
            .query(
                "SELECT pool_id, hook_address FROM v4_pool_configs WHERE chain_id = $1",
                &[&(self.chain_id as i64)],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let mut map = self.hook_pool_map.write().unwrap();
        for row in &rows {
            let pool_id_bytes: Vec<u8> = row.get("pool_id");
            let hook_addr_bytes: Vec<u8> = row.get("hook_address");
            if pool_id_bytes.len() == 32 && hook_addr_bytes.len() == 20 {
                let mut pool_id = [0u8; 32];
                let mut hook_addr = [0u8; 20];
                pool_id.copy_from_slice(&pool_id_bytes);
                hook_addr.copy_from_slice(&hook_addr_bytes);
                map.entry(hook_addr).or_insert(pool_id);
            }
        }

        let still_missing: Vec<_> = events
            .iter()
            .filter(|e| !map.contains_key(&e.contract_address))
            .map(|e| hex::encode(e.contract_address))
            .collect();
        if !still_missing.is_empty() {
            tracing::warn!(
                "V4BaseMetricsHandler: {} hook address(es) still missing from v4_pool_configs \
                 after refresh; swaps from those hooks will be skipped: {:?}",
                still_missing.len(),
                still_missing,
            );
        }

        Ok(())
    }

    /// Load all committed checkpoints into the in-memory cache at startup.
    async fn load_all_proceeds_state(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        let client = db_pool
            .inner()
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows = client
            .query(
                "SELECT pool_id, total_proceeds::text, total_tokens_sold::text \
                 FROM v4_base_proceeds_state \
                 WHERE chain_id = $1 AND source = $2 AND source_version = $3",
                &[
                    &(self.chain_id as i64),
                    &self.name(),
                    &(self.version() as i32),
                ],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let mut state = self.proceeds_state.write().unwrap();
        for row in &rows {
            let pool_id_bytes: Vec<u8> = row.get("pool_id");
            if pool_id_bytes.len() != 32 {
                continue;
            }
            let mut pool_id = [0u8; 32];
            pool_id.copy_from_slice(&pool_id_bytes);

            let proceeds_str: String = row.get("total_proceeds");
            let tokens_str: String = row.get("total_tokens_sold");

            let proceeds = U256::from_str_radix(proceeds_str.trim(), 10).unwrap_or(U256::ZERO);
            let tokens = U256::from_str_radix(tokens_str.trim(), 10).unwrap_or(U256::ZERO);

            state.insert(pool_id, (proceeds, tokens));
        }

        tracing::debug!(
            "V4BaseMetricsHandler: loaded {} proceeds checkpoints",
            state.len()
        );
        Ok(())
    }

    /// Fetch proceeds state from DB for any pool IDs missing from the in-memory cache.
    ///
    /// Pools without a checkpoint row are still marked present in the cache with (0, 0),
    /// so subsequent calls for the same pool don't re-query the DB.
    async fn refresh_proceeds_state_for_pools(
        &self,
        pool_ids: &[[u8; 32]],
    ) -> Result<(), TransformationError> {
        // Find pools not already in the cache.
        let missing: Vec<[u8; 32]> = {
            let state = self.proceeds_state.read().unwrap();
            pool_ids
                .iter()
                .copied()
                .filter(|id| !state.contains_key(id))
                .collect()
        };

        if missing.is_empty() {
            return Ok(());
        }

        let Some(pool) = self.db_pool.get() else {
            // No DB pool — seed missing entries with zero so we don't loop.
            let mut state = self.proceeds_state.write().unwrap();
            for id in &missing {
                state.entry(*id).or_insert((U256::ZERO, U256::ZERO));
            }
            return Ok(());
        };

        let client = pool
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        // Use a BYTEA[] parameter so the query stays bounded.
        let missing_bytes: Vec<&[u8]> = missing.iter().map(|id| id.as_slice()).collect();
        let rows = client
            .query(
                "SELECT pool_id, total_proceeds::text, total_tokens_sold::text \
                 FROM v4_base_proceeds_state \
                 WHERE chain_id = $1 \
                   AND source = $2 \
                   AND source_version = $3 \
                   AND pool_id = ANY($4)",
                &[
                    &(self.chain_id as i64),
                    &self.name(),
                    &(self.version() as i32),
                    &missing_bytes,
                ],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let mut state = self.proceeds_state.write().unwrap();
        for row in &rows {
            let pool_id_bytes: Vec<u8> = row.get("pool_id");
            if pool_id_bytes.len() != 32 {
                continue;
            }
            let mut pool_id = [0u8; 32];
            pool_id.copy_from_slice(&pool_id_bytes);

            let proceeds_str: String = row.get("total_proceeds");
            let tokens_str: String = row.get("total_tokens_sold");
            let proceeds = U256::from_str_radix(proceeds_str.trim(), 10).unwrap_or(U256::ZERO);
            let tokens = U256::from_str_radix(tokens_str.trim(), 10).unwrap_or(U256::ZERO);

            state.insert(pool_id, (proceeds, tokens));
        }

        // Any pool that still isn't in the cache has no checkpoint yet; seed with (0, 0)
        // so we don't re-query for it on every subsequent handle().
        for id in &missing {
            state.entry(*id).or_insert((U256::ZERO, U256::ZERO));
        }

        Ok(())
    }

    /// Extract swap inputs from V4 base Swap events, computing per-swap deltas.
    ///
    /// Events are sorted by (block_number, log_index) per pool so that cumulative
    /// deltas are always computed from the immediately preceding event.
    ///
    /// Returns (swap_inputs, new_cumulative) where new_cumulative maps each seen
    /// pool_id to its final (totalProceeds, totalTokensSold) after this batch.
    fn extract_swaps(
        &self,
        events: &[&DecodedEvent],
        prev_state: &ProceedsState,
    ) -> Result<(Vec<SwapInput>, ProceedsState), TransformationError> {
        let hook_pool_map = self.hook_pool_map.read().unwrap();

        // Group events by pool_id.
        let mut pool_events: HashMap<[u8; 32], Vec<&DecodedEvent>> = HashMap::new();
        for event in events {
            match hook_pool_map.get(&event.contract_address) {
                Some(&pool_id) => pool_events.entry(pool_id).or_default().push(event),
                None => tracing::warn!(
                    "V4BaseMetricsHandler: no pool_id for hook {} at block {}, skipping",
                    hex::encode(event.contract_address),
                    event.block_number,
                ),
            }
        }
        drop(hook_pool_map);

        let mut swaps: Vec<SwapInput> = Vec::new();
        let mut new_cumulative: ProceedsState = HashMap::new();

        for (pool_id, mut pool_evts) in pool_events {
            pool_evts.sort_by_key(|e| (e.block_number, e.log_index));

            let (mut prev_proceeds, mut prev_tokens) = prev_state
                .get(&pool_id)
                .copied()
                .unwrap_or((U256::ZERO, U256::ZERO));

            let meta = match self.metadata_cache.get(pool_id.as_ref()) {
                Some(m) => m,
                None => {
                    tracing::warn!(
                        "V4BaseMetricsHandler: no metadata for pool {}, skipping {} events",
                        hex::encode(pool_id),
                        pool_evts.len(),
                    );
                    continue;
                }
            };

            for event in pool_evts {
                let tick = event.extract_i32_flexible("currentTick")?;
                let total_proceeds = event.extract_uint256("totalProceeds")?;
                let total_tokens_sold = event.extract_uint256("totalTokensSold")?;

                // Cumulative counters should be non-decreasing; guard against wrap/reset.
                let proceeds_delta = if total_proceeds >= prev_proceeds {
                    total_proceeds - prev_proceeds
                } else {
                    tracing::warn!(
                        "V4BaseMetricsHandler: totalProceeds decreased for pool {} at block {} \
                         (prev={}, cur={}); treating as full value",
                        hex::encode(pool_id),
                        event.block_number,
                        prev_proceeds,
                        total_proceeds,
                    );
                    // NOTE: "treat as full value" (i.e. prev=0) also serves as crash-replay
                    // idempotency. If the process crashed after committing the DB row but before
                    // updating handler progress, the in-memory cache is reloaded from the
                    // post-commit checkpoint on restart, so `prev_proceeds` exceeds this event's
                    // cumulative. Using `total_proceeds` as the delta resets prev to the current
                    // event's baseline, producing identical pool_snapshots to the original run.
                    // Do NOT convert this warn to an error without preserving that invariant.
                    total_proceeds
                };

                let tokens_delta = if total_tokens_sold >= prev_tokens {
                    total_tokens_sold - prev_tokens
                } else {
                    tracing::warn!(
                        "V4BaseMetricsHandler: totalTokensSold decreased for pool {} at block {} \
                         (prev={}, cur={}); treating as full value",
                        hex::encode(pool_id),
                        event.block_number,
                        prev_tokens,
                        total_tokens_sold,
                    );
                    total_tokens_sold
                };

                // Derive sqrtPriceX96 from the current tick.
                let sqrt_price_x96 = match tick_to_sqrt_price_x96(tick) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            "V4BaseMetricsHandler: invalid tick {} at block {}: {}, skipping",
                            tick,
                            event.block_number,
                            e
                        );
                        prev_proceeds = total_proceeds;
                        prev_tokens = total_tokens_sold;
                        continue;
                    }
                };

                // Map cumulative deltas to signed amount0 / amount1.
                //
                // V4 base auctions sell base tokens for quote tokens:
                //   proceeds_delta  = quote received by the pool (inflow)
                //   tokens_delta    = base sold out of the pool   (outflow)
                //
                // If is_token_0 = true  → base is token0, quote is token1
                //   amount0 = -(tokens_delta)   [base leaves]
                //   amount1 =  proceeds_delta   [quote enters]
                //
                // If is_token_0 = false → base is token1, quote is token0
                //   amount0 =  proceeds_delta   [quote enters]
                //   amount1 = -(tokens_delta)   [base leaves]
                let (amount0, amount1) =
                    signed_amounts(proceeds_delta, tokens_delta, meta.is_token_0);

                swaps.push(SwapInput {
                    pool_id: pool_id.to_vec(),
                    transaction_hash: event.transaction_hash,
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    log_index: event.log_index,
                    amount0,
                    amount1,
                    sqrt_price_x96,
                    tick,
                    liquidity: U256::ZERO, // not available from V4 base Swap event
                });

                prev_proceeds = total_proceeds;
                prev_tokens = total_tokens_sold;
            }

            new_cumulative.insert(pool_id, (prev_proceeds, prev_tokens));
        }

        Ok((swaps, new_cumulative))
    }
}

// ─── Free helpers ─────────────────────────────────────────────────────────────

/// Compute signed amount0 / amount1 from unsigned delta values.
///
/// Exported for tests.
pub(crate) fn signed_amounts(
    proceeds_delta: U256,
    tokens_delta: U256,
    is_token_0: bool,
) -> (I256, I256) {
    // U256 → I256: safe for values well below 2^255 (bounded by token supply).
    let proceeds_i = i256_from_u256(proceeds_delta);
    let tokens_i = i256_from_u256(tokens_delta);

    if is_token_0 {
        // base = token0, quote = token1
        (-tokens_i, proceeds_i)
    } else {
        // base = token1, quote = token0
        (proceeds_i, -tokens_i)
    }
}

/// Convert U256 to I256, clamping at I256::MAX to avoid wrapping.
fn i256_from_u256(v: U256) -> I256 {
    // I256::MAX = 2^255 - 1. Token values are far below this threshold.
    let max = U256::from_limbs([u64::MAX, u64::MAX, u64::MAX, i64::MAX as u64]);
    if v > max {
        I256::MAX
    } else {
        I256::try_from(v).unwrap_or(I256::MAX)
    }
}

/// Build a `DbOperation` that upserts the cumulative checkpoint for one pool.
fn upsert_proceeds_state(
    chain_id: u64,
    pool_id: &[u8; 32],
    total_proceeds: &U256,
    total_tokens_sold: &U256,
    source: &str,
    source_version: u32,
) -> DbOperation {
    DbOperation::Upsert {
        table: "v4_base_proceeds_state".to_string(),
        conflict_columns: vec![
            "chain_id".to_string(),
            "pool_id".to_string(),
            "source".to_string(),
            "source_version".to_string(),
        ],
        update_columns: vec![
            "total_proceeds".to_string(),
            "total_tokens_sold".to_string(),
        ],
        update_condition: None,
        columns: vec![
            "chain_id".to_string(),
            "pool_id".to_string(),
            "source".to_string(),
            "source_version".to_string(),
            "total_proceeds".to_string(),
            "total_tokens_sold".to_string(),
        ],
        values: vec![
            DbValue::Int64(chain_id as i64),
            DbValue::Bytes32(*pool_id),
            DbValue::VarChar(source.to_string()),
            DbValue::Int32(source_version as i32),
            DbValue::Numeric(total_proceeds.to_string()),
            DbValue::Numeric(total_tokens_sold.to_string()),
        ],
    }
}

// ─── Registration ─────────────────────────────────────────────────────────────

pub fn register_handlers(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    cache: Arc<PoolMetadataCache>,
    oracle_cache: Arc<OraclePriceCache>,
) {
    registry.register_event_handler(V4BaseMetricsHandler {
        metadata_cache: cache,
        oracle_cache,
        decimals_init: Once::new(),
        chain_id,
        db_pool: OnceLock::new(),
        hook_pool_map: RwLock::new(HashMap::new()),
        proceeds_state: RwLock::new(HashMap::new()),
        in_flight_pre: RwLock::new(None),
        failed_ranges: RwLock::new(BTreeSet::new()),
    });
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── signed_amounts ────────────────────────────────────────────────────────

    #[test]
    fn test_signed_amounts_is_token_0_true() {
        // base = token0, quote = token1
        // tokens_delta = 100 base leaves pool  → amount0 = -100
        // proceeds_delta = 50 quote enters pool → amount1 =  +50
        let (a0, a1) = signed_amounts(U256::from(50u64), U256::from(100u64), true);
        assert_eq!(a0, I256::try_from(-100i64).unwrap());
        assert_eq!(a1, I256::try_from(50i64).unwrap());
    }

    #[test]
    fn test_signed_amounts_is_token_0_false() {
        // base = token1, quote = token0
        // proceeds_delta = 50 quote enters  → amount0 = +50
        // tokens_delta = 100 base leaves    → amount1 = -100
        let (a0, a1) = signed_amounts(U256::from(50u64), U256::from(100u64), false);
        assert_eq!(a0, I256::try_from(50i64).unwrap());
        assert_eq!(a1, I256::try_from(-100i64).unwrap());
    }

    #[test]
    fn test_signed_amounts_zero_deltas() {
        let (a0, a1) = signed_amounts(U256::ZERO, U256::ZERO, true);
        assert_eq!(a0, I256::ZERO);
        assert_eq!(a1, I256::ZERO);
    }

    // ── delta computation via extract_swaps ───────────────────────────────────

    fn make_handler(chain_id: u64) -> V4BaseMetricsHandler {
        V4BaseMetricsHandler {
            metadata_cache: Arc::new(PoolMetadataCache::new()),
            oracle_cache: Arc::new(OraclePriceCache::new()),
            decimals_init: Once::new(),
            chain_id,
            db_pool: OnceLock::new(),
            hook_pool_map: RwLock::new(HashMap::new()),
            proceeds_state: RwLock::new(HashMap::new()),
            in_flight_pre: RwLock::new(None),
            failed_ranges: RwLock::new(BTreeSet::new()),
        }
    }

    fn make_pool_id(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    fn make_hook_addr(seed: u8) -> [u8; 20] {
        [seed; 20]
    }

    fn make_event_params(
        tick: i32,
        total_proceeds: u64,
        total_tokens_sold: u64,
    ) -> std::collections::HashMap<std::sync::Arc<str>, crate::types::decoded::DecodedValue> {
        use crate::types::decoded::DecodedValue;
        use std::sync::Arc;
        let mut params = std::collections::HashMap::new();
        params.insert(Arc::from("currentTick"), DecodedValue::Int32(tick));
        params.insert(
            Arc::from("totalProceeds"),
            DecodedValue::Uint256(U256::from(total_proceeds)),
        );
        params.insert(
            Arc::from("totalTokensSold"),
            DecodedValue::Uint256(U256::from(total_tokens_sold)),
        );
        params
    }

    fn make_decoded_event(
        hook_addr: [u8; 20],
        block_number: u64,
        log_index: u32,
        tick: i32,
        total_proceeds: u64,
        total_tokens_sold: u64,
    ) -> DecodedEvent {
        DecodedEvent {
            block_number,
            block_timestamp: block_number * 12,
            transaction_hash: [0u8; 32],
            log_index,
            contract_address: hook_addr,
            source_name: SOURCE.to_string(),
            event_name: "Swap".to_string(),
            event_signature: "Swap(int24,uint256,uint256)".to_string(),
            params: make_event_params(tick, total_proceeds, total_tokens_sold),
        }
    }

    fn setup_handler_with_pool(
        handler: &V4BaseMetricsHandler,
        hook_addr: [u8; 20],
        pool_id: [u8; 32],
        is_token_0: bool,
    ) {
        use crate::transformations::util::pool_metadata::PoolMetadata;

        handler
            .hook_pool_map
            .write()
            .unwrap()
            .insert(hook_addr, pool_id);

        handler.metadata_cache.insert_if_absent(
            pool_id.to_vec(),
            PoolMetadata {
                base_token: [1u8; 20],
                quote_token: [2u8; 20],
                is_token_0,
                base_decimals: 18,
                quote_decimals: 18,
                total_supply: None,
            },
        );
    }

    #[test]
    fn test_extract_swaps_first_event_no_prev_state() {
        let handler = make_handler(8453);
        let hook_addr = make_hook_addr(0xAA);
        let pool_id = make_pool_id(0x01);
        setup_handler_with_pool(&handler, hook_addr, pool_id, true);

        let event = make_decoded_event(hook_addr, 100, 0, 0, 1000, 500);
        let events = vec![&event];

        let prev_state = HashMap::new(); // no prior checkpoint
        let (swaps, new_cum) = handler.extract_swaps(&events, &prev_state).unwrap();

        assert_eq!(swaps.len(), 1);
        let swap = &swaps[0];
        // No prev state → delta = full cumulative value
        // is_token_0 = true: amount0 = -500 (tokens out), amount1 = +1000 (proceeds in)
        assert_eq!(swap.amount0, I256::try_from(-500i64).unwrap());
        assert_eq!(swap.amount1, I256::try_from(1000i64).unwrap());
        assert_eq!(swap.tick, 0);
        assert_eq!(swap.block_number, 100);

        // Cumulative checkpoint should be updated
        let (tp, ts) = new_cum[&pool_id];
        assert_eq!(tp, U256::from(1000u64));
        assert_eq!(ts, U256::from(500u64));
    }

    #[test]
    fn test_extract_swaps_delta_from_prev_state() {
        let handler = make_handler(8453);
        let hook_addr = make_hook_addr(0xBB);
        let pool_id = make_pool_id(0x02);
        setup_handler_with_pool(&handler, hook_addr, pool_id, false);

        // Prev state: pool already has 1000 proceeds, 500 tokens sold
        let mut prev_state = HashMap::new();
        prev_state.insert(pool_id, (U256::from(1000u64), U256::from(500u64)));

        // New event: cumulative is now 1200 proceeds, 600 tokens
        let event = make_decoded_event(hook_addr, 101, 0, 100, 1200, 600);
        let events = vec![&event];

        let (swaps, new_cum) = handler.extract_swaps(&events, &prev_state).unwrap();

        assert_eq!(swaps.len(), 1);
        let swap = &swaps[0];
        // is_token_0 = false: amount0 = +200 (proceeds in), amount1 = -100 (tokens out)
        assert_eq!(swap.amount0, I256::try_from(200i64).unwrap());
        assert_eq!(swap.amount1, I256::try_from(-100i64).unwrap());

        let (tp, ts) = new_cum[&pool_id];
        assert_eq!(tp, U256::from(1200u64));
        assert_eq!(ts, U256::from(600u64));
    }

    #[test]
    fn test_extract_swaps_multiple_events_same_pool_same_block() {
        let handler = make_handler(8453);
        let hook_addr = make_hook_addr(0xCC);
        let pool_id = make_pool_id(0x03);
        setup_handler_with_pool(&handler, hook_addr, pool_id, true);

        let prev_state = HashMap::new();

        // Two swap events in the same block; second should delta from first.
        let e0 = make_decoded_event(hook_addr, 200, 0, 0, 500, 250);
        let e1 = make_decoded_event(hook_addr, 200, 1, 10, 800, 400);
        let events = vec![&e0, &e1];

        let (swaps, new_cum) = handler.extract_swaps(&events, &prev_state).unwrap();

        assert_eq!(swaps.len(), 2);
        // First swap: delta from 0 → (500, 250)
        assert_eq!(swaps[0].amount0, I256::try_from(-250i64).unwrap()); // -tokens
        assert_eq!(swaps[0].amount1, I256::try_from(500i64).unwrap()); //  proceeds

        // Second swap: delta from (500,250) → (800,400) = (300,150)
        assert_eq!(swaps[1].amount0, I256::try_from(-150i64).unwrap());
        assert_eq!(swaps[1].amount1, I256::try_from(300i64).unwrap());

        let (tp, ts) = new_cum[&pool_id];
        assert_eq!(tp, U256::from(800u64));
        assert_eq!(ts, U256::from(400u64));
    }

    #[test]
    fn test_extract_swaps_no_pool_id_skipped() {
        let handler = make_handler(8453);
        // No entry in hook_pool_map
        let hook_addr = make_hook_addr(0xDD);

        let event = make_decoded_event(hook_addr, 100, 0, 0, 1000, 500);
        let events = vec![&event];

        let (swaps, new_cum) = handler.extract_swaps(&events, &HashMap::new()).unwrap();

        assert!(swaps.is_empty());
        assert!(new_cum.is_empty());
    }

    #[test]
    fn test_proceeds_state_cache_used_instead_of_db_on_hit() {
        // After a write to the in-memory proceeds_state, the next extract_swaps
        // call should use those cached values rather than treating the pool as
        // having no prior state.
        let handler = make_handler(8453);
        let hook_addr = make_hook_addr(0xEE);
        let pool_id = make_pool_id(0x04);
        setup_handler_with_pool(&handler, hook_addr, pool_id, true);

        // Simulate an earlier handle() that populated the cache.
        handler
            .proceeds_state
            .write()
            .unwrap()
            .insert(pool_id, (U256::from(700u64), U256::from(350u64)));

        // A new event arrives with a higher cumulative total.
        let event = make_decoded_event(hook_addr, 150, 0, 0, 1000, 500);
        let events = vec![&event];

        // Simulate handle()'s snapshot read from the in-memory state:
        let prev_state: ProceedsState = {
            let state = handler.proceeds_state.read().unwrap();
            [pool_id]
                .iter()
                .filter_map(|id| state.get(id).map(|&v| (*id, v)))
                .collect()
        };

        let (swaps, new_cum) = handler.extract_swaps(&events, &prev_state).unwrap();

        assert_eq!(swaps.len(), 1);
        // delta = (1000 - 700, 500 - 350) = (300, 150)
        // is_token_0 = true: amount0 = -150, amount1 = +300
        assert_eq!(swaps[0].amount0, I256::try_from(-150i64).unwrap());
        assert_eq!(swaps[0].amount1, I256::try_from(300i64).unwrap());
        assert_eq!(new_cum[&pool_id], (U256::from(1000u64), U256::from(500u64)));
    }

    #[test]
    fn test_upsert_proceeds_state_op() {
        let pool_id = make_pool_id(0xFF);
        let op = upsert_proceeds_state(
            8453,
            &pool_id,
            &U256::from(9999u64),
            &U256::from(1234u64),
            "DopplerV4Hook",
            1,
        );
        match op {
            DbOperation::Upsert {
                table,
                conflict_columns,
                update_columns,
                ..
            } => {
                assert_eq!(table, "v4_base_proceeds_state");
                assert!(conflict_columns.contains(&"pool_id".to_string()));
                assert!(update_columns.contains(&"total_proceeds".to_string()));
                assert!(update_columns.contains(&"total_tokens_sold".to_string()));
            }
            _ => panic!("expected Upsert"),
        }
    }

    #[test]
    fn test_handler_declares_chainlink_dependency() {
        let handler = make_handler(8453);
        assert!(handler
            .call_dependencies()
            .contains(&chainlink_latest_answer_dependency()));
    }

    // ── commit hooks / gate / reorg ───────────────────────────────────────────

    #[tokio::test]
    async fn test_on_reorg_clears_all_in_memory_state() {
        let handler = make_handler(8453);
        let pool_id = make_pool_id(0x10);

        // Pre-populate all three caches/structures.
        handler
            .proceeds_state
            .write()
            .unwrap()
            .insert(pool_id, (U256::from(42u64), U256::from(21u64)));
        *handler.in_flight_pre.write().unwrap() =
            Some([(pool_id, (U256::ZERO, U256::ZERO))].into_iter().collect());
        handler.failed_ranges.write().unwrap().insert((100, 101));

        handler.on_reorg(&[100, 101]).await.unwrap();

        assert!(handler.proceeds_state.read().unwrap().is_empty());
        assert!(handler.in_flight_pre.read().unwrap().is_none());
        assert!(handler.failed_ranges.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_on_commit_failure_reverts_cache_and_records_range() {
        let handler = make_handler(8453);
        let pool_id = make_pool_id(0x11);

        // Simulate the in-flight state right before a commit failure:
        //   cache is at (100, 50) (the optimistic post-update), but the
        //   pre-update state was (70, 35).
        handler
            .proceeds_state
            .write()
            .unwrap()
            .insert(pool_id, (U256::from(100u64), U256::from(50u64)));
        *handler.in_flight_pre.write().unwrap() = Some(
            [(pool_id, (U256::from(70u64), U256::from(35u64)))]
                .into_iter()
                .collect(),
        );

        handler.on_commit_failure((200, 201)).await.unwrap();

        // Cache should have been reverted to the pre-update value.
        let state = handler.proceeds_state.read().unwrap();
        assert_eq!(
            state.get(&pool_id).copied(),
            Some((U256::from(70u64), U256::from(35u64)))
        );
        // in_flight_pre should be drained.
        assert!(handler.in_flight_pre.read().unwrap().is_none());
        // Range should be recorded as failed.
        assert!(handler.failed_ranges.read().unwrap().contains(&(200, 201)));
    }

    #[tokio::test]
    async fn test_on_commit_success_clears_pending_and_drains_range() {
        let handler = make_handler(8453);
        let pool_id = make_pool_id(0x12);

        *handler.in_flight_pre.write().unwrap() =
            Some([(pool_id, (U256::ZERO, U256::ZERO))].into_iter().collect());
        handler.failed_ranges.write().unwrap().insert((300, 301));

        handler.on_commit_success((300, 301)).await.unwrap();

        assert!(handler.in_flight_pre.read().unwrap().is_none());
        assert!(handler.failed_ranges.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_on_commit_success_for_unrelated_range_preserves_others() {
        let handler = make_handler(8453);
        handler.failed_ranges.write().unwrap().insert((100, 101));
        handler.failed_ranges.write().unwrap().insert((102, 103));

        handler.on_commit_success((102, 103)).await.unwrap();

        let remaining = handler.failed_ranges.read().unwrap();
        assert!(remaining.contains(&(100, 101)));
        assert!(!remaining.contains(&(102, 103)));
    }

    #[tokio::test]
    async fn test_on_commit_failure_with_no_in_flight_is_noop_on_cache() {
        // If handle() returned empty ops (or failed before the stash) then
        // in_flight_pre is None — on_commit_failure should still record the
        // range but leave the cache alone.
        let handler = make_handler(8453);
        let pool_id = make_pool_id(0x13);
        handler
            .proceeds_state
            .write()
            .unwrap()
            .insert(pool_id, (U256::from(999u64), U256::from(888u64)));

        handler.on_commit_failure((400, 401)).await.unwrap();

        let state = handler.proceeds_state.read().unwrap();
        assert_eq!(
            state.get(&pool_id).copied(),
            Some((U256::from(999u64), U256::from(888u64)))
        );
        assert!(handler.failed_ranges.read().unwrap().contains(&(400, 401)));
    }

    // Gate tests — exercised by constructing an empty context and driving
    // handle() with the failed_ranges set pre-populated. We don't need
    // events since the gate is the very first check.

    fn empty_ctx(range_start: u64, range_end: u64) -> TransformationContext {
        use crate::rpc::UnifiedRpcClient;
        use crate::transformations::historical::HistoricalDataReader;
        use std::collections::HashMap as StdHashMap;

        // These services aren't exercised because handle() returns before
        // touching them when the gate trips or events are empty.
        let historical = Arc::new(
            HistoricalDataReader::new("test_chain_v4_metrics")
                .expect("HistoricalDataReader::new should succeed"),
        );
        let rpc = Arc::new(
            UnifiedRpcClient::from_url("http://localhost:8545")
                .expect("RPC client construction should succeed"),
        );
        TransformationContext::new(
            "test".to_string(),
            8453,
            range_start,
            range_end,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(StdHashMap::new()),
            historical,
            rpc,
            Arc::new(StdHashMap::new()),
        )
    }

    #[tokio::test]
    async fn test_gate_blocks_later_ranges_after_failure() {
        let handler = make_handler(8453);
        // Simulate a prior commit failure for range (100, 101).
        handler.failed_ranges.write().unwrap().insert((100, 101));

        // A later range should be refused.
        let ctx = empty_ctx(200, 201);
        let result = handler.handle(&ctx).await;
        match result {
            Err(TransformationError::TransientBlocked(_)) => {}
            other => panic!("expected TransientBlocked, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_gate_allows_the_failed_range_to_retry() {
        let handler = make_handler(8453);
        // The failed range is (100, 101); retrying it should be allowed.
        handler.failed_ranges.write().unwrap().insert((100, 101));

        let ctx = empty_ctx(100, 101);
        // No events present in the context, so handle() returns Ok(vec![])
        // after passing the gate. If the gate had refused we'd get
        // TransientBlocked instead.
        let result = handler.handle(&ctx).await;
        assert!(matches!(result, Ok(ops) if ops.is_empty()));
    }

    #[tokio::test]
    async fn test_gate_allows_any_range_when_failed_ranges_empty() {
        let handler = make_handler(8453);
        let ctx = empty_ctx(9999, 10000);
        let result = handler.handle(&ctx).await;
        assert!(matches!(result, Ok(ops) if ops.is_empty()));
    }
}
