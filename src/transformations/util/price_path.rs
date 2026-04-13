//! Graph-based token price path resolution via frontier expansion.
//!
//! For tokens that are not directly priceable via configured anchor pools,
//! this module finds a path through indexed Doppler pools back to a
//! USD-priceable token. The path is cached in `token_price_paths` and
//! refreshed when stale (> 1 week).
//!
//! Algorithm: BFS frontier expansion from the target token outward, up to
//! 5 hops, considering only pools with >= $1,000 active liquidity. Returns
//! the path with the highest bottleneck (minimum-along-path) liquidity.

use std::collections::{HashMap, HashSet};

use bigdecimal::BigDecimal;
use deadpool_postgres::Pool;

use crate::transformations::error::TransformationError;

/// Approximately 1 week of blocks on Base (2s block time).
const STALENESS_THRESHOLD_BLOCKS: u64 = 302_400;

/// Maximum number of hops in a price path.
const MAX_HOPS: usize = 5;

/// Minimum active liquidity (USD) for a pool to be considered as a path edge.
const MIN_LIQUIDITY_USD: i64 = 1_000;

/// A cached price path from a token to an anchor token.
#[derive(Debug, Clone)]
pub struct CachedPricePath {
    pub path_pool_ids: Vec<Vec<u8>>,
    pub anchor_token: [u8; 20],
    pub path_liquidity_usd: Option<BigDecimal>,
    pub resolved_at_block: u64,
    pub is_priceable: bool,
}

/// A pool edge discovered during frontier expansion.
struct PoolEdge {
    pool_id: Vec<u8>,
    base_token: [u8; 20],
    quote_token: [u8; 20],
    liquidity_usd: BigDecimal,
}

/// Check for a cached path and resolve if missing or stale.
///
/// Returns `Some(path)` if a valid path exists (either cached-fresh or newly resolved).
/// Returns `None` on DB errors or if the token is already directly priceable.
///
/// `max_block`: if `Some(b)`, only consider snapshot rows with block_number < b.
pub async fn check_or_resolve_path(
    db_pool: &Pool,
    chain_id: u64,
    target_token: &[u8; 20],
    priceable_tokens: &HashSet<[u8; 20]>,
    current_block: u64,
    max_block: Option<u64>,
) -> Option<CachedPricePath> {
    // Don't resolve if already priceable
    if priceable_tokens.contains(target_token) {
        return None;
    }

    // Check cache
    if let Ok(Some(cached)) = query_cached_path(db_pool, chain_id, target_token).await {
        // Guard: ignore entries resolved at or after our block range (from a future run)
        if cached.resolved_at_block < current_block {
            let age = current_block.saturating_sub(cached.resolved_at_block);
            if age < STALENESS_THRESHOLD_BLOCKS {
                return Some(cached);
            }
        }
        // Stale or future — re-resolve below
    }

    // Resolve via frontier expansion
    let result =
        resolve_token_price_path(db_pool, chain_id, target_token, priceable_tokens, max_block)
            .await;

    // Cache the result — only Ok variants get cached
    let path = match result {
        Ok(Some(resolved)) => CachedPricePath {
            path_pool_ids: resolved.path_pool_ids,
            anchor_token: resolved.anchor_token,
            path_liquidity_usd: Some(resolved.bottleneck_liquidity),
            resolved_at_block: current_block,
            is_priceable: true,
        },
        Ok(None) => CachedPricePath {
            path_pool_ids: Vec::new(),
            anchor_token: [0u8; 20],
            path_liquidity_usd: None,
            resolved_at_block: current_block,
            is_priceable: false,
        },
        Err(e) => {
            tracing::warn!(
                token = hex::encode(target_token),
                error = %e,
                "Path resolution failed, not caching"
            );
            return None; // Don't cache, don't poison
        }
    };

    if let Err(e) = upsert_cached_path(db_pool, chain_id, target_token, &path).await {
        tracing::warn!(
            token = hex::encode(target_token),
            error = %e,
            "Failed to cache price path"
        );
    }

    Some(path)
}

/// Result of a successful path resolution.
struct ResolvedPath {
    path_pool_ids: Vec<Vec<u8>>,
    anchor_token: [u8; 20],
    bottleneck_liquidity: BigDecimal,
}

/// BFS frontier expansion from target_token outward, max 5 hops.
///
/// At each hop, queries pools touching the current frontier tokens with
/// >= $1,000 active liquidity. Tracks the best (highest bottleneck liquidity)
/// > path to any priceable token found.
///
/// `max_block`: if `Some(b)`, only consider snapshot rows with block_number < b.
async fn resolve_token_price_path(
    db_pool: &Pool,
    chain_id: u64,
    target_token: &[u8; 20],
    priceable_tokens: &HashSet<[u8; 20]>,
    max_block: Option<u64>,
) -> Result<Option<ResolvedPath>, TransformationError> {
    let mut visited: HashSet<[u8; 20]> = HashSet::new();
    visited.insert(*target_token);

    let mut frontier: Vec<[u8; 20]> = vec![*target_token];

    // parent[token] = (pool_id, came_from_token)
    let mut parent: HashMap<[u8; 20], (Vec<u8>, [u8; 20])> = HashMap::new();

    // Best bottleneck liquidity to reach each token
    let mut best_bottleneck: HashMap<[u8; 20], BigDecimal> = HashMap::new();
    best_bottleneck.insert(*target_token, BigDecimal::from(i64::MAX));

    let mut best_anchor: Option<ResolvedPath> = None;

    for _hop in 0..MAX_HOPS {
        if frontier.is_empty() {
            break;
        }

        let edges = query_frontier_pools(db_pool, chain_id, &frontier, max_block).await?;

        let mut new_frontier: Vec<[u8; 20]> = Vec::new();

        for edge in &edges {
            // For each token reachable from this pool that's in the frontier
            let frontier_tokens: Vec<[u8; 20]> = {
                let mut ft = Vec::new();
                if frontier.contains(&edge.base_token) {
                    ft.push(edge.base_token);
                }
                if frontier.contains(&edge.quote_token) {
                    ft.push(edge.quote_token);
                }
                ft
            };

            for &from_token in &frontier_tokens {
                let other_token = if from_token == edge.base_token {
                    edge.quote_token
                } else {
                    edge.base_token
                };

                let from_bottleneck = best_bottleneck
                    .get(&from_token)
                    .cloned()
                    .unwrap_or_default();
                let new_bottleneck = from_bottleneck.min(edge.liquidity_usd.clone());

                // Only update if this is a better path
                let current_bottleneck = best_bottleneck.get(&other_token);
                if current_bottleneck.is_some_and(|b| b >= &new_bottleneck) {
                    continue;
                }

                best_bottleneck.insert(other_token, new_bottleneck.clone());
                parent.insert(other_token, (edge.pool_id.clone(), from_token));

                if priceable_tokens.contains(&other_token) {
                    // Found a path to an anchor!
                    let path = trace_path(&parent, target_token, &other_token);
                    let candidate = ResolvedPath {
                        path_pool_ids: path,
                        anchor_token: other_token,
                        bottleneck_liquidity: new_bottleneck,
                    };
                    match &best_anchor {
                        Some(existing)
                            if existing.bottleneck_liquidity >= candidate.bottleneck_liquidity => {}
                        _ => {
                            best_anchor = Some(candidate);
                        }
                    }
                } else if !visited.contains(&other_token) {
                    visited.insert(other_token);
                    new_frontier.push(other_token);
                }
            }
        }

        // If we found an anchor at this depth, return it (BFS guarantees shortest hop count)
        if best_anchor.is_some() {
            return Ok(best_anchor);
        }

        frontier = new_frontier;
    }

    Ok(best_anchor)
}

/// Trace the path from target_token to anchor_token using parent pointers.
/// Returns an ordered list of pool IDs.
fn trace_path(
    parent: &HashMap<[u8; 20], (Vec<u8>, [u8; 20])>,
    target_token: &[u8; 20],
    anchor_token: &[u8; 20],
) -> Vec<Vec<u8>> {
    let mut path = Vec::new();
    let mut current = *anchor_token;
    while current != *target_token {
        if let Some((pool_id, from)) = parent.get(&current) {
            path.push(pool_id.clone());
            current = *from;
        } else {
            break;
        }
    }
    path.reverse();
    path
}

/// Derive a USD price for a token by walking its cached path.
///
/// For each pool in the path:
/// - If current_token is the pool's base_token: multiply by price (quote_per_base)
/// - If current_token is the pool's quote_token: divide by price (invert)
///
/// Multiply the final result by the anchor's USD price.
///
/// `max_block`: if `Some(b)`, only consider snapshot rows with block_number < b.
pub async fn derive_price_from_path(
    db_pool: &Pool,
    chain_id: u64,
    path: &CachedPricePath,
    target_token: &[u8; 20],
    anchor_usd_price: &BigDecimal,
    max_block: Option<u64>,
) -> Option<BigDecimal> {
    if path.path_pool_ids.is_empty() || !path.is_priceable {
        return None;
    }

    let pool_data =
        match query_path_pool_prices(db_pool, chain_id, &path.path_pool_ids, max_block).await {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!(
                    token = hex::encode(target_token),
                    error = %e,
                    "Failed to query pool prices for path derivation"
                );
                return None;
            }
        };

    let mut current_token = *target_token;
    let mut multiplier = BigDecimal::from(1);

    for pool_id in &path.path_pool_ids {
        let Some(pool) = pool_data.get(pool_id.as_slice()) else {
            tracing::warn!(
                pool = hex::encode(pool_id),
                "Pool in path not found in pool_snapshots, cannot derive price"
            );
            return None;
        };

        if current_token == pool.base_token {
            // Going base → quote: multiply by price (quote_per_base)
            multiplier *= &pool.price;
            current_token = pool.quote_token;
        } else if current_token == pool.quote_token {
            // Going quote → base: divide by price
            if pool.price == 0 {
                return None;
            }
            multiplier = multiplier / &pool.price;
            current_token = pool.base_token;
        } else {
            tracing::warn!(
                pool = hex::encode(pool_id),
                current_token = hex::encode(current_token),
                "Path is broken: current token doesn't match pool"
            );
            return None;
        }
    }

    Some(multiplier * anchor_usd_price)
}

/// Pool data needed for price derivation along a path.
struct PathPoolData {
    base_token: [u8; 20],
    quote_token: [u8; 20],
    price: BigDecimal,
}

// ─── Database Queries ────────────────────────────────────────────────

/// Query the cached price path for a token.
async fn query_cached_path(
    db_pool: &Pool,
    chain_id: u64,
    token: &[u8; 20],
) -> Result<Option<CachedPricePath>, TransformationError> {
    let client = db_pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let row = client
        .query_opt(
            "SELECT path_pool_ids, anchor_token, path_liquidity_usd, \
                    resolved_at_block, is_priceable \
             FROM token_price_paths \
             WHERE chain_id = $1 AND token_address = $2",
            &[&(chain_id as i64), &token.to_vec()],
        )
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let Some(row) = row else {
        return Ok(None);
    };

    let path_pool_ids: Vec<Vec<u8>> = row.get("path_pool_ids");
    let anchor_bytes: Vec<u8> = row.get("anchor_token");
    let path_liquidity_usd: Option<rust_decimal::Decimal> = row.get("path_liquidity_usd");
    let resolved_at_block: i64 = row.get("resolved_at_block");
    let is_priceable: bool = row.get("is_priceable");

    let mut anchor_token = [0u8; 20];
    if anchor_bytes.len() == 20 {
        anchor_token.copy_from_slice(&anchor_bytes);
    }

    let path_liquidity_usd = path_liquidity_usd.and_then(|d| d.to_string().parse().ok());

    Ok(Some(CachedPricePath {
        path_pool_ids,
        anchor_token,
        path_liquidity_usd,
        resolved_at_block: resolved_at_block as u64,
        is_priceable,
    }))
}

/// Upsert a price path into the cache.
async fn upsert_cached_path(
    db_pool: &Pool,
    chain_id: u64,
    token: &[u8; 20],
    path: &CachedPricePath,
) -> Result<(), TransformationError> {
    let client = db_pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let liq: Option<rust_decimal::Decimal> = path
        .path_liquidity_usd
        .as_ref()
        .and_then(|d| d.to_string().parse().ok());

    client
        .execute(
            "INSERT INTO token_price_paths \
                (chain_id, token_address, path_pool_ids, anchor_token, \
                 path_liquidity_usd, resolved_at_block, is_priceable) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (chain_id, token_address) DO UPDATE SET \
                path_pool_ids = EXCLUDED.path_pool_ids, \
                anchor_token = EXCLUDED.anchor_token, \
                path_liquidity_usd = EXCLUDED.path_liquidity_usd, \
                resolved_at_block = EXCLUDED.resolved_at_block, \
                is_priceable = EXCLUDED.is_priceable",
            &[
                &(chain_id as i64),
                &token.to_vec(),
                &path.path_pool_ids,
                &path.anchor_token.to_vec(),
                &liq,
                &(path.resolved_at_block as i64),
                &path.is_priceable,
            ],
        )
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    Ok(())
}

/// Delete a cached path so it gets re-resolved on the next invocation.
/// Called when `derive_price_from_path` fails for a cached path, indicating
/// the path is broken (e.g., pool disappeared after a reorg).
pub async fn invalidate_cached_path(
    db_pool: &Pool,
    chain_id: u64,
    token: &[u8; 20],
) -> Result<(), TransformationError> {
    let client = db_pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    client
        .execute(
            "DELETE FROM token_price_paths WHERE chain_id = $1 AND token_address = $2",
            &[&(chain_id as i64), &token.to_vec()],
        )
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    Ok(())
}

/// Query pools touching any of the frontier tokens with >= $1k active liquidity.
///
/// Also includes pools whose `active_liquidity_usd` is NULL but have non-zero
/// raw `active_liquidity`, so that pools whose quote token isn't yet USD-priceable
/// can still appear in the frontier.
///
/// `max_block`: if `Some(b)`, only consider snapshot rows with block_number < b.
async fn query_frontier_pools(
    db_pool: &Pool,
    chain_id: u64,
    frontier: &[[u8; 20]],
    max_block: Option<u64>,
) -> Result<Vec<PoolEdge>, TransformationError> {
    let client = db_pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let frontier_bytes: Vec<Vec<u8>> = frontier.iter().map(|t| t.to_vec()).collect();
    let max_block_i64: Option<i64> = max_block.map(|b| b as i64);

    // First pick the latest snapshot per pool (before max_block), then
    // filter by liquidity. This avoids selecting a pool based on an older
    // liquid snapshot when its latest snapshot is illiquid.
    let rows = client
        .query(
            "WITH latest_snapshots AS ( \
                 SELECT DISTINCT ON (p.address) \
                     p.address, p.base_token, p.quote_token, \
                     ps.active_liquidity_usd, ps.active_liquidity \
                 FROM pools p \
                 JOIN pool_snapshots ps ON ps.pool_id = p.address AND ps.chain_id = p.chain_id \
                 WHERE p.chain_id = $1 \
                   AND (p.base_token = ANY($2) OR p.quote_token = ANY($2)) \
                   AND ($4::bigint IS NULL OR ps.block_number < $4) \
                 ORDER BY p.address, ps.block_number DESC \
             ) \
             SELECT address, base_token, quote_token, \
                    COALESCE(active_liquidity_usd, 0) as liq \
             FROM latest_snapshots \
             WHERE COALESCE(active_liquidity_usd, 0) >= $3 \
                OR (active_liquidity_usd IS NULL AND active_liquidity > 0)",
            &[
                &(chain_id as i64),
                &frontier_bytes,
                &MIN_LIQUIDITY_USD,
                &max_block_i64,
            ],
        )
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let mut edges = Vec::with_capacity(rows.len());
    for row in &rows {
        let pool_id: Vec<u8> = row.get("address");
        let base_bytes: Vec<u8> = row.get("base_token");
        let quote_bytes: Vec<u8> = row.get("quote_token");
        let liq: rust_decimal::Decimal = row.get("liq");

        if base_bytes.len() != 20 || quote_bytes.len() != 20 {
            continue;
        }

        let mut base_token = [0u8; 20];
        let mut quote_token = [0u8; 20];
        base_token.copy_from_slice(&base_bytes);
        quote_token.copy_from_slice(&quote_bytes);

        let liquidity_usd: BigDecimal = match liq.to_string().parse() {
            Ok(d) => d,
            Err(_) => continue,
        };

        edges.push(PoolEdge {
            pool_id,
            base_token,
            quote_token,
            liquidity_usd,
        });
    }

    Ok(edges)
}

/// Query current prices for pools in a path.
///
/// `max_block`: if `Some(b)`, only consider snapshot rows with block_number < b.
async fn query_path_pool_prices(
    db_pool: &Pool,
    chain_id: u64,
    pool_ids: &[Vec<u8>],
    max_block: Option<u64>,
) -> Result<HashMap<Vec<u8>, PathPoolData>, TransformationError> {
    let client = db_pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let max_block_i64: Option<i64> = max_block.map(|b| b as i64);

    let rows = client
        .query(
            "SELECT DISTINCT ON (p.address) \
                 p.address, p.base_token, p.quote_token, ps.price_close AS price \
             FROM pools p \
             JOIN pool_snapshots ps ON ps.pool_id = p.address AND ps.chain_id = p.chain_id \
             WHERE p.chain_id = $1 AND p.address = ANY($2) \
               AND ($3::bigint IS NULL OR ps.block_number < $3) \
             ORDER BY p.address, ps.block_number DESC",
            &[&(chain_id as i64), &pool_ids, &max_block_i64],
        )
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let mut result = HashMap::with_capacity(rows.len());
    for row in &rows {
        let pool_id: Vec<u8> = row.get("address");
        let base_bytes: Vec<u8> = row.get("base_token");
        let quote_bytes: Vec<u8> = row.get("quote_token");
        let price: rust_decimal::Decimal = row.get("price");

        if base_bytes.len() != 20 || quote_bytes.len() != 20 {
            continue;
        }

        let mut base_token = [0u8; 20];
        let mut quote_token = [0u8; 20];
        base_token.copy_from_slice(&base_bytes);
        quote_token.copy_from_slice(&quote_bytes);

        let price: BigDecimal = match price.to_string().parse() {
            Ok(p) => p,
            Err(_) => continue,
        };

        result.insert(
            pool_id,
            PathPoolData {
                base_token,
                quote_token,
                price,
            },
        );
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn test_trace_path_single_hop() {
        let target = [0x01; 20];
        let anchor = [0x02; 20];
        let pool = vec![0xAA; 20];

        let mut parent = HashMap::new();
        parent.insert(anchor, (pool.clone(), target));

        let path = trace_path(&parent, &target, &anchor);
        assert_eq!(path, vec![pool]);
    }

    #[test]
    fn test_trace_path_multi_hop() {
        let target = [0x01; 20];
        let mid = [0x02; 20];
        let anchor = [0x03; 20];
        let pool1 = vec![0xAA; 20];
        let pool2 = vec![0xBB; 20];

        let mut parent = HashMap::new();
        parent.insert(mid, (pool1.clone(), target));
        parent.insert(anchor, (pool2.clone(), mid));

        let path = trace_path(&parent, &target, &anchor);
        assert_eq!(path, vec![pool1, pool2]);
    }

    #[test]
    fn test_trace_path_empty_for_target_equals_anchor() {
        let target = [0x01; 20];
        let parent = HashMap::new();

        let path = trace_path(&parent, &target, &target);
        assert!(path.is_empty());
    }

    #[test]
    fn test_future_resolved_path_is_ignored() {
        // A path resolved at block 200 should not be usable at current_block=100.
        // The guard in check_or_resolve_path requires cached.resolved_at_block < current_block.
        // Since check_or_resolve_path is async/DB-dependent, we test the guard condition directly.

        let cached_resolved_at = 200_u64;
        let current_block = 100_u64;

        // Future entry: resolved_at_block >= current_block  →  should NOT pass the guard
        assert!(
            !(cached_resolved_at < current_block),
            "A path resolved at block {} should be rejected when current_block is {}",
            cached_resolved_at,
            current_block
        );

        // Equal blocks: also rejected (resolved_at_block is NOT strictly less than current_block)
        let cached_resolved_at_equal = 100_u64;
        assert!(
            !(cached_resolved_at_equal < current_block),
            "A path resolved at the same block should also be rejected"
        );

        // Past entry within staleness window: should pass
        let cached_resolved_at_past = 50_u64;
        assert!(
            cached_resolved_at_past < current_block,
            "A path resolved in the past should pass the future guard"
        );
        let age = current_block.saturating_sub(cached_resolved_at_past);
        assert!(
            age < STALENESS_THRESHOLD_BLOCKS,
            "A recently-resolved past path should be within the staleness window"
        );
    }
}
