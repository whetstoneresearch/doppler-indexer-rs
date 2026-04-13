//! USD price resolution for pool metrics.
//!
//! Provides `OraclePriceCache` (shared across handlers, DB-backed) and
//! `UsdPriceContext` (per-invocation snapshot) to convert quote-token
//! volumes into USD.
//!
//! Quote token → USD resolution:
//! - USDC, USDT: 1.0 (stablecoin assumption)
//! - WETH / zero-address: multiply by ETH/USD from ChainlinkEthOracle
//! - Configured pool tokens (EURC, Bankr, Fxh, Noice, Zora, etc.):
//!   resolved transitively via pool prices in the `prices` table.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::sync::RwLock;

use alloy_primitives::U256;
use bigdecimal::BigDecimal;
use deadpool_postgres::Pool;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::types::config::contract::{AddressOrAddresses, Contracts};

/// Chainlink oracle returns int256 with 8 decimals.
const CHAINLINK_DECIMALS: u32 = 8;

/// Oracle rows use the zero address as a synthetic "USD quote token".
const USD_QUOTE_TOKEN: [u8; 20] = [0u8; 20];

/// The zero address, used for native ETH as a pool numeraire.
const ZERO_ADDRESS: [u8; 20] = [0u8; 20];

// ─── OraclePriceCache ────────────────────────────────────────────────

/// A single cached price entry with the block at which it was observed.
#[derive(Clone)]
struct CachedPriceEntry {
    price: BigDecimal,
    provenance_block: u64,
}

/// Shared cache for USD prices. Thread-safe, persists across handler
/// invocations. Seeded from the `prices` table on startup; updated from
/// ChainlinkEthOracle calls and pool prices during processing.
pub struct OraclePriceCache {
    inner: RwLock<HashMap<[u8; 20], CachedPriceEntry>>,
    weth_address: Option<[u8; 20]>,
    stablecoin_addresses: Vec<[u8; 20]>,
    seeded_from_db: AtomicBool,
}

impl OraclePriceCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            weth_address: None,
            stablecoin_addresses: Vec::new(),
            seeded_from_db: AtomicBool::new(false),
        }
    }

    pub fn with_contracts(contracts: &Contracts) -> Self {
        let weth_address = resolve_contract_address(contracts, "Weth");
        let mut stablecoins = Vec::new();
        if let Some(addr) = resolve_contract_address(contracts, "Usdc") {
            stablecoins.push(addr);
        }
        if let Some(addr) = resolve_contract_address(contracts, "Usdt") {
            stablecoins.push(addr);
        }

        Self {
            inner: RwLock::new(HashMap::new()),
            weth_address,
            stablecoin_addresses: stablecoins,
            seeded_from_db: AtomicBool::new(false),
        }
    }

    /// Seed the cache from the `prices` table on startup.
    /// Queries for the latest prices and resolves them transitively to USD.
    pub async fn load_from_db(
        &self,
        db_pool: &Pool,
        chain_id: u64,
    ) -> Result<(), TransformationError> {
        let mut resolved: HashMap<[u8; 20], (BigDecimal, u64)> = HashMap::new();

        // Seed stablecoins as 1.0 with provenance 0
        for addr in &self.stablecoin_addresses {
            resolved.insert(*addr, (BigDecimal::from(1), 0));
        }

        // Load ETH/USD from Chainlink (unbounded at init)
        if let Some(weth_addr) = self.weth_address {
            if let Ok((price, block_number)) =
                query_latest_price(db_pool, chain_id, &weth_addr, Some(&USD_QUOTE_TOKEN), None)
                    .await
            {
                resolved.insert(weth_addr, (price.clone(), block_number));
                resolved.insert(ZERO_ADDRESS, (price, block_number));
            }
        }

        // Load all token pair prices from the prices table (unbounded at init)
        let raw_prices = query_all_latest_prices(db_pool, chain_id, None).await?;

        // Resolve transitively: if a token's quote is already priced in USD,
        // compute the token's USD price.
        resolve_transitive_prices(&raw_prices, &mut resolved);

        let resolved_count = resolved.len();
        for (token, (price, provenance_block)) in &resolved {
            self.set(*token, price.clone(), *provenance_block);
        }

        tracing::info!(
            resolved_count,
            "OraclePriceCache seeded from DB for chain {}",
            chain_id,
        );

        Ok(())
    }

    /// Best-effort one-time DB seed for the shared cache during handler
    /// initialization. Runtime handlers are initialized sequentially, so a
    /// simple atomic guard is sufficient here.
    pub async fn load_from_db_once(
        &self,
        db_pool: &Pool,
        chain_id: u64,
    ) -> Result<(), TransformationError> {
        if self.seeded_from_db.load(Ordering::Acquire) {
            return Ok(());
        }
        self.load_from_db(db_pool, chain_id).await?;
        self.seeded_from_db.store(true, Ordering::Release);
        Ok(())
    }

    fn get(&self, token: &[u8; 20], max_block: Option<u64>) -> Option<(BigDecimal, u64)> {
        let inner = self.inner.read().unwrap();
        let entry = inner.get(token)?;
        if let Some(mb) = max_block {
            if entry.provenance_block >= mb {
                return None;
            }
        }
        Some((entry.price.clone(), entry.provenance_block))
    }

    fn set(&self, token: [u8; 20], price: BigDecimal, provenance_block: u64) {
        let mut inner = self.inner.write().unwrap();
        if let Some(existing) = inner.get(&token) {
            if provenance_block < existing.provenance_block {
                return;
            }
        }
        inner.insert(
            token,
            CachedPriceEntry {
                price,
                provenance_block,
            },
        );
    }

    fn set_many(&self, entries: &[([u8; 20], BigDecimal, u64)]) {
        let mut inner = self.inner.write().unwrap();
        for (token, price, provenance_block) in entries {
            if let Some(existing) = inner.get(token) {
                if *provenance_block < existing.provenance_block {
                    continue;
                }
            }
            inner.insert(
                *token,
                CachedPriceEntry {
                    price: price.clone(),
                    provenance_block: *provenance_block,
                },
            );
        }
    }

    fn get_all_within(&self, max_block: Option<u64>) -> Vec<([u8; 20], BigDecimal, u64)> {
        let inner = self.inner.read().unwrap();
        inner
            .iter()
            .filter(|(_, entry)| {
                if let Some(mb) = max_block {
                    entry.provenance_block < mb
                } else {
                    true
                }
            })
            .map(|(token, entry)| (*token, entry.price.clone(), entry.provenance_block))
            .collect()
    }
}

impl Default for OraclePriceCache {
    fn default() -> Self {
        Self::new()
    }
}

pub fn chainlink_latest_answer_dependency() -> (String, String) {
    ("ChainlinkEthOracle".to_string(), "latestAnswer".to_string())
}

// ─── UsdPriceContext ─────────────────────────────────────────────────

/// Per-invocation snapshot of USD prices with resolved quote token addresses.
/// Built at the top of each handler's `handle()`.
pub struct UsdPriceContext {
    /// token_address → USD price for all resolvable tokens.
    prices: HashMap<[u8; 20], BigDecimal>,
    /// WETH address, used to map the zero address (native ETH) to WETH price.
    weth_address: Option<[u8; 20]>,
}

impl UsdPriceContext {
    /// Create a UsdPriceContext for testing.
    #[cfg(test)]
    pub fn new_for_test(
        prices: HashMap<[u8; 20], BigDecimal>,
        weth_address: Option<[u8; 20]>,
    ) -> Self {
        Self {
            prices,
            weth_address,
        }
    }

    /// Convert a raw quote-side volume to USD.
    ///
    /// `volume_raw` is a U256 absolute value (sum of |amount| across swaps).
    /// `quote_token` identifies which token the volume is denominated in.
    /// `quote_decimals` is the token's decimal precision (6 for USDC, 18 for WETH, etc.)
    ///
    /// Returns None if the quote token is unknown or no price data is available.
    pub fn quote_volume_to_usd(
        &self,
        volume_raw: &U256,
        quote_token: &[u8; 20],
        quote_decimals: u8,
    ) -> Option<BigDecimal> {
        let usd_per_quote = self.quote_to_usd_multiplier(quote_token)?;
        let volume_decimal = u256_to_decimal_adjusted(volume_raw, quote_decimals);
        Some(volume_decimal * usd_per_quote)
    }

    /// Convert a pre-decimal-adjusted quote-denominated amount to USD.
    ///
    /// Used by TVL computation where the amount is already in human-readable
    /// decimal form (produced by combining base + quote contributions with the
    /// current price). Returns `None` if the quote token has no USD multiplier.
    pub fn quote_decimal_amount_to_usd(
        &self,
        amount_decimal: &BigDecimal,
        quote_token: &[u8; 20],
    ) -> Option<BigDecimal> {
        let usd_per_quote = self.quote_to_usd_multiplier(quote_token)?;
        Some(amount_decimal * usd_per_quote)
    }

    /// Resolve USD prices for tokens via graph-based path resolution.
    ///
    /// For each unresolved token, checks the `token_price_paths` cache and
    /// resolves via frontier expansion if needed. Adds any newly resolved
    /// prices to the internal map so subsequent `quote_to_usd_multiplier`
    /// calls find them.
    pub async fn resolve_path_prices(
        &mut self,
        db_pool: &Pool,
        chain_id: u64,
        current_block: u64,
        unresolved_tokens: &[[u8; 20]],
        max_block: Option<u64>,
    ) {
        use super::price_path::{check_or_resolve_path, derive_price_from_path};

        let priceable: std::collections::HashSet<[u8; 20]> =
            self.prices.keys().copied().collect();

        for token in unresolved_tokens {
            if self.prices.contains_key(token) {
                continue;
            }

            let Some(path) =
                check_or_resolve_path(db_pool, chain_id, token, &priceable, current_block, max_block)
                    .await
            else {
                continue;
            };

            if !path.is_priceable {
                continue;
            }

            // The anchor must already be in our prices map
            let Some(anchor_usd) = self.prices.get(&path.anchor_token).cloned() else {
                continue;
            };

            if let Some(usd_price) =
                derive_price_from_path(db_pool, chain_id, &path, token, &anchor_usd, max_block)
                    .await
            {
                self.prices.insert(*token, usd_price);
            }
        }
    }

    /// Get the USD multiplier for a quote token.
    /// Looks up the token in the resolved prices map.
    /// The zero address (native ETH) maps to the WETH price.
    fn quote_to_usd_multiplier(&self, quote_token: &[u8; 20]) -> Option<BigDecimal> {
        // Direct lookup first
        if let Some(price) = self.prices.get(quote_token) {
            return Some(price.clone());
        }
        // Zero address (native ETH) → WETH
        if *quote_token == ZERO_ADDRESS {
            if let Some(weth_addr) = &self.weth_address {
                return self.prices.get(weth_addr).cloned();
            }
        }
        None
    }
}

// ─── Builder ─────────────────────────────────────────────────────────

/// Build a `UsdPriceContext` for the current block range.
///
/// 1. Seeds stablecoins (USDC, USDT) as 1.0.
/// 2. Extracts latest ETH/USD from ChainlinkEthOracle calls in the range.
///    Updates the shared cache and returns DbOps to persist to `prices`.
/// 3. Queries all token pair prices from the `prices` table and resolves
///    them transitively to USD (e.g., EURC → USDC → USD, Bankr → WETH → USD).
/// 4. Falls back to cached values when no fresh data is available.
///
/// Returns `(context, price_ops)` where `price_ops` should be included in
/// the handler's returned operations to persist oracle prices to the DB.
pub async fn build_usd_price_context(
    ctx: &TransformationContext,
    cache: &OraclePriceCache,
    db_pool: &OnceLock<Pool>,
    chain_id: u64,
    contracts: &Contracts,
) -> (UsdPriceContext, Vec<DbOperation>) {
    let mut price_ops = Vec::new();
    let mut resolved: HashMap<[u8; 20], (BigDecimal, u64)> = HashMap::new();
    let max_block = ctx.blockrange_end;

    // Resolve addresses from config
    let weth_address = resolve_contract_address(contracts, "Weth");
    let usdc_address = resolve_contract_address(contracts, "Usdc");
    let usdt_address = resolve_contract_address(contracts, "Usdt");

    // Seed stablecoins (provenance = 0)
    if let Some(addr) = usdc_address {
        resolved.insert(addr, (BigDecimal::from(1), 0));
    }
    if let Some(addr) = usdt_address {
        resolved.insert(addr, (BigDecimal::from(1), 0));
    }

    // Extract ETH/USD from oracle calls in the current block range.
    let eth_usd = extract_eth_usd_from_oracle(
        ctx,
        cache,
        &weth_address,
        chain_id,
        &mut price_ops,
        max_block,
    );

    // Set WETH and zero-address prices
    if let Some((eth_price, eth_block)) = &eth_usd {
        if let Some(weth_addr) = weth_address {
            resolved.insert(weth_addr, (eth_price.clone(), *eth_block));
        }
        resolved.insert(ZERO_ADDRESS, (eth_price.clone(), *eth_block));
    }

    // Cold-start safety net: if no ETH/USD yet, fall back to DB/cache.
    if eth_usd.is_none() {
        if let Some(weth_addr) = weth_address {
            if let Some((cached_price, prov)) = cache.get(&weth_addr, Some(max_block)) {
                resolved.insert(weth_addr, (cached_price.clone(), prov));
                resolved.insert(ZERO_ADDRESS, (cached_price, prov));
            } else if let Some(pool) = db_pool.get() {
                // Last resort: query DB
                if let Ok((price, block_number)) = query_latest_price(
                    pool,
                    chain_id,
                    &weth_addr,
                    Some(&USD_QUOTE_TOKEN),
                    Some(max_block),
                )
                .await
                {
                    cache.set(weth_addr, price.clone(), block_number);
                    cache.set(ZERO_ADDRESS, price.clone(), block_number);
                    resolved.insert(weth_addr, (price.clone(), block_number));
                    resolved.insert(ZERO_ADDRESS, (price, block_number));
                }
            }
        }
    }

    // Query DB for transitive prices
    if let Some(pool) = db_pool.get() {
        match query_all_latest_prices(pool, chain_id, Some(max_block)).await {
            Ok(raw_prices) => resolve_transitive_prices(&raw_prices, &mut resolved),
            Err(e) => tracing::warn!(error = %e, "prices query failed, using cache"),
        }
    }
    // Always backfill from block-aware cache (preserving provenance)
    for (token, price, prov) in cache.get_all_within(Some(max_block)) {
        resolved.entry(token).or_insert((price, prov));
    }

    // Update the shared cache with all resolved prices (with provenance)
    let cache_entries: Vec<([u8; 20], BigDecimal, u64)> = resolved
        .iter()
        .map(|(token, (price, prov))| (*token, price.clone(), *prov))
        .collect();
    cache.set_many(&cache_entries);

    // Strip provenance for the UsdPriceContext
    let prices: HashMap<[u8; 20], BigDecimal> = resolved
        .into_iter()
        .map(|(token, (price, _))| (token, price))
        .collect();

    let usd_ctx = UsdPriceContext {
        prices,
        weth_address,
    };

    (usd_ctx, price_ops)
}

/// Build a `UsdPriceContext` with Phase 2 graph-based path resolution.
///
/// Wraps `build_usd_price_context` (Phase 1) and then resolves any quote
/// tokens from the metadata cache that are still unpriced via frontier
/// expansion through the pool graph (Phase 2).
pub async fn build_usd_price_context_with_paths(
    ctx: &TransformationContext,
    cache: &OraclePriceCache,
    db_pool: &OnceLock<Pool>,
    chain_id: u64,
    contracts: &Contracts,
    metadata_cache: &super::pool_metadata::PoolMetadataCache,
) -> (UsdPriceContext, Vec<DbOperation>) {
    let (mut usd_ctx, price_ops) =
        build_usd_price_context(ctx, cache, db_pool, chain_id, contracts).await;

    // Phase 2: resolve quote tokens not covered by configured anchor pools.
    if let Some(pool) = db_pool.get() {
        let quote_tokens = metadata_cache.unique_quote_tokens();
        let unresolved: Vec<[u8; 20]> = quote_tokens
            .into_iter()
            .filter(|t| usd_ctx.quote_to_usd_multiplier(t).is_none())
            .collect();

        if !unresolved.is_empty() {
            usd_ctx
                .resolve_path_prices(
                    pool,
                    chain_id,
                    ctx.blockrange_end,
                    &unresolved,
                    Some(ctx.blockrange_end),
                )
                .await;
        }
    }

    (usd_ctx, price_ops)
}

/// Extract the latest ETH/USD price from ChainlinkEthOracle calls in the current
/// block range. Updates the cache and emits DbOps to persist to the prices table.
fn extract_eth_usd_from_oracle(
    ctx: &TransformationContext,
    cache: &OraclePriceCache,
    weth_address: &Option<[u8; 20]>,
    chain_id: u64,
    price_ops: &mut Vec<DbOperation>,
    max_block: u64,
) -> Option<(BigDecimal, u64)> {
    // Find the latest oracle call in the current range
    let mut latest: Option<(u64, u64, BigDecimal)> = None; // (block_number, timestamp, price)

    for call in ctx.calls_of_type("ChainlinkEthOracle", "latestAnswer") {
        if call.is_reverted {
            continue;
        }
        let Ok(answer) = call.extract_int256("latestAnswer") else {
            continue;
        };

        // Chainlink returns int256 with 8 decimals. Convert to BigDecimal.
        let price = chainlink_answer_to_decimal(&answer);

        match &latest {
            Some((bn, _, _)) if call.block_number <= *bn => {}
            _ => {
                latest = Some((call.block_number, call.block_timestamp, price));
            }
        }
    }

    if let Some((block_number, block_timestamp, price)) = latest {
        // Update cache with actual oracle block_number
        if let Some(weth_addr) = weth_address {
            cache.set(*weth_addr, price.clone(), block_number);
            cache.set(ZERO_ADDRESS, price.clone(), block_number);
        }

        // Emit DB op to persist to prices table
        if let Some(weth_addr) = weth_address {
            price_ops.push(build_oracle_price_op(
                chain_id,
                block_number,
                block_timestamp,
                weth_addr,
                &price,
            ));
        }

        Some((price, block_number))
    } else {
        // No oracle data in range — use cached value with block awareness
        weth_address.and_then(|addr| cache.get(&addr, Some(max_block)))
    }
}

/// Convert a Chainlink `latestAnswer` (int256, 8 decimals) to BigDecimal.
fn chainlink_answer_to_decimal(answer: &alloy_primitives::I256) -> BigDecimal {
    // I256 doesn't have as_i128(); parse via string representation.
    let raw: BigDecimal = answer.to_string().parse().unwrap_or_default();
    let divisor = BigDecimal::from(10u64.pow(CHAINLINK_DECIMALS));
    raw / divisor
}

/// Build a DbOperation to persist an oracle price to the `prices` table.
fn build_oracle_price_op(
    chain_id: u64,
    block_number: u64,
    block_timestamp: u64,
    token_address: &[u8; 20],
    price: &BigDecimal,
) -> DbOperation {
    DbOperation::Upsert {
        table: "prices".into(),
        columns: vec![
            "timestamp".into(),
            "block_number".into(),
            "chain_id".into(),
            "token".into(),
            "quote_token".into(),
            "price".into(),
        ],
        values: vec![
            DbValue::Timestamp(block_timestamp as i64),
            DbValue::Int64(block_number as i64),
            DbValue::Int64(chain_id as i64),
            DbValue::Address(*token_address),
            DbValue::Address(USD_QUOTE_TOKEN),
            DbValue::Numeric(price.to_string()),
        ],
        // Handler-level source/source_version are injected later by the executor.
        conflict_columns: vec!["timestamp".into(), "chain_id".into(), "token".into()],
        update_columns: vec!["block_number".into(), "quote_token".into(), "price".into()],
        update_condition: None,
    }
}

// ─── Transitive Resolution ──────────────────────────────────────────

/// A raw price row from the `prices` table: (token, quote_token, price, block_number).
struct RawTokenPrice {
    token: [u8; 20],
    quote_token: [u8; 20],
    price: BigDecimal,
    block_number: u64,
}

/// Resolve raw token pair prices transitively into USD prices.
///
/// Given a set of already-known USD prices (stablecoins, WETH) and a list
/// of raw (token, quote_token, price, block_number) rows from the prices table,
/// iteratively computes `token_usd = price * quote_token_usd` for each token
/// whose quote token is already resolved. Repeats until no progress is made,
/// naturally handling multi-hop chains (e.g., EURC → USDC → USD, Bankr → WETH → USD).
///
/// Provenance for derived prices is `max(raw.block_number, quote_provenance_block)`.
fn resolve_transitive_prices(
    raw_prices: &[RawTokenPrice],
    resolved: &mut HashMap<[u8; 20], (BigDecimal, u64)>,
) {
    loop {
        let mut made_progress = false;
        for raw in raw_prices {
            if resolved.contains_key(&raw.token) {
                continue;
            }
            if let Some((quote_usd, quote_provenance)) = resolved.get(&raw.quote_token) {
                let token_usd = &raw.price * quote_usd;
                let provenance = raw.block_number.max(*quote_provenance);
                resolved.insert(raw.token, (token_usd, provenance));
                made_progress = true;
            }
        }
        if !made_progress {
            break;
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────

/// Resolve a contract address from the config by name.
/// Returns None if the contract isn't configured (not an error — some chains
/// may not have all tokens).
fn resolve_contract_address(contracts: &Contracts, name: &str) -> Option<[u8; 20]> {
    let config = contracts.get(name)?;
    match &config.address {
        AddressOrAddresses::Single(addr) => Some(addr.0 .0),
        AddressOrAddresses::Multiple(addrs) => addrs.first().map(|a| a.0 .0),
    }
}

/// Query the latest price from the `prices` table for a token.
/// When `max_block` is Some, only considers rows with block_number < max_block.
async fn query_latest_price(
    pool: &Pool,
    chain_id: u64,
    token_address: &[u8; 20],
    quote_token_filter: Option<&[u8; 20]>,
    max_block: Option<u64>,
) -> Result<(BigDecimal, u64), TransformationError> {
    let client = pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let chain_id_param = chain_id as i64;
    let token_param = token_address.to_vec();
    let max_block_param: Option<i64> = max_block.map(|b| b as i64);

    let row = if let Some(quote_token) = quote_token_filter {
        let quote_token_param = quote_token.to_vec();
        client
            .query_opt(
                "SELECT price, block_number FROM prices \
                 WHERE chain_id = $1 AND token = $2 AND quote_token = $3 \
                 AND ($4::bigint IS NULL OR block_number < $4) \
                 ORDER BY block_number DESC LIMIT 1",
                &[
                    &chain_id_param,
                    &token_param,
                    &quote_token_param,
                    &max_block_param,
                ],
            )
            .await
    } else {
        client
            .query_opt(
                "SELECT price, block_number FROM prices \
                 WHERE chain_id = $1 AND token = $2 \
                 AND ($3::bigint IS NULL OR block_number < $3) \
                 ORDER BY block_number DESC LIMIT 1",
                &[&chain_id_param, &token_param, &max_block_param],
            )
            .await
    };

    let row = row
        .map_err(|e| TransformationError::DatabaseError(e.into()))?
        .ok_or_else(|| {
            TransformationError::ConfigError(format!(
                "No price found for token {}",
                hex::encode(token_address)
            ))
        })?;

    let price: rust_decimal::Decimal = row.get("price");
    let block_number: i64 = row.get("block_number");
    let price = price
        .to_string()
        .parse::<BigDecimal>()
        .map_err(|e| TransformationError::ConfigError(format!("Invalid price value: {}", e)))?;

    Ok((price, block_number as u64))
}

/// Query the latest price for every token from the `prices` table.
/// Returns one (token, quote_token, price, block_number) per token, the most recent by block_number.
/// When `max_block` is Some, only considers rows with block_number < max_block.
async fn query_all_latest_prices(
    pool: &Pool,
    chain_id: u64,
    max_block: Option<u64>,
) -> Result<Vec<RawTokenPrice>, TransformationError> {
    let client = pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let chain_id_param = chain_id as i64;
    let max_block_param: Option<i64> = max_block.map(|b| b as i64);

    let rows = client
        .query(
            "SELECT DISTINCT ON (token) token, quote_token, price, block_number \
             FROM prices \
             WHERE chain_id = $1 AND ($2::bigint IS NULL OR block_number < $2) \
             ORDER BY token, block_number DESC",
            &[&chain_id_param, &max_block_param],
        )
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let mut result = Vec::with_capacity(rows.len());
    for row in &rows {
        let token_bytes: Vec<u8> = row.get("token");
        let quote_bytes: Vec<u8> = row.get("quote_token");
        if token_bytes.len() != 20 || quote_bytes.len() != 20 {
            continue;
        }
        let mut token = [0u8; 20];
        let mut quote_token = [0u8; 20];
        token.copy_from_slice(&token_bytes);
        quote_token.copy_from_slice(&quote_bytes);

        let price: rust_decimal::Decimal = row.get("price");
        let price: BigDecimal = match price.to_string().parse() {
            Ok(p) => p,
            Err(_) => continue,
        };

        let block_number: i64 = row.get("block_number");

        result.push(RawTokenPrice {
            token,
            quote_token,
            price,
            block_number: block_number as u64,
        });
    }

    Ok(result)
}

/// Convert a U256 to BigDecimal, dividing by 10^decimals.
///
/// Parses the full U256 range via BigDecimal's string parser; previously this
/// went through i128 and silently clamped values above i128::MAX to zero,
/// which would have mis-computed TVL for pools with very large position
/// amounts (phase 7). Volumes in phase 6 fit in i128 in practice, so no
/// existing behavior changes.
pub(crate) fn u256_to_decimal_adjusted(value: &U256, decimals: u8) -> BigDecimal {
    let raw: BigDecimal = value
        .to_string()
        .parse()
        .expect("U256::to_string produces a valid decimal integer");
    if decimals == 0 {
        return raw;
    }
    let divisor: BigDecimal = {
        let mut d = BigDecimal::from(1u64);
        let ten = BigDecimal::from(10u64);
        for _ in 0..decimals {
            d = &d * &ten;
        }
        d
    };
    raw / divisor
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use crate::transformations::executor::inject_source_version;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    const WETH: [u8; 20] = [0x01; 20];
    const USDC: [u8; 20] = [0x02; 20];
    const USDT: [u8; 20] = [0x03; 20];
    const EURC: [u8; 20] = [0x04; 20];
    const BANKR: [u8; 20] = [0x05; 20];

    fn make_ctx(eth_usd: Option<&str>, eurc_usd: Option<&str>) -> UsdPriceContext {
        let mut prices = HashMap::new();
        prices.insert(USDC, BigDecimal::from(1));
        prices.insert(USDT, BigDecimal::from(1));
        if let Some(p) = eth_usd {
            prices.insert(WETH, bd(p));
        }
        if let Some(p) = eurc_usd {
            prices.insert(EURC, bd(p));
        }
        UsdPriceContext {
            prices,
            weth_address: Some(WETH),
        }
    }

    #[test]
    fn test_weth_volume_to_usd() {
        let ctx = make_ctx(Some("2000"), None);
        // 1.5 WETH raw = 1_500_000_000_000_000_000 (18 decimals)
        let volume = U256::from(1_500_000_000_000_000_000u64);
        let usd = ctx.quote_volume_to_usd(&volume, &WETH, 18).unwrap();
        assert_eq!(usd, bd("3000"));
    }

    #[test]
    fn test_usdc_volume_to_usd() {
        let ctx = make_ctx(Some("2000"), None);
        // 500 USDC raw = 500_000_000 (6 decimals)
        let volume = U256::from(500_000_000u64);
        let usd = ctx.quote_volume_to_usd(&volume, &USDC, 6).unwrap();
        assert_eq!(usd, bd("500"));
    }

    #[test]
    fn test_usdt_volume_to_usd() {
        let ctx = make_ctx(Some("2000"), None);
        let volume = U256::from(1_000_000u64); // 1 USDT
        let usd = ctx.quote_volume_to_usd(&volume, &USDT, 6).unwrap();
        assert_eq!(usd, bd("1"));
    }

    #[test]
    fn test_eurc_volume_to_usd() {
        let ctx = make_ctx(None, Some("1.08"));
        // 100 EURC raw = 100_000_000 (6 decimals)
        let volume = U256::from(100_000_000u64);
        let usd = ctx.quote_volume_to_usd(&volume, &EURC, 6).unwrap();
        assert_eq!(usd, bd("108"));
    }

    #[test]
    fn test_unknown_token_returns_none() {
        let ctx = make_ctx(Some("2000"), None);
        let unknown = [0xFF; 20];
        let volume = U256::from(1_000_000u64);
        assert!(ctx.quote_volume_to_usd(&volume, &unknown, 18).is_none());
    }

    #[test]
    fn test_no_eth_price_returns_none_for_weth() {
        let ctx = make_ctx(None, None);
        let volume = U256::from(1_000_000_000_000_000_000u64);
        assert!(ctx.quote_volume_to_usd(&volume, &WETH, 18).is_none());
    }

    #[test]
    fn test_zero_address_maps_to_weth() {
        let ctx = make_ctx(Some("2500"), None);
        let zero = [0u8; 20];
        // The zero address should resolve to the WETH price
        let volume = U256::from(1_000_000_000_000_000_000u64); // 1 ETH
        let usd = ctx.quote_volume_to_usd(&volume, &zero, 18).unwrap();
        assert_eq!(usd, bd("2500"));
    }

    #[test]
    fn test_transitive_resolution() {
        // Bankr is priced at 0.5 WETH, WETH is $2000
        let raw_prices = vec![RawTokenPrice {
            token: BANKR,
            quote_token: WETH,
            price: bd("0.5"),
            block_number: 100,
        }];

        let mut resolved: HashMap<[u8; 20], (BigDecimal, u64)> = HashMap::new();
        resolved.insert(WETH, (bd("2000"), 50));

        resolve_transitive_prices(&raw_prices, &mut resolved);

        assert_eq!(resolved.get(&BANKR).unwrap().0, bd("1000"));
    }

    #[test]
    fn test_multi_hop_transitive_resolution() {
        // Chain: TokenA → EURC → USDC → USD
        // TokenA = 10 EURC, EURC = 1.08 USDC, USDC = 1.0 USD
        let token_a = [0x10; 20];
        let raw_prices = vec![
            RawTokenPrice {
                token: EURC,
                quote_token: USDC,
                price: bd("1.08"),
                block_number: 100,
            },
            RawTokenPrice {
                token: token_a,
                quote_token: EURC,
                price: bd("10"),
                block_number: 200,
            },
        ];

        let mut resolved: HashMap<[u8; 20], (BigDecimal, u64)> = HashMap::new();
        resolved.insert(USDC, (BigDecimal::from(1), 0));

        resolve_transitive_prices(&raw_prices, &mut resolved);

        assert_eq!(resolved.get(&EURC).unwrap().0, bd("1.08"));
        assert_eq!(resolved.get(&token_a).unwrap().0, bd("10.80"));
    }

    #[test]
    fn test_transitive_provenance_propagation() {
        // BANKR/WETH@block50, WETH/USD@block100
        // Derived BANKR/USD provenance should be max(50, 100) = 100
        // With max_block=80, BANKR/USD should NOT be visible in cache
        let raw_prices = vec![RawTokenPrice {
            token: BANKR,
            quote_token: WETH,
            price: bd("0.5"),
            block_number: 50,
        }];

        let mut resolved: HashMap<[u8; 20], (BigDecimal, u64)> = HashMap::new();
        resolved.insert(WETH, (bd("2000"), 100));

        resolve_transitive_prices(&raw_prices, &mut resolved);

        let (bankr_price, bankr_provenance) = resolved.get(&BANKR).unwrap();
        assert_eq!(bankr_price, &bd("1000"));
        assert_eq!(*bankr_provenance, 100);

        // Put into cache and verify block-awareness
        let cache = OraclePriceCache::new();
        cache.set(BANKR, bankr_price.clone(), *bankr_provenance);

        // max_block=80 should NOT see the price (provenance=100 >= 80)
        assert!(cache.get(&BANKR, Some(80)).is_none());
        // max_block=101 should see the price (provenance=100 < 101)
        assert_eq!(cache.get(&BANKR, Some(101)).unwrap().0, bd("1000"));
    }

    #[test]
    fn test_cache_respects_block_bounds() {
        let cache = OraclePriceCache::new();
        cache.set(WETH, bd("2000"), 200);

        // max_block=100 should NOT see the price (provenance=200 >= 100)
        assert!(cache.get(&WETH, Some(100)).is_none());
        // max_block=201 should see the price (provenance=200 < 201)
        let (price, prov) = cache.get(&WETH, Some(201)).unwrap();
        assert_eq!(price, bd("2000"));
        assert_eq!(prov, 200);
        // No max_block should see the price
        assert_eq!(cache.get(&WETH, None).unwrap().0, bd("2000"));
    }

    #[test]
    fn test_cache_get_all_within_filters_future() {
        let cache = OraclePriceCache::new();
        cache.set(USDC, bd("1"), 50);
        cache.set(WETH, bd("2000"), 100);
        cache.set(BANKR, bd("500"), 200);

        // max_block=150: should see USDC@50 and WETH@100, but NOT BANKR@200
        let within = cache.get_all_within(Some(150));
        let within_map: HashMap<[u8; 20], (BigDecimal, u64)> = within
            .into_iter()
            .map(|(t, p, prov)| (t, (p, prov)))
            .collect();
        assert_eq!(within_map.len(), 2);
        assert_eq!(within_map.get(&USDC).unwrap().0, bd("1"));
        assert_eq!(within_map.get(&WETH).unwrap().0, bd("2000"));
        assert!(within_map.get(&BANKR).is_none());

        // None: should see all
        let all = cache.get_all_within(None);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_chainlink_answer_conversion() {
        // Chainlink ETH/USD = 200000000000 = $2000.00 (8 decimals)
        let answer = alloy_primitives::I256::try_from(200_000_000_000i64).unwrap();
        let price = chainlink_answer_to_decimal(&answer);
        assert_eq!(price, bd("2000"));
    }

    #[test]
    fn test_u256_to_decimal_adjusted() {
        // 1.5 tokens with 18 decimals
        let raw = U256::from(1_500_000_000_000_000_000u64);
        let result = u256_to_decimal_adjusted(&raw, 18);
        assert_eq!(result, bd("1.5"));

        // 500 USDC (6 decimals)
        let raw = U256::from(500_000_000u64);
        let result = u256_to_decimal_adjusted(&raw, 6);
        assert_eq!(result, bd("500"));
    }

    #[test]
    fn test_oracle_price_op_relies_on_executor_for_source_injection() {
        let op = build_oracle_price_op(8453, 123, 456, &[0x01; 20], &bd("2000"));
        let ops = inject_source_version(vec![op], "SwapHandler", 7);
        let op = ops.into_iter().next().unwrap();

        let DbOperation::Upsert {
            columns,
            values,
            conflict_columns,
            ..
        } = op
        else {
            panic!("expected upsert");
        };

        assert_eq!(
            columns
                .iter()
                .filter(|column| column.as_str() == "source")
                .count(),
            1
        );
        assert_eq!(
            columns
                .iter()
                .filter(|column| column.as_str() == "source_version")
                .count(),
            1
        );
        assert_eq!(
            conflict_columns
                .iter()
                .filter(|column| column.as_str() == "source")
                .count(),
            1
        );
        assert_eq!(
            conflict_columns
                .iter()
                .filter(|column| column.as_str() == "source_version")
                .count(),
            1
        );
        assert!(
            matches!(values.get(values.len() - 2), Some(DbValue::Text(source)) if source == "SwapHandler")
        );
        assert!(matches!(values.last(), Some(DbValue::Int32(7))));
    }
}
