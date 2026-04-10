//! Pool metadata cache for metrics handlers.
//!
//! Loads pool metadata from the `pools` table on handler initialization and
//! provides fast lookups by pool_id during event processing.
//!
//! Queries join against `active_versions` to select only the currently active
//! source_version for each handler source, so a create-handler version bump
//! does not cause the cache to load stale rows from the previous version.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use alloy_primitives::{Address, U256};
use deadpool_postgres::Pool;

use crate::db::DbPool;
use crate::transformations::error::TransformationError;
use crate::types::config::contract::{AddressOrAddresses, Contracts};

/// Metadata for a single pool needed by metrics handlers.
#[derive(Debug, Clone)]
pub struct PoolMetadata {
    pub pool_id: Vec<u8>,
    pub base_token: [u8; 20],
    pub quote_token: [u8; 20],
    pub is_token_0: bool,
    pub pool_type: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    /// Total supply of the base token, resolved lazily via LEFT JOIN on `tokens`.
    /// `None` means the tokens row had not been committed when the cache was last
    /// (re-)loaded, or the token row had a NULL total_supply. Downstream consumers
    /// (e.g. market-cap computation) propagate the missing value.
    pub total_supply: Option<U256>,
}

/// Thread-safe cache of pool metadata keyed by pool_id bytes.
pub struct PoolMetadataCache {
    inner: RwLock<HashMap<Vec<u8>, PoolMetadata>>,
    loaded: AtomicBool,
}

impl PoolMetadataCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            loaded: AtomicBool::new(false),
        }
    }

    /// Load all pool metadata from the `pools` table for a given chain.
    ///
    /// Quote token decimals default to 18. Call `resolve_quote_decimals()`
    /// with the contracts config to set correct values.
    pub async fn load_from_db(
        db_pool: &DbPool,
        chain_id: u64,
    ) -> Result<Self, TransformationError> {
        let client = db_pool
            .inner()
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows = client
            .query(
                "SELECT p.address, p.base_token, p.quote_token, p.is_token_0, p.type, \
                        t.total_supply::text AS total_supply_text \
                 FROM pools p \
                 JOIN active_versions av \
                   ON p.source = av.source AND p.source_version = av.active_version \
                 LEFT JOIN tokens t \
                   ON t.chain_id = p.chain_id AND t.address = p.base_token \
                 WHERE p.chain_id = $1",
                &[&(chain_id as i64)],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let mut map = HashMap::with_capacity(rows.len());

        for row in &rows {
            let pool_id: Vec<u8> = row.get("address");
            let base_token_bytes: Vec<u8> = row.get("base_token");
            let quote_token_bytes: Vec<u8> = row.get("quote_token");
            let is_token_0: bool = row.get("is_token_0");
            let pool_type: String = row.get("type");
            let total_supply = parse_total_supply(row.get::<_, Option<String>>("total_supply_text"));

            if base_token_bytes.len() != 20 || quote_token_bytes.len() != 20 {
                continue;
            }

            let mut base_token = [0u8; 20];
            let mut quote_token = [0u8; 20];
            base_token.copy_from_slice(&base_token_bytes);
            quote_token.copy_from_slice(&quote_token_bytes);

            let base_decimals = 18u8; // all launched tokens are 18 decimals
            let quote_decimals = 18u8; // resolved later via resolve_quote_decimals()

            map.insert(
                pool_id.clone(),
                PoolMetadata {
                    pool_id,
                    base_token,
                    quote_token,
                    is_token_0,
                    pool_type,
                    base_decimals,
                    quote_decimals,
                    total_supply,
                },
            );
        }

        tracing::info!(
            "PoolMetadataCache loaded {} pools for chain {}",
            map.len(),
            chain_id
        );

        Ok(Self {
            inner: RwLock::new(map),
            loaded: AtomicBool::new(true),
        })
    }

    /// Load metadata from DB into an existing (shared) cache instance.
    /// Uses atomic compare-exchange to ensure exactly one concurrent caller loads.
    pub async fn load_into(
        &self,
        db_pool: &DbPool,
        chain_id: u64,
    ) -> Result<(), TransformationError> {
        if self
            .loaded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let result = Self::load_from_db(db_pool, chain_id).await;
        match result {
            Ok(loaded_cache) => {
                let entries = loaded_cache.inner.into_inner().unwrap();
                let mut inner = self.inner.write().unwrap();
                inner.extend(entries);
                Ok(())
            }
            Err(e) => {
                // Reset flag so a subsequent call can retry
                self.loaded.store(false, Ordering::Release);
                Err(e)
            }
        }
    }

    /// Update quote token decimals for all cached entries using contracts config.
    /// Called on first handle() when the TransformationContext is available.
    pub fn resolve_quote_decimals(&self, contracts: &Contracts) {
        let mut inner = self.inner.write().unwrap();
        for meta in inner.values_mut() {
            if let Some(decimals) = quote_token_decimals(&meta.quote_token, contracts) {
                meta.quote_decimals = decimals;
            }
        }
    }

    /// Look up metadata by pool_id.
    pub fn get(&self, pool_id: &[u8]) -> Option<PoolMetadata> {
        self.inner.read().unwrap().get(pool_id).cloned()
    }

    /// Insert metadata for a pool if not already cached.
    /// Used by Create handlers to populate the cache in-memory before their
    /// DB transaction commits, so Swap handlers in the same range/block can
    /// find newly created pools without a DB round-trip.
    pub fn insert_if_absent(&self, pool_id: Vec<u8>, meta: PoolMetadata) {
        let mut inner = self.inner.write().unwrap();
        inner.entry(pool_id).or_insert(meta);
    }

    /// Re-query the `pools` table and insert any pools not already cached.
    ///
    /// Also resolves `quote_decimals` for all newly inserted entries while
    /// still holding the write lock, eliminating the two-lock race that
    /// existed when `resolve_quote_decimals` was called separately.
    ///
    /// Returns the number of newly discovered pools.
    pub async fn refresh(
        &self,
        pool: &Pool,
        chain_id: u64,
        contracts: &crate::types::config::contract::Contracts,
    ) -> Result<usize, TransformationError> {
        let client = pool
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows = client
            .query(
                "SELECT p.address, p.base_token, p.quote_token, p.is_token_0, p.type, \
                        t.total_supply::text AS total_supply_text \
                 FROM pools p \
                 JOIN active_versions av \
                   ON p.source = av.source AND p.source_version = av.active_version \
                 LEFT JOIN tokens t \
                   ON t.chain_id = p.chain_id AND t.address = p.base_token \
                 WHERE p.chain_id = $1",
                &[&(chain_id as i64)],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let mut inner = self.inner.write().unwrap();
        let mut new_count = 0;

        for row in &rows {
            let pool_id: Vec<u8> = row.get("address");
            let total_supply = parse_total_supply(row.get::<_, Option<String>>("total_supply_text"));

            // If the pool is already cached but had no total_supply at load time,
            // backfill it opportunistically from the refreshed row. This closes
            // the bootstrap gap where the create handler populated the cache
            // before the tokens row was committed.
            if let Some(existing) = inner.get_mut(&pool_id) {
                if existing.total_supply.is_none() && total_supply.is_some() {
                    existing.total_supply = total_supply;
                }
                continue;
            }

            let base_token_bytes: Vec<u8> = row.get("base_token");
            let quote_token_bytes: Vec<u8> = row.get("quote_token");
            let is_token_0: bool = row.get("is_token_0");
            let pool_type: String = row.get("type");

            if base_token_bytes.len() != 20 || quote_token_bytes.len() != 20 {
                continue;
            }

            let mut base_token = [0u8; 20];
            let mut quote_token = [0u8; 20];
            base_token.copy_from_slice(&base_token_bytes);
            quote_token.copy_from_slice(&quote_token_bytes);

            inner.insert(
                pool_id.clone(),
                PoolMetadata {
                    pool_id,
                    base_token,
                    quote_token,
                    is_token_0,
                    pool_type,
                    base_decimals: 18,
                    quote_decimals: 18,
                    total_supply,
                },
            );
            new_count += 1;
        }

        // Resolve quote_decimals for entries that still carry the placeholder value (18).
        // This runs under the same write lock as the insert loop, so there is no window
        // in which another thread can read a partially-initialised entry.
        // All known non-WETH quote tokens (USDC, USDT, EURC) have decimals != 18, so
        // resolving every entry with quote_decimals == 18 is safe and idempotent.
        for meta in inner.values_mut() {
            if meta.quote_decimals == 18 {
                if let Some(decimals) = quote_token_decimals(&meta.quote_token, contracts) {
                    meta.quote_decimals = decimals;
                }
            }
        }

        Ok(new_count)
    }
}

impl Default for PoolMetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Known quote token decimals keyed by config name.
struct QuoteTokenConfig {
    config_name: &'static str,
    decimals: u8,
}

const KNOWN_QUOTE_TOKENS: &[QuoteTokenConfig] = &[
    QuoteTokenConfig {
        config_name: "Weth",
        decimals: 18,
    },
    QuoteTokenConfig {
        config_name: "Usdc",
        decimals: 6,
    },
    QuoteTokenConfig {
        config_name: "Usdt",
        decimals: 6,
    },
    QuoteTokenConfig {
        config_name: "Eurc",
        decimals: 6,
    },
];

/// Parse a NUMERIC-as-text representation of `tokens.total_supply` into a U256.
///
/// Postgres NUMERIC can exceed rust_decimal's ~28-digit range, so the caller
/// passes the column through `::text` to preserve full precision. The string
/// is an integer with an optional trailing `.0` or similar when cast from
/// NUMERIC — we tolerate that by stripping any fractional part.
fn parse_total_supply(text: Option<String>) -> Option<U256> {
    let raw = text?;
    let trimmed = raw.trim();
    // NUMERIC::text may emit an integer without a decimal point for integral
    // values, or "1000.00" style for non-integral. total_supply is always an
    // integer value in token base units, so we truncate any fractional part.
    let integer_part = trimmed.split('.').next()?;
    if integer_part.is_empty() || integer_part == "-" {
        return None;
    }
    U256::from_str_radix(integer_part, 10).ok()
}

/// Resolve decimals for a known quote token address by checking against
/// the contract configuration. Returns None if the address is not a known
/// quote token.
pub fn quote_token_decimals(address: &[u8; 20], contracts: &Contracts) -> Option<u8> {
    let addr = Address::from(*address);
    for qt in KNOWN_QUOTE_TOKENS {
        if let Some(config) = contracts.get(qt.config_name) {
            let matches = match &config.address {
                AddressOrAddresses::Single(a) => *a == addr,
                AddressOrAddresses::Multiple(addrs) => addrs.contains(&addr),
            };
            if matches {
                return Some(qt.decimals);
            }
        }
    }
    None
}
