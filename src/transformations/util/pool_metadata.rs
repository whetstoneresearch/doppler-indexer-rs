//! Pool metadata cache for metrics handlers.
//!
//! Loads pool metadata from the `pools` table on handler initialization and
//! provides fast lookups by pool_id during event processing.
//!
//! Queries are explicitly scoped to the handler source/version pairs that own
//! the pool metadata rows for a given cache. This keeps reads tied to the
//! currently running handler versions instead of whatever happens to be active
//! globally in `active_versions`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use alloy_primitives::{Address, U256};
use deadpool_postgres::Pool;
use tokio_postgres::{Client, Row};

use crate::db::DbPool;
use crate::transformations::error::TransformationError;
use crate::types::config::contract::{AddressOrAddresses, Contracts};

/// Metadata for a single pool needed by metrics handlers.
#[derive(Debug, Clone)]
pub struct PoolMetadata {
    pub quote_token: [u8; 20],
    pub is_token_0: bool,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    /// Total supply of the base token, resolved lazily via LEFT JOIN on `tokens`.
    /// `None` means the tokens row had not been committed when the cache was last
    /// (re-)loaded, or the token row had a NULL total_supply. Downstream consumers
    /// (e.g. market-cap computation) propagate the missing value.
    pub total_supply: Option<U256>,
}

/// A concrete `(source, version)` pair used to scope versioned table reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedSource {
    pub source: &'static str,
    pub version: u32,
}

impl VersionedSource {
    pub const fn new(source: &'static str, version: u32) -> Self {
        Self { source, version }
    }
}

/// Thread-safe cache of pool metadata keyed by pool_id bytes.
pub struct PoolMetadataCache {
    inner: RwLock<HashMap<Vec<u8>, Arc<PoolMetadata>>>,
    loaded: AtomicBool,
    pool_scopes: Vec<VersionedSource>,
    token_scopes: Vec<VersionedSource>,
}

impl PoolMetadataCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self::with_scopes(Vec::new(), Vec::new())
    }

    /// Create a cache where pools and tokens are read from the same handler scopes.
    pub fn with_shared_scopes(scopes: Vec<VersionedSource>) -> Self {
        Self::with_scopes(scopes.clone(), scopes)
    }

    /// Create a cache with independent scopes for `pools` and `tokens`.
    pub fn with_scopes(
        pool_scopes: Vec<VersionedSource>,
        token_scopes: Vec<VersionedSource>,
    ) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            loaded: AtomicBool::new(false),
            pool_scopes,
            token_scopes,
        }
    }

    /// Load all pool metadata from version-scoped `pools` / `tokens` rows for a given chain.
    ///
    /// Quote token decimals default to 18. Call `resolve_quote_decimals()`
    /// with the contracts config to set correct values.
    pub async fn load_from_db(
        db_pool: &DbPool,
        chain_id: u64,
        pool_scopes: &[VersionedSource],
        token_scopes: &[VersionedSource],
    ) -> Result<Self, TransformationError> {
        let client = db_pool
            .inner()
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows =
            query_pool_metadata_rows(&client, chain_id, pool_scopes, token_scopes, &[]).await?;

        let mut map = HashMap::with_capacity(rows.len());

        for row in &rows {
            let pool_id: Vec<u8> = row.get("address");
            let quote_token_bytes: Vec<u8> = row.get("quote_token");
            let is_token_0: bool = row.get("is_token_0");
            let total_supply =
                parse_total_supply(row.get::<_, Option<String>>("total_supply_text"));

            if quote_token_bytes.len() != 20 {
                continue;
            }

            let mut quote_token = [0u8; 20];
            quote_token.copy_from_slice(&quote_token_bytes);

            let base_decimals = 18u8; // all launched tokens are 18 decimals
            let quote_decimals = 18u8; // resolved later via resolve_quote_decimals()

            map.insert(
                pool_id,
                Arc::new(PoolMetadata {
                    quote_token,
                    is_token_0,
                    base_decimals,
                    quote_decimals,
                    total_supply,
                }),
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
            pool_scopes: pool_scopes.to_vec(),
            token_scopes: token_scopes.to_vec(),
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
        let result =
            Self::load_from_db(db_pool, chain_id, &self.pool_scopes, &self.token_scopes).await;
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
                Arc::make_mut(meta).quote_decimals = decimals;
            }
        }
    }

    /// Look up metadata by pool_id.
    pub fn get(&self, pool_id: &[u8]) -> Option<Arc<PoolMetadata>> {
        self.inner.read().unwrap().get(pool_id).cloned()
    }

    /// Insert metadata for a pool if not already cached.
    /// Used by Create handlers to populate the cache in-memory before their
    /// DB transaction commits, so Swap handlers in the same range/block can
    /// find newly created pools without a DB round-trip.
    #[allow(dead_code)]
    pub fn insert_if_absent(&self, pool_id: Vec<u8>, meta: PoolMetadata) {
        let mut inner = self.inner.write().unwrap();
        inner.entry(pool_id).or_insert_with(|| Arc::new(meta));
    }

    /// Return all unique quote token addresses in the cache.
    pub fn unique_quote_tokens(&self) -> Vec<[u8; 20]> {
        let inner = self.inner.read().unwrap();
        let mut seen = std::collections::HashSet::new();
        for meta in inner.values() {
            seen.insert(meta.quote_token);
        }
        seen.into_iter().collect()
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
        addresses: &[Vec<u8>],
    ) -> Result<usize, TransformationError> {
        let client = pool
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows = query_pool_metadata_rows(
            &client,
            chain_id,
            &self.pool_scopes,
            &self.token_scopes,
            addresses,
        )
        .await?;

        let mut inner = self.inner.write().unwrap();
        let mut new_count = 0;

        for row in &rows {
            let pool_id: Vec<u8> = row.get("address");
            let total_supply =
                parse_total_supply(row.get::<_, Option<String>>("total_supply_text"));

            // If the pool is already cached but had no total_supply at load time,
            // backfill it opportunistically from the refreshed row. This closes
            // the bootstrap gap where the create handler populated the cache
            // before the tokens row was committed.
            if let Some(existing) = inner.get_mut(&pool_id) {
                if existing.total_supply.is_none() && total_supply.is_some() {
                    Arc::make_mut(existing).total_supply = total_supply;
                }
                continue;
            }

            let quote_token_bytes: Vec<u8> = row.get("quote_token");
            let is_token_0: bool = row.get("is_token_0");

            if quote_token_bytes.len() != 20 {
                continue;
            }

            let mut quote_token = [0u8; 20];
            quote_token.copy_from_slice(&quote_token_bytes);

            inner.insert(
                pool_id,
                Arc::new(PoolMetadata {
                    quote_token,
                    is_token_0,
                    base_decimals: 18,
                    quote_decimals: 18,
                    total_supply,
                }),
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
                    Arc::make_mut(meta).quote_decimals = decimals;
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
    QuoteTokenConfig {
        config_name: "Fxh",
        decimals: 18,
    },
    QuoteTokenConfig {
        config_name: "Noice",
        decimals: 18,
    },
    QuoteTokenConfig {
        config_name: "Zora",
        decimals: 18,
    },
    QuoteTokenConfig {
        config_name: "Bankr",
        decimals: 18,
    },
    QuoteTokenConfig {
        config_name: "Mon",
        decimals: 18,
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

fn scope_params(scopes: &[VersionedSource]) -> (Vec<String>, Vec<i32>) {
    (
        scopes
            .iter()
            .map(|scope| scope.source.to_string())
            .collect(),
        scopes.iter().map(|scope| scope.version as i32).collect(),
    )
}

async fn query_pool_metadata_rows(
    client: &Client,
    chain_id: u64,
    pool_scopes: &[VersionedSource],
    token_scopes: &[VersionedSource],
    addresses: &[Vec<u8>],
) -> Result<Vec<Row>, TransformationError> {
    if pool_scopes.is_empty() {
        return Err(TransformationError::ConfigError(
            "PoolMetadataCache is missing pool scopes".to_string(),
        ));
    }
    if token_scopes.is_empty() {
        return Err(TransformationError::ConfigError(
            "PoolMetadataCache is missing token scopes".to_string(),
        ));
    }

    let (pool_sources, pool_versions) = scope_params(pool_scopes);
    let (token_sources, token_versions) = scope_params(token_scopes);

    if addresses.is_empty() {
        client
            .query(
                r#"
WITH pool_scope AS (
  SELECT * FROM unnest($2::text[], $3::int4[]) AS scope(source, source_version)
),
token_scope AS (
  SELECT * FROM unnest($4::text[], $5::int4[]) AS scope(source, source_version)
)
SELECT p.address, p.base_token, p.quote_token, p.is_token_0,
       t.total_supply::text AS total_supply_text
FROM pools p
JOIN pool_scope ps
  ON p.source = ps.source
 AND p.source_version = ps.source_version
LEFT JOIN pools orig
  ON p.chain_id = orig.chain_id
 AND p.migrated_from = orig.address
 AND EXISTS (
      SELECT 1
      FROM token_scope ts
      WHERE orig.source = ts.source
        AND orig.source_version = ts.source_version
 )
LEFT JOIN tokens t
  ON t.chain_id = p.chain_id
 AND t.address = p.base_token
 AND (
      (orig.address IS NOT NULL
       AND t.source = orig.source
       AND t.source_version = orig.source_version)
   OR (p.migrated_from IS NULL
       AND t.source = p.source
       AND t.source_version = p.source_version
       AND EXISTS (
            SELECT 1
            FROM token_scope ts
            WHERE p.source = ts.source
              AND p.source_version = ts.source_version
       ))
 )
WHERE p.chain_id = $1
"#,
                &[
                    &(chain_id as i64),
                    &pool_sources,
                    &pool_versions,
                    &token_sources,
                    &token_versions,
                ],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))
    } else {
        client
            .query(
                r#"
WITH pool_scope AS (
  SELECT * FROM unnest($2::text[], $3::int4[]) AS scope(source, source_version)
),
token_scope AS (
  SELECT * FROM unnest($4::text[], $5::int4[]) AS scope(source, source_version)
)
SELECT p.address, p.base_token, p.quote_token, p.is_token_0,
       t.total_supply::text AS total_supply_text
FROM pools p
JOIN pool_scope ps
  ON p.source = ps.source
 AND p.source_version = ps.source_version
LEFT JOIN pools orig
  ON p.chain_id = orig.chain_id
 AND p.migrated_from = orig.address
 AND EXISTS (
      SELECT 1
      FROM token_scope ts
      WHERE orig.source = ts.source
        AND orig.source_version = ts.source_version
 )
LEFT JOIN tokens t
  ON t.chain_id = p.chain_id
 AND t.address = p.base_token
 AND (
      (orig.address IS NOT NULL
       AND t.source = orig.source
       AND t.source_version = orig.source_version)
   OR (p.migrated_from IS NULL
       AND t.source = p.source
       AND t.source_version = p.source_version
       AND EXISTS (
            SELECT 1
            FROM token_scope ts
            WHERE p.source = ts.source
              AND p.source_version = ts.source_version
       ))
 )
WHERE p.chain_id = $1
  AND p.address = ANY($6)
"#,
                &[
                    &(chain_id as i64),
                    &pool_sources,
                    &pool_versions,
                    &token_sources,
                    &token_versions,
                    &addresses,
                ],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))
    }
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
