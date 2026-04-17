//! Pool metadata cache for metrics handlers.
//!
//! Loads pool metadata from the `pools` table on handler initialization and
//! provides fast lookups by pool_id during event processing.
//!
//! Queries are explicitly scoped to the handler source/version pairs that own
//! the pool metadata rows for a given cache. This keeps reads tied to the
//! currently running handler versions instead of whatever happens to be active
//! globally in `active_versions`.

use std::sync::Arc;

use alloy_primitives::{Address, U256};
use deadpool_postgres::Pool;
use moka::sync::Cache;
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

/// Thread-safe LRU cache of pool metadata keyed by pool_id bytes.
pub struct PoolMetadataCache {
    inner: Cache<Vec<u8>, Arc<PoolMetadata>>,
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
            inner: Cache::new(500_000),
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
            query_pool_metadata_rows(&client, chain_id, pool_scopes, token_scopes, &[], None)
                .await?;

        let cache = Cache::new(500_000);

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

            cache.insert(
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
            cache.entry_count(),
            chain_id
        );

        Ok(Self {
            inner: cache,
            pool_scopes: pool_scopes.to_vec(),
            token_scopes: token_scopes.to_vec(),
        })
    }

    /// Update quote token decimals for all cached entries using contracts config.
    /// Called on first handle() when the TransformationContext is available.
    pub fn resolve_quote_decimals(&self, contracts: &Contracts) {
        // moka iter() yields (Arc<K>, Arc<V>), so we clone the inner key
        let to_update: Vec<(Vec<u8>, Arc<PoolMetadata>)> = self
            .inner
            .iter()
            .filter_map(|(k, v)| {
                quote_token_decimals(&v.quote_token, contracts).and_then(|d| {
                    if v.quote_decimals == d {
                        return None; // already correct
                    }
                    let mut updated = (*v).clone();
                    updated.quote_decimals = d;
                    Some(((*k).clone(), Arc::new(updated)))
                })
            })
            .collect();
        for (k, v) in to_update {
            self.inner.insert(k, v);
        }
    }

    /// Look up metadata by pool_id.
    pub fn get(&self, pool_id: &[u8]) -> Option<Arc<PoolMetadata>> {
        self.inner.get(pool_id)
    }

    /// Insert metadata for a pool if not already cached.
    /// Used by Create handlers to populate the cache in-memory before their
    /// DB transaction commits, so Swap handlers in the same range/block can
    /// find newly created pools without a DB round-trip.
    #[allow(dead_code)]
    pub fn insert_if_absent(&self, pool_id: Vec<u8>, meta: PoolMetadata) {
        self.inner.entry(pool_id).or_insert_with(|| Arc::new(meta));
    }

    /// Return all unique quote token addresses in the cache.
    pub fn unique_quote_tokens(&self) -> Vec<[u8; 20]> {
        let mut seen = std::collections::HashSet::new();
        for (_k, v) in self.inner.iter() {
            seen.insert(v.quote_token);
        }
        seen.into_iter().collect()
    }

    /// Re-query the `pools` table and insert any pools not already cached.
    ///
    /// Accepts an optional `addresses` filter to restrict the query to specific
    /// pool addresses, and an optional `block_window` to also include pools
    /// created within a given block range.
    ///
    /// Also resolves `quote_decimals` for newly inserted entries.
    ///
    /// Returns the number of newly discovered pools.
    pub async fn refresh(
        &self,
        pool: &Pool,
        chain_id: u64,
        contracts: &crate::types::config::contract::Contracts,
        addresses: &[Vec<u8>],
        block_window: Option<(u64, u64)>,
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
            block_window,
        )
        .await?;

        let mut new_count = 0;

        for row in &rows {
            let pool_id: Vec<u8> = row.get("address");
            let total_supply =
                parse_total_supply(row.get::<_, Option<String>>("total_supply_text"));

            if let Some(existing) = self.inner.get(pool_id.as_slice()) {
                if existing.total_supply.is_none() && total_supply.is_some() {
                    let mut updated = (*existing).clone();
                    updated.total_supply = total_supply;
                    if let Some(decimals) = quote_token_decimals(&updated.quote_token, contracts) {
                        updated.quote_decimals = decimals;
                    }
                    self.inner.insert(pool_id, Arc::new(updated));
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
            let mut quote_decimals = 18u8;
            if let Some(d) = quote_token_decimals(&quote_token, contracts) {
                quote_decimals = d;
            }

            self.inner.insert(
                pool_id,
                Arc::new(PoolMetadata {
                    quote_token,
                    is_token_0,
                    base_decimals: 18,
                    quote_decimals,
                    total_supply,
                }),
            );
            new_count += 1;
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
    block_window: Option<(u64, u64)>,
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

    let has_addresses = !addresses.is_empty();
    let has_window = block_window.is_some();

    match (has_addresses, has_window) {
        (true, true) => {
            let (start, end) = block_window.unwrap();
            // Convert Vec<Vec<u8>> to Vec<&[u8]> for the query parameter
            let addr_refs: Vec<&[u8]> = addresses.iter().map(|a| a.as_slice()).collect();
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
  AND (p.address = ANY($6) OR (p.block_number >= $7 AND p.block_number <= $8))
"#,
                    &[
                        &(chain_id as i64),
                        &pool_sources,
                        &pool_versions,
                        &token_sources,
                        &token_versions,
                        &addr_refs as &(dyn tokio_postgres::types::ToSql + Sync),
                        &(start as i64),
                        &(end as i64),
                    ],
                )
                .await
                .map_err(|e| TransformationError::DatabaseError(e.into()))
        }
        (true, false) => {
            let addr_refs: Vec<&[u8]> = addresses.iter().map(|a| a.as_slice()).collect();
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
                        &addr_refs as &(dyn tokio_postgres::types::ToSql + Sync),
                    ],
                )
                .await
                .map_err(|e| TransformationError::DatabaseError(e.into()))
        }
        (false, true) => {
            let (start, end) = block_window.unwrap();
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
  AND p.block_number >= $6 AND p.block_number <= $7
"#,
                    &[
                        &(chain_id as i64),
                        &pool_sources,
                        &pool_versions,
                        &token_sources,
                        &token_versions,
                        &(start as i64),
                        &(end as i64),
                    ],
                )
                .await
                .map_err(|e| TransformationError::DatabaseError(e.into()))
        }
        (false, false) => {
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
        }
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
