//! Pool metadata cache for metrics handlers.
//!
//! Loads pool metadata from the `pools` table on handler initialization and
//! provides fast lookups by pool_id during event processing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use alloy_primitives::Address;
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
                "SELECT address, base_token, quote_token, is_token_0, type \
                 FROM pools WHERE chain_id = $1",
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

            if base_token_bytes.len() != 20 || quote_token_bytes.len() != 20 {
                continue;
            }

            let mut base_token = [0u8; 20];
            let mut quote_token = [0u8; 20];
            base_token.copy_from_slice(&base_token_bytes);
            quote_token.copy_from_slice(&quote_token_bytes);

            let base_decimals = 18; // all launched tokens are 18 decimals
            let quote_decimals = quote_token_decimals(&quote_token, contracts).unwrap_or(18);

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
    /// Returns the number of newly discovered pools.
    pub async fn refresh(
        &self,
        pool: &Pool,
        chain_id: u64,
    ) -> Result<usize, TransformationError> {
        let client = pool
            .get()
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let rows = client
            .query(
                "SELECT address, base_token, quote_token, is_token_0, type \
                 FROM pools WHERE chain_id = $1",
                &[&(chain_id as i64)],
            )
            .await
            .map_err(|e| TransformationError::DatabaseError(e.into()))?;

        let mut inner = self.inner.write().unwrap();
        let mut new_count = 0;

        for row in &rows {
            let pool_id: Vec<u8> = row.get("address");
            if inner.contains_key(&pool_id) {
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
                },
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
];

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
