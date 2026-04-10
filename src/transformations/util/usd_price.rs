//! USD price resolution for pool metrics.
//!
//! Provides `OraclePriceCache` (shared across handlers, DB-backed) and
//! `UsdPriceContext` (per-invocation snapshot) to convert quote-token
//! volumes into USD.
//!
//! Quote token → USD resolution:
//! - WETH: multiply by ETH/USD from ChainlinkEthOracle
//! - USDC, USDT: 1.0 (stablecoin assumption)
//! - EURC: multiply by EURC/USDC from prices table

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

// ─── OraclePriceCache ────────────────────────────────────────────────

/// Shared cache for oracle-derived USD prices. Thread-safe, persists across
/// handler invocations. Seeded from the `prices` table on startup; updated
/// from ChainlinkEthOracle calls during processing.
pub struct OraclePriceCache {
    inner: RwLock<CachedPrices>,
    weth_address: Option<[u8; 20]>,
    eurc_address: Option<[u8; 20]>,
    seeded_from_db: AtomicBool,
}

#[derive(Default)]
struct CachedPrices {
    eth_usd: Option<BigDecimal>,
    eurc_usd: Option<BigDecimal>,
}

impl OraclePriceCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(CachedPrices::default()),
            weth_address: None,
            eurc_address: None,
            seeded_from_db: AtomicBool::new(false),
        }
    }

    pub fn with_contracts(contracts: &Contracts) -> Self {
        Self {
            inner: RwLock::new(CachedPrices::default()),
            weth_address: resolve_contract_address(contracts, "Weth"),
            eurc_address: resolve_contract_address(contracts, "Eurc"),
            seeded_from_db: AtomicBool::new(false),
        }
    }

    /// Seed the cache from the `prices` table on startup.
    /// Queries for the latest ETH/USD and EURC/USD prices.
    pub async fn load_from_db(
        &self,
        db_pool: &Pool,
        chain_id: u64,
    ) -> Result<(), TransformationError> {
        if let Some(weth_addr) = self.weth_address {
            if let Ok(price) =
                query_latest_price(db_pool, chain_id, &weth_addr, Some(&USD_QUOTE_TOKEN)).await
            {
                self.inner.write().unwrap().eth_usd = Some(price);
            }
        }

        // EURC/USD is written by PriceHandler as EURC/USDC, which we treat as
        // EURC/USD for the metrics use case.
        if let Some(eurc_addr) = self.eurc_address {
            if let Ok(price) = query_latest_price(db_pool, chain_id, &eurc_addr, None).await {
                self.inner.write().unwrap().eurc_usd = Some(price);
            }
        }

        let cache = self.inner.read().unwrap();
        tracing::info!(
            eth_usd = ?cache.eth_usd.as_ref().map(|p| p.to_string()),
            eurc_usd = ?cache.eurc_usd.as_ref().map(|p| p.to_string()),
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

    fn get_eth_usd(&self) -> Option<BigDecimal> {
        self.inner.read().unwrap().eth_usd.clone()
    }

    fn get_eurc_usd(&self) -> Option<BigDecimal> {
        self.inner.read().unwrap().eurc_usd.clone()
    }

    fn set_eth_usd(&self, price: BigDecimal) {
        self.inner.write().unwrap().eth_usd = Some(price);
    }

    fn set_eurc_usd(&self, price: BigDecimal) {
        self.inner.write().unwrap().eurc_usd = Some(price);
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
    pub eth_usd: Option<BigDecimal>,
    pub eurc_usd: Option<BigDecimal>,
    weth_address: Option<[u8; 20]>,
    usdc_address: Option<[u8; 20]>,
    usdt_address: Option<[u8; 20]>,
    eurc_address: Option<[u8; 20]>,
}

impl UsdPriceContext {
    /// Create a UsdPriceContext for testing. Allows setting arbitrary addresses.
    #[cfg(test)]
    pub fn new_for_test(
        eth_usd: Option<BigDecimal>,
        eurc_usd: Option<BigDecimal>,
        weth_address: Option<[u8; 20]>,
        usdc_address: Option<[u8; 20]>,
        usdt_address: Option<[u8; 20]>,
        eurc_address: Option<[u8; 20]>,
    ) -> Self {
        Self {
            eth_usd,
            eurc_usd,
            weth_address,
            usdc_address,
            usdt_address,
            eurc_address,
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

    /// Get the USD multiplier for a quote token.
    /// WETH → ETH/USD price, USDC/USDT → 1.0, EURC → EURC/USD price.
    fn quote_to_usd_multiplier(&self, quote_token: &[u8; 20]) -> Option<BigDecimal> {
        if self.weth_address.as_ref() == Some(quote_token) {
            return self.eth_usd.clone();
        }
        if self.usdc_address.as_ref() == Some(quote_token)
            || self.usdt_address.as_ref() == Some(quote_token)
        {
            return Some(BigDecimal::from(1));
        }
        if self.eurc_address.as_ref() == Some(quote_token) {
            return self.eurc_usd.clone();
        }
        None
    }
}

// ─── Builder ─────────────────────────────────────────────────────────

/// Build a `UsdPriceContext` for the current block range.
///
/// 1. Extracts latest ETH/USD from ChainlinkEthOracle calls in the range.
///    Updates the shared cache and returns DbOps to persist to `prices`.
/// 2. Falls back to cached value if no oracle data in range.
/// 3. Reads EURC/USD from cache (seeded from DB at startup, updated by PriceHandler).
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

    // Resolve quote token addresses from config
    let weth_address = resolve_contract_address(contracts, "Weth");
    let usdc_address = resolve_contract_address(contracts, "Usdc");
    let usdt_address = resolve_contract_address(contracts, "Usdt");
    let eurc_address = resolve_contract_address(contracts, "Eurc");

    // Extract ETH/USD from oracle calls in the current block range.
    let mut eth_usd =
        extract_eth_usd_from_oracle(ctx, cache, &weth_address, chain_id, &mut price_ops);

    // Cold-start safety net: if the shared cache is still empty, fall back to
    // the latest persisted ETH/USD row.
    if eth_usd.is_none() {
        if let Some(pool) = db_pool.get() {
            if let Some(weth_addr) = &weth_address {
                if let Ok(price) =
                    query_latest_price(pool, chain_id, weth_addr, Some(&USD_QUOTE_TOKEN)).await
                {
                    cache.set_eth_usd(price.clone());
                    eth_usd = Some(price);
                }
            }
        }
    }

    // EURC/USD: try to refresh from DB if cache is empty
    let mut eurc_usd = cache.get_eurc_usd();
    if eurc_usd.is_none() {
        if let Some(pool) = db_pool.get() {
            if let Some(eurc_addr) = &eurc_address {
                if let Ok(price) = query_latest_price(pool, chain_id, eurc_addr, None).await {
                    cache.set_eurc_usd(price.clone());
                    eurc_usd = Some(price);
                }
            }
        }
    }

    let usd_ctx = UsdPriceContext {
        eth_usd,
        eurc_usd,
        weth_address,
        usdc_address,
        usdt_address,
        eurc_address,
    };

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
) -> Option<BigDecimal> {
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
        // Update cache
        cache.set_eth_usd(price.clone());

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

        Some(price)
    } else {
        // No oracle data in range — use cached value
        cache.get_eth_usd()
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
async fn query_latest_price(
    pool: &Pool,
    chain_id: u64,
    token_address: &[u8; 20],
    quote_token_filter: Option<&[u8; 20]>,
) -> Result<BigDecimal, TransformationError> {
    let client = pool
        .get()
        .await
        .map_err(|e| TransformationError::DatabaseError(e.into()))?;

    let chain_id_param = chain_id as i64;
    let token_param = token_address.to_vec();

    let row = if let Some(quote_token) = quote_token_filter {
        let quote_token_param = quote_token.to_vec();
        client
            .query_opt(
                "SELECT price FROM prices \
                 WHERE chain_id = $1 AND token = $2 AND quote_token = $3 \
                 ORDER BY block_number DESC LIMIT 1",
                &[&chain_id_param, &token_param, &quote_token_param],
            )
            .await
    } else {
        client
            .query_opt(
                "SELECT price FROM prices \
                 WHERE chain_id = $1 AND token = $2 \
                 ORDER BY block_number DESC LIMIT 1",
                &[&chain_id_param, &token_param],
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
    price
        .to_string()
        .parse::<BigDecimal>()
        .map_err(|e| TransformationError::ConfigError(format!("Invalid price value: {}", e)))
}

/// Convert a U256 to BigDecimal, dividing by 10^decimals.
fn u256_to_decimal_adjusted(value: &U256, decimals: u8) -> BigDecimal {
    let raw = BigDecimal::from(value.to_string().parse::<i128>().unwrap_or(0));
    if decimals == 0 {
        return raw;
    }
    let divisor = BigDecimal::from(10u64.pow(decimals as u32));
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

    fn make_ctx(eth_usd: Option<&str>, eurc_usd: Option<&str>) -> UsdPriceContext {
        UsdPriceContext {
            eth_usd: eth_usd.map(|s| bd(s)),
            eurc_usd: eurc_usd.map(|s| bd(s)),
            weth_address: Some([0x01; 20]),
            usdc_address: Some([0x02; 20]),
            usdt_address: Some([0x03; 20]),
            eurc_address: Some([0x04; 20]),
        }
    }

    #[test]
    fn test_weth_volume_to_usd() {
        let ctx = make_ctx(Some("2000"), None);
        let weth = [0x01; 20];
        // 1.5 WETH raw = 1_500_000_000_000_000_000 (18 decimals)
        let volume = U256::from(1_500_000_000_000_000_000u64);
        let usd = ctx.quote_volume_to_usd(&volume, &weth, 18).unwrap();
        assert_eq!(usd, bd("3000"));
    }

    #[test]
    fn test_usdc_volume_to_usd() {
        let ctx = make_ctx(Some("2000"), None);
        let usdc = [0x02; 20];
        // 500 USDC raw = 500_000_000 (6 decimals)
        let volume = U256::from(500_000_000u64);
        let usd = ctx.quote_volume_to_usd(&volume, &usdc, 6).unwrap();
        assert_eq!(usd, bd("500"));
    }

    #[test]
    fn test_usdt_volume_to_usd() {
        let ctx = make_ctx(Some("2000"), None);
        let usdt = [0x03; 20];
        let volume = U256::from(1_000_000u64); // 1 USDT
        let usd = ctx.quote_volume_to_usd(&volume, &usdt, 6).unwrap();
        assert_eq!(usd, bd("1"));
    }

    #[test]
    fn test_eurc_volume_to_usd() {
        let ctx = make_ctx(None, Some("1.08"));
        let eurc = [0x04; 20];
        // 100 EURC raw = 100_000_000 (6 decimals)
        let volume = U256::from(100_000_000u64);
        let usd = ctx.quote_volume_to_usd(&volume, &eurc, 6).unwrap();
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
        let weth = [0x01; 20];
        let volume = U256::from(1_000_000_000_000_000_000u64);
        assert!(ctx.quote_volume_to_usd(&volume, &weth, 18).is_none());
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
