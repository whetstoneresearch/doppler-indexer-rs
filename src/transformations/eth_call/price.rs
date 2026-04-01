use async_trait::async_trait;

use crate::db::{DbOperation, DbPool, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EthCallHandler, EthCallTrigger, TransformationHandler};
use crate::transformations::util::price::sqrt_price_x96_to_price;
use crate::types::config::contract::{AddressOrAddresses, Contracts};

struct PoolConfig {
    /// Contract source name from config (e.g., "EurcUsdcPool")
    source: &'static str,
    /// Function name: "slot0" for V3, "getSlot0" for V4
    function_name: &'static str,
    /// Config name of the token being priced (e.g., "Eurc")
    token: &'static str,
    /// Config name of the quote token (e.g., "Usdc")
    quote_token: &'static str,
    /// Decimals of the token being priced
    token_decimals: u8,
    /// Decimals of the quote token
    quote_decimals: u8,
    /// Whether the token is token0 in the pool
    is_token0: bool,
}

const POOL_CONFIGS: &[PoolConfig] = &[
    // Eurc/Usdc — V4 pool (getSlot0)
    PoolConfig {
        source: "EurcUsdcPool",
        function_name: "getSlot0",
        token: "Eurc",
        quote_token: "Usdc",
        token_decimals: 6,
        quote_decimals: 6,
        is_token0: false,
    },
    // Fxh/Weth — V3 pool (slot0)
    PoolConfig {
        source: "FxhWethPool",
        function_name: "slot0",
        token: "Fxh",
        quote_token: "Weth",
        token_decimals: 18,
        quote_decimals: 18,
        is_token0: false,
    },
    // Noice/Weth — V3 pool (slot0)
    PoolConfig {
        source: "NoiceWethPool",
        function_name: "slot0",
        token: "Noice",
        quote_token: "Weth",
        token_decimals: 18,
        quote_decimals: 18,
        is_token0: false,
    },
    // Zora/Usdc — V3 pool (slot0)
    PoolConfig {
        source: "ZoraUsdcPool",
        function_name: "slot0",
        token: "Zora",
        quote_token: "Usdc",
        token_decimals: 18,
        quote_decimals: 6,
        is_token0: false,
    },
];

fn resolve_address(contracts: &Contracts, name: &str) -> Result<[u8; 20], TransformationError> {
    let config = contracts.get(name).ok_or_else(|| {
        TransformationError::ConfigError(format!("contract '{}' not found in config", name))
    })?;
    match &config.address {
        AddressOrAddresses::Single(addr) => Ok(addr.0 .0),
        AddressOrAddresses::Multiple(addrs) => addrs.first().map(|a| a.0 .0).ok_or_else(|| {
            TransformationError::ConfigError(format!("contract '{}' has no addresses", name))
        }),
    }
}

pub struct PriceHandler;

#[async_trait]
impl TransformationHandler for PriceHandler {
    fn name(&self) -> &'static str {
        "PriceHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec!["migrations/tables/prices.sql"]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["prices"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for config in POOL_CONFIGS {
            let token_address = resolve_address(&ctx.contracts, config.token)?;
            let quote_token_address = resolve_address(&ctx.contracts, config.quote_token)?;

            for call in ctx.calls_of_type(config.source, config.function_name) {
                let sqrt_price_x96 = call.extract_uint256("sqrtPriceX96")?;

                if sqrt_price_x96.is_zero() {
                    continue;
                }

                let price = sqrt_price_x96_to_price(
                    &sqrt_price_x96,
                    config.token_decimals,
                    config.quote_decimals,
                    config.is_token0,
                );

                ops.push(DbOperation::Upsert {
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
                        DbValue::Timestamp(call.block_timestamp as i64),
                        DbValue::Int64(call.block_number as i64),
                        DbValue::Int64(ctx.chain_id as i64),
                        DbValue::Address(token_address),
                        DbValue::Address(quote_token_address),
                        DbValue::Numeric(format!("{:.18}", price)),
                    ],
                    conflict_columns: vec!["timestamp".into(), "chain_id".into(), "token".into()],
                    update_columns: vec![
                        "block_number".into(),
                        "quote_token".into(),
                        "price".into(),
                    ],
                });
            }
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("PriceHandler initialized");
        Ok(())
    }
}

impl EthCallHandler for PriceHandler {
    fn triggers(&self) -> Vec<EthCallTrigger> {
        POOL_CONFIGS
            .iter()
            .map(|c| EthCallTrigger::new(c.source, c.function_name))
            .collect()
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_call_handler(PriceHandler);
}
