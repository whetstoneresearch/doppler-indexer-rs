use async_trait::async_trait;

use crate::db::{DbOperation, DbPool, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EthCallHandler, EthCallTrigger, TransformationHandler};
use crate::transformations::util::price::sqrt_price_x96_to_price;
use crate::types::config::contract::{AddressOrAddresses, Contracts};

/// Hardcoded decimal map for configured anchor pool tokens.
/// This avoids a DB lookup and covers all tokens that appear in tokens.json pool configs.
fn decimals_for_token(name: &str) -> Option<u8> {
    match name {
        "Weth" => Some(18),
        "Usdc" => Some(6),
        "Usdt" => Some(6),
        "Eurc" => Some(6),
        "Fxh" => Some(18),
        "Noice" => Some(18),
        "Zora" => Some(18),
        "Bankr" => Some(18),
        "Mon" => Some(18),
        _ => None,
    }
}

/// A pool config discovered dynamically from the contracts config at registration time.
struct DynamicPoolConfig {
    /// Contract source name from config (e.g., "BankrWethPool")
    source: String,
    /// Function name without parens (e.g., "slot0")
    function_name: String,
    /// Resolved address of the token being priced
    token_address: [u8; 20],
    /// Resolved address of the quote token
    quote_token_address: [u8; 20],
    /// Decimals of the token being priced
    token_decimals: u8,
    /// Decimals of the quote token
    quote_decimals: u8,
    /// Whether the token is token0 in the pool
    is_token0: bool,
}

/// Strip parenthesized parameters from a function signature.
/// e.g., "slot0()" -> "slot0", "getSlot0(bytes32)" -> "getSlot0"
fn strip_function_params(sig: &str) -> &str {
    sig.split('(').next().unwrap_or(sig)
}

/// Resolve a single address from the contracts config by name.
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

/// Discover pool configs from the contracts config.
///
/// Scans for entries ending in "Pool" that have `calls` containing a slot0-like function.
/// Parses the naming convention `<Base><Quote>Pool` to extract token pair info.
fn discover_pool_configs(contracts: &Contracts) -> Vec<DynamicPoolConfig> {
    // Collect non-Pool entries as the set of known token names.
    let mut token_names: Vec<String> = contracts
        .keys()
        .filter(|name| !name.ends_with("Pool"))
        .cloned()
        .collect();
    // Sort by length descending so longer names match first (avoids "Usdc" matching
    // inside a longer name like "BUsdc").
    token_names.sort_by(|a, b| b.len().cmp(&a.len()));

    let mut configs = Vec::new();

    for (name, config) in contracts {
        if !name.ends_with("Pool") {
            continue;
        }

        // Must have calls with a slot0-like function
        let calls = match &config.calls {
            Some(calls) if !calls.is_empty() => calls,
            _ => continue,
        };

        // Find the slot0-like call (either "slot0()" or "getSlot0(...)")
        let slot0_call = calls.iter().find(|c| {
            let fn_name = strip_function_params(&c.function);
            fn_name == "slot0" || fn_name == "getSlot0"
        });
        let slot0_call = match slot0_call {
            Some(c) => c,
            None => continue,
        };

        let function_name = strip_function_params(&slot0_call.function).to_string();

        // Strip "Pool" suffix to get the pair name (e.g., "BankrWeth")
        let pair_name = &name[..name.len() - 4];

        // Identify the quote token by finding which known token name the pair ends with.
        let quote_match = token_names.iter().find(|t| pair_name.ends_with(t.as_str()));
        let quote_token_name = match quote_match {
            Some(name) => name.clone(),
            None => {
                tracing::warn!(
                    pool = %name,
                    "Could not identify quote token from pool name, skipping"
                );
                continue;
            }
        };

        // Base token is the remainder after stripping the quote suffix.
        let base_token_name = &pair_name[..pair_name.len() - quote_token_name.len()];
        if base_token_name.is_empty() {
            tracing::warn!(
                pool = %name,
                "Empty base token name after parsing, skipping"
            );
            continue;
        }

        // Resolve addresses from the config.
        let token_address = match resolve_address(contracts, base_token_name) {
            Ok(addr) => addr,
            Err(_) => {
                tracing::warn!(
                    pool = %name,
                    base_token = %base_token_name,
                    "Base token not found in contracts config, skipping"
                );
                continue;
            }
        };
        let quote_token_address = match resolve_address(contracts, &quote_token_name) {
            Ok(addr) => addr,
            Err(_) => {
                tracing::warn!(
                    pool = %name,
                    quote_token = %quote_token_name,
                    "Quote token not found in contracts config, skipping"
                );
                continue;
            }
        };

        // Look up decimals from the hardcoded map.
        let token_decimals = match decimals_for_token(base_token_name) {
            Some(d) => d,
            None => {
                tracing::warn!(
                    pool = %name,
                    token = %base_token_name,
                    "Unknown decimals for base token, skipping"
                );
                continue;
            }
        };
        let quote_decimals = match decimals_for_token(&quote_token_name) {
            Some(d) => d,
            None => {
                tracing::warn!(
                    pool = %name,
                    token = %quote_token_name,
                    "Unknown decimals for quote token, skipping"
                );
                continue;
            }
        };

        let is_token0 = token_address < quote_token_address;

        tracing::info!(
            pool = %name,
            base = %base_token_name,
            quote = %quote_token_name,
            function = %function_name,
            is_token0,
            "Discovered price pool config"
        );

        configs.push(DynamicPoolConfig {
            source: name.clone(),
            function_name,
            token_address,
            quote_token_address,
            token_decimals,
            quote_decimals,
            is_token0,
        });
    }

    configs
}

pub struct PriceHandler {
    configs: Vec<DynamicPoolConfig>,
}

impl PriceHandler {
    fn from_contracts(contracts: &Contracts) -> Self {
        Self {
            configs: discover_pool_configs(contracts),
        }
    }

    fn empty() -> Self {
        Self {
            configs: Vec::new(),
        }
    }
}

#[async_trait]
impl TransformationHandler for PriceHandler {
    fn name(&self) -> &'static str {
        "PriceHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/prices.sql",
            "migrations/tables/pools_token_indexes.sql",
            "migrations/tables/token_price_paths.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["prices"]
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for config in &self.configs {
            for call in ctx.calls_of_type(&config.source, &config.function_name) {
                let sqrt_price_x96 = call.extract_uint256("sqrtPriceX96")?;

                let Some(price) = sqrt_price_x96_to_price(
                    &sqrt_price_x96,
                    config.token_decimals,
                    config.quote_decimals,
                    config.is_token0,
                ) else {
                    continue;
                };

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
                        DbValue::Address(config.token_address),
                        DbValue::Address(config.quote_token_address),
                        DbValue::Numeric(price.to_string()),
                    ],
                    conflict_columns: vec!["timestamp".into(), "chain_id".into(), "token".into()],
                    update_columns: vec![
                        "block_number".into(),
                        "quote_token".into(),
                        "price".into(),
                    ],
                    update_condition: None,
                });
            }
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!(pool_count = self.configs.len(), "PriceHandler initialized");
        Ok(())
    }
}

impl EthCallHandler for PriceHandler {
    fn triggers(&self) -> Vec<EthCallTrigger> {
        self.configs
            .iter()
            .map(|c| EthCallTrigger::new(&c.source, &c.function_name))
            .collect()
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry, contracts: Option<&Contracts>) {
    let handler = match contracts {
        Some(contracts) => PriceHandler::from_contracts(contracts),
        None => PriceHandler::empty(),
    };
    registry.register_call_handler(handler);
}
