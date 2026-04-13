use async_trait::async_trait;

use alloy_primitives::Address;

use crate::db::{DbOperation, DbPool};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::transformations::util::db::pool::{insert_pool, PoolData};
use crate::transformations::util::db::token::{insert_token, TokenData};
use crate::transformations::util::pool_metadata::VersionedSource;
use crate::transformations::util::sanitize::strip_nul_bytes;
use crate::types::uniswap::v4::{PoolAddressOrPoolId, PoolKey};

const SOURCE: &str = "ZoraFactory";
const ZERO_ADDRESS: [u8; 20] = [0u8; 20];
pub const ZORA_CREATE_HANDLER_NAME: &str = "ZoraCreateHandler";
pub const ZORA_CREATE_HANDLER_VERSION: u32 = 1;
pub const ZORA_CREATE_HANDLER_SCOPE: VersionedSource =
    VersionedSource::new(ZORA_CREATE_HANDLER_NAME, ZORA_CREATE_HANDLER_VERSION);

#[derive(Clone, Copy)]
enum CoinKind {
    Creator,
    Content,
}

impl CoinKind {
    fn event_name(self) -> &'static str {
        match self {
            Self::Creator => "CreatorCoinCreated",
            Self::Content => "CoinCreatedV4",
        }
    }

    fn pool_type(self) -> &'static str {
        match self {
            Self::Creator => "zora_creator_coin",
            Self::Content => "zora_content_coin",
        }
    }

    fn is_creator_coin(self) -> bool {
        matches!(self, Self::Creator)
    }

    fn is_content_coin(self) -> bool {
        matches!(self, Self::Content)
    }
}

struct KnownQuoteTokenMetadata {
    name: &'static str,
    symbol: &'static str,
    decimals: u8,
}

pub struct ZoraCreateHandler;

#[async_trait]
impl TransformationHandler for ZoraCreateHandler {
    fn name(&self) -> &'static str {
        ZORA_CREATE_HANDLER_NAME
    }

    fn version(&self) -> u32 {
        ZORA_CREATE_HANDLER_VERSION
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/tokens.sql",
            "migrations/tables/pools.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["tokens", "pools"]
    }

    fn requires_sequential(&self) -> bool {
        false
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let mut ops = Vec::new();

        for kind in [CoinKind::Creator, CoinKind::Content] {
            self.process_kind(ctx, kind, &mut ops)?;
        }

        Ok(ops)
    }

    async fn initialize(&self, _db_pool: &DbPool) -> Result<(), TransformationError> {
        tracing::info!("ZoraCreateHandler initialized");
        Ok(())
    }
}

impl EventHandler for ZoraCreateHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new(
                SOURCE,
                "CreatorCoinCreated(address,address,address,address,string,string,string,address,(address,address,uint24,int24,address),bytes32,string)",
            ),
            EventTrigger::new(
                SOURCE,
                "CoinCreatedV4(address,address,address,address,string,string,string,address,(address,address,uint24,int24,address),bytes32,string)",
            ),
        ]
    }
}

impl ZoraCreateHandler {
    fn process_kind(
        &self,
        ctx: &TransformationContext,
        kind: CoinKind,
        ops: &mut Vec<DbOperation>,
    ) -> Result<(), TransformationError> {
        for event in ctx.events_of_type(SOURCE, kind.event_name()) {
            let caller = event.extract_address("caller")?;
            let payout_recipient = event.extract_address("payoutRecipient")?;
            let platform_referrer = event.extract_address("platformReferrer")?;
            let currency = event.extract_address("currency")?;
            let uri = strip_nul_bytes(event.extract_string("uri")?);
            let name = strip_nul_bytes(event.extract_string("name")?);
            let symbol = strip_nul_bytes(event.extract_string("symbol")?);
            let coin = event.extract_address("coin")?;

            let pool_key = PoolKey {
                currency0: event.extract_address("poolKey.currency0")?.into(),
                currency1: event.extract_address("poolKey.currency1")?.into(),
                fee: event.extract_u32("poolKey.fee")?,
                tick_spacing: event.extract_i32_flexible("poolKey.tickSpacing")?,
                hooks: event.extract_address("poolKey.hooks")?.into(),
            };

            let pool_id = event.extract_bytes32("poolKeyHash")?;
            let computed_pool_id = pool_key.pool_id();
            if computed_pool_id.0 != pool_id {
                tracing::warn!(
                    event = kind.event_name(),
                    pool_id = hex::encode(pool_id),
                    computed_pool_id = hex::encode(computed_pool_id),
                    block = event.block_number,
                    "Zora create event poolKeyHash does not match computed pool id; using event value"
                );
            }

            ops.push(insert_token(
                &TokenData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    tx_hash: &event.transaction_hash,
                    creator_address: Some(&payout_recipient),
                    integrator: non_zero_address(platform_referrer).as_ref(),
                    token_address: &coin,
                    pool: Some(&PoolAddressOrPoolId::PoolId(pool_id)),
                    name: name.as_ref(),
                    symbol: symbol.as_ref(),
                    decimals: 18,
                    total_supply: None,
                    token_uri: Some(uri.as_ref()),
                    is_derc20: false,
                    is_creator_coin: kind.is_creator_coin(),
                    is_content_coin: kind.is_content_coin(),
                    creator_coin_pool: None,
                    governance: None,
                },
                ctx,
            ));

            if let Some(meta) = known_quote_token_metadata(ctx, currency) {
                ops.push(insert_token(
                    &TokenData {
                        block_number: event.block_number,
                        block_timestamp: event.block_timestamp,
                        tx_hash: &event.transaction_hash,
                        creator_address: None,
                        integrator: None,
                        token_address: &currency,
                        pool: None,
                        name: meta.name,
                        symbol: meta.symbol,
                        decimals: meta.decimals,
                        total_supply: None,
                        token_uri: None,
                        is_derc20: false,
                        is_creator_coin: false,
                        is_content_coin: false,
                        creator_coin_pool: None,
                        governance: None,
                    },
                    ctx,
                ));
            }

            ops.push(insert_pool(
                &PoolData {
                    block_number: event.block_number,
                    block_timestamp: event.block_timestamp,
                    address: PoolAddressOrPoolId::PoolId(pool_id),
                    base_token: coin,
                    quote_token: currency,
                    is_token_0: coin < currency,
                    pool_type: kind.pool_type().to_string(),
                    integrator: platform_referrer,
                    initializer: caller,
                    fee: pool_key.fee,
                    min_threshold: alloy_primitives::U256::ZERO,
                    max_threshold: alloy_primitives::U256::ZERO,
                    migrator: ZERO_ADDRESS,
                    migrated_at: None,
                    migration_pool: PoolAddressOrPoolId::Address(ZERO_ADDRESS),
                    migration_type: "unknown".to_string(),
                    lock_duration: None,
                    beneficiaries: None,
                    pool_key: Some(pool_key),
                    starting_time: 0,
                    ending_time: 0,
                },
                ctx,
            ));
        }

        Ok(())
    }
}

fn non_zero_address(address: [u8; 20]) -> Option<[u8; 20]> {
    (!Address::from(address).is_zero()).then_some(address)
}

fn known_quote_token_metadata(
    ctx: &TransformationContext,
    address: [u8; 20],
) -> Option<KnownQuoteTokenMetadata> {
    if Address::from(address).is_zero() {
        return Some(KnownQuoteTokenMetadata {
            name: "Native Ether",
            symbol: "ETH",
            decimals: 18,
        });
    }

    match ctx.get_contract_name_by_address(address) {
        Some("Weth") => Some(KnownQuoteTokenMetadata {
            name: "Wrapped Ether",
            symbol: "WETH",
            decimals: 18,
        }),
        Some("Usdc") => Some(KnownQuoteTokenMetadata {
            name: "USD Coin",
            symbol: "USDC",
            decimals: 6,
        }),
        Some("Usdt") => Some(KnownQuoteTokenMetadata {
            name: "Tether USD",
            symbol: "USDT",
            decimals: 6,
        }),
        Some("Eurc") => Some(KnownQuoteTokenMetadata {
            name: "EURC",
            symbol: "EURC",
            decimals: 6,
        }),
        Some("Zora") => Some(KnownQuoteTokenMetadata {
            name: "ZORA",
            symbol: "ZORA",
            decimals: 18,
        }),
        _ => None,
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(ZoraCreateHandler);
}
