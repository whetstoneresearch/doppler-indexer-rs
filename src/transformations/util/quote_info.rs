//! Quote token identification and USD price resolution.
//!
//! Given a token address, determines the quote token type (ETH, ZORA, stablecoin,
//! creator coin, etc.) and fetches its USD price from parquet-based oracle data.

use std::collections::HashSet;
use std::path::PathBuf;

use alloy::primitives::{Address, U256};
use arrow::array::{Array, BinaryArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::constants::CHAINLINK_ETH_DECIMALS;
use super::price_fetch::{PriceFetcher, Token};
use crate::transformations::error::TransformationError;
use crate::types::config::chain::ChainConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteToken {
    Eth,
    Zora,
    Fxh,
    Noice,
    Mon,
    Usdc,
    Usdt,
    Eurc,
    CreatorCoin,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct QuoteInfo {
    pub quote_token: QuoteToken,
    pub quote_price: Option<U256>,
    /// Token decimals (e.g. 18 for ETH, 6 for USDC).
    pub quote_decimals: u8,
    /// Price feed decimals (e.g. 8 for Chainlink, 18 for sqrtPriceX96-derived).
    pub quote_price_decimals: u8,
}

/// Resolves quote token type and USD price for a given address.
///
/// Identifies known tokens from chain config addresses, detects creator coins
/// from factory parquets, and fetches USD prices via [`PriceFetcher`].
pub struct QuoteResolver {
    weth: Address,
    zora: Address,
    fxh: Address,
    noice: Address,
    mon: Address,
    usdc: Address,
    usdt: Address,
    eurc: Address,
    creator_coins: HashSet<Address>,
}

impl QuoteResolver {
    /// Build a resolver from chain config. Loads creator coin addresses from
    /// `data/derived/{chain}/factories/ZoraCreatorCoinV4/` parquets.
    pub fn new(config: &ChainConfig) -> Result<Self, TransformationError> {
        let addr =
            |key: &str| config.tokens.get(key).map(|t| t.address).unwrap_or(Address::ZERO);

        Ok(Self {
            weth: addr("Weth"),
            zora: addr("Zora"),
            fxh: addr("Fxh"),
            noice: addr("Noice"),
            mon: addr("Mon"),
            usdc: addr("Usdc"),
            usdt: addr("Usdt"),
            eurc: addr("Eurc"),
            creator_coins: load_creator_coins(&config.name)?,
        })
    }

    /// Identify the quote token and optionally fetch its USD price.
    ///
    /// Pass `block_number = None` to skip price fetching (returns metadata only).
    pub fn resolve(
        &self,
        address: Address,
        block_number: Option<u64>,
        price_fetcher: &PriceFetcher,
    ) -> Result<QuoteInfo, TransformationError> {
        let z = Address::ZERO;
        let is_eth = address == z || address == self.weth;
        let is_zora = address != z && address == self.zora;
        let is_fxh = address != z && address == self.fxh;
        let is_noice = address != z && address == self.noice;
        let is_mon = address != z && address == self.mon;
        let is_usdc = address != z && address == self.usdc;
        let is_usdt = address != z && address == self.usdt;
        let is_eurc = address != z && address == self.eurc;

        // Only check creator coin cache if no known token matched.
        let is_creator_coin =
            !(is_zora || is_fxh || is_noice || is_mon || is_usdc || is_usdt || is_eurc)
                && self.creator_coins.contains(&address);

        let qt = if is_zora {
            QuoteToken::Zora
        } else if is_fxh {
            QuoteToken::Fxh
        } else if is_noice {
            QuoteToken::Noice
        } else if is_mon {
            QuoteToken::Mon
        } else if is_usdc {
            QuoteToken::Usdc
        } else if is_usdt {
            QuoteToken::Usdt
        } else if is_eurc {
            QuoteToken::Eurc
        } else if is_creator_coin {
            QuoteToken::CreatorCoin
        } else if is_eth {
            QuoteToken::Eth
        } else {
            QuoteToken::Unknown
        };

        let quote_decimals = match qt {
            QuoteToken::Usdc | QuoteToken::Usdt | QuoteToken::Eurc => 6,
            _ => 18,
        };

        // Chainlink feeds use 8 decimals; EURC price derived from sqrtPriceX96 has 18.
        let quote_price_decimals = match qt {
            QuoteToken::Eth | QuoteToken::Usdc | QuoteToken::Usdt => 8,
            QuoteToken::Eurc => 18,
            _ => quote_decimals,
        };

        let quote_price = block_number
            .map(|b| self.fetch_price(qt, b, price_fetcher))
            .transpose()?;

        Ok(QuoteInfo {
            quote_token: qt,
            quote_price,
            quote_decimals,
            quote_price_decimals,
        })
    }

    fn fetch_price(
        &self,
        qt: QuoteToken,
        block: u64,
        pf: &PriceFetcher,
    ) -> Result<U256, TransformationError> {
        Ok(match qt {
            QuoteToken::Eth => pf.fetch_price(Token::Eth, block)?,
            QuoteToken::Zora => pf.fetch_price(Token::Zora, block)?,
            QuoteToken::Mon => pf.fetch_price(Token::Monad, block)?,
            QuoteToken::Usdc => pf.fetch_price(Token::Usdc, block)?,
            QuoteToken::Usdt => pf.fetch_price(Token::Usdt, block)?,
            QuoteToken::Eurc => pf.fetch_price(Token::Eurc, block)?,
            QuoteToken::Fxh => {
                let eth = pf.fetch_price(Token::Eth, block)?;
                let fxh = pf.fetch_price(Token::Fxh, block)?;
                (fxh * eth) / CHAINLINK_ETH_DECIMALS
            }
            QuoteToken::Noice => {
                let eth = pf.fetch_price(Token::Eth, block)?;
                let noice = pf.fetch_price(Token::Noice, block)?;
                (noice * eth) / CHAINLINK_ETH_DECIMALS
            }
            // TODO: compute from pool sqrtPriceX96 * zoraPrice / WAD when pool data available
            QuoteToken::CreatorCoin | QuoteToken::Unknown => U256::ZERO,
        })
    }
}

// ---------------------------------------------------------------------------
// Creator coin index from factory parquets
// ---------------------------------------------------------------------------

fn load_creator_coins(chain_name: &str) -> Result<HashSet<Address>, TransformationError> {
    let dir = PathBuf::from(format!(
        "data/derived/{}/factories/ZoraCreatorCoinV4",
        chain_name
    ));

    if !dir.exists() {
        return Ok(HashSet::new());
    }

    let mut paths: Vec<_> = std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|x| x == "parquet")
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    paths.sort();

    let mut coins = HashSet::new();
    for path in paths {
        let file = std::fs::File::open(&path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        for batch in reader {
            let batch = batch?;
            let addrs = batch
                .column_by_name("factory_address")
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| {
                    TransformationError::MissingColumn("factory_address".to_string())
                })?;

            for i in 0..batch.num_rows() {
                if addrs.is_null(i) {
                    continue;
                }
                let bytes = addrs.value(i);
                if bytes.len() == 20 {
                    coins.insert(Address::from_slice(bytes));
                }
            }
        }
    }

    tracing::info!(
        "Loaded {} creator coin addresses for {}",
        coins.len(),
        chain_name
    );
    Ok(coins)
}
