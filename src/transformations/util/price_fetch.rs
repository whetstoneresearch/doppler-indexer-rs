//! Price fetcher for reading token prices from decoded eth_call parquet files.
//!
//! Reads price oracle data (Chainlink, Uniswap V3/V4 pool slot0) from decoded
//! parquet files at `data/derived/{chain}/decoded/eth_calls/`. Caches one block
//! range per source for efficiency during sequential block processing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use alloy::primitives::U256;
use arrow::array::{Array, StringArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::constants::WAD;
use super::price::compute_price_from_sqrt_price_x96;
use crate::transformations::error::TransformationError;

const MAX_LOOKBACK_FILES: usize = 10;

/// Token whose price can be fetched from parquet data.
#[derive(Debug, Clone, Copy)]
pub enum Token {
    Eth,
    Zora,
    Fxh,
    Noice,
    Monad,
    Eurc,
    Usdc,
    Usdt,
}

/// How to convert the raw parquet value into a price.
enum PriceTransform {
    /// Return the raw value as-is (e.g. Chainlink latestAnswer).
    Raw,
    /// Compute price from sqrtPriceX96 via the V3/V4 formula.
    SqrtPrice {
        is_token0: bool,
        decimals: u8,
        quote_decimals: u8,
    },
}

/// Configuration for looking up a token's price from parquet files.
struct TokenLookup {
    /// Required chain name (None = any chain).
    chain: Option<&'static str>,
    /// Parquet source directory name.
    source: &'static str,
    /// Parquet function directory name.
    function: &'static str,
    /// Column name to read the value from.
    column: &'static str,
    transform: PriceTransform,
    /// Returned when the fetcher's chain doesn't match `chain`.
    chain_default: U256,
    /// Returned when no data is found (None = return an error instead).
    miss_default: Option<U256>,
}

impl Token {
    fn name(self) -> &'static str {
        match self {
            Token::Eth => "ETH",
            Token::Zora => "ZORA",
            Token::Fxh => "FXH",
            Token::Noice => "NOICE",
            Token::Monad => "MON",
            Token::Eurc => "EURC",
            Token::Usdc => "USDC",
            Token::Usdt => "USDT",
        }
    }

    fn lookup(self) -> TokenLookup {
        match self {
            Token::Eth => TokenLookup {
                chain: None,
                source: "ChainlinkEthOracle",
                function: "latestAnswer",
                column: "decoded_value",
                transform: PriceTransform::Raw,
                chain_default: WAD, // unused since chain is None
                miss_default: None,
            },
            Token::Zora => TokenLookup {
                chain: Some("base"),
                source: "Zora_pool",
                function: "slot0",
                column: "sqrtPriceX96",
                transform: PriceTransform::SqrtPrice { is_token0: true, decimals: 18, quote_decimals: 6 },
                chain_default: WAD,
                miss_default: None,
            },
            Token::Fxh => TokenLookup {
                chain: Some("base"),
                source: "Fxh_pool",
                function: "slot0",
                column: "sqrtPriceX96",
                transform: PriceTransform::SqrtPrice { is_token0: false, decimals: 18, quote_decimals: 18 },
                chain_default: WAD,
                miss_default: None,
            },
            Token::Noice => TokenLookup {
                chain: Some("base"),
                source: "Noice_pool",
                function: "slot0",
                column: "sqrtPriceX96",
                transform: PriceTransform::SqrtPrice { is_token0: false, decimals: 18, quote_decimals: 18 },
                chain_default: WAD,
                miss_default: None,
            },
            Token::Monad => TokenLookup {
                chain: Some("monad"),
                source: "Mon_pool",
                function: "slot0",
                column: "sqrtPriceX96",
                transform: PriceTransform::SqrtPrice { is_token0: true, decimals: 18, quote_decimals: 6 },
                chain_default: WAD,
                // Default 0.02 USD = 2 * 10^16 when pool not configured
                miss_default: Some(U256::from(2u64) * U256::from(10u64).pow(U256::from(16u32))),
            },
            Token::Eurc => TokenLookup {
                chain: Some("base"),
                source: "Eurc_pool",
                function: "getSlot0",
                column: "sqrtPriceX96",
                transform: PriceTransform::SqrtPrice { is_token0: true, decimals: 6, quote_decimals: 6 },
                // Default ~$1.15 = 115 * 10^16
                chain_default: U256::from(115u64) * U256::from(10u64).pow(U256::from(16u32)),
                miss_default: Some(U256::from(115u64) * U256::from(10u64).pow(U256::from(16u32))),
            },
            Token::Usdc | Token::Usdt => unreachable!("hardcoded tokens handled before lookup"),
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PriceEntry {
    block_number: u64,
    value: U256,
}

struct CachedFile {
    range_start: u64,
    range_end: u64,
    entries: Vec<PriceEntry>,
}

#[derive(Clone)]
struct FileRange {
    range_start: u64,
    range_end: u64,
    path: PathBuf,
}

/// Price fetcher that reads from decoded eth_call parquet files.
///
/// Caches one file's worth of data per (source, function) pair, since
/// sequential block processing means lookups cluster in the same range.
pub struct PriceFetcher {
    chain_name: String,
    file_index: HashMap<(String, String), Vec<FileRange>>,
    cache: Mutex<HashMap<(String, String), CachedFile>>,
}

impl PriceFetcher {
    pub fn new(chain_name: &str) -> Result<Self, TransformationError> {
        let base = PathBuf::from(format!("data/derived/{}/decoded/eth_calls", chain_name));
        let mut file_index = HashMap::new();

        if base.exists() {
            Self::build_index(&base, &mut file_index)?;
        }

        Ok(Self {
            chain_name: chain_name.to_string(),
            file_index,
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// Fetch a token price at or before the given block.
    pub fn fetch_price(
        &self,
        token: Token,
        block_number: u64,
    ) -> Result<U256, TransformationError> {
        // Hardcoded stablecoins — no parquet lookup needed.
        match token {
            Token::Usdc | Token::Usdt => return Ok(U256::from(100_000_000u64)),
            _ => {}
        }

        let lookup = token.lookup();

        // Chain gate: return default if this fetcher is for a different chain.
        if let Some(required) = lookup.chain {
            if self.chain_name != required {
                return Ok(lookup.chain_default);
            }
        }

        // Look up raw value from parquet.
        let raw = self.find_at_or_before(
            lookup.source,
            lookup.function,
            lookup.column,
            block_number,
        )?;

        let raw = match raw {
            Some(v) => v,
            None => {
                return match lookup.miss_default {
                    Some(d) => Ok(d),
                    None => Err(TransformationError::handler(
                        "PriceFetcher",
                        format!(
                            "No {} price found for chain {} at block {}",
                            token.name(),
                            self.chain_name,
                            block_number,
                        ),
                    )),
                }
            }
        };

        // Apply transform.
        Ok(match lookup.transform {
            PriceTransform::Raw => raw,
            PriceTransform::SqrtPrice {
                is_token0,
                decimals,
                quote_decimals,
            } => compute_price_from_sqrt_price_x96(raw, is_token0, decimals, quote_decimals),
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn build_index(
        base: &Path,
        index: &mut HashMap<(String, String), Vec<FileRange>>,
    ) -> Result<(), TransformationError> {
        for source_entry in std::fs::read_dir(base)? {
            let source_entry = source_entry?;
            if !source_entry.file_type()?.is_dir() {
                continue;
            }
            let source = source_entry.file_name().to_string_lossy().to_string();

            for func_entry in std::fs::read_dir(source_entry.path())? {
                let func_entry = func_entry?;
                if !func_entry.file_type()?.is_dir() {
                    continue;
                }
                let func = func_entry.file_name().to_string_lossy().to_string();

                let mut files = Vec::new();
                for file_entry in std::fs::read_dir(func_entry.path())? {
                    let file_entry = file_entry?;
                    let path = file_entry.path();
                    if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                        if let Some((start, end)) = parse_range(&path) {
                            files.push(FileRange {
                                range_start: start,
                                range_end: end,
                                path,
                            });
                        }
                    }
                }
                files.sort_by_key(|f| f.range_start);
                index.insert((source.clone(), func), files);
            }
        }
        Ok(())
    }

    fn find_at_or_before(
        &self,
        source: &str,
        function: &str,
        column: &str,
        block_number: u64,
    ) -> Result<Option<U256>, TransformationError> {
        let key = (source.to_string(), function.to_string());
        let files = match self.file_index.get(&key) {
            Some(f) => f,
            None => return Ok(None),
        };

        let file_idx = match files.iter().rposition(|f| f.range_start <= block_number) {
            Some(i) => i,
            None => return Ok(None),
        };

        let start = file_idx.saturating_sub(MAX_LOOKBACK_FILES);
        for idx in (start..=file_idx).rev() {
            let entries = self.load_entries(source, function, &files[idx], column)?;
            if let Some(entry) = binary_search_at_or_before(&entries, block_number) {
                return Ok(Some(entry.value));
            }
        }

        Ok(None)
    }

    fn load_entries(
        &self,
        source: &str,
        function: &str,
        file: &FileRange,
        column: &str,
    ) -> Result<Vec<PriceEntry>, TransformationError> {
        let key = (source.to_string(), function.to_string());
        let mut cache = self.cache.lock().unwrap();

        if let Some(cached) = cache.get(&key) {
            if cached.range_start == file.range_start && cached.range_end == file.range_end {
                return Ok(cached.entries.clone());
            }
        }

        let entries = read_entries(&file.path, column)?;
        cache.insert(
            key,
            CachedFile {
                range_start: file.range_start,
                range_end: file.range_end,
                entries: entries.clone(),
            },
        );
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

fn parse_range(path: &Path) -> Option<(u64, u64)> {
    let stem = path.file_stem()?.to_str()?;
    let parts: Vec<&str> = stem.trim_start_matches("decoded_").split('-').collect();
    if parts.len() == 2 {
        Some((parts[0].parse().ok()?, parts[1].parse().ok()?))
    } else {
        None
    }
}

fn read_entries(path: &Path, column: &str) -> Result<Vec<PriceEntry>, TransformationError> {
    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut entries = Vec::new();

    for batch in reader {
        let batch = batch?;
        let blocks = batch
            .column_by_name("block_number")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| TransformationError::MissingColumn("block_number".to_string()))?;
        let values = batch
            .column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| TransformationError::MissingColumn(column.to_string()))?;

        for row in 0..batch.num_rows() {
            if values.is_null(row) {
                continue;
            }
            if let Ok(value) = U256::from_str_radix(values.value(row), 10) {
                entries.push(PriceEntry {
                    block_number: blocks.value(row),
                    value,
                });
            }
        }
    }

    entries.sort_by_key(|e| e.block_number);
    Ok(entries)
}

fn binary_search_at_or_before(entries: &[PriceEntry], block_number: u64) -> Option<&PriceEntry> {
    if entries.is_empty() {
        return None;
    }
    match entries.binary_search_by_key(&block_number, |e| e.block_number) {
        Ok(i) => Some(&entries[i]),
        Err(i) if i > 0 => Some(&entries[i - 1]),
        _ => None,
    }
}
