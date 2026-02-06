use alloy::primitives::U256;

use super::constants::WAD;

/// Calculate market capitalization in USD.
///
/// Formula: (price * total_supply) / 10^asset_decimals * quote_price_usd / 10^decimals
///
/// - `price`: asset price with 18 decimals (WAD)
/// - `total_supply`: total supply with `asset_decimals` precision
/// - `quote_price_usd`: USD price of the quote currency with `decimals` precision (e.g. 8 for Chainlink)
/// - `asset_decimals`: decimals of the asset token (default 18)
/// - `decimals`: decimals of the quote price feed (default 8)
///
/// Returns a value with 18 decimals of precision.
pub fn calculate_market_cap(
    price: U256,
    total_supply: U256,
    quote_price_usd: U256,
    asset_decimals: Option<u8>,
    decimals: Option<u8>,
) -> U256 {
    let asset_decimals = asset_decimals.unwrap_or(18);
    let decimals = decimals.unwrap_or(8);

    let asset_decimal_factor = U256::from(10u64).pow(U256::from(asset_decimals));
    let price_decimal_factor = U256::from(10u64).pow(U256::from(decimals));

    let market_cap_in_quote = (price * total_supply) / asset_decimal_factor;
    (market_cap_in_quote * quote_price_usd) / price_decimal_factor
}

/// Calculate total liquidity in USD.
///
/// Computes asset value + quote value, both converted to USD.
///
/// - `asset_balance`: balance of the asset token
/// - `quote_balance`: balance of the quote token
/// - `price`: asset price in quote currency with 18 decimals (WAD)
/// - `quote_price_usd`: USD price of the quote currency with `decimals` precision
/// - `is_quote_usd`: if true, quote is already denominated in USD
/// - `decimals`: decimals of the quote price feed (default 8)
/// - `asset_decimals`: decimals of the asset token (default 18)
/// - `quote_decimals`: decimals of the quote token (default 18)
pub fn calculate_liquidity(
    asset_balance: U256,
    quote_balance: U256,
    price: U256,
    quote_price_usd: U256,
    is_quote_usd: bool,
    decimals: Option<u8>,
    asset_decimals: Option<u8>,
    quote_decimals: Option<u8>,
) -> U256 {
    let decimals = decimals.unwrap_or(8);
    let asset_decimals = asset_decimals.unwrap_or(18);
    let quote_decimals = quote_decimals.unwrap_or(18);

    let asset_value_in_quote = (asset_balance * price) / WAD;

    if !is_quote_usd {
        let price_factor = U256::from(10u64).pow(U256::from(18 - decimals));
        let quote_decimal_factor = U256::from(10u64).pow(U256::from(18 - quote_decimals));
        let asset_decimal_factor = U256::from(10u64).pow(U256::from(18 - asset_decimals));

        let asset_value_usd =
            (asset_value_in_quote * quote_price_usd * price_factor * asset_decimal_factor) / WAD;
        let quote_value_usd =
            (quote_balance * quote_price_usd * price_factor * quote_decimal_factor) / WAD;
        return asset_value_usd + quote_value_usd;
    }

    asset_value_in_quote + quote_balance
}

/// Calculate swap volume in USD.
///
/// Uses the larger of `amount_in`/`amount_out` as the volume indicator.
///
/// - `amount_in`: input amount of the swap
/// - `amount_out`: output amount of the swap
/// - `quote_price_usd`: USD price of the quote currency with `decimals` precision
/// - `is_quote_usd`: if true, quote is already denominated in USD
/// - `quote_decimals`: decimals of the quote token (default 18)
/// - `decimals`: decimals of the quote price feed (default 8)
pub fn calculate_volume(
    amount_in: U256,
    amount_out: U256,
    quote_price_usd: U256,
    is_quote_usd: bool,
    quote_decimals: Option<u8>,
    decimals: Option<u8>,
) -> U256 {
    if amount_in.is_zero() && amount_out.is_zero() {
        return U256::ZERO;
    }

    let decimals = decimals.unwrap_or(8);
    let quote_decimals = quote_decimals.unwrap_or(18);

    let swap_amount = if !amount_in.is_zero() {
        amount_in
    } else {
        amount_out
    };

    if !is_quote_usd {
        let scale_factor = U256::from(10u64).pow(U256::from(18 - decimals));
        let quote_decimal_factor = U256::from(10u64).pow(U256::from(quote_decimals));
        return (swap_amount * quote_price_usd * scale_factor) / quote_decimal_factor;
    }

    swap_amount
}
