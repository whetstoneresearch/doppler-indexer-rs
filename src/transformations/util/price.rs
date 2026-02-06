use alloy::primitives::U256;

use super::constants::{Q192, WAD};

/// Computes price from sqrtPriceX96 (used by V3 and V4 protocols).
/// Returns price with 18 decimals of precision (WAD).
///
/// The price represents: how much quote token per 1 base token.
/// Adjusts for decimal differences between base and quote tokens.
pub fn compute_price_from_sqrt_price_x96(
    sqrt_price_x96: U256,
    is_token0: bool,
    decimals: u8,
    quote_decimals: u8,
) -> U256 {
    let ratio_x192 = sqrt_price_x96 * sqrt_price_x96;

    // 18 + decimals - quote_decimals (can't underflow for realistic token decimals)
    let scaling_exponent = 18u32 + decimals as u32 - quote_decimals as u32;
    let scaling_factor = U256::from(10u64).pow(U256::from(scaling_exponent));

    if is_token0 {
        (ratio_x192 * scaling_factor) / Q192
    } else {
        (Q192 * scaling_factor) / ratio_x192
    }
}

/// Computes price from reserves (used by V2 protocol).
/// Uses the constant product formula: price = quote_balance / asset_balance,
/// normalized to 18 decimals (WAD).
///
/// Panics if `asset_balance` is zero.
pub fn compute_price_from_reserves(
    asset_balance: U256,
    quote_balance: U256,
    asset_decimals: u8,
    quote_decimals: u8,
) -> U256 {
    assert!(!asset_balance.is_zero(), "Asset balance cannot be zero");

    if asset_decimals >= quote_decimals {
        let diff = asset_decimals - quote_decimals;
        let scale = U256::from(10u64).pow(U256::from(diff));
        (quote_balance * WAD * scale) / asset_balance
    } else {
        let diff = quote_decimals - asset_decimals;
        let scale = U256::from(10u64).pow(U256::from(diff));
        (quote_balance * WAD) / (asset_balance * scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqrt_price_token0_equal_decimals() {
        // sqrtPriceX96 = 2^96 means price = 1.0 (i.e. WAD)
        let sqrt_price = U256::from(1u64) << 96;
        let price = compute_price_from_sqrt_price_x96(sqrt_price, true, 18, 18);
        assert_eq!(price, WAD);
    }

    #[test]
    fn test_sqrt_price_token1_equal_decimals() {
        let sqrt_price = U256::from(1u64) << 96;
        let price = compute_price_from_sqrt_price_x96(sqrt_price, false, 18, 18);
        assert_eq!(price, WAD);
    }

    #[test]
    fn test_reserves_equal_decimals() {
        let asset = U256::from(1_000_000_000_000_000_000u64); // 1e18
        let quote = U256::from(2_000_000_000_000_000_000u64); // 2e18
        let price = compute_price_from_reserves(asset, quote, 18, 18);
        assert_eq!(price, U256::from(2u64) * WAD);
    }

    #[test]
    fn test_reserves_different_decimals() {
        // 1 WETH (18 dec) = 2000 USDC (6 dec)
        let asset = U256::from(1_000_000_000_000_000_000u64); // 1e18 WETH
        let quote = U256::from(2_000_000_000u64); // 2000e6 USDC
        let price = compute_price_from_reserves(asset, quote, 18, 6);
        assert_eq!(price, U256::from(2000u64) * WAD);
    }

    #[test]
    #[should_panic(expected = "Asset balance cannot be zero")]
    fn test_reserves_zero_asset_panics() {
        compute_price_from_reserves(U256::ZERO, U256::from(1u64), 18, 18);
    }
}
