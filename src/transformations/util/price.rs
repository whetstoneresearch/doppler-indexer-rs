/// Convert sqrtPriceX96 to a decimal-adjusted price.
///
/// sqrtPriceX96 encodes: sqrt(price_of_token0_in_token1) * 2^96
///
/// - is_token0 = true:  price = (sqrtPriceX96 / 2^96)^2 * 10^(token_dec - quote_dec)
/// - is_token0 = false: price = (2^96 / sqrtPriceX96)^2 * 10^(token_dec - quote_dec)
pub fn sqrt_price_x96_to_price(
    sqrt_price_x96: &alloy_primitives::U256,
    token_decimals: u8,
    quote_decimals: u8,
    is_token0: bool,
) -> f64 {
    let sqrt_price_f64: f64 = sqrt_price_x96.to_string().parse().unwrap_or(0.0);
    let two_96: f64 = 2.0_f64.powi(96);

    let price_raw = if is_token0 {
        let ratio = sqrt_price_f64 / two_96;
        ratio * ratio
    } else {
        let ratio = two_96 / sqrt_price_f64;
        ratio * ratio
    };

    let decimal_adjustment = 10.0_f64.powi(token_decimals as i32 - quote_decimals as i32);
    price_raw * decimal_adjustment
}
