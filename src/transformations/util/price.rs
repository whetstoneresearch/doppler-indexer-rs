//! Arbitrary-precision price conversion from sqrtPriceX96.
//!
//! sqrtPriceX96 is a Q64.96 fixed-point integer encoding sqrt(token1/token0) * 2^96.
//! Converting through f64 loses ~14 digits of precision; this module uses `bigdecimal`
//! to preserve full precision before reducing to price space.

use std::sync::OnceLock;

use alloy_primitives::U256;
use bigdecimal::{BigDecimal, Zero};

/// 2^96 as a decimal string (exact value: 79228162514264337593543950336).
static Q96: OnceLock<BigDecimal> = OnceLock::new();

fn q96() -> &'static BigDecimal {
    Q96.get_or_init(|| {
        "79228162514264337593543950336"
            .parse()
            .expect("Q96 is a valid decimal")
    })
}

/// Convert sqrtPriceX96 to a decimal-adjusted price, with arbitrary precision.
///
/// Returns `None` when:
/// - `sqrt_price_x96` is zero (invalid pool state)
/// - the result underflows to zero after the decimal adjustment (extreme tick)
///
/// # Formula
/// ```text
/// is_token0 = true:  price = (sqrtPriceX96 / 2^96)^2 * 10^(token_dec - quote_dec)
/// is_token0 = false: price = (2^96 / sqrtPriceX96)^2 * 10^(token_dec - quote_dec)
/// ```
///
/// The result is suitable for direct insertion into a Postgres NUMERIC column via
/// `BigDecimal::to_string()`, which always produces plain decimal notation.
pub fn sqrt_price_x96_to_price(
    sqrt_price_x96: &U256,
    token_decimals: u8,
    quote_decimals: u8,
    is_token0: bool,
) -> Option<BigDecimal> {
    if sqrt_price_x96.is_zero() {
        return None;
    }

    // Parse exact integer value — valid U256 strings always succeed.
    let sqrt_bd: BigDecimal = sqrt_price_x96
        .to_string()
        .parse()
        .expect("U256 to_string produces a valid decimal");

    let q96 = q96();

    let ratio = if is_token0 {
        &sqrt_bd / q96
    } else {
        q96 / &sqrt_bd
    };
    let price_raw = &ratio * &ratio;

    // Apply decimal adjustment: 10^(token_decimals - quote_decimals).
    let exp = token_decimals as i32 - quote_decimals as i32;
    let price = apply_decimal_exp(price_raw, exp);

    if price.is_zero() {
        None
    } else {
        Some(price)
    }
}

/// Multiply a BigDecimal by 10^exp (positive or negative).
fn apply_decimal_exp(value: BigDecimal, exp: i32) -> BigDecimal {
    if exp == 0 {
        return value;
    }
    // 10^|exp| — max |exp| is 255 (u8 difference), so 10^255 is fine for BigDecimal.
    let abs_exp = exp.unsigned_abs() as u64;
    let factor: BigDecimal = {
        let mut f = BigDecimal::from(1u64);
        let ten = BigDecimal::from(10u64);
        for _ in 0..abs_exp {
            f = &f * &ten;
        }
        f
    };
    if exp > 0 {
        value * factor
    } else {
        value / factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformations::util::tick_math::{MAX_SQRT_RATIO, MIN_SQRT_RATIO};

    fn bd(s: &str) -> BigDecimal {
        s.parse().unwrap()
    }

    #[test]
    fn test_zero_sqrt_price_returns_none() {
        assert!(sqrt_price_x96_to_price(&U256::ZERO, 18, 18, true).is_none());
        assert!(sqrt_price_x96_to_price(&U256::ZERO, 18, 6, false).is_none());
    }

    #[test]
    fn test_tick_zero_price_is_one() {
        // sqrtPriceX96 = 2^96 → price = (2^96 / 2^96)^2 * 10^0 = 1
        let q96 = U256::from_str_radix("79228162514264337593543950336", 10).unwrap();
        let price = sqrt_price_x96_to_price(&q96, 18, 18, true).unwrap();
        // Should be very close to 1 (exact integer division gives exactly 1)
        assert_eq!(price, bd("1"));
    }

    #[test]
    fn test_is_token0_false_symmetric() {
        // At tick 0, both orientations give price = 1
        let q96 = U256::from_str_radix("79228162514264337593543950336", 10).unwrap();
        let p_true = sqrt_price_x96_to_price(&q96, 18, 18, true).unwrap();
        let p_false = sqrt_price_x96_to_price(&q96, 18, 18, false).unwrap();
        assert_eq!(p_true, bd("1"));
        assert_eq!(p_false, bd("1"));
    }

    #[test]
    fn test_decimal_shift_usdc_quote() {
        // With 18-decimal base and 6-decimal quote, the price at tick 0 should
        // be 1 * 10^(18-6) = 1_000_000_000_000
        let q96 = U256::from_str_radix("79228162514264337593543950336", 10).unwrap();
        let price = sqrt_price_x96_to_price(&q96, 18, 6, true).unwrap();
        assert_eq!(price, bd("1000000000000"));
    }

    #[test]
    fn test_max_sqrt_price_no_overflow() {
        // MAX_SQRT_RATIO must not panic or produce None (it's a valid non-zero value)
        let price = sqrt_price_x96_to_price(&MAX_SQRT_RATIO, 18, 18, true);
        assert!(price.is_some());
    }

    #[test]
    fn test_min_sqrt_price_is_token0_false() {
        // MIN_SQRT_RATIO with is_token0=false gives the maximum possible price.
        // Should be Some (non-zero).
        let price = sqrt_price_x96_to_price(&MIN_SQRT_RATIO, 18, 18, false);
        assert!(price.is_some());
    }

    #[test]
    fn test_precision_better_than_f64() {
        // Use a sqrtPriceX96 value that would round to a different result in f64.
        // sqrtPriceX96 = 2^96 + 1 → price should be slightly above 1, not exactly 1.
        let sqrt = U256::from_str_radix("79228162514264337593543950337", 10).unwrap(); // 2^96 + 1
        let price = sqrt_price_x96_to_price(&sqrt, 18, 18, true).unwrap();
        // BigDecimal: price > 1.0 because (2^96+1)^2 / (2^96)^2 > 1
        assert!(price > bd("1"));
    }
}
