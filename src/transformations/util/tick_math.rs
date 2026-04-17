//! Tick math utilities ported from Uniswap v3's TickMath.sol and LiquidityAmounts.sol.
//!
//! Provides precise conversion between ticks and sqrtPriceX96 values, and
//! computes token amounts for a position given its tick range, current
//! sqrt price, and liquidity. All algorithms mirror the on-chain contracts.

use alloy_primitives::U256;
use bigdecimal::num_bigint::{BigInt, Sign};

use crate::transformations::error::TransformationError;

/// Minimum tick that can be used on any pool.
pub const MIN_TICK: i32 = -887272;
/// Maximum tick that can be used on any pool.
pub const MAX_TICK: i32 = 887272;

/// Minimum sqrtPriceX96 (at MIN_TICK).
pub const MIN_SQRT_RATIO: U256 = U256::from_limbs([4295128739, 0, 0, 0]);
/// Maximum sqrtPriceX96 (at MAX_TICK).
pub const MAX_SQRT_RATIO: U256 =
    U256::from_limbs([6743328256752651558, 17280870778742802505, 4294805859, 0]);

/// Q128.128 magic constants: sqrt(1.0001^(2^i)) for i in 0..20.
/// Each encodes a factor used in the bit-by-bit tick→sqrtPrice conversion.
const TICK_RATIOS: [U256; 20] = [
    U256::from_limbs([0xaa2d162d1a594001, 0xfffcb933bd6fad37, 0, 0]), // bit 0: sqrt(1.0001^1)
    U256::from_limbs([0x59a46990580e213a, 0xfff97272373d4132, 0, 0]), // bit 1: sqrt(1.0001^2)
    U256::from_limbs([0xef12357cf3c7fdcc, 0xfff2e50f5f656932, 0, 0]), // bit 2: sqrt(1.0001^4)
    U256::from_limbs([0x1c3624eaa0941cd0, 0xffe5caca7e10e4e6, 0, 0]), // bit 3: sqrt(1.0001^8)
    U256::from_limbs([0xc9db58835c926644, 0xffcb9843d60f6159, 0, 0]), // bit 4: sqrt(1.0001^16)
    U256::from_limbs([0x472e6896dfb254c0, 0xff973b41fa98c081, 0, 0]), // bit 5: sqrt(1.0001^32)
    U256::from_limbs([0x43ec78b326b52861, 0xff2ea16466c96a38, 0, 0]), // bit 6: sqrt(1.0001^64)
    U256::from_limbs([0x11c461f1969c3053, 0xfe5dee046a99a2a8, 0, 0]), // bit 7: sqrt(1.0001^128)
    U256::from_limbs([0xdcffc83b479aa3a4, 0xfcbe86c7900a88ae, 0, 0]), // bit 8: sqrt(1.0001^256)
    U256::from_limbs([0x6f2b074cf7815e54, 0xf987a7253ac41317, 0, 0]), // bit 9: sqrt(1.0001^512)
    U256::from_limbs([0x940c7a398e4b70f3, 0xf3392b0822b70005, 0, 0]), // bit 10: sqrt(1.0001^1024)
    U256::from_limbs([0x43b29c7fa6e889d9, 0xe7159475a2c29b74, 0, 0]), // bit 11: sqrt(1.0001^2048)
    U256::from_limbs([0x845ad8f792aa5825, 0xd097f3bdfd2022b8, 0, 0]), // bit 12: sqrt(1.0001^4096)
    U256::from_limbs([0x8a65dc1f90e061e5, 0xa9f746462d870fdf, 0, 0]), // bit 13: sqrt(1.0001^8192)
    U256::from_limbs([0x90bb3df62baf32f7, 0x70d869a156d2a1b8, 0, 0]), // bit 14: sqrt(1.0001^16384)
    U256::from_limbs([0x81231505542fcfa6, 0x31be135f97d08fd9, 0, 0]), // bit 15: sqrt(1.0001^32768)
    U256::from_limbs([0xc677de54f3e99bc9, 0x09aa508b5b7a84e1, 0, 0]), // bit 16: sqrt(1.0001^65536)
    U256::from_limbs([0x6699c329225ee604, 0x005d6af8dedb8119, 0, 0]), // bit 17: sqrt(1.0001^131072)
    U256::from_limbs([0x1ea926041bedfe98, 0x00002216e584f5fa, 0, 0]), // bit 18: sqrt(1.0001^262144)
    U256::from_limbs([0x91f7dc42444e8fa2, 0x00000000048a1703, 0, 0]), // bit 19: sqrt(1.0001^524288)
];

/// Convert a tick to its corresponding sqrtPriceX96 value.
///
/// Ports Uniswap v3's `TickMath.getSqrtRatioAtTick(int24 tick)`.
/// The result is a Q64.96 fixed-point number representing sqrt(1.0001^tick) * 2^96.
///
/// Returns `Err` if tick is outside [MIN_TICK, MAX_TICK].
pub fn tick_to_sqrt_price_x96(tick: i32) -> Result<U256, TransformationError> {
    if !(MIN_TICK..=MAX_TICK).contains(&tick) {
        return Err(TransformationError::TypeConversion(format!(
            "tick {} out of range [{}, {}]",
            tick, MIN_TICK, MAX_TICK
        )));
    }

    let abs_tick = tick.unsigned_abs();

    // Start with the base ratio, matching Solidity's bit-by-bit multiplication.
    let mut ratio: U256 = if abs_tick & 0x1 != 0 {
        TICK_RATIOS[0]
    } else {
        U256::from(1u64) << 128
    };

    // Multiply by each factor whose corresponding bit is set
    for i in 1..20u32 {
        if abs_tick & (1 << i) != 0 {
            ratio = (ratio * TICK_RATIOS[i as usize]) >> 128;
        }
    }

    // For positive ticks, invert the ratio
    if tick > 0 {
        ratio = U256::MAX / ratio;
    }

    // Shift from Q128.128 to Q64.96 and round up
    let remainder = ratio % (U256::from(1u64) << 32);
    let shift = if remainder > U256::ZERO {
        U256::from(1u64)
    } else {
        U256::ZERO
    };
    Ok((ratio >> 32) + shift)
}

// ─── Position amount math (LiquidityAmounts.sol port) ───────────────

fn u256_to_bigint(x: &U256) -> BigInt {
    BigInt::parse_bytes(x.to_string().as_bytes(), 10).expect("U256 stringifies to a valid decimal")
}

fn bigint_to_u256(x: &BigInt) -> Result<U256, TransformationError> {
    if x.sign() == Sign::Minus {
        return Err(TransformationError::TypeConversion(format!(
            "cannot convert negative BigInt to U256: {}",
            x
        )));
    }
    U256::from_str_radix(&x.to_str_radix(10), 10).map_err(|e| {
        TransformationError::TypeConversion(format!("BigInt exceeds U256 range: {}", e))
    })
}

/// Computes the amount of token0 in a position between two sqrtPriceX96 bounds.
///
/// Ports Uniswap v3 `LiquidityAmounts.getAmount0ForLiquidity` (equivalent to
/// `SqrtPriceMath.getAmount0Delta(sqrtA, sqrtB, L, false)`).
///
/// Formula: `L * 2^96 * (sqrtB - sqrtA) / sqrtA / sqrtB`, computed via `BigInt`
/// to avoid intermediate overflow.
///
/// The arguments are reordered internally to satisfy `sqrtA <= sqrtB`.
/// Returns `Err` if either sqrt price is zero.
pub fn get_amount0_delta(
    sqrt_lower_x96: U256,
    sqrt_upper_x96: U256,
    liquidity: u128,
) -> Result<U256, TransformationError> {
    let (a, b) = if sqrt_lower_x96 > sqrt_upper_x96 {
        (sqrt_upper_x96, sqrt_lower_x96)
    } else {
        (sqrt_lower_x96, sqrt_upper_x96)
    };
    if a.is_zero() || b.is_zero() {
        return Err(TransformationError::TypeConversion(
            "sqrtPriceX96 cannot be zero in get_amount0_delta".to_string(),
        ));
    }
    if liquidity == 0 || a == b {
        return Ok(U256::ZERO);
    }

    let a_bi = u256_to_bigint(&a);
    let b_bi = u256_to_bigint(&b);
    let l_bi = BigInt::from(liquidity);
    let q96 = BigInt::from(1u64) << 96;

    let numerator = &l_bi * &q96 * (&b_bi - &a_bi);
    let denominator = &a_bi * &b_bi;
    let result = numerator / denominator; // integer truncation, matches Solidity
    bigint_to_u256(&result)
}

/// Computes the amount of token1 in a position between two sqrtPriceX96 bounds.
///
/// Ports Uniswap v3 `LiquidityAmounts.getAmount1ForLiquidity` (equivalent to
/// `SqrtPriceMath.getAmount1Delta(sqrtA, sqrtB, L, false)`).
///
/// Formula: `L * (sqrtB - sqrtA) / 2^96`, computed via `BigInt`.
///
/// The arguments are reordered internally to satisfy `sqrtA <= sqrtB`.
pub fn get_amount1_delta(
    sqrt_lower_x96: U256,
    sqrt_upper_x96: U256,
    liquidity: u128,
) -> Result<U256, TransformationError> {
    let (a, b) = if sqrt_lower_x96 > sqrt_upper_x96 {
        (sqrt_upper_x96, sqrt_lower_x96)
    } else {
        (sqrt_lower_x96, sqrt_upper_x96)
    };
    if liquidity == 0 || a == b {
        return Ok(U256::ZERO);
    }

    let a_bi = u256_to_bigint(&a);
    let b_bi = u256_to_bigint(&b);
    let l_bi = BigInt::from(liquidity);
    let q96 = BigInt::from(1u64) << 96;

    let result = (&l_bi * (&b_bi - &a_bi)) / &q96;
    bigint_to_u256(&result)
}

/// Computes the `(amount0, amount1)` in raw token units for a position with
/// the given tick range, current sqrt price, and liquidity.
///
/// Dispatches based on where the current price sits relative to the position:
/// - `current <= sqrt_lower`: position is above current → all `token0`
/// - `current >= sqrt_upper`: position is below current → all `token1`
/// - otherwise (straddling): both tokens present
///
/// Returns `Err` if `tick_lower >= tick_upper` or either tick is out of range.
pub fn get_amounts_for_position(
    tick_lower: i32,
    tick_upper: i32,
    current_sqrt_price_x96: U256,
    liquidity: u128,
) -> Result<(U256, U256), TransformationError> {
    if tick_lower >= tick_upper {
        return Err(TransformationError::TypeConversion(format!(
            "tick_lower ({}) must be strictly less than tick_upper ({})",
            tick_lower, tick_upper
        )));
    }
    if liquidity == 0 {
        return Ok((U256::ZERO, U256::ZERO));
    }

    let sqrt_lower = tick_to_sqrt_price_x96(tick_lower)?;
    let sqrt_upper = tick_to_sqrt_price_x96(tick_upper)?;

    if current_sqrt_price_x96 <= sqrt_lower {
        // Current price is at or below the lower bound — position holds only token0.
        let amount0 = get_amount0_delta(sqrt_lower, sqrt_upper, liquidity)?;
        Ok((amount0, U256::ZERO))
    } else if current_sqrt_price_x96 >= sqrt_upper {
        // Current price is at or above the upper bound — position holds only token1.
        let amount1 = get_amount1_delta(sqrt_lower, sqrt_upper, liquidity)?;
        Ok((U256::ZERO, amount1))
    } else {
        // Current price straddles the position — split across both tokens.
        let amount0 = get_amount0_delta(current_sqrt_price_x96, sqrt_upper, liquidity)?;
        let amount1 = get_amount1_delta(sqrt_lower, current_sqrt_price_x96, liquidity)?;
        Ok((amount0, amount1))
    }
}

/// Convert a sqrtPriceX96 value to the greatest tick whose sqrt ratio does not
/// exceed the input.
///
/// This mirrors Uniswap's `getTickAtSqrtRatio` contract behavior closely
/// enough for transformation use cases, while staying simple and exact by
/// binary-searching the monotonic tick→sqrt mapping.
pub fn sqrt_price_x96_to_tick(sqrt_price_x96: &U256) -> Result<i32, TransformationError> {
    if *sqrt_price_x96 < MIN_SQRT_RATIO || *sqrt_price_x96 > MAX_SQRT_RATIO {
        return Err(TransformationError::TypeConversion(format!(
            "sqrtPriceX96 {} out of range [{}, {}]",
            sqrt_price_x96, MIN_SQRT_RATIO, MAX_SQRT_RATIO
        )));
    }

    let mut lo = MIN_TICK;
    let mut hi = MAX_TICK;

    while lo < hi {
        let mid = lo + (hi - lo + 1) / 2;
        let mid_sqrt = tick_to_sqrt_price_x96(mid)?;
        if mid_sqrt <= *sqrt_price_x96 {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }

    Ok(lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_0() {
        // tick 0 = price 1.0, sqrtPriceX96 = 2^96
        let result = tick_to_sqrt_price_x96(0).unwrap();
        let expected = U256::from(1u64) << 96;
        assert_eq!(result, expected);
    }

    #[test]
    fn test_min_tick() {
        let result = tick_to_sqrt_price_x96(MIN_TICK).unwrap();
        assert_eq!(result, MIN_SQRT_RATIO);
    }

    #[test]
    fn test_max_tick() {
        let result = tick_to_sqrt_price_x96(MAX_TICK).unwrap();
        assert_eq!(result, MAX_SQRT_RATIO);
    }

    #[test]
    fn test_tick_1() {
        // Known value from Uniswap: tick 1 = 79232123823359799118286999568
        let result = tick_to_sqrt_price_x96(1).unwrap();
        let expected = U256::from_str_radix("79232123823359799118286999568", 10).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tick_negative_1() {
        // Known Uniswap value: tick -1 = 79224201403219477170569942574
        let result = tick_to_sqrt_price_x96(-1).unwrap();
        let expected = U256::from_str_radix("79224201403219477170569942574", 10).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_positive_negative_symmetry() {
        // sqrt(1.0001^tick) * sqrt(1.0001^(-tick)) ≈ 1
        // So sqrtPrice(tick) * sqrtPrice(-tick) ≈ 2^192
        let pos = tick_to_sqrt_price_x96(100).unwrap();
        let neg = tick_to_sqrt_price_x96(-100).unwrap();
        let product = pos * neg;
        let one_q192 = U256::from(1u64) << 192;
        // Allow small rounding error
        let diff = if product > one_q192 {
            product - one_q192
        } else {
            one_q192 - product
        };
        // Relative error should be tiny
        assert!(diff < one_q192 / U256::from(1_000_000u64));
    }

    #[test]
    fn test_tick_too_low() {
        assert!(tick_to_sqrt_price_x96(MIN_TICK - 1).is_err());
    }

    #[test]
    fn test_tick_too_high() {
        assert!(tick_to_sqrt_price_x96(MAX_TICK + 1).is_err());
    }

    // ─── Position amount math tests ─────────────────────────────────

    #[test]
    fn test_position_zero_liquidity() {
        let (a0, a1) = get_amounts_for_position(-60, 60, U256::from(1u64) << 96, 0).unwrap();
        assert_eq!(a0, U256::ZERO);
        assert_eq!(a1, U256::ZERO);
    }

    #[test]
    fn test_position_invalid_tick_order() {
        let result = get_amounts_for_position(100, 100, U256::from(1u64) << 96, 1_000);
        assert!(result.is_err());
        let result = get_amounts_for_position(200, 100, U256::from(1u64) << 96, 1_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_position_entirely_above_current_is_all_token0() {
        // Current at tick 0 (sqrt = 2^96). Position (60, 120) is strictly above.
        let current = U256::from(1u64) << 96;
        let (a0, a1) =
            get_amounts_for_position(60, 120, current, 1_000_000_000_000_000_000).unwrap();
        assert!(
            a0 > U256::ZERO,
            "amount0 should be non-zero for above-tick position"
        );
        assert_eq!(
            a1,
            U256::ZERO,
            "amount1 must be zero for above-tick position"
        );
    }

    #[test]
    fn test_position_entirely_below_current_is_all_token1() {
        // Current at tick 0. Position (-120, -60) is strictly below.
        let current = U256::from(1u64) << 96;
        let (a0, a1) =
            get_amounts_for_position(-120, -60, current, 1_000_000_000_000_000_000).unwrap();
        assert_eq!(
            a0,
            U256::ZERO,
            "amount0 must be zero for below-tick position"
        );
        assert!(
            a1 > U256::ZERO,
            "amount1 should be non-zero for below-tick position"
        );
    }

    #[test]
    fn test_position_straddling_has_both_tokens() {
        // Current at tick 0, position (-60, 60) straddles it.
        let current = U256::from(1u64) << 96;
        let l = 1_000_000_000_000_000_000u128; // 1e18
        let (a0, a1) = get_amounts_for_position(-60, 60, current, l).unwrap();
        assert!(a0 > U256::ZERO);
        assert!(a1 > U256::ZERO);
        // Symmetric around tick 0 at current=tick 0 ⇒ amount0 and amount1 are within 1% of each other.
        let diff = if a0 > a1 { a0 - a1 } else { a1 - a0 };
        let tolerance = a0 / U256::from(100u64);
        assert!(
            diff <= tolerance,
            "expected a0≈a1 for symmetric straddling position, got a0={} a1={}",
            a0,
            a1
        );
    }

    #[test]
    fn test_position_current_at_tick_lower_is_all_token0() {
        // Current sqrt price exactly equals sqrt_lower ⇒ handled by the "<= lower" branch.
        let sqrt_lower = tick_to_sqrt_price_x96(-60).unwrap();
        let (a0, a1) =
            get_amounts_for_position(-60, 60, sqrt_lower, 1_000_000_000_000_000_000).unwrap();
        assert!(a0 > U256::ZERO);
        assert_eq!(a1, U256::ZERO);
    }

    #[test]
    fn test_position_current_at_tick_upper_is_all_token1() {
        let sqrt_upper = tick_to_sqrt_price_x96(60).unwrap();
        let (a0, a1) =
            get_amounts_for_position(-60, 60, sqrt_upper, 1_000_000_000_000_000_000).unwrap();
        assert_eq!(a0, U256::ZERO);
        assert!(a1 > U256::ZERO);
    }

    #[test]
    fn test_amount0_delta_symmetric_bounds_gives_zero() {
        // sqrt_lower == sqrt_upper → no width, zero amount
        let q96 = U256::from(1u64) << 96;
        let amount = get_amount0_delta(q96, q96, 1_000_000).unwrap();
        assert_eq!(amount, U256::ZERO);
    }

    #[test]
    fn test_amount1_delta_symmetric_bounds_gives_zero() {
        let q96 = U256::from(1u64) << 96;
        let amount = get_amount1_delta(q96, q96, 1_000_000).unwrap();
        assert_eq!(amount, U256::ZERO);
    }

    #[test]
    fn test_amount_delta_argument_order_independent() {
        // get_amount0_delta should sort its inputs, so swapping args gives same result.
        let sqrt_a = tick_to_sqrt_price_x96(-60).unwrap();
        let sqrt_b = tick_to_sqrt_price_x96(60).unwrap();
        let l = 1_000_000_000_000u128;
        let a_forward = get_amount0_delta(sqrt_a, sqrt_b, l).unwrap();
        let a_reverse = get_amount0_delta(sqrt_b, sqrt_a, l).unwrap();
        assert_eq!(a_forward, a_reverse);

        let b_forward = get_amount1_delta(sqrt_a, sqrt_b, l).unwrap();
        let b_reverse = get_amount1_delta(sqrt_b, sqrt_a, l).unwrap();
        assert_eq!(b_forward, b_reverse);
    }

    #[test]
    fn test_amount0_delta_zero_sqrt_price_errors() {
        let q96 = U256::from(1u64) << 96;
        assert!(get_amount0_delta(U256::ZERO, q96, 1_000).is_err());
        assert!(get_amount0_delta(q96, U256::ZERO, 1_000).is_err());
    }

    // ─── sqrt_price_x96_to_tick tests ─────────────────────────────────

    #[test]
    fn test_sqrt_price_to_tick_zero() {
        let sqrt = tick_to_sqrt_price_x96(0).unwrap();
        assert_eq!(sqrt_price_x96_to_tick(&sqrt).unwrap(), 0);
    }

    #[test]
    fn test_sqrt_price_to_tick_positive() {
        let sqrt = tick_to_sqrt_price_x96(12345).unwrap();
        assert_eq!(sqrt_price_x96_to_tick(&sqrt).unwrap(), 12345);
    }

    #[test]
    fn test_sqrt_price_to_tick_negative() {
        let sqrt = tick_to_sqrt_price_x96(-54321).unwrap();
        assert_eq!(sqrt_price_x96_to_tick(&sqrt).unwrap(), -54321);
    }

    #[test]
    fn test_sqrt_price_to_tick_out_of_range() {
        assert!(sqrt_price_x96_to_tick(&U256::ZERO).is_err());
    }
}
