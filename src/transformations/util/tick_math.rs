//! Tick math utilities ported from Uniswap v3's TickMath.sol.
//!
//! Provides precise conversion between ticks and sqrtPriceX96 values
//! using the same algorithm as the on-chain contracts.

use alloy_primitives::U256;

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
