//! Tick math utilities ported from Uniswap v3's TickMath.sol.
//!
//! Provides precise conversion between ticks and sqrtPriceX96 values
//! using the same algorithm as the on-chain contracts.

use alloy_primitives::U256;

/// Minimum tick that can be used on any pool.
pub const MIN_TICK: i32 = -887272;
/// Maximum tick that can be used on any pool.
pub const MAX_TICK: i32 = 887272;

/// Minimum sqrtPriceX96 (at MIN_TICK).
pub const MIN_SQRT_RATIO: U256 = U256::from_limbs([4295128739, 0, 0, 0]);
/// Maximum sqrtPriceX96 (at MAX_TICK).
pub const MAX_SQRT_RATIO: U256 =
    U256::from_limbs([6743328256752651558, 17280870778742802505, 4294805859, 0]);

/// Convert a tick to its corresponding sqrtPriceX96 value.
///
/// Ports Uniswap v3's `TickMath.getSqrtRatioAtTick(int24 tick)`.
/// The result is a Q64.96 fixed-point number representing sqrt(1.0001^tick) * 2^96.
///
/// # Panics
/// Panics if tick is outside [MIN_TICK, MAX_TICK].
pub fn tick_to_sqrt_price_x96(tick: i32) -> U256 {
    assert!(
        (MIN_TICK..=MAX_TICK).contains(&tick),
        "tick {} out of range [{}, {}]",
        tick,
        MIN_TICK,
        MAX_TICK
    );

    let abs_tick = tick.unsigned_abs();

    // Start with the base ratio, matching Solidity's bit-by-bit multiplication.
    // Each magic number corresponds to sqrt(1.0001^(2^i)) in Q128.128 format.
    let mut ratio: U256 = if abs_tick & 0x1 != 0 {
        U256::from_str_radix("fffcb933bd6fad37aa2d162d1a594001", 16).unwrap()
    } else {
        U256::from(1u64) << 128
    };

    // Bit 1: sqrt(1.0001^2)
    if abs_tick & 0x2 != 0 {
        ratio = (ratio * U256::from_str_radix("fff97272373d413259a46990580e213a", 16).unwrap())
            >> 128;
    }
    // Bit 2: sqrt(1.0001^4)
    if abs_tick & 0x4 != 0 {
        ratio = (ratio * U256::from_str_radix("fff2e50f5f656932ef12357cf3c7fdcc", 16).unwrap())
            >> 128;
    }
    // Bit 3: sqrt(1.0001^8)
    if abs_tick & 0x8 != 0 {
        ratio = (ratio * U256::from_str_radix("ffe5caca7e10e4e61c3624eaa0941cd0", 16).unwrap())
            >> 128;
    }
    // Bit 4: sqrt(1.0001^16)
    if abs_tick & 0x10 != 0 {
        ratio = (ratio * U256::from_str_radix("ffcb9843d60f6159c9db58835c926644", 16).unwrap())
            >> 128;
    }
    // Bit 5: sqrt(1.0001^32)
    if abs_tick & 0x20 != 0 {
        ratio = (ratio * U256::from_str_radix("ff973b41fa98c081472e6896dfb254c0", 16).unwrap())
            >> 128;
    }
    // Bit 6: sqrt(1.0001^64)
    if abs_tick & 0x40 != 0 {
        ratio = (ratio * U256::from_str_radix("ff2ea16466c96a3843ec78b326b52861", 16).unwrap())
            >> 128;
    }
    // Bit 7: sqrt(1.0001^128)
    if abs_tick & 0x80 != 0 {
        ratio = (ratio * U256::from_str_radix("fe5dee046a99a2a811c461f1969c3053", 16).unwrap())
            >> 128;
    }
    // Bit 8: sqrt(1.0001^256)
    if abs_tick & 0x100 != 0 {
        ratio = (ratio * U256::from_str_radix("fcbe86c7900a88aedcffc83b479aa3a4", 16).unwrap())
            >> 128;
    }
    // Bit 9: sqrt(1.0001^512)
    if abs_tick & 0x200 != 0 {
        ratio = (ratio * U256::from_str_radix("f987a7253ac413176f2b074cf7815e54", 16).unwrap())
            >> 128;
    }
    // Bit 10: sqrt(1.0001^1024)
    if abs_tick & 0x400 != 0 {
        ratio = (ratio * U256::from_str_radix("f3392b0822b70005940c7a398e4b70f3", 16).unwrap())
            >> 128;
    }
    // Bit 11: sqrt(1.0001^2048)
    if abs_tick & 0x800 != 0 {
        ratio = (ratio * U256::from_str_radix("e7159475a2c29b7443b29c7fa6e889d9", 16).unwrap())
            >> 128;
    }
    // Bit 12: sqrt(1.0001^4096)
    if abs_tick & 0x1000 != 0 {
        ratio = (ratio * U256::from_str_radix("d097f3bdfd2022b8845ad8f792aa5825", 16).unwrap())
            >> 128;
    }
    // Bit 13: sqrt(1.0001^8192)
    if abs_tick & 0x2000 != 0 {
        ratio = (ratio * U256::from_str_radix("a9f746462d870fdf8a65dc1f90e061e5", 16).unwrap())
            >> 128;
    }
    // Bit 14: sqrt(1.0001^16384)
    if abs_tick & 0x4000 != 0 {
        ratio = (ratio * U256::from_str_radix("70d869a156d2a1b890bb3df62baf32f7", 16).unwrap())
            >> 128;
    }
    // Bit 15: sqrt(1.0001^32768)
    if abs_tick & 0x8000 != 0 {
        ratio = (ratio * U256::from_str_radix("31be135f97d08fd981231505542fcfa6", 16).unwrap())
            >> 128;
    }
    // Bit 16: sqrt(1.0001^65536)
    if abs_tick & 0x10000 != 0 {
        ratio = (ratio * U256::from_str_radix("9aa508b5b7a84e1c677de54f3e99bc9", 16).unwrap())
            >> 128;
    }
    // Bit 17: sqrt(1.0001^131072)
    if abs_tick & 0x20000 != 0 {
        ratio = (ratio
            * U256::from_str_radix("5d6af8dedb81196699c329225ee604", 16).unwrap())
            >> 128;
    }
    // Bit 18: sqrt(1.0001^262144)
    if abs_tick & 0x40000 != 0 {
        ratio =
            (ratio * U256::from_str_radix("2216e584f5fa1ea926041bedfe98", 16).unwrap()) >> 128;
    }
    // Bit 19: sqrt(1.0001^524288)
    if abs_tick & 0x80000 != 0 {
        ratio = (ratio * U256::from_str_radix("48a170391f7dc42444e8fa2", 16).unwrap()) >> 128;
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
    (ratio >> 32) + shift
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_0() {
        // tick 0 = price 1.0, sqrtPriceX96 = 2^96
        let result = tick_to_sqrt_price_x96(0);
        let expected = U256::from(1u64) << 96;
        assert_eq!(result, expected);
    }

    #[test]
    fn test_min_tick() {
        let result = tick_to_sqrt_price_x96(MIN_TICK);
        assert_eq!(result, MIN_SQRT_RATIO);
    }

    #[test]
    fn test_max_tick() {
        let result = tick_to_sqrt_price_x96(MAX_TICK);
        assert_eq!(result, MAX_SQRT_RATIO);
    }

    #[test]
    fn test_tick_1() {
        // Known value from Uniswap: tick 1 = 79232123823359799118286999568
        let result = tick_to_sqrt_price_x96(1);
        let expected =
            U256::from_str_radix("79232123823359799118286999568", 10).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_tick_negative_1() {
        // Known Uniswap value: tick -1 = 79224201403219477170569942574
        let result = tick_to_sqrt_price_x96(-1);
        let expected =
            U256::from_str_radix("79224201403219477170569942574", 10).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_positive_negative_symmetry() {
        // sqrt(1.0001^tick) * sqrt(1.0001^(-tick)) ≈ 1
        // So sqrtPrice(tick) * sqrtPrice(-tick) ≈ 2^192
        let pos = tick_to_sqrt_price_x96(100);
        let neg = tick_to_sqrt_price_x96(-100);
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
    #[should_panic(expected = "tick")]
    fn test_tick_too_low() {
        tick_to_sqrt_price_x96(MIN_TICK - 1);
    }

    #[test]
    #[should_panic(expected = "tick")]
    fn test_tick_too_high() {
        tick_to_sqrt_price_x96(MAX_TICK + 1);
    }
}
