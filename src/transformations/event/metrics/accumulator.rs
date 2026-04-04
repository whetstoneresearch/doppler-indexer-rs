//! Block-level swap accumulator for OHLC and volume aggregation.
//!
//! Each pool gets one BlockAccumulator per block. Multiple swap events
//! in the same block are aggregated into a single snapshot row.

use alloy_primitives::{I256, U256};
use bigdecimal::BigDecimal;

/// Accumulates swap events within a single block for one pool.
#[derive(Debug)]
pub struct BlockAccumulator {
    pub price_open: Option<BigDecimal>,
    pub price_close: Option<BigDecimal>,
    pub price_high: Option<BigDecimal>,
    pub price_low: Option<BigDecimal>,
    /// Raw absolute volume of token0 (sum of |amount0| across swaps).
    pub volume0: U256,
    /// Raw absolute volume of token1 (sum of |amount1| across swaps).
    pub volume1: U256,
    pub swap_count: u32,
    pub last_tick: i32,
    pub last_sqrt_price_x96: U256,
    pub last_liquidity: U256,
    pub block_timestamp: u64,
}

impl BlockAccumulator {
    pub fn new(block_timestamp: u64) -> Self {
        Self {
            price_open: None,
            price_close: None,
            price_high: None,
            price_low: None,
            volume0: U256::ZERO,
            volume1: U256::ZERO,
            swap_count: 0,
            last_tick: 0,
            last_sqrt_price_x96: U256::ZERO,
            last_liquidity: U256::ZERO,
            block_timestamp,
        }
    }

    /// Record a swap event into this accumulator.
    pub fn record_swap(
        &mut self,
        price: BigDecimal,
        amount0: I256,
        amount1: I256,
        tick: i32,
        sqrt_price_x96: U256,
        liquidity: U256,
    ) {
        // OHLC
        if self.price_open.is_none() {
            self.price_open = Some(price.clone());
        }
        self.price_close = Some(price.clone());
        self.price_high = Some(match self.price_high.take() {
            Some(h) => h.max(price.clone()),
            None => price.clone(),
        });
        self.price_low = Some(match self.price_low.take() {
            Some(l) => l.min(price),
            None => price,
        });

        // Volume (absolute values)
        self.volume0 += amount0.unsigned_abs();
        self.volume1 += amount1.unsigned_abs();
        self.swap_count += 1;

        // Latest state
        self.last_tick = tick;
        self.last_sqrt_price_x96 = sqrt_price_x96;
        self.last_liquidity = liquidity;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn test_single_swap() {
        let mut acc = BlockAccumulator::new(1000);
        acc.record_swap(
            bd("1.5"),
            I256::try_from(100i64).unwrap(),
            I256::try_from(-150i64).unwrap(),
            100,
            U256::from(1u64) << 96,
            U256::from(1000u64),
        );
        assert_eq!(acc.price_open, Some(bd("1.5")));
        assert_eq!(acc.price_close, Some(bd("1.5")));
        assert_eq!(acc.price_high, Some(bd("1.5")));
        assert_eq!(acc.price_low, Some(bd("1.5")));
        assert_eq!(acc.volume0, U256::from(100u64));
        assert_eq!(acc.volume1, U256::from(150u64));
        assert_eq!(acc.swap_count, 1);
    }

    #[test]
    fn test_multiple_swaps_ohlc() {
        let mut acc = BlockAccumulator::new(1000);
        acc.record_swap(
            bd("2.0"),
            I256::try_from(10i64).unwrap(),
            I256::try_from(-20i64).unwrap(),
            100,
            U256::from(1u64),
            U256::from(1u64),
        );
        acc.record_swap(
            bd("3.0"),
            I256::try_from(5i64).unwrap(),
            I256::try_from(-15i64).unwrap(),
            200,
            U256::from(2u64),
            U256::from(2u64),
        );
        acc.record_swap(
            bd("1.0"),
            I256::try_from(20i64).unwrap(),
            I256::try_from(-20i64).unwrap(),
            50,
            U256::from(3u64),
            U256::from(3u64),
        );

        assert_eq!(acc.price_open, Some(bd("2.0")));
        assert_eq!(acc.price_close, Some(bd("1.0")));
        assert_eq!(acc.price_high, Some(bd("3.0")));
        assert_eq!(acc.price_low, Some(bd("1.0")));
        assert_eq!(acc.volume0, U256::from(35u64));
        assert_eq!(acc.volume1, U256::from(55u64));
        assert_eq!(acc.swap_count, 3);
        assert_eq!(acc.last_tick, 50);
    }
}
