use alloy::primitives::U256;

pub const SECONDS_IN_15_MINUTES: u64 = 900;
pub const SECONDS_IN_30_MINUTES: u64 = 1800;
pub const SECONDS_IN_HOUR: u64 = 3600;
pub const SECONDS_IN_DAY: u64 = 86400;

/// 2^192
pub const Q192: U256 = U256::from_limbs([0, 0, 0, 1]);

/// 10^18
pub const WAD: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// 10^8 (Chainlink ETH price feed decimals)
pub const CHAINLINK_ETH_DECIMALS: U256 = U256::from_limbs([100_000_000, 0, 0, 0]);
