use alloy::primitives::{keccak256, Address, Bytes, B256, I256, U256};
use alloy::sol;
use alloy::sol_types::SolValue;

sol! {
    #[derive(Debug, PartialEq, Eq)]
    struct SolPoolKey {
        address currency0;
        address currency1;
        uint24 fee;
        int24 tickSpacing;
        address hooks;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PoolKey {
    pub currency0: Address,
    pub currency1: Address,
    pub fee: u32,
    pub tick_spacing: i32,
    pub hooks: Address,
}

impl PoolKey {
    pub fn pool_id(&self) -> B256 {
        let sol_key = SolPoolKey {
            currency0: self.currency0,
            currency1: self.currency1,
            fee: self.fee.try_into().expect("fee exceeds uint24"),
            tickSpacing: self.tick_spacing.try_into().expect("tick_spacing exceeds int24"),
            hooks: self.hooks,
        };
        keccak256(sol_key.abi_encode())
    }
}

#[derive(Debug, Clone)]
pub struct V4PoolConfig {
    pub num_tokens_to_sell: U256,
    pub min_proceeds: U256,
    pub max_proceeds: U256,
    pub starting_time: u64,
    pub ending_time: u64,
    pub starting_tick: i32,
    pub ending_tick: i32,
    pub epoch_length: U256,
    pub gamma: u32,
    pub is_token_0: bool,
    pub num_pds_slugs: U256,
}

#[derive(Debug, Clone)]
pub struct PositionData {
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub liquidity: U256,
    pub salt: u32,
}

#[derive(Debug, Clone)]
pub struct Slot0Data {
    pub sqrt_price: U256,
    pub tick: i32,
    pub protocol_fee: u32,
    pub lp_fee: u32,
}

#[derive(Debug, Clone)]
pub struct V4PoolData {
    pub pool_key: PoolKey,
    pub slot0_data: Slot0Data,
    pub liquidity: U256,
    pub price: U256,
    pub pool_config: V4PoolConfig,
}

#[derive(Debug, Clone)]
pub struct QuoteExactSingleParams {
    pub pool_key: PoolKey,
    pub zero_for_one: bool,
    pub exact_amount: I256,
    pub hook_data: Bytes,
}

#[derive(Debug, Clone)]
pub struct DHookPoolConfig {
    pub numeraire: Address,
    pub total_tokens_on_bonding_curve: U256,
    pub doppler_hook: Address,
    pub status: u8,
    pub far_tick: i32,
    pub is_token0: bool,
}

#[derive(Debug, Clone)]
pub struct DHookPoolData {
    pub pool_key: PoolKey,
    pub slot0_data: Slot0Data,
    pub liquidity: U256,
    pub price: U256,
    pub pool_config: DHookPoolConfig,
}
