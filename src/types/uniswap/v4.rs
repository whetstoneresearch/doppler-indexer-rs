use alloy::primitives::{keccak256, Address, B256, U256};
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

pub enum PoolAddressOrPoolId {
    Address([u8; 20]),
    PoolId([u8; 32]),
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
            tickSpacing: self
                .tick_spacing
                .try_into()
                .expect("tick_spacing exceeds int24"),
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
    pub num_pd_slugs: U256,
}
