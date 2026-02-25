use alloy_primitives::{Address, U256};

use crate::types::uniswap::v4::PoolAddressOrPoolId;

pub struct TokenMetadata {
    pub(crate) name: String,
    pub(crate) symbol: String,
    pub(crate) decimals: u8,
}

pub struct AssetTokenMetadata {
    pub(crate) name: String,
    pub(crate) symbol: String,
    pub(crate) decimals: u8,
    pub(crate) token_uri: String,
    pub(crate) total_supply: U256,
    pub(crate) governance: Address,
    pub(crate) integrator: Address,
    pub(crate) initializer: Address,
    pub(crate) migrator: Address,
    pub(crate) migration_pool: PoolAddressOrPoolId,
}