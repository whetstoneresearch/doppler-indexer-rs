use alloy_primitives::{Address, U256};

use crate::types::v4::PoolAddressOrPoolId;

struct TokenMetadata {
    name: String,
    symbol: String,
    decimals: u8,
}

struct AssetTokenMetadata {
    name: String,
    symbol: String,
    decimals: u8,
    token_uri: String,
    total_supply: U256,
    governance: Address,
    integrator: Address,
    initializer: Address,
    migrator: Address,
    migration_pool: PoolAddressOrPoolId,
}