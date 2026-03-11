use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, FixedBytes};
use serde::Deserialize;

use crate::types::config::eth_call::EthCallConfig;
use crate::types::config::loader::load_config_from_path;

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct TokenConfig {
    pub address: Address,
    pub pool: Option<PoolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PoolType {
    V2,
    V3,
    V4,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AddressOrPoolId {
    PoolId(FixedBytes<32>),
    Address(Address),
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct PoolConfig {
    #[serde(rename = "type")]
    pub pool_type: PoolType,
    pub address: AddressOrPoolId,
    pub quote_token: String,
    #[serde(default)]
    pub calls: Option<Vec<EthCallConfig>>,
}

pub type Tokens = HashMap<String, TokenConfig>;

/// Load tokens from a path (file or directory).
///
/// Uses the generic config loader with duplicate key detection.
/// Panics on error for backwards compatibility with existing code.
pub fn load_tokens_from_path(base_dir: &Path, path: &str) -> anyhow::Result<Tokens> {
    load_config_from_path::<Tokens>(base_dir, path)
        .map_err(|e| panic!("Failed to load tokens: {}", e))
}
