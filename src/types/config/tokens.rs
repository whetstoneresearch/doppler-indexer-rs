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
pub fn load_tokens_from_path(base_dir: &Path, path: &str) -> anyhow::Result<Tokens> {
    load_config_from_path::<Tokens>(base_dir, path)
        .map_err(|e| anyhow::anyhow!("Failed to load tokens: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_tokens_nonexistent_path_returns_err() {
        let dir = TempDir::new().unwrap();
        let result = load_tokens_from_path(dir.path(), "nonexistent.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_tokens_invalid_json_returns_err() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("tokens.json"), "not valid json").unwrap();
        let result = load_tokens_from_path(dir.path(), "tokens.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_tokens_valid_returns_ok() {
        let dir = TempDir::new().unwrap();
        let json = r#"{
            "WETH": {
                "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            }
        }"#;
        fs::write(dir.path().join("tokens.json"), json).unwrap();
        let result = load_tokens_from_path(dir.path(), "tokens.json");
        assert!(result.is_ok());
        let tokens = result.unwrap();
        assert!(tokens.contains_key("WETH"));
    }
}
