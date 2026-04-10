use std::path::Path;

use alloy_primitives::U256;
use serde::{Deserialize, Serialize};

use crate::types::config::contract::{
    load_contracts_from_path, load_factory_collections_from_path, Contracts, FactoryCollections,
};
use crate::types::config::generic::InlineOrPath;

/// RPC method to use for fetching block receipts.
///
/// Different chains and RPC providers support different methods.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(clippy::enum_variant_names)]
pub enum BlockReceiptsMethod {
    #[serde(rename = "eth_getBlockReceipts")]
    EthGetBlockReceipts,
    #[serde(rename = "alchemy_getTransactionReceipts")]
    AlchemyGetTransactionReceipts,
    #[serde(rename = "parity_getBlockReceipts")]
    ParityGetBlockReceipts,
}

impl BlockReceiptsMethod {
    pub fn as_str(&self) -> &str {
        match self {
            Self::EthGetBlockReceipts => "eth_getBlockReceipts",
            Self::AlchemyGetTransactionReceipts => "alchemy_getTransactionReceipts",
            Self::ParityGetBlockReceipts => "parity_getBlockReceipts",
        }
    }
}

/// Configuration for a shared RPC rate limit group.
///
/// Multiple chains can reference the same group to share a single rate limit budget.
#[derive(Debug, Clone, Deserialize)]
pub struct RpcRateLimitGroup {
    pub units_per_second: u32,
}

/// RPC client configuration for a chain.
///
/// All fields are optional and fall back to defaults in `defaults::rpc`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RpcConfig {
    /// Max concurrent in-flight RPC requests (default: 100)
    pub concurrency: Option<usize>,
    /// Generic rate limit units per second.
    /// For standard providers this is requests/sec; for Alchemy-like providers it is CU/sec.
    pub requests_per_second: Option<u32>,
    /// Legacy alias for `requests_per_second`, kept for backward compatibility.
    pub compute_units_per_second: Option<u32>,
    /// Max batch size for RPC requests (default: 100)
    pub batch_size: Option<u32>,
    /// Name of a shared rate limit group (defined in top-level rpc_rate_limits).
    /// Mutually exclusive with `requests_per_second` and `compute_units_per_second`.
    #[serde(default)]
    pub rate_limit_group: Option<String>,
}

impl RpcConfig {
    pub fn units_per_second(&self) -> Option<u32> {
        self.requests_per_second.or(self.compute_units_per_second)
    }

    pub fn has_explicit_rate_limit(&self) -> bool {
        self.requests_per_second.is_some() || self.compute_units_per_second.is_some()
    }
}

#[derive(Debug, Deserialize)]
pub struct ChainConfigRaw {
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env_var: String,
    /// Environment variable name for WebSocket URL (for live mode).
    pub ws_url_env_var: Option<String>,
    pub start_block: Option<U256>,
    pub contracts: InlineOrPath<Contracts>,
    pub block_receipts_method: Option<BlockReceiptsMethod>,
    #[serde(default)]
    pub factory_collections: Option<InlineOrPath<FactoryCollections>>,
    /// RPC client configuration (rate limiting, concurrency).
    #[serde(default)]
    pub rpc: RpcConfig,
}

#[derive(Debug, Clone)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env_var: String,
    /// Environment variable name for WebSocket URL (for live mode).
    pub ws_url_env_var: Option<String>,
    pub start_block: Option<U256>,
    pub contracts: Contracts,
    pub block_receipts_method: Option<BlockReceiptsMethod>,
    pub factory_collections: FactoryCollections,
    /// RPC client configuration (rate limiting, concurrency).
    pub rpc: RpcConfig,
}

pub fn resolve_chain_config(
    raw_config: ChainConfigRaw,
    base_dir: &Path,
) -> anyhow::Result<ChainConfig> {
    let contracts = match raw_config.contracts {
        InlineOrPath::Inline(contracts) => contracts,
        InlineOrPath::Path(p) => load_contracts_from_path(base_dir, &p)
            .map_err(|e| anyhow::anyhow!("Failed to load contracts from path {}: {}", p, e))?,
    };

    let factory_collections = match raw_config.factory_collections {
        Some(InlineOrPath::Inline(collections)) => collections,
        Some(InlineOrPath::Path(p)) => {
            load_factory_collections_from_path(base_dir, &p).map_err(|e| {
                anyhow::anyhow!("Failed to load factory collections from path {}: {}", p, e)
            })?
        }
        None => FactoryCollections::new(),
    };

    Ok(ChainConfig {
        name: raw_config.name,
        chain_id: raw_config.chain_id,
        rpc_url_env_var: raw_config.rpc_url_env_var,
        ws_url_env_var: raw_config.ws_url_env_var,
        start_block: raw_config.start_block,
        contracts,
        block_receipts_method: raw_config.block_receipts_method,
        factory_collections,
        rpc: raw_config.rpc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_receipts_method_serde_roundtrip() {
        let json = r#""eth_getBlockReceipts""#;
        let method: BlockReceiptsMethod = serde_json::from_str(json).unwrap();
        assert_eq!(method.as_str(), "eth_getBlockReceipts");
        let serialized = serde_json::to_string(&method).unwrap();
        assert_eq!(serialized, json);

        let json = r#""alchemy_getTransactionReceipts""#;
        let method: BlockReceiptsMethod = serde_json::from_str(json).unwrap();
        assert_eq!(method.as_str(), "alchemy_getTransactionReceipts");
        let serialized = serde_json::to_string(&method).unwrap();
        assert_eq!(serialized, json);

        let json = r#""parity_getBlockReceipts""#;
        let method: BlockReceiptsMethod = serde_json::from_str(json).unwrap();
        assert_eq!(method.as_str(), "parity_getBlockReceipts");
        let serialized = serde_json::to_string(&method).unwrap();
        assert_eq!(serialized, json);
    }

    #[test]
    fn test_block_receipts_method_invalid() {
        let json = r#""invalid_method""#;
        let result = serde_json::from_str::<BlockReceiptsMethod>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_block_receipts_method_optional_none() {
        let json = r#"null"#;
        let method: Option<BlockReceiptsMethod> = serde_json::from_str(json).unwrap();
        assert!(method.is_none());
    }

    #[test]
    fn test_resolve_chain_config_nonexistent_contracts_returns_err() {
        let raw = ChainConfigRaw {
            name: "test".to_string(),
            chain_id: 1,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            contracts: InlineOrPath::Path("nonexistent/contracts.json".to_string()),
            block_receipts_method: None,
            factory_collections: None,
            rpc: RpcConfig::default(),
        };
        let result = resolve_chain_config(raw, Path::new("/tmp"));
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_chain_config_inline_returns_ok() {
        let raw = ChainConfigRaw {
            name: "test".to_string(),
            chain_id: 1,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            contracts: InlineOrPath::Inline(Contracts::new()),
            block_receipts_method: Some(BlockReceiptsMethod::EthGetBlockReceipts),
            factory_collections: None,
            rpc: RpcConfig::default(),
        };
        let result = resolve_chain_config(raw, Path::new("/tmp"));
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.name, "test");
        assert_eq!(
            config.block_receipts_method.unwrap().as_str(),
            "eth_getBlockReceipts"
        );
    }

    #[test]
    fn test_rpc_config_prefers_requests_per_second() {
        let rpc = RpcConfig {
            concurrency: None,
            requests_per_second: Some(25),
            compute_units_per_second: Some(7500),
            batch_size: None,
            rate_limit_group: None,
        };

        assert_eq!(rpc.units_per_second(), Some(25));
    }
}
