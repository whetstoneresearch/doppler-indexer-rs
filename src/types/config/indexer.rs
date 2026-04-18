use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::types::config::chain::{
    resolve_chain_config, ChainConfig, ChainConfigRaw, RpcRateLimitGroup,
};
use crate::types::config::metrics::MetricsConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::config::storage::StorageConfig;
use crate::types::config::transformations::TransformationConfig;

#[derive(Debug, Deserialize)]
pub struct IndexerConfigRaw {
    pub chains: Vec<ChainConfigRaw>,
    pub raw_data_collection: RawDataCollectionConfig,
    #[serde(default)]
    pub transformations: Option<TransformationConfig>,
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub rpc_rate_limits: Option<HashMap<String, RpcRateLimitGroup>>,
}

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub chains: Vec<ChainConfig>,
    pub raw_data_collection: RawDataCollectionConfig,
    pub transformations: Option<TransformationConfig>,
    pub metrics: Option<MetricsConfig>,
    pub storage: Option<StorageConfig>,
    pub rpc_rate_limits: Option<HashMap<String, RpcRateLimitGroup>>,
}

impl IndexerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let base_dir = path.parent().unwrap_or(Path::new("."));
        let content = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("Failed to read config file at {}: {}", path.display(), e)
        })?;

        let raw_config: IndexerConfigRaw = serde_json::from_str(&content).map_err(|e| {
            anyhow::anyhow!("Failed to parse config file at {}: {}", path.display(), e)
        })?;

        let chains: Vec<ChainConfig> = raw_config
            .chains
            .into_iter()
            .map(|chain| resolve_chain_config(chain, base_dir))
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| anyhow::anyhow!("Failed to resolve chain config: {}", e))?;

        let rpc_rate_limits = raw_config.rpc_rate_limits;

        // Validate rate limit group references
        for chain in &chains {
            if chain.rpc.requests_per_second.is_some()
                && chain.rpc.compute_units_per_second.is_some()
            {
                anyhow::bail!(
                    "Chain '{}' has both requests_per_second and compute_units_per_second; use requests_per_second going forward",
                    chain.name
                );
            }
            if let Some(ref group_name) = chain.rpc.rate_limit_group {
                let valid = rpc_rate_limits
                    .as_ref()
                    .is_some_and(|groups| groups.contains_key(group_name));
                if !valid {
                    anyhow::bail!(
                        "Chain '{}' references rate_limit_group '{}' which is not defined in rpc_rate_limits",
                        chain.name, group_name
                    );
                }
            }
            if chain.rpc.rate_limit_group.is_some() && chain.rpc.has_explicit_rate_limit() {
                anyhow::bail!(
                    "Chain '{}' has both rate_limit_group and an explicit per-chain rate limit; the group defines the rate budget",
                    chain.name
                );
            }
            if chain.rpc.rate_limit_group.is_some() && chain.rpc.concurrency.is_some() {
                anyhow::bail!(
                    "Chain '{}' is in a rate_limit_group and must not set rpc.concurrency; set concurrency on the group instead",
                    chain.name
                );
            }
        }

        Ok(IndexerConfig {
            chains,
            raw_data_collection: raw_config.raw_data_collection,
            transformations: raw_config.transformations,
            metrics: raw_config.metrics,
            storage: raw_config.storage,
            rpc_rate_limits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_nonexistent_file_returns_err() {
        let result = IndexerConfig::load(Path::new("/tmp/nonexistent_config_12345.json"));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to read config file"));
    }

    #[test]
    fn test_load_invalid_json_returns_err() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");
        fs::write(&path, "not valid json").unwrap();
        let result = IndexerConfig::load(&path);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to parse config file"));
    }

    fn raw_data_collection_json() -> &'static str {
        r#"{
            "fields": {
                "block_fields": ["number", "timestamp"],
                "receipt_fields": ["block_number"],
                "log_fields": ["block_number", "address", "topics", "data"]
            }
        }"#
    }

    #[test]
    fn test_load_valid_config_returns_ok() {
        let dir = TempDir::new().unwrap();
        let config_json = format!(
            r#"{{
                "chains": [
                    {{
                        "name": "test",
                        "chain_id": 1,
                        "rpc_url_env_var": "RPC_URL",
                        "contracts": {{}},
                        "block_receipts_method": "eth_getBlockReceipts"
                    }}
                ],
                "raw_data_collection": {}
            }}"#,
            raw_data_collection_json()
        );
        let path = dir.path().join("config.json");
        fs::write(&path, config_json).unwrap();
        let result = IndexerConfig::load(&path);
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.chains.len(), 1);
        assert_eq!(config.chains[0].name, "test");
    }

    #[test]
    fn test_load_config_with_bad_chain_path_returns_err() {
        let dir = TempDir::new().unwrap();
        let config_json = format!(
            r#"{{
                "chains": [
                    {{
                        "name": "test",
                        "chain_id": 1,
                        "rpc_url_env_var": "RPC_URL",
                        "contracts": "nonexistent/contracts.json",
                        "block_receipts_method": "eth_getBlockReceipts"
                    }}
                ],
                "raw_data_collection": {}
            }}"#,
            raw_data_collection_json()
        );
        let path = dir.path().join("config.json");
        fs::write(&path, config_json).unwrap();
        let result = IndexerConfig::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_chain_in_group_with_concurrency_returns_err() {
        let dir = TempDir::new().unwrap();
        let config_json = format!(
            r#"{{
                "chains": [
                    {{
                        "name": "test",
                        "chain_id": 1,
                        "rpc_url_env_var": "RPC_URL",
                        "contracts": {{}},
                        "rpc": {{
                            "rate_limit_group": "shared",
                            "concurrency": 50
                        }}
                    }}
                ],
                "rpc_rate_limits": {{
                    "shared": {{ "units_per_second": 7500, "concurrency": 25 }}
                }},
                "raw_data_collection": {}
            }}"#,
            raw_data_collection_json()
        );
        let path = dir.path().join("config.json");
        fs::write(&path, config_json).unwrap();
        let result = IndexerConfig::load(&path);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("must not set rpc.concurrency"));
    }

    #[test]
    fn test_chain_in_group_with_group_provider_returns_ok() {
        let dir = TempDir::new().unwrap();
        let config_json = format!(
            r#"{{
                "chains": [
                    {{
                        "name": "test",
                        "chain_id": 1,
                        "rpc_url_env_var": "RPC_URL",
                        "contracts": {{}},
                        "rpc": {{
                            "rate_limit_group": "shared"
                        }}
                    }}
                ],
                "rpc_rate_limits": {{
                    "shared": {{ "units_per_second": 7500, "concurrency": 25, "provider": "alchemy" }}
                }},
                "raw_data_collection": {}
            }}"#,
            raw_data_collection_json()
        );
        let path = dir.path().join("config.json");
        fs::write(&path, config_json).unwrap();
        let result = IndexerConfig::load(&path);
        assert!(result.is_ok());
    }
}
