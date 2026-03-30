use std::path::Path;

use serde::Deserialize;

use crate::types::config::chain::{resolve_chain_config, ChainConfig, ChainConfigRaw};
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
}

#[derive(Debug)]
pub struct IndexerConfig {
    pub chains: Vec<ChainConfig>,
    pub raw_data_collection: RawDataCollectionConfig,
    pub transformations: Option<TransformationConfig>,
    pub metrics: Option<MetricsConfig>,
    pub storage: Option<StorageConfig>,
}

impl IndexerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let base_dir = path.parent().unwrap_or(Path::new("."));
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file at {}: {}", path.display(), e))?;

        let raw_config: IndexerConfigRaw = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file at {}: {}", path.display(), e))?;

        let chains: Vec<ChainConfig> = raw_config
            .chains
            .into_iter()
            .map(|chain| resolve_chain_config(chain, base_dir))
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| anyhow::anyhow!("Failed to resolve chain config: {}", e))?;

        Ok(IndexerConfig {
            chains,
            raw_data_collection: raw_config.raw_data_collection,
            transformations: raw_config.transformations,
            metrics: raw_config.metrics,
            storage: raw_config.storage,
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
}
