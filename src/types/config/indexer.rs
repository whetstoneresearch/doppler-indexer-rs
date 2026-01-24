use std::path::Path;

use serde::Deserialize;

use crate::types::config::chain::{ChainConfig, ChainConfigRaw, resolve_chain_config};
use crate::types::config::raw_data::RawDataCollectionConfig;

#[derive(Debug, Deserialize)]
pub struct IndexerConfigRaw {
    pub chains: Vec<ChainConfigRaw>,
    pub raw_data_collection: RawDataCollectionConfig,
}

#[derive(Debug)]
pub struct IndexerConfig {
    pub chains: Vec<ChainConfig>,
    pub raw_data_collection: RawDataCollectionConfig,
}

impl IndexerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let base_dir = path.parent().unwrap_or(Path::new("."));
        let content = std::fs::read_to_string(path);
        match content {
            Ok(content) => {
                let raw_config: Result<IndexerConfigRaw, _> = serde_json::from_str(&content);
                match raw_config {
                    Ok(raw_config) => {
                        let chains: Vec<ChainConfig> = raw_config
                            .chains
                            .into_iter()
                            .map(|chain| {
                                match resolve_chain_config(chain, base_dir) {
                                    Ok(config) => config,
                                    Err(e) => panic!("Failed to resolve chain config: {}", e),
                                }
                            })
                            .collect();

                        Ok(IndexerConfig {
                            chains,
                            raw_data_collection: raw_config.raw_data_collection,
                        })
                    }
                    Err(e) => {
                        panic!("Failed to parse config file at {}: {}", path.display(), e);
                    }
                }
            }
            Err(e) => {
                panic!("Failed to read config file at {}: {}", path.display(), e);
            }
        }
    }
}
