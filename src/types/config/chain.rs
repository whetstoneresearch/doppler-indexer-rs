use std::path::Path;

use alloy_primitives::U256;
use serde::Deserialize;

use crate::types::config::contract::{
    load_contracts_from_path, load_factory_collections_from_path, Contracts, FactoryCollections,
};
use crate::types::config::generic::InlineOrPath;
use crate::types::config::tokens::{load_tokens_from_path, Tokens};

#[derive(Debug, Deserialize)]
pub struct ChainConfigRaw {
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env_var: String,
    /// Environment variable name for WebSocket URL (for live mode).
    pub ws_url_env_var: Option<String>,
    pub start_block: Option<U256>,
    pub contracts: InlineOrPath<Contracts>,
    pub tokens: InlineOrPath<Tokens>,
    pub block_receipts_method: Option<String>,
    #[serde(default)]
    pub factory_collections: Option<InlineOrPath<FactoryCollections>>,
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
    pub tokens: Tokens,
    pub block_receipts_method: Option<String>,
    pub factory_collections: FactoryCollections,
}

pub fn resolve_chain_config(
    raw_config: ChainConfigRaw,
    base_dir: &Path,
) -> anyhow::Result<ChainConfig> {
    let contracts = match raw_config.contracts {
        InlineOrPath::Inline(contracts) => contracts,
        InlineOrPath::Path(p) => {
            let contracts = load_contracts_from_path(base_dir, &p);
            match contracts {
                Ok(contracts) => contracts,
                Err(e) => {
                    panic!("Failed to load contracts from path {}: {}", p, e);
                }
            }
        }
    };

    let tokens = match raw_config.tokens {
        InlineOrPath::Inline(tokens) => tokens,
        InlineOrPath::Path(p) => {
            let tokens = load_tokens_from_path(base_dir, &p);
            match tokens {
                Ok(tokens) => tokens,
                Err(e) => {
                    panic!("Failed to load tokens from path {}: {}", p, e);
                }
            }
        }
    };

    let factory_collections = match raw_config.factory_collections {
        Some(InlineOrPath::Inline(collections)) => collections,
        Some(InlineOrPath::Path(p)) => {
            let collections = load_factory_collections_from_path(base_dir, &p);
            match collections {
                Ok(collections) => collections,
                Err(e) => {
                    panic!("Failed to load factory collections from path {}: {}", p, e);
                }
            }
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
        tokens,
        block_receipts_method: raw_config.block_receipts_method,
        factory_collections,
    })
}
