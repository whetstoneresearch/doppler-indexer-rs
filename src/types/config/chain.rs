use std::path::Path;

use alloy_primitives::U256;
use serde::Deserialize;

use crate::types::config::contract::{Contracts, ContractsOrPath, load_contracts_from_path};
use crate::types::config::tokens::{Tokens, TokensOrPath, load_tokens_from_path};

#[derive(Debug, Deserialize)]
pub struct ChainConfigRaw {
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env_var: String,
    pub start_block: Option<U256>,
    pub contracts: ContractsOrPath,
    pub tokens: TokensOrPath,
}

#[derive(Debug, Clone)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env_var: String,
    pub start_block: Option<U256>,
    pub contracts: Contracts,
    pub tokens: Tokens,
}

pub fn resolve_chain_config(raw_config: ChainConfigRaw, base_dir: &Path) -> anyhow::Result<ChainConfig> {
    let contracts = match raw_config.contracts {
        ContractsOrPath::Inline(contracts) => contracts,
        ContractsOrPath::Path(p) => {
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
        TokensOrPath::Inline(tokens) => tokens,
        TokensOrPath::Path(p) => {
            let tokens = load_tokens_from_path(base_dir, &p);
            match tokens {
                Ok(tokens) => tokens,
                Err(e) => {
                    panic!("Failed to load tokens from path {}: {}", p, e);
                }
            }
        }
    };

    Ok(ChainConfig {
        name: raw_config.name,
        chain_id: raw_config.chain_id,
        rpc_url_env_var: raw_config.rpc_url_env_var,
        start_block: raw_config.start_block,
        contracts,
        tokens,
    })
}
