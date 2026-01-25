use crate::types::config::chain::ChainConfig;

pub fn collect_blocks(chain: &ChainConfig) {
    let rpc_url = match std::env::var(&chain.rpc_url_env_var) {
        Ok(rpc_url) => { rpc_url },
        Err(e) => { panic!("Failed to load rpc url env var {} for chain {}: {}", chain.rpc_url_env_var, chain.name, e); }
    };

    
}