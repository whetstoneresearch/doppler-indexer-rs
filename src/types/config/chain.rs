use std::path::Path;

use alloy_primitives::U256;
use serde::{Deserialize, Serialize};

use crate::types::chain::ChainType;
use crate::types::config::contract::{
    load_contracts_from_path, load_factory_collections_from_path, Contracts, FactoryCollections,
};
use crate::types::config::generic::InlineOrPath;
#[cfg(feature = "solana")]
use crate::types::config::solana::{
    load_solana_programs_from_path, SolanaCommitment, SolanaPrograms,
};

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
    /// Opt in to HTTP/2 for the RPC transport.
    ///
    /// When true, the underlying reqwest client is built with HTTP/2 prior knowledge
    /// (no HTTP/1.1 fallback), BDP-based adaptive window sizing, and idle keep-alive.
    /// This is best for HTTPS endpoints that support h2 (e.g., Alchemy, Infura, QuickNode)
    /// and want maximum stream multiplexing for concurrent JSON-RPC calls.
    ///
    /// When false or unset, the default transport is used (alloy's `RootProvider::new_http`),
    /// which still negotiates h2 opportunistically via ALPN on HTTPS endpoints but also
    /// allows HTTP/1.1 fallback.
    #[serde(default)]
    pub http2: Option<bool>,
}

impl RpcConfig {
    pub fn units_per_second(&self) -> Option<u32> {
        self.requests_per_second.or(self.compute_units_per_second)
    }

    pub fn has_explicit_rate_limit(&self) -> bool {
        self.requests_per_second.is_some() || self.compute_units_per_second.is_some()
    }

    /// Returns the resolved HTTP/2 opt-in flag. Defaults to false when unset.
    pub fn http2_enabled(&self) -> bool {
        self.http2.unwrap_or(false)
    }
}

#[derive(Debug, Deserialize)]
pub struct ChainConfigRaw {
    pub name: String,
    pub chain_id: u64,
    #[serde(default)]
    pub chain_type: ChainType,
    pub rpc_url_env_var: String,
    /// Environment variable name for WebSocket URL (for live mode).
    pub ws_url_env_var: Option<String>,
    pub start_block: Option<U256>,
    /// Operational override: start processing from this block instead of `start_block`.
    /// Set via config JSON or `--from-block chain:block` CLI flag.
    #[serde(default)]
    pub from_block: Option<u64>,
    /// Operational override: stop processing at this block (inclusive).
    /// Suppresses live mode when set. Set via config JSON or `--to-block chain:block` CLI flag.
    #[serde(default)]
    pub to_block: Option<u64>,
    pub contracts: InlineOrPath<Contracts>,
    pub block_receipts_method: Option<BlockReceiptsMethod>,
    #[serde(default)]
    pub factory_collections: Option<InlineOrPath<FactoryCollections>>,
    /// RPC client configuration (rate limiting, concurrency).
    #[serde(default)]
    pub rpc: RpcConfig,
    /// Solana program configuration. Inline or file/directory path.
    /// Resolved into `ChainConfig.solana_programs`.
    #[cfg(feature = "solana")]
    #[serde(default)]
    pub programs: Option<InlineOrPath<SolanaPrograms>>,
    /// Solana commitment level. Defaults to `confirmed` when absent.
    #[cfg(feature = "solana")]
    #[serde(default)]
    pub commitment: Option<SolanaCommitment>,
}

#[derive(Debug, Clone)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    pub chain_type: ChainType,
    pub rpc_url_env_var: String,
    /// Environment variable name for WebSocket URL (for live mode).
    pub ws_url_env_var: Option<String>,
    pub start_block: Option<U256>,
    /// Operational override: start processing from this block instead of `start_block`.
    pub from_block: Option<u64>,
    /// Operational override: stop processing at this block (inclusive).
    /// Suppresses live mode when set.
    pub to_block: Option<u64>,
    pub contracts: Contracts,
    pub block_receipts_method: Option<BlockReceiptsMethod>,
    pub factory_collections: FactoryCollections,
    /// RPC client configuration (rate limiting, concurrency).
    pub rpc: RpcConfig,
    /// Resolved Solana program map. Empty when no `programs` was set.
    #[cfg(feature = "solana")]
    pub solana_programs: SolanaPrograms,
    /// Resolved Solana commitment level. Defaults to `Confirmed`.
    #[cfg(feature = "solana")]
    pub commitment: SolanaCommitment,
}

impl ChainConfig {
    /// Effective start block: `from_block` overrides `start_block` when set.
    pub fn effective_start_block(&self) -> u64 {
        self.from_block
            .or(self.start_block.map(|u| u.to::<u64>()))
            .unwrap_or(0)
    }

    /// Effective upper bound: clamp `chain_head` down to `to_block` when set.
    pub fn effective_upper_bound(&self, chain_head: u64) -> u64 {
        match self.to_block {
            Some(cap) => cap.min(chain_head),
            None => chain_head,
        }
    }

    /// Check if a half-open range `[range_start, range_end)` overlaps
    /// the active scope `[from_block, to_block]`.
    pub fn range_in_scope(&self, range_start: u64, range_end_exclusive: u64) -> bool {
        if range_end_exclusive <= range_start {
            return false;
        }
        let end_inclusive = range_end_exclusive - 1;
        if let Some(from) = self.from_block {
            if end_inclusive < from {
                return false;
            }
        }
        if let Some(to) = self.to_block {
            if range_start > to {
                return false;
            }
        }
        true
    }
}

pub fn resolve_chain_config(
    raw_config: ChainConfigRaw,
    base_dir: &Path,
) -> anyhow::Result<ChainConfig> {
    #[cfg(not(feature = "solana"))]
    if raw_config.chain_type == ChainType::Solana {
        anyhow::bail!(
            "Chain '{}' is configured with chain_type='solana', but this build does not include Solana support. Rebuild with --features solana.",
            raw_config.name
        );
    }

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

    #[cfg(feature = "solana")]
    let solana_programs = match raw_config.programs {
        Some(InlineOrPath::Inline(programs)) => programs,
        Some(InlineOrPath::Path(p)) => {
            load_solana_programs_from_path(base_dir, &p).map_err(|e| {
                anyhow::anyhow!("Failed to load Solana programs from path {}: {}", p, e)
            })?
        }
        None => SolanaPrograms::new(),
    };

    #[cfg(feature = "solana")]
    let solana_programs = {
        let mut programs = solana_programs;
        for (_name, config) in programs.iter_mut() {
            if let Some(ref idl_path) = config.idl_path {
                let resolved = base_dir.join(idl_path);
                config.idl_path = Some(resolved.to_string_lossy().into_owned());
            }
        }
        programs
    };

    #[cfg(feature = "solana")]
    let commitment = raw_config.commitment.unwrap_or_default();

    Ok(ChainConfig {
        name: raw_config.name,
        chain_id: raw_config.chain_id,
        chain_type: raw_config.chain_type,
        rpc_url_env_var: raw_config.rpc_url_env_var,
        ws_url_env_var: raw_config.ws_url_env_var,
        start_block: raw_config.start_block,
        from_block: raw_config.from_block,
        to_block: raw_config.to_block,
        contracts,
        block_receipts_method: raw_config.block_receipts_method,
        factory_collections,
        rpc: raw_config.rpc,
        #[cfg(feature = "solana")]
        solana_programs,
        #[cfg(feature = "solana")]
        commitment,
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
            chain_type: ChainType::Evm,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            from_block: None,
            to_block: None,
            contracts: InlineOrPath::Path("nonexistent/contracts.json".to_string()),
            block_receipts_method: None,
            factory_collections: None,
            rpc: RpcConfig::default(),
            #[cfg(feature = "solana")]
            programs: None,
            #[cfg(feature = "solana")]
            commitment: None,
        };
        let result = resolve_chain_config(raw, Path::new("/tmp"));
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_chain_config_inline_returns_ok() {
        let raw = ChainConfigRaw {
            name: "test".to_string(),
            chain_id: 1,
            chain_type: ChainType::Evm,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            from_block: None,
            to_block: None,
            contracts: InlineOrPath::Inline(Contracts::new()),
            block_receipts_method: Some(BlockReceiptsMethod::EthGetBlockReceipts),
            factory_collections: None,
            rpc: RpcConfig::default(),
            #[cfg(feature = "solana")]
            programs: None,
            #[cfg(feature = "solana")]
            commitment: None,
        };
        let result = resolve_chain_config(raw, Path::new("/tmp"));
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.name, "test");
        assert_eq!(config.chain_type, ChainType::Evm);
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
            http2: None,
        };

        assert_eq!(rpc.units_per_second(), Some(25));
    }

    #[test]
    fn test_chain_type_defaults_to_evm() {
        let json = r#"{
            "name": "test",
            "chain_id": 1,
            "rpc_url_env_var": "RPC_URL",
            "contracts": {}
        }"#;

        let raw: ChainConfigRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.chain_type, ChainType::Evm);
    }

    #[test]
    fn test_chain_type_deserializes() {
        let json = r#"{
            "name": "test",
            "chain_id": 1,
            "chain_type": "solana",
            "rpc_url_env_var": "RPC_URL",
            "contracts": {}
        }"#;

        let raw: ChainConfigRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.chain_type, ChainType::Solana);
    }

    #[cfg(not(feature = "solana"))]
    #[test]
    fn test_resolve_chain_config_rejects_solana_without_feature() {
        let raw = ChainConfigRaw {
            name: "solana".to_string(),
            chain_id: 101,
            chain_type: ChainType::Solana,
            rpc_url_env_var: "SOLANA_RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            from_block: None,
            to_block: None,
            contracts: InlineOrPath::Inline(Contracts::new()),
            block_receipts_method: None,
            factory_collections: None,
            rpc: RpcConfig::default(),
        };

        let err = resolve_chain_config(raw, Path::new("/tmp")).unwrap_err();
        assert!(
            err.to_string().contains("Rebuild with --features solana"),
            "unexpected error: {err}"
        );
    }

    #[cfg(feature = "solana")]
    mod solana_tests {
        use super::*;
        use crate::types::config::solana::SolanaCommitment;

        #[test]
        fn chain_config_raw_solana_with_inline_programs_deserializes() {
            let json = r#"{
                "name": "solana-mainnet",
                "chain_id": 101,
                "chain_type": "solana",
                "rpc_url_env_var": "SOLANA_RPC_URL",
                "contracts": {},
                "commitment": "finalized",
                "programs": {
                    "whirlpool": {
                        "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
                        "events": [{ "name": "Traded" }]
                    }
                }
            }"#;

            let raw: ChainConfigRaw = serde_json::from_str(json).unwrap();
            assert_eq!(raw.chain_type, ChainType::Solana);
            assert_eq!(raw.commitment, Some(SolanaCommitment::Finalized));
            let programs = raw.programs.expect("programs present");
            match programs {
                InlineOrPath::Inline(map) => {
                    assert_eq!(map.len(), 1);
                    assert!(map.contains_key("whirlpool"));
                }
                InlineOrPath::Path(_) => panic!("expected inline programs"),
            }
        }

        #[test]
        fn chain_config_raw_solana_fields_default_to_none() {
            let json = r#"{
                "name": "test",
                "chain_id": 1,
                "rpc_url_env_var": "RPC_URL",
                "contracts": {}
            }"#;

            let raw: ChainConfigRaw = serde_json::from_str(json).unwrap();
            assert!(raw.programs.is_none());
            assert!(raw.commitment.is_none());
        }

        #[test]
        fn resolve_chain_config_solana_programs_inline() {
            let json = r#"{
                "name": "solana",
                "chain_id": 101,
                "chain_type": "solana",
                "rpc_url_env_var": "SOLANA_RPC_URL",
                "contracts": {},
                "commitment": "processed",
                "programs": {
                    "whirlpool": {
                        "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
                    }
                }
            }"#;

            let raw: ChainConfigRaw = serde_json::from_str(json).unwrap();
            let resolved = resolve_chain_config(raw, Path::new("/tmp")).unwrap();
            assert_eq!(resolved.chain_type, ChainType::Solana);
            assert_eq!(resolved.commitment, SolanaCommitment::Processed);
            assert_eq!(resolved.solana_programs.len(), 1);
            let program = resolved
                .solana_programs
                .get("whirlpool")
                .expect("whirlpool present");
            assert_eq!(
                program.program_id,
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
            );
        }

        #[test]
        fn resolve_chain_config_solana_programs_missing_path_returns_err() {
            let raw = ChainConfigRaw {
                name: "solana".to_string(),
                chain_id: 101,
                chain_type: ChainType::Solana,
                rpc_url_env_var: "SOLANA_RPC_URL".to_string(),
                ws_url_env_var: None,
                start_block: None,
                from_block: None,
                to_block: None,
                contracts: InlineOrPath::Inline(Contracts::new()),
                block_receipts_method: None,
                factory_collections: None,
                rpc: RpcConfig::default(),
                programs: Some(InlineOrPath::Path("nonexistent/programs.json".to_string())),
                commitment: None,
            };
            let result = resolve_chain_config(raw, Path::new("/tmp"));
            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("Failed to load Solana programs from path"));
        }

        #[test]
        fn resolve_chain_config_solana_commitment_defaults_to_confirmed() {
            let raw = ChainConfigRaw {
                name: "solana".to_string(),
                chain_id: 101,
                chain_type: ChainType::Solana,
                rpc_url_env_var: "SOLANA_RPC_URL".to_string(),
                ws_url_env_var: None,
                start_block: None,
                from_block: None,
                to_block: None,
                contracts: InlineOrPath::Inline(Contracts::new()),
                block_receipts_method: None,
                factory_collections: None,
                rpc: RpcConfig::default(),
                programs: None,
                commitment: None,
            };
            let resolved = resolve_chain_config(raw, Path::new("/tmp")).unwrap();
            assert_eq!(resolved.commitment, SolanaCommitment::Confirmed);
            assert!(resolved.solana_programs.is_empty());
        }
    }

    #[test]
    fn test_rpc_config_http2_default_is_false() {
        let rpc = RpcConfig::default();
        assert_eq!(rpc.http2, None);
        assert!(!rpc.http2_enabled());
    }

    #[test]
    fn test_rpc_config_http2_deserializes_from_json() {
        let json = r#"{"concurrency": 100, "requests_per_second": 7500, "http2": true}"#;
        let rpc: RpcConfig = serde_json::from_str(json).unwrap();
        assert_eq!(rpc.http2, Some(true));
        assert!(rpc.http2_enabled());
    }

    #[test]
    fn test_rpc_config_http2_false_deserializes() {
        let json = r#"{"http2": false}"#;
        let rpc: RpcConfig = serde_json::from_str(json).unwrap();
        assert_eq!(rpc.http2, Some(false));
        assert!(!rpc.http2_enabled());
    }

    #[test]
    fn test_rpc_config_http2_absent_defaults_to_none() {
        let json = r#"{"concurrency": 50}"#;
        let rpc: RpcConfig = serde_json::from_str(json).unwrap();
        assert_eq!(rpc.http2, None);
        assert!(!rpc.http2_enabled());
    }

    fn make_chain_config(from_block: Option<u64>, to_block: Option<u64>) -> ChainConfig {
        ChainConfig {
            name: "test".to_string(),
            chain_id: 1,
            chain_type: ChainType::Evm,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: Some(U256::from(1000)),
            from_block,
            to_block,
            contracts: Contracts::new(),
            block_receipts_method: None,
            factory_collections: FactoryCollections::new(),
            rpc: RpcConfig::default(),
            #[cfg(feature = "solana")]
            solana_programs: Default::default(),
            #[cfg(feature = "solana")]
            commitment: Default::default(),
        }
    }

    #[test]
    fn test_effective_start_block_uses_from_block_when_set() {
        let config = make_chain_config(Some(500), None);
        assert_eq!(config.effective_start_block(), 500);
    }

    #[test]
    fn test_effective_start_block_falls_back_to_start_block() {
        let config = make_chain_config(None, None);
        assert_eq!(config.effective_start_block(), 1000);
    }

    #[test]
    fn test_effective_upper_bound_clamps_to_to_block() {
        let config = make_chain_config(None, Some(5000));
        assert_eq!(config.effective_upper_bound(10000), 5000);
        assert_eq!(config.effective_upper_bound(3000), 3000);
    }

    #[test]
    fn test_effective_upper_bound_no_cap() {
        let config = make_chain_config(None, None);
        assert_eq!(config.effective_upper_bound(10000), 10000);
    }

    #[test]
    fn test_range_in_scope_both_bounds() {
        let config = make_chain_config(Some(200), Some(299));
        assert!(config.range_in_scope(100, 201)); // overlaps at 200
        assert!(config.range_in_scope(299, 300)); // overlaps at 299
        assert!(config.range_in_scope(250, 260)); // fully inside
        assert!(!config.range_in_scope(100, 200)); // ends before from_block
        assert!(!config.range_in_scope(300, 400)); // starts after to_block
    }

    #[test]
    fn test_range_in_scope_no_bounds() {
        let config = make_chain_config(None, None);
        assert!(config.range_in_scope(0, 1000));
        assert!(config.range_in_scope(999999, 1000000));
    }

    #[test]
    fn test_range_in_scope_only_from() {
        let config = make_chain_config(Some(500), None);
        assert!(!config.range_in_scope(0, 500));
        assert!(config.range_in_scope(0, 501));
        assert!(config.range_in_scope(500, 1000));
    }

    #[test]
    fn test_range_in_scope_only_to() {
        let config = make_chain_config(None, Some(999));
        assert!(config.range_in_scope(0, 1000));
        assert!(config.range_in_scope(999, 1000));
        assert!(!config.range_in_scope(1000, 2000));
    }
}
