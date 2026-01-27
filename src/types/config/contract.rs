use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{keccak256, Address, U256};
use serde::Deserialize;
use crate::types::config::eth_call::EthCallConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct ContractConfig {
    pub address: AddressOrAddresses,
    pub start_block: Option<U256>,
    pub calls: Option<Vec<EthCallConfig>>,
    #[serde(default)]
    pub factories: Option<Vec<FactoryConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FactoryConfig {
    pub collection_name: String,
    pub factory_events: FactoryEventConfig,
    pub factory_parameters: String,
    #[serde(default)]
    pub calls: Vec<EthCallConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FactoryEventConfig {
    pub name: String,
    pub topics_signature: String,
    pub data_signature: String,
}

#[derive(Debug, Clone)]
pub enum FactoryParameterLocation {
    Topic(usize),
    Data(usize),
}

impl FactoryConfig {
    pub fn compute_event_signature(&self) -> [u8; 32] {
        let mut all_types = Vec::new();

        if !self.factory_events.topics_signature.is_empty() {
            all_types.push(self.factory_events.topics_signature.as_str());
        }
        if !self.factory_events.data_signature.is_empty() {
            all_types.push(self.factory_events.data_signature.as_str());
        }

        let full_sig = format!("{}({})",
            self.factory_events.name,
            all_types.join(","));

        keccak256(full_sig.as_bytes()).0
    }

    pub fn parse_factory_parameter(&self) -> FactoryParameterLocation {
        let param = self.factory_parameters.trim();

        if param.starts_with("topics[") {
            let idx_str = param
                .strip_prefix("topics[")
                .and_then(|s| s.strip_suffix(']'))
                .unwrap_or("0");
            let idx = idx_str.parse::<usize>().unwrap_or(0);
            FactoryParameterLocation::Topic(idx)
        } else if param.starts_with("data[") {
            let idx_str = param
                .strip_prefix("data[")
                .and_then(|s| s.strip_suffix(']'))
                .unwrap_or("0");
            let idx = idx_str.parse::<usize>().unwrap_or(0);
            FactoryParameterLocation::Data(idx)
        } else {
            FactoryParameterLocation::Data(0)
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AddressOrAddresses {
    Single(Address),
    Multiple(Vec<Address>),
}

pub type Contracts = HashMap<String, ContractConfig>;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ContractsOrPath {
    Inline(Contracts),
    Path(String),
}

pub fn load_contracts_from_path(base_dir: &Path, path: &str) -> anyhow::Result<Contracts> {
    let full_path = base_dir.join(path);

    if full_path.is_dir() {
        load_contracts_from_dir(&full_path)
    } else {
        load_contracts_from_file(&full_path)
    }
}

fn load_contracts_from_file(path: &Path) -> anyhow::Result<Contracts> {
    let content = std::fs::read_to_string(path);
    match content {
        Ok(content) => {
            let contracts: Result<Contracts, _> = serde_json::from_str(&content);
            match contracts {
                Ok(contracts) => Ok(contracts),
                Err(e) => {
                    panic!("Failed to parse contracts file at {}: {}", path.display(), e);
                }
            }
        }
        Err(e) => {
            panic!("Failed to load contracts file at {}: {}", path.display(), e);
        }
    }
}

fn load_contracts_from_dir(path: &Path) -> anyhow::Result<Contracts> {
    let mut merged = Contracts::new();

    let entries = std::fs::read_dir(path);
    let mut entries: Vec<_> = match entries {
        Ok(entries) => {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "json")
                        .unwrap_or(false)
                })
                .collect()
        }
        Err(e) => {
            panic!("Failed to read contracts directory at {}: {}", path.display(), e);
        }
    };

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let contracts = load_contracts_from_file(&entry.path());
        match contracts {
            Ok(contracts) => {
                for key in contracts.keys() {
                    if merged.contains_key(key) {
                        panic!("Duplicate contract key '{}' found in {}", key, path.display());
                    }
                }
                merged.extend(contracts);
            }
            Err(e) => {
                panic!("Failed to load contracts file at {}: {}", path.display(), e);
            }
        }
    }

    Ok(merged)
}
