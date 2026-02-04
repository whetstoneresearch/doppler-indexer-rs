use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{keccak256, Address, U256};
use serde::Deserialize;
use crate::types::config::eth_call::EthCallConfig;

/// Configuration for an event to decode
/// Signature format: "Transfer(address indexed from, address indexed to, uint256 value)"
#[derive(Debug, Clone, Deserialize)]
pub struct EventConfig {
    /// Full ABI signature string
    pub signature: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContractConfig {
    pub address: AddressOrAddresses,
    pub start_block: Option<U256>,
    pub calls: Option<Vec<EthCallConfig>>,
    #[serde(default)]
    pub factories: Option<Vec<FactoryConfig>>,
    /// Events to decode from this contract's logs
    #[serde(default)]
    pub events: Option<Vec<EventConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FactoryConfig {
    /// Collection name - can use either "collection" or "collection_name" in JSON
    #[serde(alias = "collection_name")]
    pub collection: String,
    pub factory_events: FactoryEventConfigOrArray,
    /// Inline calls - merged with collection type calls if present
    #[serde(default)]
    pub calls: Option<Vec<EthCallConfig>>,
    /// Events to decode from factory-created contract logs - merged with collection type events
    #[serde(default)]
    pub events: Option<Vec<EventConfig>>,
}

/// Shared configuration for a factory collection type
/// Defined at the chain level and referenced by collection name
#[derive(Debug, Clone, Deserialize, Default)]
pub struct FactoryCollectionType {
    #[serde(default)]
    pub calls: Vec<EthCallConfig>,
    #[serde(default)]
    pub events: Option<Vec<EventConfig>>,
}

/// Map of collection name to shared collection type config
pub type FactoryCollections = HashMap<String, FactoryCollectionType>;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum FactoryCollectionsOrPath {
    Inline(FactoryCollections),
    Path(String),
}

/// Resolved factory config with collection type merged in
#[derive(Debug, Clone)]
pub struct ResolvedFactoryConfig {
    pub collection_name: String,
    pub factory_events: Vec<FactoryEventConfig>,
    pub calls: Vec<EthCallConfig>,
    pub events: Option<Vec<EventConfig>>,
}

/// Resolve a factory config by merging collection type config with inline overrides
pub fn resolve_factory_config(
    factory: &FactoryConfig,
    collection_types: &FactoryCollections,
) -> ResolvedFactoryConfig {
    let collection_type = collection_types.get(&factory.collection);

    // Start with collection type config or empty defaults
    let mut calls = collection_type
        .map(|ct| ct.calls.clone())
        .unwrap_or_default();

    let mut events = collection_type.and_then(|ct| ct.events.clone());

    // Merge inline calls (extend, don't replace)
    if let Some(inline_calls) = &factory.calls {
        calls.extend(inline_calls.iter().cloned());
    }

    // Merge inline events (extend, don't replace)
    if let Some(inline_events) = &factory.events {
        match &mut events {
            Some(existing) => existing.extend(inline_events.iter().cloned()),
            None => events = Some(inline_events.clone()),
        }
    }

    ResolvedFactoryConfig {
        collection_name: factory.collection.clone(),
        factory_events: factory.factory_events.clone().into_vec(),
        calls,
        events,
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FactoryEventConfig {
    pub name: String,
    pub topics_signature: String,
    #[serde(default)]
    pub data_signature: Option<String>,
    pub factory_parameters: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FactoryEventConfigOrArray {
    Single(FactoryEventConfig),
    Multiple(Vec<FactoryEventConfig>),
}

impl FactoryEventConfigOrArray {
    pub fn into_vec(self) -> Vec<FactoryEventConfig> {
        match self {
            Self::Single(config) => vec![config],
            Self::Multiple(configs) => configs,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FactoryParameterLocation {
    Topic(usize),
    Data(Vec<usize>),
}

fn parse_bracket_indices(param: &str) -> Vec<usize> {
    let mut indices = Vec::new();
    let mut chars = param.chars().peekable();

    // Skip prefix (data/topics)
    while let Some(c) = chars.peek() {
        if *c == '[' {
            break;
        }
        chars.next();
    }

    while let Some(c) = chars.next() {
        if c == '[' {
            let mut num_str = String::new();
            while let Some(&nc) = chars.peek() {
                if nc == ']' {
                    chars.next();
                    break;
                }
                num_str.push(nc);
                chars.next();
            }
            if let Ok(idx) = num_str.parse::<usize>() {
                indices.push(idx);
            }
        }
    }

    if indices.is_empty() {
        indices.push(0);
    }
    indices
}

impl FactoryEventConfig {
    pub fn compute_event_signature(&self) -> [u8; 32] {
        let mut all_types = Vec::new();

        if !self.topics_signature.is_empty() {
            all_types.push(self.topics_signature.as_str());
        }
        if let Some(ref data_sig) = self.data_signature {
            if !data_sig.is_empty() {
                all_types.push(data_sig.as_str());
            }
        }

        let full_sig = format!("{}({})", self.name, all_types.join(","));
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
            FactoryParameterLocation::Data(parse_bracket_indices(param))
        } else {
            FactoryParameterLocation::Data(vec![0])
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

/// Load factory collections from a path (file or directory)
pub fn load_factory_collections_from_path(
    base_dir: &Path,
    path: &str,
) -> anyhow::Result<FactoryCollections> {
    let full_path = base_dir.join(path);

    if full_path.is_dir() {
        load_factory_collections_from_dir(&full_path)
    } else {
        load_factory_collections_from_file(&full_path)
    }
}

fn load_factory_collections_from_file(path: &Path) -> anyhow::Result<FactoryCollections> {
    let content = std::fs::read_to_string(path);
    match content {
        Ok(content) => {
            let collections: Result<FactoryCollections, _> = serde_json::from_str(&content);
            match collections {
                Ok(collections) => Ok(collections),
                Err(e) => {
                    panic!(
                        "Failed to parse factory collections file at {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }
        Err(e) => {
            panic!(
                "Failed to load factory collections file at {}: {}",
                path.display(),
                e
            );
        }
    }
}

fn load_factory_collections_from_dir(path: &Path) -> anyhow::Result<FactoryCollections> {
    let mut merged = FactoryCollections::new();

    let entries = std::fs::read_dir(path);
    let mut entries: Vec<_> = match entries {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .collect(),
        Err(e) => {
            panic!(
                "Failed to read factory collections directory at {}: {}",
                path.display(),
                e
            );
        }
    };

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let collections = load_factory_collections_from_file(&entry.path());
        match collections {
            Ok(collections) => {
                for key in collections.keys() {
                    if merged.contains_key(key) {
                        panic!(
                            "Duplicate factory collection key '{}' found in {}",
                            key,
                            path.display()
                        );
                    }
                }
                merged.extend(collections);
            }
            Err(e) => {
                panic!(
                    "Failed to load factory collections file at {}: {}",
                    path.display(),
                    e
                );
            }
        }
    }

    Ok(merged)
}
