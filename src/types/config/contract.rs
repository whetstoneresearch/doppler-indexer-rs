use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{keccak256, Address, U256};
use serde::Deserialize;
use crate::types::config::eth_call::EthCallConfig;
use crate::types::config::generic::{SingleOrMultiple};
use crate::types::config::loader::{load_config_from_path, ConfigLoadError};

/// Configuration for an event to decode
/// Signature format: "Transfer(address indexed from, address indexed to, uint256 value)"
#[derive(Debug, Clone, Deserialize)]
pub struct EventConfig {
    /// Full ABI signature string
    pub signature: String,
    /// Optional custom name for output directory (defaults to event name from signature)
    #[serde(default)]
    pub name: Option<String>,
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

/// Resolved factory config with collection type merged in
#[derive(Debug, Clone)]
pub struct ResolvedFactoryConfig {
    pub collection_name: String,
    pub _factory_events: Vec<FactoryEventConfig>,
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
        _factory_events: factory.factory_events.clone().into_vec(),
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

/// Factory event config: single event or multiple events
pub type FactoryEventConfigOrArray = SingleOrMultiple<FactoryEventConfig>;

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

/// Load contracts from a path (file or directory).
///
/// Uses the generic config loader with duplicate key detection.
/// Panics on error for backwards compatibility with existing code.
pub fn load_contracts_from_path(base_dir: &Path, path: &str) -> anyhow::Result<Contracts> {
    load_config_from_path::<Contracts>(base_dir, path)
        .map_err(|e| panic_on_load_error("contracts", e))
}

/// Load factory collections from a path (file or directory).
///
/// Uses the generic config loader with duplicate key detection.
/// Panics on error for backwards compatibility with existing code.
pub fn load_factory_collections_from_path(
    base_dir: &Path,
    path: &str,
) -> anyhow::Result<FactoryCollections> {
    load_config_from_path::<FactoryCollections>(base_dir, path)
        .map_err(|e| panic_on_load_error("factory collections", e))
}

/// Helper to panic with a formatted error message (for backwards compatibility).
fn panic_on_load_error(config_type: &str, error: ConfigLoadError) -> anyhow::Error {
    panic!("Failed to load {}: {}", config_type, error)
}
