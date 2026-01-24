use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, U256};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ContractConfig {
    pub address: AddressOrAddresses,
    pub start_block: Option<U256>,
}

#[derive(Debug, Deserialize)]
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
