use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, FixedBytes};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct TokenConfig {
    pub address: Address,
    pub pool: Option<PoolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PoolType {
    V2,
    V3,
    V4,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AddressOrPoolId {
    PoolId(FixedBytes<32>),
    Address(Address),
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolConfig {
    #[serde(rename = "type")]
    pub pool_type: PoolType,
    pub address: AddressOrPoolId,
    pub quote_token: String,
}

pub type Tokens = HashMap<String, TokenConfig>;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TokensOrPath {
    Inline(Tokens),
    Path(String),
}

pub fn load_tokens_from_path(base_dir: &Path, path: &str) -> anyhow::Result<Tokens> {
    let full_path = base_dir.join(path);

    if full_path.is_dir() {
        load_tokens_from_dir(&full_path)
    } else {
        load_tokens_from_file(&full_path)
    }
}

fn load_tokens_from_file(path: &Path) -> anyhow::Result<Tokens> {
    let content = std::fs::read_to_string(path);
    match content {
        Ok(content) => {
            let tokens: Result<Tokens, _> = serde_json::from_str(&content);
            match tokens {
                Ok(tokens) => Ok(tokens),
                Err(e) => {
                    panic!("Failed to parse tokens file at {}: {}", path.display(), e);
                }
            }
        }
        Err(e) => {
            panic!("Failed to load tokens file at {}: {}", path.display(), e);
        }
    }
}

fn load_tokens_from_dir(path: &Path) -> anyhow::Result<Tokens> {
    let mut merged = Tokens::new();

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
            panic!("Failed to read tokens directory at {}: {}", path.display(), e);
        }
    };

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let tokens = load_tokens_from_file(&entry.path());
        match tokens {
            Ok(tokens) => {
                for key in tokens.keys() {
                    if merged.contains_key(key) {
                        panic!("Duplicate token key '{}' found in {}", key, path.display());
                    }
                }
                merged.extend(tokens);
            }
            Err(e) => {
                panic!("Failed to load tokens file at {}: {}", path.display(), e);
            }
        }
    }

    Ok(merged)
}
