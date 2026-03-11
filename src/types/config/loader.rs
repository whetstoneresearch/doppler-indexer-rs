//! Generic config loading utilities.
//!
//! This module provides a trait-based approach to loading config from
//! files or directories with consistent error handling and duplicate detection.

use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::hash::Hash;
use std::path::Path;
use thiserror::Error;

/// Errors that can occur during config loading.
#[derive(Debug, Error)]
pub enum ConfigLoadError {
    /// Failed to read a file
    #[error("Failed to read file at {path}: {source}")]
    ReadError {
        path: String,
        source: std::io::Error,
    },

    /// Failed to parse JSON content
    #[error("Failed to parse JSON in {path}: {source}")]
    ParseError {
        path: String,
        source: serde_json::Error,
    },

    /// Failed to read directory
    #[error("Failed to read directory at {path}: {source}")]
    DirectoryError {
        path: String,
        source: std::io::Error,
    },

    /// Duplicate key found when merging configs
    #[error("Duplicate key '{key}' found in {path}")]
    DuplicateKey { key: String, path: String },
}

/// Trait for config types that can be loaded from files.
///
/// Implement this trait to enable loading config from JSON files or directories
/// using the generic loader functions.
pub trait MergeableConfig: DeserializeOwned {
    /// The key type used for duplicate detection
    type Key: Eq + Hash + Clone + ToString;

    /// Get all keys in this config for duplicate detection
    fn keys(&self) -> Vec<Self::Key>;

    /// Check if this config contains a specific key
    fn contains_key(&self, key: &Self::Key) -> bool;

    /// Create an empty config
    fn empty() -> Self;

    /// Merge another config into this one
    fn merge(&mut self, other: Self);
}

/// Blanket implementation for HashMap-based configs
impl<K, V> MergeableConfig for HashMap<K, V>
where
    K: Eq + Hash + Clone + ToString + DeserializeOwned,
    V: DeserializeOwned,
{
    type Key = K;

    fn keys(&self) -> Vec<Self::Key> {
        HashMap::keys(self).cloned().collect()
    }

    fn contains_key(&self, key: &Self::Key) -> bool {
        HashMap::contains_key(self, key)
    }

    fn empty() -> Self {
        HashMap::new()
    }

    fn merge(&mut self, other: Self) {
        self.extend(other);
    }
}

/// Load config from a path, which can be either a file or directory.
///
/// If the path points to a file, the file is read and parsed as JSON.
/// If the path points to a directory, all .json files in the directory
/// are read, parsed, and merged with duplicate key detection.
///
/// # Arguments
/// * `base_dir` - The base directory for resolving relative paths
/// * `path` - The relative path to the config file or directory
///
/// # Returns
/// The loaded and merged config, or an error if loading fails
pub fn load_config_from_path<T: MergeableConfig>(
    base_dir: &Path,
    path: &str,
) -> Result<T, ConfigLoadError> {
    let full_path = base_dir.join(path);

    if full_path.is_dir() {
        load_config_from_dir(&full_path)
    } else {
        load_config_from_file(&full_path)
    }
}

/// Load config from a single JSON file.
pub fn load_config_from_file<T: DeserializeOwned>(path: &Path) -> Result<T, ConfigLoadError> {
    let content = std::fs::read_to_string(path).map_err(|e| ConfigLoadError::ReadError {
        path: path.display().to_string(),
        source: e,
    })?;

    serde_json::from_str(&content).map_err(|e| ConfigLoadError::ParseError {
        path: path.display().to_string(),
        source: e,
    })
}

/// Load config from a directory, merging all .json files.
///
/// Files are processed in sorted order for deterministic behavior.
/// Duplicate keys across files will result in an error.
pub fn load_config_from_dir<T: MergeableConfig>(path: &Path) -> Result<T, ConfigLoadError> {
    let mut merged = T::empty();

    let mut entries: Vec<_> = std::fs::read_dir(path)
        .map_err(|e| ConfigLoadError::DirectoryError {
            path: path.display().to_string(),
            source: e,
        })?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect();

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let file_path = entry.path();
        let config: T = load_config_from_file(&file_path)?;

        // Check for duplicate keys
        for key in config.keys() {
            if merged.contains_key(&key) {
                return Err(ConfigLoadError::DuplicateKey {
                    key: key.to_string(),
                    path: path.display().to_string(),
                });
            }
        }

        merged.merge(config);
    }

    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_config_from_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.json");
        fs::write(&file_path, r#"{"key1": "value1", "key2": "value2"}"#).unwrap();

        let config: HashMap<String, String> = load_config_from_file(&file_path).unwrap();
        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
        assert_eq!(config.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_load_config_from_dir_merges_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.json"), r#"{"key1": "value1"}"#).unwrap();
        fs::write(dir.path().join("b.json"), r#"{"key2": "value2"}"#).unwrap();

        let config: HashMap<String, String> = load_config_from_dir(dir.path()).unwrap();
        assert_eq!(config.len(), 2);
        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
        assert_eq!(config.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_load_config_from_dir_detects_duplicates() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.json"), r#"{"key1": "value1"}"#).unwrap();
        fs::write(dir.path().join("b.json"), r#"{"key1": "value2"}"#).unwrap();

        let result: Result<HashMap<String, String>, _> = load_config_from_dir(dir.path());
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), ConfigLoadError::DuplicateKey { key, .. } if key == "key1")
        );
    }

    #[test]
    fn test_load_config_from_dir_ignores_non_json() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.json"), r#"{"key1": "value1"}"#).unwrap();
        fs::write(dir.path().join("readme.txt"), "not json").unwrap();

        let config: HashMap<String, String> = load_config_from_dir(dir.path()).unwrap();
        assert_eq!(config.len(), 1);
    }

    #[test]
    fn test_load_config_from_path_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.json");
        fs::write(&file_path, r#"{"key1": "value1"}"#).unwrap();

        let config: HashMap<String, String> =
            load_config_from_path(dir.path(), "test.json").unwrap();
        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_load_config_from_path_dir() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("configs");
        fs::create_dir(&subdir).unwrap();
        fs::write(subdir.join("a.json"), r#"{"key1": "value1"}"#).unwrap();

        let config: HashMap<String, String> = load_config_from_path(dir.path(), "configs").unwrap();
        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_read_error() {
        let result: Result<HashMap<String, String>, _> =
            load_config_from_file(Path::new("/nonexistent/path.json"));
        assert!(matches!(
            result.unwrap_err(),
            ConfigLoadError::ReadError { .. }
        ));
    }

    #[test]
    fn test_parse_error() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("invalid.json");
        fs::write(&file_path, "not valid json").unwrap();

        let result: Result<HashMap<String, String>, _> = load_config_from_file(&file_path);
        assert!(matches!(
            result.unwrap_err(),
            ConfigLoadError::ParseError { .. }
        ));
    }
}
