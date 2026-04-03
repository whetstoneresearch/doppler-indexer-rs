//! Contract index tracking for factory collection completeness.
//!
//! Each data layer (factories, eth_calls, decoded logs) writes a `contract_index.json`
//! sidecar alongside its parquet files. The index records which factory source contracts
//! (and their addresses) were included when each block range was processed.
//!
//! On catchup, ranges are reprocessed when the current config lists contracts or
//! addresses absent from the index.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use arrow::array::Array;
use serde::{Deserialize, Serialize};

use crate::types::config::contract::{AddressOrAddresses, Contracts};

/// Per-range tracking of which contracts (and their addresses) were processed.
///
/// Outer key: range string like `"0-999"` (inclusive end, matching parquet filenames).
/// Inner key: contract name (e.g. `"Airlock"`).
/// Inner value: sorted list of lowercase hex addresses with `0x` prefix.
pub type ContractIndex = HashMap<String, HashMap<String, Vec<String>>>;

/// The contracts currently expected from configuration.
///
/// Key: contract name. Value: sorted list of lowercase hex addresses.
pub type ExpectedContracts = HashMap<String, Vec<String>>;

const INDEX_FILENAME: &str = "contract_index.json";

// ---------------------------------------------------------------------------
// Range key helper
// ---------------------------------------------------------------------------

/// Build the range key used as the outer key in `ContractIndex`.
///
/// Both `start` and `end_inclusive` match the parquet filename convention.
pub fn range_key(start: u64, end_inclusive: u64) -> String {
    format!("{}-{}", start, end_inclusive)
}

// ---------------------------------------------------------------------------
// Expected contracts from config
// ---------------------------------------------------------------------------

/// Build expected factory source contracts per collection from the contracts config.
///
/// Returns `collection_name -> { contract_name -> [sorted hex addresses] }`.
pub fn build_expected_factory_contracts(contracts: &Contracts) -> HashMap<String, ExpectedContracts> {
    let mut result: HashMap<String, ExpectedContracts> = HashMap::new();

    for (contract_name, config) in contracts {
        let mut addresses = addresses_to_hex(&config.address);
        addresses.sort();

        if let Some(factories) = &config.factories {
            for factory in factories {
                result
                    .entry(factory.collection.clone())
                    .or_default()
                    .insert(contract_name.clone(), addresses.clone());
            }
        }
    }
    result
}

/// Convert `AddressOrAddresses` to a list of lowercase `0x`-prefixed hex strings.
fn addresses_to_hex(addr: &AddressOrAddresses) -> Vec<String> {
    match addr {
        AddressOrAddresses::Single(a) => vec![format!("0x{}", hex::encode(a.0 .0))],
        AddressOrAddresses::Multiple(addrs) => addrs
            .iter()
            .map(|a| format!("0x{}", hex::encode(a.0 .0)))
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Missing-contracts check
// ---------------------------------------------------------------------------

/// Return the subset of `expected` contracts that are missing from the index
/// for the given range key.
///
/// A contract is considered missing if:
/// - Its name is absent from the index entry, or
/// - Any of its configured addresses are absent from the indexed list.
///
/// Returns an empty map when the range is fully covered.
pub fn get_missing_contracts(
    index: &ContractIndex,
    rk: &str,
    expected: &ExpectedContracts,
) -> ExpectedContracts {
    let indexed = match index.get(rk) {
        Some(entry) => entry,
        None => return expected.clone(), // no entry at all → everything is missing
    };

    let mut missing = ExpectedContracts::new();

    for (contract_name, expected_addrs) in expected {
        match indexed.get(contract_name) {
            None => {
                // Entire contract is missing.
                missing.insert(contract_name.clone(), expected_addrs.clone());
            }
            Some(indexed_addrs) => {
                let absent: Vec<String> = expected_addrs
                    .iter()
                    .filter(|a| !indexed_addrs.contains(a))
                    .cloned()
                    .collect();
                if !absent.is_empty() {
                    missing.insert(contract_name.clone(), absent);
                }
            }
        }
    }

    missing
}

// ---------------------------------------------------------------------------
// Update helper
// ---------------------------------------------------------------------------

/// Merge `contracts` into the index for the given range key.
///
/// Overwrites the entry for `rk` with the provided contracts map.
pub fn update_contract_index(
    index: &mut ContractIndex,
    rk: &str,
    contracts: &ExpectedContracts,
) {
    index.insert(rk.to_string(), contracts.clone());
}

// ---------------------------------------------------------------------------
// I/O – sync (called via spawn_blocking)
// ---------------------------------------------------------------------------

/// Read `contract_index.json` from `dir`. Returns an empty index if the file
/// is missing or cannot be parsed.
pub fn read_contract_index(dir: &Path) -> ContractIndex {
    let path = dir.join(INDEX_FILENAME);
    match std::fs::read_to_string(&path) {
        Ok(content) => {
            let index: ContractIndex = serde_json::from_str(&content).unwrap_or_default();
            tracing::debug!(
                "Read contract index from {}: {} ranges tracked",
                path.display(),
                index.len()
            );
            index
        }
        Err(e) => {
            tracing::debug!(
                "No contract index at {} ({}), starting fresh",
                path.display(),
                e.kind()
            );
            HashMap::new()
        }
    }
}

/// Atomically write `contract_index.json` to `dir`.
///
/// Uses the write-to-temp + sync + rename pattern so a crash never leaves a
/// partially-written file.
pub fn write_contract_index(dir: &Path, index: &ContractIndex) -> Result<(), io::Error> {
    use std::io::Write;

    std::fs::create_dir_all(dir)?;

    let path = dir.join(INDEX_FILENAME);
    let content = serde_json::to_string_pretty(index)
        .map_err(|e| io::Error::other(format!("JSON serialize error: {}", e)))?;

    // Atomic write: temp file with random suffix → sync → rename.
    let random_suffix: u64 = rand::random();
    let tmp_name = format!("{}.{:x}.tmp", INDEX_FILENAME, random_suffix);
    let tmp_path = dir.join(tmp_name);

    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }

    std::fs::rename(&tmp_path, &path)?;

    tracing::debug!(
        "Wrote contract index to {}: {} ranges tracked",
        path.display(),
        index.len()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// I/O – async wrappers
// ---------------------------------------------------------------------------

/// Async wrapper around [`read_contract_index`].
pub async fn read_contract_index_async(dir: std::path::PathBuf) -> ContractIndex {
    tokio::task::spawn_blocking(move || read_contract_index(&dir))
        .await
        .unwrap_or_default()
}

/// Async wrapper around [`write_contract_index`].
pub async fn write_contract_index_async(
    dir: std::path::PathBuf,
    index: ContractIndex,
) -> Result<(), io::Error> {
    tokio::task::spawn_blocking(move || write_contract_index(&dir, &index))
        .await
        .map_err(|e| io::Error::other(format!("JoinError: {}", e)))?
}

// ---------------------------------------------------------------------------
// Migration: build contract_index from existing log parquet files
// ---------------------------------------------------------------------------

/// Address-to-contract lookup built from factory matchers.
///
/// Maps `[u8; 20]` factory source addresses → `(contract_name, hex_address)`.
pub type AddressContractMap = HashMap<[u8; 20], (String, String)>;

/// Build a lookup from factory source addresses to `(contract_name, hex_addr)`.
///
/// This is used during migration to determine which contracts were active
/// when an existing range was processed, by checking which source addresses
/// appear in the log parquet.
pub fn build_address_contract_map(contracts: &Contracts) -> HashMap<String, AddressContractMap> {
    let mut result: HashMap<String, AddressContractMap> = HashMap::new();

    for (contract_name, config) in contracts {
        let addrs: Vec<([u8; 20], String)> = match &config.address {
            AddressOrAddresses::Single(a) => {
                vec![(a.0 .0, format!("0x{}", hex::encode(a.0 .0)))]
            }
            AddressOrAddresses::Multiple(addrs) => addrs
                .iter()
                .map(|a| (a.0 .0, format!("0x{}", hex::encode(a.0 .0))))
                .collect(),
        };

        if let Some(factories) = &config.factories {
            for factory in factories {
                let map = result.entry(factory.collection.clone()).or_default();
                for (raw, hex_str) in &addrs {
                    map.insert(*raw, (contract_name.clone(), hex_str.clone()));
                }
            }
        }
    }
    result
}

/// Scan a log parquet file and return which factory source contracts have
/// matching addresses in the `address` column.
///
/// Returns an `ExpectedContracts` map containing only the contracts whose
/// source addresses actually appear in the log file. This tells us what was
/// configured when the range was originally processed.
pub fn detect_contracts_in_log_parquet(
    log_path: &Path,
    address_maps: &HashMap<String, AddressContractMap>,
) -> io::Result<HashMap<String, ExpectedContracts>> {
    use std::collections::HashSet;
    use std::fs::File;

    let file = File::open(log_path).map_err(|e| {
        io::Error::other(format!("Failed to open log parquet {}: {}", log_path.display(), e))
    })?;

    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::other(format!("Parquet reader error: {}", e)))?;

    let schema = builder.schema().clone();
    let addr_col_idx = match schema.index_of("address") {
        Ok(idx) => idx,
        Err(_) => return Ok(HashMap::new()), // no address column
    };

    let mask =
        parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), vec![addr_col_idx]);
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|e| io::Error::other(format!("Parquet build error: {}", e)))?;

    // Collect unique 20-byte addresses from the column
    let mut unique_addresses: HashSet<[u8; 20]> = HashSet::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(_) => continue,
        };
        let col = match batch
            .column_by_name("address")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>())
        {
            Some(c) => c,
            None => continue,
        };
        for i in 0..col.len() {
            let bytes = col.value(i);
            if bytes.len() == 20 {
                let mut addr = [0u8; 20];
                addr.copy_from_slice(bytes);
                unique_addresses.insert(addr);
            }
        }
    }

    // Match against each collection's address map
    let mut result: HashMap<String, ExpectedContracts> = HashMap::new();
    for (collection, addr_map) in address_maps {
        let mut contracts_found: ExpectedContracts = ExpectedContracts::new();
        for addr in &unique_addresses {
            if let Some((contract_name, hex_addr)) = addr_map.get(addr) {
                contracts_found
                    .entry(contract_name.clone())
                    .or_default()
                    .push(hex_addr.clone());
            }
        }
        // Sort addresses for deterministic comparison
        for addrs in contracts_found.values_mut() {
            addrs.sort();
            addrs.dedup();
        }
        if !contracts_found.is_empty() {
            result.insert(collection.clone(), contracts_found);
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_expected() -> ExpectedContracts {
        let mut m = ExpectedContracts::new();
        m.insert(
            "Airlock".to_string(),
            vec!["0xaaa".to_string()],
        );
        m.insert(
            "OtherInit".to_string(),
            vec!["0xbbb".to_string(), "0xccc".to_string()],
        );
        m
    }

    #[test]
    fn test_range_key() {
        assert_eq!(range_key(0, 999), "0-999");
        assert_eq!(range_key(1000, 1999), "1000-1999");
    }

    #[test]
    fn test_missing_contracts_empty_index() {
        let index = ContractIndex::new();
        let expected = sample_expected();
        let missing = get_missing_contracts(&index, "0-999", &expected);
        assert_eq!(missing, expected);
    }

    #[test]
    fn test_missing_contracts_complete() {
        let mut index = ContractIndex::new();
        index.insert("0-999".to_string(), sample_expected());
        let expected = sample_expected();
        let missing = get_missing_contracts(&index, "0-999", &expected);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_missing_contracts_partial_name() {
        let mut index = ContractIndex::new();
        let mut partial = ExpectedContracts::new();
        partial.insert("Airlock".to_string(), vec!["0xaaa".to_string()]);
        index.insert("0-999".to_string(), partial);

        let expected = sample_expected();
        let missing = get_missing_contracts(&index, "0-999", &expected);
        assert!(missing.contains_key("OtherInit"));
        assert!(!missing.contains_key("Airlock"));
    }

    #[test]
    fn test_missing_contracts_partial_address() {
        let mut index = ContractIndex::new();
        let mut partial = sample_expected();
        // Remove one address from OtherInit
        partial.insert(
            "OtherInit".to_string(),
            vec!["0xbbb".to_string()], // missing 0xccc
        );
        index.insert("0-999".to_string(), partial);

        let expected = sample_expected();
        let missing = get_missing_contracts(&index, "0-999", &expected);
        assert!(!missing.contains_key("Airlock"));
        assert_eq!(
            missing.get("OtherInit").unwrap(),
            &vec!["0xccc".to_string()]
        );
    }

    #[test]
    fn test_read_write_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut index = ContractIndex::new();
        index.insert("0-999".to_string(), sample_expected());

        write_contract_index(dir.path(), &index).unwrap();
        let loaded = read_contract_index(dir.path());
        assert_eq!(loaded, index);
    }

    #[test]
    fn test_read_missing_file() {
        let dir = TempDir::new().unwrap();
        let loaded = read_contract_index(dir.path());
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_update_contract_index() {
        let mut index = ContractIndex::new();
        let expected = sample_expected();
        update_contract_index(&mut index, "0-999", &expected);
        assert_eq!(index.get("0-999").unwrap(), &expected);

        // Update same range overwrites
        let mut new_expected = ExpectedContracts::new();
        new_expected.insert("NewContract".to_string(), vec!["0xddd".to_string()]);
        update_contract_index(&mut index, "0-999", &new_expected);
        assert_eq!(index.get("0-999").unwrap(), &new_expected);
    }
}
