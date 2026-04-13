//! Solana address discovery — the equivalent of EVM factory address collection.
//!
//! Discovers new account addresses from decoded events (e.g. a `PoolInitialized`
//! event contains the new pool's pubkey) and provides bootstrap via
//! `getProgramAccounts`.
//!
//! Phase 5, Stream 4 of the Solana support roadmap.

use std::collections::{HashMap, HashSet};
use std::io;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_sdk::hash::hash as sha256;
use solana_sdk::pubkey::Pubkey;

use crate::solana::rpc::{SolanaRpcClient, SolanaRpcError};
use crate::storage::paths::solana_discovery_dir;
use crate::types::config::solana::{SolanaDiscoveryConfig, SolanaPrograms};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const KNOWN_ACCOUNTS_FILENAME: &str = "known_accounts.json";

/// Anchor account discriminator length (first 8 bytes of sha256("account:<Type>")).
const DISCRIMINATOR_LEN: usize = 8;

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// Message from extractor to DiscoveryManager.
#[derive(Debug)]
pub enum DiscoveryMessage {
    /// Decoded events from a range — scan for discovery triggers.
    DecodedEvents {
        range_start: u64,
        range_end: u64,
        source_name: String,
        events: Vec<DiscoveryEventData>,
    },
    /// Range complete.
    RangeComplete { range_start: u64, range_end: u64 },
    /// All ranges complete (shutdown).
    AllComplete,
}

/// Minimal event data needed for discovery (avoids depending on full DecodedEvent).
#[derive(Debug, Clone)]
pub struct DiscoveryEventData {
    pub event_name: String,
    pub fields: HashMap<String, DiscoveryFieldValue>,
}

/// Simplified field value for discovery — we only care about pubkeys.
#[derive(Debug, Clone)]
pub enum DiscoveryFieldValue {
    Pubkey([u8; 32]),
    Other,
}

/// Newly discovered addresses to send to account reader.
#[derive(Debug, Clone)]
pub struct DiscoveryAddresses {
    pub program_name: String,
    pub account_type: String,
    pub addresses: Vec<Pubkey>,
}

/// Persisted discovery state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistedDiscoveryState {
    /// program_name -> account_type -> list of base58-encoded addresses.
    pub known_accounts: HashMap<String, HashMap<String, Vec<String>>>,
}

// ---------------------------------------------------------------------------
// DiscoveryManager
// ---------------------------------------------------------------------------

/// Manages discovered Solana account addresses across programs.
///
/// Tracks known addresses in memory and persists them to disk for restart
/// recovery. Processes decoded events to discover new addresses and supports
/// bootstrap via `getProgramAccounts` with Anchor discriminator filters.
pub struct DiscoveryManager {
    /// program_name -> account_type -> set of known addresses.
    known_accounts: HashMap<String, HashMap<String, HashSet<Pubkey>>>,
    /// program_name -> discovery configs.
    discovery_configs: HashMap<String, Vec<SolanaDiscoveryConfig>>,
}

impl DiscoveryManager {
    /// Create from program configs, extracting discovery configs.
    pub fn new(programs: &SolanaPrograms) -> Self {
        let mut discovery_configs: HashMap<String, Vec<SolanaDiscoveryConfig>> = HashMap::new();

        for (name, config) in programs {
            if let Some(ref discovery) = config.discovery {
                if !discovery.is_empty() {
                    discovery_configs.insert(name.clone(), discovery.clone());
                }
            }
        }

        Self {
            known_accounts: HashMap::new(),
            discovery_configs,
        }
    }

    /// Load persisted state from disk, merging with config-derived state.
    pub fn load_persisted(chain: &str, programs: &SolanaPrograms) -> Self {
        let mut manager = Self::new(programs);

        let dir = solana_discovery_dir(chain);
        let path = dir.join(KNOWN_ACCOUNTS_FILENAME);

        if !path.exists() {
            tracing::debug!(
                path = %path.display(),
                "No persisted discovery state found, starting fresh"
            );
            return manager;
        }

        match std::fs::read_to_string(&path) {
            Ok(content) => match serde_json::from_str::<PersistedDiscoveryState>(&content) {
                Ok(state) => {
                    let mut total = 0usize;
                    for (program_name, account_types) in &state.known_accounts {
                        for (account_type, addresses) in account_types {
                            let set = manager
                                .known_accounts
                                .entry(program_name.clone())
                                .or_default()
                                .entry(account_type.clone())
                                .or_default();

                            for addr_str in addresses {
                                if let Ok(pubkey) = Pubkey::from_str(addr_str) {
                                    set.insert(pubkey);
                                    total += 1;
                                } else {
                                    tracing::warn!(
                                        program = %program_name,
                                        account_type = %account_type,
                                        address = %addr_str,
                                        "Skipping invalid pubkey in persisted discovery state"
                                    );
                                }
                            }
                        }
                    }
                    tracing::info!(
                        path = %path.display(),
                        total_addresses = total,
                        "Loaded persisted discovery state"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to parse persisted discovery state, starting fresh"
                    );
                }
            },
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Failed to read persisted discovery state, starting fresh"
                );
            }
        }

        manager
    }

    /// Persist current state to disk (atomic write).
    ///
    /// Writes to `data/{chain}/discovery/known_accounts.json`.
    pub fn persist(&self, chain: &str) -> Result<(), io::Error> {
        use std::io::Write;

        let dir = solana_discovery_dir(chain);
        std::fs::create_dir_all(&dir)?;

        let state = self.to_persisted_state();
        let content = serde_json::to_string_pretty(&state)
            .map_err(|e| io::Error::other(format!("JSON serialize error: {}", e)))?;

        // Atomic write: temp file with random suffix -> sync -> rename.
        let random_suffix: u64 = rand::random();
        let tmp_name = format!("{}.{:x}.tmp", KNOWN_ACCOUNTS_FILENAME, random_suffix);
        let tmp_path = dir.join(tmp_name);
        let final_path = dir.join(KNOWN_ACCOUNTS_FILENAME);

        {
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(content.as_bytes())?;
            f.sync_all()?;
        }

        std::fs::rename(&tmp_path, &final_path)?;

        tracing::debug!(
            path = %final_path.display(),
            total_addresses = self.total_known_addresses(),
            "Persisted discovery state"
        );
        Ok(())
    }

    /// Process discovery events, returning any newly discovered addresses.
    ///
    /// For each event matching a discovery config:
    ///   1. Check if `event_name` matches `config.event_name`
    ///   2. Extract pubkey from `config.address_field`
    ///   3. If new (not already known), add to known set and return in result
    pub fn process_events(
        &mut self,
        program_name: &str,
        events: &[DiscoveryEventData],
    ) -> Vec<DiscoveryAddresses> {
        let configs = match self.discovery_configs.get(program_name) {
            Some(c) => c.clone(),
            None => return Vec::new(),
        };

        // account_type -> newly discovered pubkeys
        let mut new_addresses: HashMap<String, Vec<Pubkey>> = HashMap::new();

        for event in events {
            for config in &configs {
                if event.event_name != config.event_name {
                    continue;
                }

                let account_type = config
                    .account_type
                    .as_deref()
                    .unwrap_or("unknown")
                    .to_string();

                if let Some(field_value) = event.fields.get(&config.address_field) {
                    if let DiscoveryFieldValue::Pubkey(bytes) = field_value {
                        let pubkey = Pubkey::new_from_array(*bytes);

                        let known_set = self
                            .known_accounts
                            .entry(program_name.to_string())
                            .or_default()
                            .entry(account_type.clone())
                            .or_default();

                        if known_set.insert(pubkey) {
                            // Newly discovered
                            new_addresses
                                .entry(account_type.clone())
                                .or_default()
                                .push(pubkey);
                        }
                    }
                }
            }
        }

        new_addresses
            .into_iter()
            .map(|(account_type, addresses)| DiscoveryAddresses {
                program_name: program_name.to_string(),
                account_type,
                addresses,
            })
            .collect()
    }

    /// Bootstrap: discover existing accounts via `getProgramAccounts` with
    /// discriminator filter.
    ///
    /// For each program that has discovery configs with an `account_type`,
    /// computes the Anchor discriminator (`sha256("account:<Type>")[..8]`) and
    /// fetches all matching accounts from the RPC node.
    ///
    /// This is expensive and should run once at startup.
    pub async fn bootstrap(
        &mut self,
        rpc: &SolanaRpcClient,
        programs: &SolanaPrograms,
    ) -> Result<(), SolanaRpcError> {
        for (program_name, program_config) in programs {
            let discovery = match &program_config.discovery {
                Some(d) if !d.is_empty() => d,
                _ => continue,
            };

            let program_id = Pubkey::from_str(&program_config.program_id)
                .map_err(|e| SolanaRpcError::InvalidPubkey(e.to_string()))?;

            // Collect unique account types from discovery configs.
            let account_types: HashSet<&str> = discovery
                .iter()
                .filter_map(|d| d.account_type.as_deref())
                .collect();

            for account_type in account_types {
                let discriminator = anchor_account_discriminator(account_type);

                let filter =
                    RpcFilterType::Memcmp(Memcmp::new_raw_bytes(0, discriminator.to_vec()));

                tracing::info!(
                    program = %program_name,
                    account_type = %account_type,
                    program_id = %program_id,
                    "Bootstrapping discovery via getProgramAccounts"
                );

                let accounts = rpc
                    .get_program_accounts(&program_id, Some(vec![filter]))
                    .await?;

                let known_set = self
                    .known_accounts
                    .entry(program_name.clone())
                    .or_default()
                    .entry(account_type.to_string())
                    .or_default();

                let mut new_count = 0usize;
                for (pubkey, _account) in &accounts {
                    if known_set.insert(*pubkey) {
                        new_count += 1;
                    }
                }

                tracing::info!(
                    program = %program_name,
                    account_type = %account_type,
                    total_fetched = accounts.len(),
                    newly_discovered = new_count,
                    total_known = known_set.len(),
                    "Bootstrap discovery complete for account type"
                );
            }
        }

        Ok(())
    }

    /// Get all known addresses for a program and account type.
    pub fn known_addresses(&self, program_name: &str, account_type: &str) -> Vec<Pubkey> {
        self.known_accounts
            .get(program_name)
            .and_then(|types| types.get(account_type))
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Total number of known addresses across all programs.
    pub fn total_known_addresses(&self) -> usize {
        self.known_accounts
            .values()
            .flat_map(|types| types.values())
            .map(|set| set.len())
            .sum()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Convert in-memory state to the serializable persisted format.
    fn to_persisted_state(&self) -> PersistedDiscoveryState {
        let mut known_accounts: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

        for (program_name, account_types) in &self.known_accounts {
            for (account_type, pubkeys) in account_types {
                let mut addresses: Vec<String> =
                    pubkeys.iter().map(|pk| pk.to_string()).collect();
                addresses.sort();

                known_accounts
                    .entry(program_name.clone())
                    .or_default()
                    .insert(account_type.clone(), addresses);
            }
        }

        PersistedDiscoveryState { known_accounts }
    }
}

impl std::fmt::Debug for DiscoveryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryManager")
            .field("total_known_addresses", &self.total_known_addresses())
            .field(
                "programs_with_discovery",
                &self.discovery_configs.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Discriminator helpers
// ---------------------------------------------------------------------------

/// Compute the Anchor account discriminator for a given account type name.
///
/// The discriminator is the first 8 bytes of `sha256("account:<AccountTypeName>")`.
pub fn anchor_account_discriminator(account_type: &str) -> [u8; DISCRIMINATOR_LEN] {
    let preimage = format!("account:{}", account_type);
    let hash = sha256(preimage.as_bytes());
    let mut discriminator = [0u8; DISCRIMINATOR_LEN];
    discriminator.copy_from_slice(&hash.to_bytes()[..DISCRIMINATOR_LEN]);
    discriminator
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::config::solana::SolanaProgramConfig;

    /// Helper: build a minimal SolanaPrograms map with one program that has discovery.
    fn make_programs_with_discovery() -> SolanaPrograms {
        let mut programs = HashMap::new();
        programs.insert(
            "orca_whirlpool".to_string(),
            SolanaProgramConfig {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string(),
                idl_path: None,
                events: None,
                accounts: None,
                discovery: Some(vec![SolanaDiscoveryConfig {
                    event_name: "PoolInitialized".to_string(),
                    address_field: "whirlpool".to_string(),
                    account_type: Some("Whirlpool".to_string()),
                }]),
                start_slot: None,
            },
        );
        programs
    }

    /// Helper: build a SolanaPrograms map with no discovery configs.
    fn make_programs_without_discovery() -> SolanaPrograms {
        let mut programs = HashMap::new();
        programs.insert(
            "some_program".to_string(),
            SolanaProgramConfig {
                program_id: "11111111111111111111111111111111".to_string(),
                idl_path: None,
                events: Some(vec![]),
                accounts: None,
                discovery: None,
                start_slot: None,
            },
        );
        programs
    }

    /// Helper: build a DiscoveryEventData simulating a PoolInitialized event.
    fn make_pool_initialized_event(pubkey_bytes: [u8; 32]) -> DiscoveryEventData {
        let mut fields = HashMap::new();
        fields.insert(
            "whirlpool".to_string(),
            DiscoveryFieldValue::Pubkey(pubkey_bytes),
        );
        fields.insert("tick_spacing".to_string(), DiscoveryFieldValue::Other);

        DiscoveryEventData {
            event_name: "PoolInitialized".to_string(),
            fields,
        }
    }

    // -----------------------------------------------------------------------
    // Test: new() extracts configs
    // -----------------------------------------------------------------------

    #[test]
    fn new_extracts_discovery_configs() {
        let programs = make_programs_with_discovery();
        let manager = DiscoveryManager::new(&programs);

        assert!(manager.discovery_configs.contains_key("orca_whirlpool"));
        let configs = &manager.discovery_configs["orca_whirlpool"];
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].event_name, "PoolInitialized");
        assert_eq!(configs[0].address_field, "whirlpool");
        assert_eq!(configs[0].account_type.as_deref(), Some("Whirlpool"));
    }

    #[test]
    fn new_skips_programs_without_discovery() {
        let programs = make_programs_without_discovery();
        let manager = DiscoveryManager::new(&programs);

        assert!(manager.discovery_configs.is_empty());
        assert_eq!(manager.total_known_addresses(), 0);
    }

    // -----------------------------------------------------------------------
    // Test: process_events discovers addresses
    // -----------------------------------------------------------------------

    #[test]
    fn process_events_discovers_address() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        let pubkey_bytes = [42u8; 32];
        let events = vec![make_pool_initialized_event(pubkey_bytes)];

        let discovered = manager.process_events("orca_whirlpool", &events);

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].program_name, "orca_whirlpool");
        assert_eq!(discovered[0].account_type, "Whirlpool");
        assert_eq!(discovered[0].addresses.len(), 1);
        assert_eq!(
            discovered[0].addresses[0],
            Pubkey::new_from_array(pubkey_bytes)
        );
    }

    // -----------------------------------------------------------------------
    // Test: process_events deduplicates
    // -----------------------------------------------------------------------

    #[test]
    fn process_events_deduplicates() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        let pubkey_bytes = [42u8; 32];
        let events = vec![make_pool_initialized_event(pubkey_bytes)];

        // First call: discovers the address.
        let discovered = manager.process_events("orca_whirlpool", &events);
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].addresses.len(), 1);

        // Second call with same event: no new addresses.
        let discovered = manager.process_events("orca_whirlpool", &events);
        assert!(
            discovered.is_empty() || discovered.iter().all(|d| d.addresses.is_empty()),
            "expected no new addresses on duplicate event"
        );
    }

    #[test]
    fn process_events_discovers_multiple_in_batch() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        let events = vec![
            make_pool_initialized_event([1u8; 32]),
            make_pool_initialized_event([2u8; 32]),
            make_pool_initialized_event([1u8; 32]), // duplicate
        ];

        let discovered = manager.process_events("orca_whirlpool", &events);

        let total_new: usize = discovered.iter().map(|d| d.addresses.len()).sum();
        assert_eq!(total_new, 2, "should discover exactly 2 unique addresses");
        assert_eq!(manager.total_known_addresses(), 2);
    }

    // -----------------------------------------------------------------------
    // Test: persist/load roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn persist_load_roundtrip() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        // Discover some addresses.
        let events = vec![
            make_pool_initialized_event([10u8; 32]),
            make_pool_initialized_event([20u8; 32]),
        ];
        manager.process_events("orca_whirlpool", &events);
        assert_eq!(manager.total_known_addresses(), 2);

        // Use a temp directory as the "chain" to avoid writing to real data dir.
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let chain = tmp_dir.path().to_str().unwrap();

        // Persist.
        manager.persist(chain).unwrap();

        // Load into a fresh manager.
        let loaded = DiscoveryManager::load_persisted(chain, &programs);
        assert_eq!(loaded.total_known_addresses(), 2);

        // Verify the actual addresses match.
        let original_addrs = {
            let mut v = manager.known_addresses("orca_whirlpool", "Whirlpool");
            v.sort();
            v
        };
        let loaded_addrs = {
            let mut v = loaded.known_addresses("orca_whirlpool", "Whirlpool");
            v.sort();
            v
        };
        assert_eq!(original_addrs, loaded_addrs);
    }

    // -----------------------------------------------------------------------
    // Test: known_addresses
    // -----------------------------------------------------------------------

    #[test]
    fn known_addresses_returns_correct_set() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        // Initially empty.
        assert!(manager
            .known_addresses("orca_whirlpool", "Whirlpool")
            .is_empty());

        // Discover addresses.
        let events = vec![
            make_pool_initialized_event([10u8; 32]),
            make_pool_initialized_event([20u8; 32]),
        ];
        manager.process_events("orca_whirlpool", &events);

        let addrs = manager.known_addresses("orca_whirlpool", "Whirlpool");
        assert_eq!(addrs.len(), 2);
        assert!(addrs.contains(&Pubkey::new_from_array([10u8; 32])));
        assert!(addrs.contains(&Pubkey::new_from_array([20u8; 32])));
    }

    #[test]
    fn known_addresses_wrong_program_returns_empty() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        let events = vec![make_pool_initialized_event([10u8; 32])];
        manager.process_events("orca_whirlpool", &events);

        assert!(manager
            .known_addresses("nonexistent_program", "Whirlpool")
            .is_empty());
        assert!(manager
            .known_addresses("orca_whirlpool", "WrongType")
            .is_empty());
    }

    // -----------------------------------------------------------------------
    // Test: no discovery config
    // -----------------------------------------------------------------------

    #[test]
    fn no_discovery_config_returns_empty() {
        let programs = make_programs_without_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        let events = vec![DiscoveryEventData {
            event_name: "SomeEvent".to_string(),
            fields: {
                let mut fields = HashMap::new();
                fields.insert(
                    "addr".to_string(),
                    DiscoveryFieldValue::Pubkey([99u8; 32]),
                );
                fields
            },
        }];

        let discovered = manager.process_events("some_program", &events);
        assert!(discovered.is_empty());
        assert_eq!(manager.total_known_addresses(), 0);
    }

    // -----------------------------------------------------------------------
    // Test: anchor discriminator computation
    // -----------------------------------------------------------------------

    #[test]
    fn anchor_discriminator_is_deterministic() {
        let disc1 = anchor_account_discriminator("Whirlpool");
        let disc2 = anchor_account_discriminator("Whirlpool");
        assert_eq!(disc1, disc2);
    }

    #[test]
    fn anchor_discriminator_differs_for_different_types() {
        let disc_whirlpool = anchor_account_discriminator("Whirlpool");
        let disc_position = anchor_account_discriminator("Position");
        assert_ne!(disc_whirlpool, disc_position);
    }

    #[test]
    fn anchor_discriminator_is_8_bytes() {
        let disc = anchor_account_discriminator("Whirlpool");
        assert_eq!(disc.len(), 8);
    }

    #[test]
    fn anchor_discriminator_matches_known_value() {
        // The Anchor discriminator for "account:Whirlpool" is sha256("account:Whirlpool")[..8].
        // We verify by computing it directly with solana_sdk::hash.
        let preimage = b"account:Whirlpool";
        let hash = sha256(preimage);
        let expected: [u8; 8] = hash.to_bytes()[..8].try_into().unwrap();
        let computed = anchor_account_discriminator("Whirlpool");
        assert_eq!(computed, expected);
    }

    // -----------------------------------------------------------------------
    // Test: process_events ignores non-pubkey fields
    // -----------------------------------------------------------------------

    #[test]
    fn process_events_ignores_non_pubkey_fields() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        // Event with the right name but the address_field holds Other, not Pubkey.
        let event = DiscoveryEventData {
            event_name: "PoolInitialized".to_string(),
            fields: {
                let mut fields = HashMap::new();
                fields.insert("whirlpool".to_string(), DiscoveryFieldValue::Other);
                fields
            },
        };

        let discovered = manager.process_events("orca_whirlpool", &[event]);
        assert!(discovered.is_empty());
        assert_eq!(manager.total_known_addresses(), 0);
    }

    // -----------------------------------------------------------------------
    // Test: process_events ignores missing address_field
    // -----------------------------------------------------------------------

    #[test]
    fn process_events_ignores_missing_address_field() {
        let programs = make_programs_with_discovery();
        let mut manager = DiscoveryManager::new(&programs);

        // Event with right name but missing the "whirlpool" field entirely.
        let event = DiscoveryEventData {
            event_name: "PoolInitialized".to_string(),
            fields: HashMap::new(),
        };

        let discovered = manager.process_events("orca_whirlpool", &[event]);
        assert!(discovered.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test: total_known_addresses across programs
    // -----------------------------------------------------------------------

    #[test]
    fn total_known_addresses_across_programs() {
        let mut programs = make_programs_with_discovery();
        programs.insert(
            "another_program".to_string(),
            SolanaProgramConfig {
                program_id: "22222222222222222222222222222222".to_string(),
                idl_path: None,
                events: None,
                accounts: None,
                discovery: Some(vec![SolanaDiscoveryConfig {
                    event_name: "VaultCreated".to_string(),
                    address_field: "vault".to_string(),
                    account_type: Some("Vault".to_string()),
                }]),
                start_slot: None,
            },
        );

        let mut manager = DiscoveryManager::new(&programs);

        // Discover in first program.
        let events1 = vec![make_pool_initialized_event([1u8; 32])];
        manager.process_events("orca_whirlpool", &events1);

        // Discover in second program.
        let events2 = vec![DiscoveryEventData {
            event_name: "VaultCreated".to_string(),
            fields: {
                let mut fields = HashMap::new();
                fields.insert(
                    "vault".to_string(),
                    DiscoveryFieldValue::Pubkey([2u8; 32]),
                );
                fields
            },
        }];
        manager.process_events("another_program", &events2);

        assert_eq!(manager.total_known_addresses(), 2);
    }

    // -----------------------------------------------------------------------
    // Test: PersistedDiscoveryState serialization
    // -----------------------------------------------------------------------

    #[test]
    fn persisted_state_serialization_roundtrip() {
        let mut known_accounts = HashMap::new();
        let mut whirlpool_types = HashMap::new();
        whirlpool_types.insert(
            "Whirlpool".to_string(),
            vec![
                "HJPjoWMnRgKd1234567890123456789012345678901".to_string(),
                "7qbRF6YsyGuL1234567890123456789012345678901".to_string(),
            ],
        );
        known_accounts.insert("orca_whirlpool".to_string(), whirlpool_types);

        let state = PersistedDiscoveryState { known_accounts };

        let json = serde_json::to_string_pretty(&state).unwrap();
        let deserialized: PersistedDiscoveryState = serde_json::from_str(&json).unwrap();

        assert_eq!(
            deserialized.known_accounts["orca_whirlpool"]["Whirlpool"].len(),
            2
        );
    }

    // -----------------------------------------------------------------------
    // Test: load_persisted with missing file starts fresh
    // -----------------------------------------------------------------------

    #[test]
    fn load_persisted_missing_file_starts_fresh() {
        let programs = make_programs_with_discovery();
        let manager = DiscoveryManager::load_persisted("nonexistent_chain_xyz", &programs);

        assert_eq!(manager.total_known_addresses(), 0);
        assert!(manager.discovery_configs.contains_key("orca_whirlpool"));
    }

    // -----------------------------------------------------------------------
    // Test: discovery with account_type = None uses "unknown"
    // -----------------------------------------------------------------------

    #[test]
    fn discovery_without_account_type_uses_unknown() {
        let mut programs = HashMap::new();
        programs.insert(
            "test_program".to_string(),
            SolanaProgramConfig {
                program_id: "11111111111111111111111111111111".to_string(),
                idl_path: None,
                events: None,
                accounts: None,
                discovery: Some(vec![SolanaDiscoveryConfig {
                    event_name: "Created".to_string(),
                    address_field: "account".to_string(),
                    account_type: None, // no account_type
                }]),
                start_slot: None,
            },
        );

        let mut manager = DiscoveryManager::new(&programs);

        let events = vec![DiscoveryEventData {
            event_name: "Created".to_string(),
            fields: {
                let mut fields = HashMap::new();
                fields.insert(
                    "account".to_string(),
                    DiscoveryFieldValue::Pubkey([55u8; 32]),
                );
                fields
            },
        }];

        let discovered = manager.process_events("test_program", &events);
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].account_type, "unknown");

        // Verify it shows up under "unknown" account type.
        let addrs = manager.known_addresses("test_program", "unknown");
        assert_eq!(addrs.len(), 1);
    }
}
