//! Solana program configuration types.
//!
//! Phase 3 of the Solana support roadmap (`docs/solana/roadmap.md`).
//!
//! These types are pure declarative configuration. They are deserialized from
//! the same chain config JSON that EVM chains use, and resolved into a
//! `SolanaPrograms` map alongside the rest of `ChainConfig`. Nothing in this
//! module touches RPC, decoding, or runtime state — those land in Phases 4–8.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::types::config::eth_call::Frequency;
use crate::types::config::loader::load_config_from_path;

/// Map of program key (a human-friendly name chosen by the user) to its
/// configuration. Mirrors the `Contracts` type used by EVM chains.
pub type SolanaPrograms = HashMap<String, SolanaProgramConfig>;

/// Configuration for one Solana program the indexer should follow.
#[derive(Debug, Clone, Deserialize)]
pub struct SolanaProgramConfig {
    /// Base58-encoded program id pubkey. Validated lazily by the Phase 4
    /// RPC layer; we keep it as a string here so config loading does not
    /// depend on a base58 decoder.
    pub program_id: String,

    /// Optional path to an Anchor IDL JSON file. Resolved against the
    /// config base directory by the Phase 6 IDL loader.
    #[serde(default)]
    pub idl_path: Option<String>,

    /// IDL format. Currently only "anchor" (default). Future: "shank".
    #[serde(default)]
    pub idl_format: Option<String>,

    /// Built-in decoder name (e.g., "spl_token"). Takes precedence over idl_path.
    #[serde(default)]
    pub decoder: Option<String>,

    /// Events to decode and dispatch to handlers.
    #[serde(default)]
    pub events: Option<Vec<SolanaEventConfig>>,

    /// Account state reads. Live-mode-only — see roadmap §8.3.
    #[serde(default)]
    pub accounts: Option<Vec<SolanaAccountReadConfig>>,

    /// Address discovery rules — events whose decoded fields contain new
    /// account addresses to track.
    #[serde(default)]
    pub discovery: Option<Vec<SolanaDiscoveryConfig>>,

    /// First slot to start indexing from. Defaults to the chain's
    /// `start_block` (interpreted as a slot) if absent.
    #[serde(default)]
    pub start_slot: Option<u64>,
}

/// One event to subscribe to within a program.
#[derive(Debug, Clone, Deserialize)]
pub struct SolanaEventConfig {
    /// Anchor event name (e.g. `"Traded"`).
    pub name: String,
    /// 8-byte hex discriminator. If absent, computed by the Phase 6 IDL
    /// loader as `sha256("event:<name>")[..8]`.
    #[serde(default)]
    pub discriminator: Option<String>,
}

/// One account state read to perform in live mode.
#[derive(Debug, Clone, Deserialize)]
pub struct SolanaAccountReadConfig {
    /// Logical name for this account read (used as parquet/source key).
    pub name: String,
    /// IDL account type to deserialize the raw account data as.
    pub account_type: String,
    /// Reuses the existing `Frequency` enum from eth_call config. The
    /// expected variants for Solana account reads are `OnEvents` (the common
    /// case — read after a decoded event triggers it) and `Once`. Block-
    /// cadence variants will be mapped to slot cadence by the Phase 8 live
    /// account reader.
    #[serde(default)]
    pub frequency: Frequency,
}

/// Address discovery rule: when a particular event fires, treat the named
/// field as the address of a newly created account to start tracking.
#[derive(Debug, Clone, Deserialize)]
pub struct SolanaDiscoveryConfig {
    /// Decoded event name that triggers discovery (e.g. `"PoolInitialized"`).
    pub event_name: String,
    /// Field in the decoded event whose `DecodedValue::Pubkey` value is the
    /// newly created account address.
    pub address_field: String,
    /// Optional IDL account type to associate with the discovered address.
    #[serde(default)]
    pub account_type: Option<String>,
}

/// Solana commitment level. Replaces the roadmap's `Option<String>` so that
/// invalid values are rejected at deserialization time rather than at first
/// RPC call. Default is `Confirmed`, the Solana ecosystem default and the
/// safest middle ground for the Phase 8 reorg detector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SolanaCommitment {
    Processed,
    #[default]
    Confirmed,
    Finalized,
}

/// Provider strategy for Solana historical transaction discovery.
///
/// `Auto` uses Helius' archival `getTransactionsForAddress` when the configured
/// RPC URL appears to point at Helius, and falls back to standard
/// `getSignaturesForAddress` on unsupported-provider or transient transport
/// failures. `Helius` and `Standard` force one path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SolanaHistoricalProvider {
    #[default]
    Auto,
    Helius,
    Standard,
}

/// Load Solana programs from a path (file or directory). Mirrors
/// `load_contracts_from_path` in `contract.rs` and inherits HashMap-based
/// merging plus duplicate-key detection from `loader::MergeableConfig`.
pub fn load_solana_programs_from_path(
    base_dir: &Path,
    path: &str,
) -> anyhow::Result<SolanaPrograms> {
    load_config_from_path::<SolanaPrograms>(base_dir, path)
        .map_err(|e| anyhow::anyhow!("Failed to load Solana programs: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn solana_commitment_default_is_confirmed() {
        assert_eq!(SolanaCommitment::default(), SolanaCommitment::Confirmed);
    }

    #[test]
    fn solana_commitment_serde_roundtrip() {
        for (json, variant) in [
            (r#""processed""#, SolanaCommitment::Processed),
            (r#""confirmed""#, SolanaCommitment::Confirmed),
            (r#""finalized""#, SolanaCommitment::Finalized),
        ] {
            let parsed: SolanaCommitment = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, variant);
            let serialized = serde_json::to_string(&variant).unwrap();
            assert_eq!(serialized, json);
        }
    }

    #[test]
    fn solana_commitment_rejects_unknown_value() {
        let result: Result<SolanaCommitment, _> = serde_json::from_str(r#""safe""#);
        assert!(result.is_err());
    }

    #[test]
    fn solana_historical_provider_default_is_auto() {
        assert_eq!(
            SolanaHistoricalProvider::default(),
            SolanaHistoricalProvider::Auto
        );
    }

    #[test]
    fn solana_historical_provider_serde_roundtrip() {
        for (json, variant) in [
            (r#""auto""#, SolanaHistoricalProvider::Auto),
            (r#""helius""#, SolanaHistoricalProvider::Helius),
            (r#""standard""#, SolanaHistoricalProvider::Standard),
        ] {
            let parsed: SolanaHistoricalProvider = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, variant);
            let serialized = serde_json::to_string(&variant).unwrap();
            assert_eq!(serialized, json);
        }
    }

    #[test]
    fn solana_program_config_minimal_deserializes() {
        let json = r#"{ "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" }"#;
        let cfg: SolanaProgramConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            cfg.program_id,
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
        );
        assert!(cfg.idl_path.is_none());
        assert!(cfg.events.is_none());
        assert!(cfg.accounts.is_none());
        assert!(cfg.discovery.is_none());
        assert!(cfg.start_slot.is_none());
    }

    #[test]
    fn solana_program_config_full_deserializes() {
        let json = r#"{
            "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            "idl_path": "idls/whirlpool.json",
            "start_slot": 200000000,
            "events": [
                { "name": "Traded" },
                { "name": "PoolInitialized", "discriminator": "0102030405060708" }
            ],
            "accounts": [
                { "name": "whirlpool_state", "account_type": "Whirlpool", "frequency": "once" }
            ],
            "discovery": [
                {
                    "event_name": "PoolInitialized",
                    "address_field": "whirlpool",
                    "account_type": "Whirlpool"
                }
            ]
        }"#;

        let cfg: SolanaProgramConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.idl_path.as_deref(), Some("idls/whirlpool.json"));
        assert_eq!(cfg.start_slot, Some(200_000_000));

        let events = cfg.events.expect("events present");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].name, "Traded");
        assert!(events[0].discriminator.is_none());
        assert_eq!(events[1].name, "PoolInitialized");
        assert_eq!(events[1].discriminator.as_deref(), Some("0102030405060708"));

        let accounts = cfg.accounts.expect("accounts present");
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].name, "whirlpool_state");
        assert_eq!(accounts[0].account_type, "Whirlpool");
        assert!(accounts[0].frequency.is_once());

        let discovery = cfg.discovery.expect("discovery present");
        assert_eq!(discovery.len(), 1);
        assert_eq!(discovery[0].event_name, "PoolInitialized");
        assert_eq!(discovery[0].address_field, "whirlpool");
        assert_eq!(discovery[0].account_type.as_deref(), Some("Whirlpool"));
    }

    #[test]
    fn solana_account_read_config_accepts_on_events_frequency() {
        let json = r#"{
            "name": "whirlpool_state",
            "account_type": "Whirlpool",
            "frequency": {
                "on_events": [
                    { "source": "whirlpool", "event": "Traded" }
                ]
            }
        }"#;

        let cfg: SolanaAccountReadConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.frequency.is_on_events());
        let triggers = cfg.frequency.event_configs();
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].source, "whirlpool");
        assert_eq!(triggers[0].event, "Traded");
    }

    #[test]
    fn solana_program_config_with_decoder_fields() {
        let json = r#"{
            "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "decoder": "spl_token"
        }"#;
        let cfg: SolanaProgramConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.decoder.as_deref(), Some("spl_token"));
        assert!(cfg.idl_path.is_none());
        assert!(cfg.idl_format.is_none());
    }

    #[test]
    fn solana_program_config_with_idl_format() {
        let json = r#"{
            "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            "idl_path": "idls/whirlpool.json",
            "idl_format": "anchor"
        }"#;
        let cfg: SolanaProgramConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.idl_format.as_deref(), Some("anchor"));
        assert_eq!(cfg.idl_path.as_deref(), Some("idls/whirlpool.json"));
    }

    #[test]
    fn load_solana_programs_from_path_missing_file_returns_err() {
        let result = load_solana_programs_from_path(Path::new("/tmp"), "nonexistent/programs.json");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Failed to load Solana programs"));
    }

    #[test]
    fn load_solana_programs_from_path_inline_file_succeeds() {
        let dir = tempfile::TempDir::new().unwrap();
        let file_path = dir.path().join("programs.json");
        std::fs::write(
            &file_path,
            r#"{
                "whirlpool": {
                    "program_id": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
                }
            }"#,
        )
        .unwrap();

        let programs = load_solana_programs_from_path(dir.path(), "programs.json").unwrap();
        assert_eq!(programs.len(), 1);
        let entry = programs.get("whirlpool").expect("whirlpool present");
        assert_eq!(
            entry.program_id,
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
        );
    }
}
