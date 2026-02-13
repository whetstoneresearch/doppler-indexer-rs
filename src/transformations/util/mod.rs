//! Utility functions for transformations.
//!
//! Common helpers for working with decoded values, addresses, hashes, etc.

pub mod constants;
pub mod db;
pub mod market;
pub mod price;
pub mod price_fetch;
pub mod quote_info;
pub mod sanitize;

use alloy::primitives::{Address, B256};

/// Convert a 20-byte array to an alloy Address.
pub fn bytes_to_address(bytes: [u8; 20]) -> Address {
    Address::from(bytes)
}

/// Convert an alloy Address to a 20-byte array.
pub fn address_to_bytes(addr: Address) -> [u8; 20] {
    addr.0 .0
}

/// Convert a 32-byte array to an alloy B256.
pub fn bytes_to_b256(bytes: [u8; 32]) -> B256 {
    B256::from(bytes)
}

/// Convert an alloy B256 to a 32-byte array.
pub fn b256_to_bytes(hash: B256) -> [u8; 32] {
    hash.0
}

/// Format an address as a hex string with 0x prefix.
pub fn format_address(addr: [u8; 20]) -> String {
    format!("0x{}", hex::encode(addr))
}

/// Format a bytes32 as a hex string with 0x prefix.
pub fn format_bytes32(bytes: [u8; 32]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Parse a hex string (with or without 0x prefix) to bytes.
pub fn parse_hex(s: &str) -> Result<Vec<u8>, hex::FromHexError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s)
}
