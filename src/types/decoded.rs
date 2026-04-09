use std::str::FromStr;

use alloy::primitives::{I256, U256};
use serde::{Deserialize, Serialize};

use crate::types::chain::ChainAddress;

/// A decoded value from an event parameter, eth_call result, or account-state field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum DecodedValue {
    Address([u8; 20]),
    ChainAddress(ChainAddress),
    Uint256(U256),
    Int256(I256),
    Uint128(u128),
    Int128(i128),
    Uint64(u64),
    Int64(i64),
    Uint32(u32),
    Int32(i32),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
    Bytes32([u8; 32]),
    Bytes(Vec<u8>),
    String(String),
    /// Named tuple of (field_name, field_value) pairs
    NamedTuple(Vec<(String, DecodedValue)>),
    /// Unnamed tuple of values (no field names)
    UnnamedTuple(Vec<DecodedValue>),
    /// Array of values
    Array(Vec<DecodedValue>),
}

#[allow(dead_code)]
impl DecodedValue {
    /// Try to get as an address.
    pub fn as_address(&self) -> Option<[u8; 20]> {
        match self {
            DecodedValue::Address(a) => Some(*a),
            DecodedValue::ChainAddress(ChainAddress::Evm(a)) => Some(*a),
            _ => None,
        }
    }

    /// Try to get as a chain-specific address.
    pub fn as_chain_address(&self) -> Option<ChainAddress> {
        match self {
            DecodedValue::Address(a) => Some(ChainAddress::Evm(*a)),
            DecodedValue::ChainAddress(address) => Some(*address),
            _ => None,
        }
    }

    /// Try to get as a Solana pubkey.
    pub fn as_pubkey(&self) -> Option<[u8; 32]> {
        match self {
            DecodedValue::ChainAddress(ChainAddress::Solana(pubkey)) => Some(*pubkey),
            _ => None,
        }
    }

    /// Try to get as bytes32.
    pub fn as_bytes32(&self) -> Option<[u8; 32]> {
        match self {
            DecodedValue::Bytes32(b) => Some(*b),
            DecodedValue::ChainAddress(ChainAddress::Solana(pubkey)) => Some(*pubkey),
            _ => None,
        }
    }

    /// Try to get as U256.
    pub fn as_uint256(&self) -> Option<U256> {
        match self {
            DecodedValue::Uint256(v) => Some(*v),
            DecodedValue::Uint128(v) => Some(U256::from(*v)),
            DecodedValue::Uint64(v) => Some(U256::from(*v)),
            DecodedValue::Uint32(v) => Some(U256::from(*v)),
            DecodedValue::Uint8(v) => Some(U256::from(*v)),
            DecodedValue::String(s) => U256::from_str(s.trim()).ok(),
            _ => None,
        }
    }

    /// Try to get as I256.
    pub fn as_int256(&self) -> Option<I256> {
        match self {
            DecodedValue::Int256(v) => Some(*v),
            DecodedValue::Int128(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::Int64(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::Int32(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::Int8(v) => Some(I256::try_from(*v).unwrap_or_default()),
            DecodedValue::String(s) => I256::from_str(s.trim()).ok(),
            _ => None,
        }
    }

    /// Try to get as u64.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            DecodedValue::Uint64(v) => Some(*v),
            DecodedValue::Uint32(v) => Some(*v as u64),
            DecodedValue::Uint8(v) => Some(*v as u64),
            DecodedValue::Uint256(v) => v.try_into().ok(),
            DecodedValue::Uint128(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            DecodedValue::Int64(v) => Some(*v),
            DecodedValue::Int32(v) => Some(*v as i64),
            DecodedValue::Int8(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Try to get as i32 (for tick values).
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            DecodedValue::Int32(v) => Some(*v),
            DecodedValue::Int8(v) => Some(*v as i32),
            DecodedValue::Int64(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as u32 (for fee values).
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            DecodedValue::Uint32(v) => Some(*v),
            DecodedValue::Uint8(v) => Some(*v as u32),
            DecodedValue::Uint64(v) => (*v).try_into().ok(),
            DecodedValue::Uint256(v) => v.try_into().ok(),
            DecodedValue::Uint128(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as u8.
    pub fn as_u8(&self) -> Option<u8> {
        match self {
            DecodedValue::Uint8(v) => Some(*v),
            DecodedValue::Uint64(v) => (*v).try_into().ok(),
            DecodedValue::Uint32(v) => (*v).try_into().ok(),
            DecodedValue::Uint256(v) => v.try_into().ok(),
            DecodedValue::Uint128(v) => (*v).try_into().ok(),
            DecodedValue::String(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    /// Try to get as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            DecodedValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as string.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            DecodedValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            DecodedValue::Bytes(b) => Some(b),
            DecodedValue::Bytes32(b) => Some(b),
            DecodedValue::Address(a) => Some(a),
            DecodedValue::ChainAddress(ChainAddress::Evm(address)) => Some(address),
            DecodedValue::ChainAddress(ChainAddress::Solana(pubkey)) => Some(pubkey),
            _ => None,
        }
    }

    /// Get a field from a named tuple.
    pub fn get_field(&self, name: &str) -> Option<&DecodedValue> {
        match self {
            DecodedValue::NamedTuple(fields) => {
                fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
            }
            _ => None,
        }
    }

    /// Convert to a numeric string (for database storage).
    pub fn to_numeric_string(&self) -> Option<String> {
        match self {
            DecodedValue::Uint256(v) => Some(v.to_string()),
            DecodedValue::Int256(v) => Some(v.to_string()),
            DecodedValue::Uint128(v) => Some(v.to_string()),
            DecodedValue::Int128(v) => Some(v.to_string()),
            DecodedValue::Uint64(v) => Some(v.to_string()),
            DecodedValue::Int64(v) => Some(v.to_string()),
            DecodedValue::Uint32(v) => Some(v.to_string()),
            DecodedValue::Int32(v) => Some(v.to_string()),
            DecodedValue::Uint8(v) => Some(v.to_string()),
            DecodedValue::Int8(v) => Some(v.to_string()),
            DecodedValue::String(s) => {
                if U256::from_str(s.trim()).is_ok() || I256::from_str(s.trim()).is_ok() {
                    Some(s.trim().to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
