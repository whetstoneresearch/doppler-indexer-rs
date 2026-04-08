use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

const MAX_INNER_INSTRUCTIONS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChainAddress {
    Evm([u8; 20]),
    Solana([u8; 32]),
}

impl ChainAddress {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            ChainAddress::Evm(a) => a,
            ChainAddress::Solana(p) => p,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ChainAddress::Evm(_) => 20,
            ChainAddress::Solana(_) => 32,
        }
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn to_hex(&self) -> String {
        match self {
            ChainAddress::Evm(a) => format!("0x{}", hex::encode(a)),
            ChainAddress::Solana(p) => hex::encode(p),
        }
    }
}

impl std::fmt::Display for ChainAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxId {
    Evm([u8; 32]),
    Solana(#[serde(with = "BigArray")] [u8; 64]),
}

impl TxId {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            TxId::Evm(h) => h,
            TxId::Solana(s) => s,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogPosition {
    Evm { log_index: u32 },
    Solana {
        instruction_index: u16,
        inner_instruction_index: Option<u16>,
    },
}

impl LogPosition {
    pub fn ordinal(&self) -> u64 {
        match self {
            LogPosition::Evm { log_index } => *log_index as u64,
            LogPosition::Solana {
                instruction_index,
                inner_instruction_index,
            } => {
                (*instruction_index as u64) * MAX_INNER_INSTRUCTIONS
                    + inner_instruction_index.unwrap_or(0) as u64
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainType {
    Evm,
    Solana,
}

impl Default for ChainType {
    fn default() -> Self {
        Self::Evm
    }
}
