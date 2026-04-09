use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
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
        match self {
            ChainAddress::Evm(_) => write!(f, "{}", self.to_hex()),
            ChainAddress::Solana(pubkey) => write!(f, "{}", encode_base58(pubkey)),
        }
    }
}

fn encode_base58(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    let mut digits = bytes.to_vec();
    let leading_zeroes = digits.iter().take_while(|&&byte| byte == 0).count();
    let mut start_at = leading_zeroes;
    let mut encoded = Vec::with_capacity(bytes.len() * 138 / 100 + 1);

    while start_at < digits.len() {
        let mut remainder = 0u32;
        for digit in digits.iter_mut().skip(start_at) {
            let value = (remainder << 8) + u32::from(*digit);
            *digit = (value / 58) as u8;
            remainder = value % 58;
        }

        encoded.push(BASE58_ALPHABET[remainder as usize]);
        while start_at < digits.len() && digits[start_at] == 0 {
            start_at += 1;
        }
    }

    let mut output = String::with_capacity(leading_zeroes + encoded.len());
    for _ in 0..leading_zeroes {
        output.push('1');
    }
    for ch in encoded.iter().rev() {
        output.push(*ch as char);
    }
    output
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
    Evm {
        log_index: u32,
    },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_address_display_formats_evm_as_hex() {
        let address = ChainAddress::Evm([0xabu8; 20]);
        assert_eq!(
            address.to_string(),
            "0xabababababababababababababababababababab"
        );
    }

    #[test]
    fn test_chain_address_display_formats_solana_as_base58() {
        let address = ChainAddress::Solana([0u8; 32]);
        assert_eq!(address.to_string(), "11111111111111111111111111111111");
    }

    #[test]
    fn test_chain_address_display_preserves_leading_zeroes_for_solana() {
        let mut pubkey = [0u8; 32];
        pubkey[31] = 1;

        let address = ChainAddress::Solana(pubkey);
        assert_eq!(address.to_string(), "11111111111111111111111111111112");
    }
}
