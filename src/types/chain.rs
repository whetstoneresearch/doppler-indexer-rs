use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
/// Reserve sort slot 0 for the outer instruction itself.
/// Inner instructions are shifted by 1 so the first CPI sorts immediately after
/// the outer instruction without colliding with it.
const SOLANA_OUTER_INSTRUCTION_SORT_SLOT: u64 = 0;
const SOLANA_INNER_INSTRUCTION_OFFSET: u64 = 1;
/// Lossless packed stride for BIGINT-backed storage. This intentionally exceeds
/// i32::MAX at the upper bound, so it must not be used with INT/INTEGER columns.
const SOLANA_PACKED_ORDINAL_STRIDE: i64 = u16::MAX as i64 + 2;

/// A blockchain address, sized appropriately for the source chain.
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
        false // addresses are never empty
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

/// A transaction identifier, sized for the source chain.
/// Clone but NOT Copy — at 65 bytes for the Solana variant, implicit copies
/// in hot loops are invisible overhead.
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

/// Position of an event within its transaction/block.
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
    /// Stable ordering within a transaction without flattening away the
    /// instruction/inner-instruction structure.
    pub fn sort_key(&self) -> (u64, u64) {
        match self {
            LogPosition::Evm { log_index } => (*log_index as u64, 0),
            LogPosition::Solana {
                instruction_index,
                inner_instruction_index,
            } => (
                *instruction_index as u64,
                match inner_instruction_index {
                    None => SOLANA_OUTER_INSTRUCTION_SORT_SLOT,
                    Some(inner) => *inner as u64 + SOLANA_INNER_INSTRUCTION_OFFSET,
                },
            ),
        }
    }

    /// Lossless packed ordinal for BIGINT-backed database columns.
    pub fn packed_ordinal_i64(&self) -> i64 {
        match self {
            LogPosition::Evm { log_index } => i64::from(*log_index),
            LogPosition::Solana { .. } => {
                let (instruction_index, inner_slot) = self.sort_key();
                let instruction_index =
                    i64::try_from(instruction_index).expect("u16 instruction index fits in i64");
                let inner_slot = i64::try_from(inner_slot).expect("inner sort slot fits in i64");
                instruction_index * SOLANA_PACKED_ORDINAL_STRIDE + inner_slot
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
    fn chain_address_evm_equality() {
        let a = ChainAddress::Evm([1u8; 20]);
        let b = ChainAddress::Evm([1u8; 20]);
        let c = ChainAddress::Evm([2u8; 20]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn chain_address_solana_equality() {
        let a = ChainAddress::Solana([3u8; 32]);
        let b = ChainAddress::Solana([3u8; 32]);
        let c = ChainAddress::Solana([4u8; 32]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn chain_address_cross_variant_inequality() {
        let evm = ChainAddress::Evm([0u8; 20]);
        let sol = ChainAddress::Solana([0u8; 32]);
        assert_ne!(evm, sol);
    }

    #[test]
    fn chain_address_is_copy() {
        let a = ChainAddress::Evm([5u8; 20]);
        let b = a; // Copy
        let _ = a; // Still usable after copy
        assert_eq!(a, b);
    }

    #[test]
    fn chain_address_len() {
        assert_eq!(ChainAddress::Evm([0u8; 20]).len(), 20);
        assert_eq!(ChainAddress::Solana([0u8; 32]).len(), 32);
    }

    #[test]
    fn chain_address_is_empty_always_false() {
        assert!(!ChainAddress::Evm([0u8; 20]).is_empty());
        assert!(!ChainAddress::Solana([0u8; 32]).is_empty());
    }

    #[test]
    fn chain_address_to_hex() {
        let evm = ChainAddress::Evm([0xab; 20]);
        assert!(evm.to_hex().starts_with("0x"));
        assert_eq!(evm.to_hex(), format!("0x{}", "ab".repeat(20)));

        let sol = ChainAddress::Solana([0xcd; 32]);
        assert!(!sol.to_hex().starts_with("0x"));
        assert_eq!(sol.to_hex(), "cd".repeat(32));
    }

    #[test]
    fn chain_address_display() {
        let evm = ChainAddress::Evm([0xff; 20]);
        let display = format!("{}", evm);
        assert_eq!(display, evm.to_hex());

        let sol = ChainAddress::Solana([0u8; 32]);
        assert_eq!(format!("{}", sol), "11111111111111111111111111111111");
    }

    #[test]
    fn chain_address_display_preserves_leading_zeroes_for_solana() {
        let mut pubkey = [0u8; 32];
        pubkey[31] = 1;

        let address = ChainAddress::Solana(pubkey);
        assert_eq!(address.to_string(), "11111111111111111111111111111112");
    }

    #[test]
    fn txid_clone_not_copy() {
        let a = TxId::Evm([1u8; 32]);
        let b = a.clone(); // Must use .clone()
        assert_eq!(a, b);

        let c = TxId::Solana([2u8; 64]);
        let d = c.clone();
        assert_eq!(c, d);
    }

    #[test]
    fn txid_as_bytes() {
        let evm = TxId::Evm([0xaa; 32]);
        assert_eq!(evm.as_bytes().len(), 32);
        assert_eq!(evm.as_bytes()[0], 0xaa);

        let sol = TxId::Solana([0xbb; 64]);
        assert_eq!(sol.as_bytes().len(), 64);
        assert_eq!(sol.as_bytes()[0], 0xbb);
    }

    #[test]
    fn log_position_evm_sort_key() {
        let pos = LogPosition::Evm { log_index: 42 };
        assert_eq!(pos.sort_key(), (42, 0));
    }

    #[test]
    fn log_position_evm_packed_ordinal() {
        let pos = LogPosition::Evm { log_index: 42 };
        assert_eq!(pos.packed_ordinal_i64(), 42);
    }

    #[test]
    fn log_position_solana_sort_key_no_inner() {
        let pos = LogPosition::Solana {
            instruction_index: 3,
            inner_instruction_index: None,
        };
        assert_eq!(pos.sort_key(), (3, 0));
    }

    #[test]
    fn log_position_solana_sort_key_with_inner() {
        let pos = LogPosition::Solana {
            instruction_index: 2,
            inner_instruction_index: Some(5),
        };
        assert_eq!(pos.sort_key(), (2, 6));
    }

    #[test]
    fn log_position_solana_outer_and_first_inner_have_distinct_sort_keys() {
        let outer = LogPosition::Solana {
            instruction_index: 7,
            inner_instruction_index: None,
        };
        let first_inner = LogPosition::Solana {
            instruction_index: 7,
            inner_instruction_index: Some(0),
        };
        assert_ne!(outer.sort_key(), first_inner.sort_key());
    }

    #[test]
    fn log_position_solana_sort_keys_are_ordered() {
        let pos_a = LogPosition::Solana {
            instruction_index: 1,
            inner_instruction_index: Some(u16::MAX),
        };
        let pos_b = LogPosition::Solana {
            instruction_index: 2,
            inner_instruction_index: None,
        };
        assert!(pos_a.sort_key() < pos_b.sort_key());
    }

    #[test]
    fn log_position_solana_max_inner_and_next_outer_do_not_collide() {
        let last_inner = LogPosition::Solana {
            instruction_index: 11,
            inner_instruction_index: Some(u16::MAX),
        };
        let next_outer = LogPosition::Solana {
            instruction_index: 12,
            inner_instruction_index: None,
        };
        assert_ne!(last_inner.sort_key(), next_outer.sort_key());
        assert!(last_inner.sort_key() < next_outer.sort_key());
    }

    #[test]
    fn log_position_solana_packed_ordinal_no_inner() {
        let pos = LogPosition::Solana {
            instruction_index: 3,
            inner_instruction_index: None,
        };
        assert_eq!(
            pos.packed_ordinal_i64(),
            3 * SOLANA_PACKED_ORDINAL_STRIDE
        );
    }

    #[test]
    fn log_position_solana_packed_ordinal_with_inner() {
        let pos = LogPosition::Solana {
            instruction_index: 2,
            inner_instruction_index: Some(5),
        };
        assert_eq!(
            pos.packed_ordinal_i64(),
            2 * SOLANA_PACKED_ORDINAL_STRIDE + 6
        );
    }

    #[test]
    fn log_position_solana_packed_ordinals_are_ordered() {
        let last_inner = LogPosition::Solana {
            instruction_index: 11,
            inner_instruction_index: Some(u16::MAX),
        };
        let next_outer = LogPosition::Solana {
            instruction_index: 12,
            inner_instruction_index: None,
        };
        assert!(last_inner.packed_ordinal_i64() < next_outer.packed_ordinal_i64());
    }

    #[test]
    fn log_position_solana_packed_ordinal_requires_bigint_storage() {
        let max_pos = LogPosition::Solana {
            instruction_index: u16::MAX,
            inner_instruction_index: Some(u16::MAX),
        };
        assert!(max_pos.packed_ordinal_i64() > i64::from(i32::MAX));
    }

    #[test]
    fn chain_type_default_is_evm() {
        assert_eq!(ChainType::default(), ChainType::Evm);
    }

    #[test]
    fn chain_type_serde_roundtrip() {
        let evm = ChainType::Evm;
        let json = serde_json::to_string(&evm).unwrap();
        assert_eq!(json, "\"evm\"");
        let parsed: ChainType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ChainType::Evm);

        let sol = ChainType::Solana;
        let json = serde_json::to_string(&sol).unwrap();
        assert_eq!(json, "\"solana\"");
        let parsed: ChainType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ChainType::Solana);
    }
}
