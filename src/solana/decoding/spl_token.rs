use std::collections::HashMap;

use crate::types::chain::ChainAddress;
use crate::types::decoded::DecodedValue;

use super::traits::{
    DecodedAccountFields, DecodedEventFields, DecodedInstructionFields, ProgramDecoder,
    SolanaDecodeError,
};

/// SPL Token program ID: TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
const SPL_TOKEN_PROGRAM_ID: [u8; 32] = [
    6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28, 180, 133,
    237, 95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
];

/// Hand-written `ProgramDecoder` for the SPL Token program.
///
/// SPL Token uses 1-byte instruction tags (not 8-byte Anchor discriminators)
/// and fixed-layout account structures with `COption` (4-byte tag) for optional
/// fields.
pub struct SplTokenDecoder;

impl SplTokenDecoder {
    pub fn new() -> Self {
        SplTokenDecoder
    }
}

impl Default for SplTokenDecoder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read a little-endian u64 from a byte slice at the given offset.
fn read_u64_le(data: &[u8], offset: usize) -> Result<u64, SolanaDecodeError> {
    let end = offset + 8;
    if data.len() < end {
        return Err(SolanaDecodeError::UnexpectedEof {
            needed: end,
            available: data.len(),
        });
    }
    let bytes: [u8; 8] = data[offset..end].try_into().unwrap();
    Ok(u64::from_le_bytes(bytes))
}

/// Read a single byte at the given offset.
fn read_u8(data: &[u8], offset: usize) -> Result<u8, SolanaDecodeError> {
    if offset >= data.len() {
        return Err(SolanaDecodeError::UnexpectedEof {
            needed: offset + 1,
            available: data.len(),
        });
    }
    Ok(data[offset])
}

/// Read a 32-byte pubkey at the given offset.
fn read_pubkey(data: &[u8], offset: usize) -> Result<[u8; 32], SolanaDecodeError> {
    let end = offset + 32;
    if data.len() < end {
        return Err(SolanaDecodeError::UnexpectedEof {
            needed: end,
            available: data.len(),
        });
    }
    let bytes: [u8; 32] = data[offset..end].try_into().unwrap();
    Ok(bytes)
}

/// Read a COption<Pubkey> (4-byte LE tag + 32-byte pubkey).
/// Returns DecodedValue::Null for None, DecodedValue::ChainAddress for Some.
fn read_coption_pubkey(data: &[u8], offset: usize) -> Result<DecodedValue, SolanaDecodeError> {
    let tag_end = offset + 4;
    if data.len() < tag_end {
        return Err(SolanaDecodeError::UnexpectedEof {
            needed: tag_end,
            available: data.len(),
        });
    }
    let tag = u32::from_le_bytes(data[offset..tag_end].try_into().unwrap());
    match tag {
        0 => Ok(DecodedValue::Null),
        1 => {
            let pubkey = read_pubkey(data, tag_end)?;
            Ok(DecodedValue::ChainAddress(ChainAddress::Solana(pubkey)))
        }
        _ => Err(SolanaDecodeError::InvalidEnumVariant(tag as usize)),
    }
}

/// Read a COption<u64> (4-byte LE tag + 8-byte LE u64).
/// Returns DecodedValue::Null for None, DecodedValue::Uint64 for Some.
fn read_coption_u64(data: &[u8], offset: usize) -> Result<DecodedValue, SolanaDecodeError> {
    let tag_end = offset + 4;
    if data.len() < tag_end {
        return Err(SolanaDecodeError::UnexpectedEof {
            needed: tag_end,
            available: data.len(),
        });
    }
    let tag = u32::from_le_bytes(data[offset..tag_end].try_into().unwrap());
    match tag {
        0 => Ok(DecodedValue::Null),
        1 => {
            let value = read_u64_le(data, tag_end)?;
            Ok(DecodedValue::Uint64(value))
        }
        _ => Err(SolanaDecodeError::InvalidEnumVariant(tag as usize)),
    }
}

/// Extract named accounts from the accounts slice, returning only as many as
/// names are provided. Extra accounts are silently ignored, and missing accounts
/// leave the name out of the result.
fn extract_named_accounts(
    accounts: &[[u8; 32]],
    names: &[&str],
) -> HashMap<String, [u8; 32]> {
    let mut map = HashMap::new();
    for (i, name) in names.iter().enumerate() {
        if let Some(pubkey) = accounts.get(i) {
            map.insert(name.to_string(), *pubkey);
        }
    }
    map
}

// ---------------------------------------------------------------------------
// ProgramDecoder impl
// ---------------------------------------------------------------------------

impl ProgramDecoder for SplTokenDecoder {
    fn program_id(&self) -> [u8; 32] {
        SPL_TOKEN_PROGRAM_ID
    }

    fn program_name(&self) -> &str {
        "spl_token"
    }

    /// SPL Token does not emit Anchor-style events.
    fn decode_event(
        &self,
        _discriminator: &[u8],
        _data: &[u8],
    ) -> Result<Option<DecodedEventFields>, SolanaDecodeError> {
        Ok(None)
    }

    fn decode_instruction(
        &self,
        data: &[u8],
        accounts: &[[u8; 32]],
    ) -> Result<Option<DecodedInstructionFields>, SolanaDecodeError> {
        if data.is_empty() {
            return Err(SolanaDecodeError::UnexpectedEof {
                needed: 1,
                available: 0,
            });
        }

        let tag = data[0];
        let payload = &data[1..];

        match tag {
            // Transfer
            3 => {
                let amount = read_u64_le(payload, 0)?;
                let mut args = HashMap::new();
                args.insert("amount".to_string(), DecodedValue::Uint64(amount));
                let named_accounts =
                    extract_named_accounts(accounts, &["source", "destination", "authority"]);
                Ok(Some(DecodedInstructionFields {
                    instruction_name: "Transfer".to_string(),
                    args,
                    named_accounts,
                }))
            }
            // Approve
            4 => {
                let amount = read_u64_le(payload, 0)?;
                let mut args = HashMap::new();
                args.insert("amount".to_string(), DecodedValue::Uint64(amount));
                let named_accounts =
                    extract_named_accounts(accounts, &["source", "delegate", "owner"]);
                Ok(Some(DecodedInstructionFields {
                    instruction_name: "Approve".to_string(),
                    args,
                    named_accounts,
                }))
            }
            // MintTo
            7 => {
                let amount = read_u64_le(payload, 0)?;
                let mut args = HashMap::new();
                args.insert("amount".to_string(), DecodedValue::Uint64(amount));
                let named_accounts =
                    extract_named_accounts(accounts, &["mint", "destination", "authority"]);
                Ok(Some(DecodedInstructionFields {
                    instruction_name: "MintTo".to_string(),
                    args,
                    named_accounts,
                }))
            }
            // Burn
            8 => {
                let amount = read_u64_le(payload, 0)?;
                let mut args = HashMap::new();
                args.insert("amount".to_string(), DecodedValue::Uint64(amount));
                let named_accounts =
                    extract_named_accounts(accounts, &["account", "mint", "authority"]);
                Ok(Some(DecodedInstructionFields {
                    instruction_name: "Burn".to_string(),
                    args,
                    named_accounts,
                }))
            }
            // TransferChecked
            12 => {
                let amount = read_u64_le(payload, 0)?;
                let decimals = read_u8(payload, 8)?;
                let mut args = HashMap::new();
                args.insert("amount".to_string(), DecodedValue::Uint64(amount));
                args.insert("decimals".to_string(), DecodedValue::Uint8(decimals));
                let named_accounts = extract_named_accounts(
                    accounts,
                    &["source", "mint", "destination", "authority"],
                );
                Ok(Some(DecodedInstructionFields {
                    instruction_name: "TransferChecked".to_string(),
                    args,
                    named_accounts,
                }))
            }
            // ApproveChecked
            13 => {
                let amount = read_u64_le(payload, 0)?;
                let decimals = read_u8(payload, 8)?;
                let mut args = HashMap::new();
                args.insert("amount".to_string(), DecodedValue::Uint64(amount));
                args.insert("decimals".to_string(), DecodedValue::Uint8(decimals));
                let named_accounts =
                    extract_named_accounts(accounts, &["source", "mint", "delegate", "owner"]);
                Ok(Some(DecodedInstructionFields {
                    instruction_name: "ApproveChecked".to_string(),
                    args,
                    named_accounts,
                }))
            }
            // Unknown instruction tag
            _ => Ok(None),
        }
    }

    fn decode_account(
        &self,
        data: &[u8],
    ) -> Result<Option<DecodedAccountFields>, SolanaDecodeError> {
        match data.len() {
            82 => self.decode_mint_account(data),
            165 => self.decode_token_account(data),
            _ => Ok(None),
        }
    }

    fn event_types(&self) -> Vec<String> {
        vec![]
    }

    fn instruction_types(&self) -> Vec<String> {
        vec![
            "Transfer".to_string(),
            "Approve".to_string(),
            "MintTo".to_string(),
            "Burn".to_string(),
            "TransferChecked".to_string(),
            "ApproveChecked".to_string(),
        ]
    }

    fn account_types(&self) -> Vec<String> {
        vec!["Mint".to_string(), "TokenAccount".to_string()]
    }
}

impl SplTokenDecoder {
    /// Decode a 82-byte Mint account.
    ///
    /// Layout:
    ///   [0..36]  COption<Pubkey> mint_authority
    ///   [36..44] u64 supply
    ///   [44]     u8 decimals
    ///   [45]     bool is_initialized
    ///   [46..82] COption<Pubkey> freeze_authority
    fn decode_mint_account(
        &self,
        data: &[u8],
    ) -> Result<Option<DecodedAccountFields>, SolanaDecodeError> {
        let mut fields = HashMap::new();

        let mint_authority = read_coption_pubkey(data, 0)?;
        fields.insert("mint_authority".to_string(), mint_authority);

        let supply = read_u64_le(data, 36)?;
        fields.insert("supply".to_string(), DecodedValue::Uint64(supply));

        let decimals = read_u8(data, 44)?;
        fields.insert("decimals".to_string(), DecodedValue::Uint8(decimals));

        let is_initialized = read_u8(data, 45)? != 0;
        fields.insert(
            "is_initialized".to_string(),
            DecodedValue::Bool(is_initialized),
        );

        let freeze_authority = read_coption_pubkey(data, 46)?;
        fields.insert("freeze_authority".to_string(), freeze_authority);

        Ok(Some(DecodedAccountFields {
            account_type: "Mint".to_string(),
            fields,
        }))
    }

    /// Decode a 165-byte Token Account.
    ///
    /// Layout:
    ///   [0..32]    Pubkey mint
    ///   [32..64]   Pubkey owner
    ///   [64..72]   u64 amount
    ///   [72..108]  COption<Pubkey> delegate
    ///   [108]      u8 state (0=Uninitialized, 1=Initialized, 2=Frozen)
    ///   [109..121] COption<u64> is_native
    ///   [121..129] u64 delegated_amount
    ///   [129..165] COption<Pubkey> close_authority
    fn decode_token_account(
        &self,
        data: &[u8],
    ) -> Result<Option<DecodedAccountFields>, SolanaDecodeError> {
        let mut fields = HashMap::new();

        let mint = read_pubkey(data, 0)?;
        fields.insert(
            "mint".to_string(),
            DecodedValue::ChainAddress(ChainAddress::Solana(mint)),
        );

        let owner = read_pubkey(data, 32)?;
        fields.insert(
            "owner".to_string(),
            DecodedValue::ChainAddress(ChainAddress::Solana(owner)),
        );

        let amount = read_u64_le(data, 64)?;
        fields.insert("amount".to_string(), DecodedValue::Uint64(amount));

        let delegate = read_coption_pubkey(data, 72)?;
        fields.insert("delegate".to_string(), delegate);

        let state = read_u8(data, 108)?;
        fields.insert("state".to_string(), DecodedValue::Uint8(state));

        let is_native = read_coption_u64(data, 109)?;
        fields.insert("is_native".to_string(), is_native);

        let delegated_amount = read_u64_le(data, 121)?;
        fields.insert(
            "delegated_amount".to_string(),
            DecodedValue::Uint64(delegated_amount),
        );

        let close_authority = read_coption_pubkey(data, 129)?;
        fields.insert("close_authority".to_string(), close_authority);

        Ok(Some(DecodedAccountFields {
            account_type: "TokenAccount".to_string(),
            fields,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_program_id_correct() {
        let expected = bs58::decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
            .into_vec()
            .unwrap();
        assert_eq!(expected.len(), 32);
        let expected_bytes: [u8; 32] = expected.try_into().unwrap();
        assert_eq!(SPL_TOKEN_PROGRAM_ID, expected_bytes);

        let decoder = SplTokenDecoder::new();
        assert_eq!(decoder.program_id(), SPL_TOKEN_PROGRAM_ID);
    }

    #[test]
    fn test_decode_transfer() {
        let decoder = SplTokenDecoder::new();

        // tag=3, amount=1000u64 LE
        let amount: u64 = 1000;
        let mut data = vec![3u8];
        data.extend_from_slice(&amount.to_le_bytes());

        let accounts = vec![[10u8; 32], [20u8; 32], [30u8; 32]];

        let result = decoder.decode_instruction(&data, &accounts).unwrap();
        assert!(result.is_some());

        let fields = result.unwrap();
        assert_eq!(fields.instruction_name, "Transfer");
        assert_eq!(
            fields.args.get("amount"),
            Some(&DecodedValue::Uint64(1000))
        );
        assert_eq!(fields.named_accounts.get("source"), Some(&[10u8; 32]));
        assert_eq!(
            fields.named_accounts.get("destination"),
            Some(&[20u8; 32])
        );
        assert_eq!(fields.named_accounts.get("authority"), Some(&[30u8; 32]));
    }

    #[test]
    fn test_decode_transfer_checked() {
        let decoder = SplTokenDecoder::new();

        // tag=12, amount=5000u64 LE, decimals=6u8
        let amount: u64 = 5000;
        let mut data = vec![12u8];
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(6u8); // decimals

        let accounts = vec![[11u8; 32], [22u8; 32], [33u8; 32], [44u8; 32]];

        let result = decoder.decode_instruction(&data, &accounts).unwrap();
        assert!(result.is_some());

        let fields = result.unwrap();
        assert_eq!(fields.instruction_name, "TransferChecked");
        assert_eq!(
            fields.args.get("amount"),
            Some(&DecodedValue::Uint64(5000))
        );
        assert_eq!(fields.args.get("decimals"), Some(&DecodedValue::Uint8(6)));
        assert_eq!(fields.named_accounts.get("source"), Some(&[11u8; 32]));
        assert_eq!(fields.named_accounts.get("mint"), Some(&[22u8; 32]));
        assert_eq!(
            fields.named_accounts.get("destination"),
            Some(&[33u8; 32])
        );
        assert_eq!(fields.named_accounts.get("authority"), Some(&[44u8; 32]));
    }

    #[test]
    fn test_unknown_tag_returns_none() {
        let decoder = SplTokenDecoder::new();
        // tag=255, some payload
        let data = vec![255u8, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decoder.decode_instruction(&data, &[]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_event_returns_none() {
        let decoder = SplTokenDecoder::new();
        let result = decoder.decode_event(&[1, 2, 3, 4, 5, 6, 7, 8], &[10, 20]).unwrap();
        assert!(result.is_none());
        assert!(decoder.event_types().is_empty());
    }

    #[test]
    fn test_decode_mint_account() {
        let decoder = SplTokenDecoder::new();

        // Build a 82-byte Mint account
        let mut data = vec![0u8; 82];

        // mint_authority: COption<Pubkey> = Some([0xAA; 32])
        // tag=1 (LE u32)
        data[0..4].copy_from_slice(&1u32.to_le_bytes());
        data[4..36].copy_from_slice(&[0xAA; 32]);

        // supply: u64 = 1_000_000
        let supply: u64 = 1_000_000;
        data[36..44].copy_from_slice(&supply.to_le_bytes());

        // decimals: u8 = 9
        data[44] = 9;

        // is_initialized: bool = true
        data[45] = 1;

        // freeze_authority: COption<Pubkey> = None
        // tag=0 (LE u32), pubkey bytes are zero (don't matter)
        data[46..50].copy_from_slice(&0u32.to_le_bytes());

        let result = decoder.decode_account(&data).unwrap();
        assert!(result.is_some());

        let fields = result.unwrap();
        assert_eq!(fields.account_type, "Mint");
        assert_eq!(
            fields.fields.get("supply"),
            Some(&DecodedValue::Uint64(1_000_000))
        );
        assert_eq!(
            fields.fields.get("decimals"),
            Some(&DecodedValue::Uint8(9))
        );
        assert_eq!(
            fields.fields.get("is_initialized"),
            Some(&DecodedValue::Bool(true))
        );
        assert_eq!(
            fields.fields.get("mint_authority"),
            Some(&DecodedValue::ChainAddress(ChainAddress::Solana(
                [0xAA; 32]
            )))
        );
        assert_eq!(
            fields.fields.get("freeze_authority"),
            Some(&DecodedValue::Null)
        );
    }

    #[test]
    fn test_decode_token_account() {
        let decoder = SplTokenDecoder::new();

        // Build a 165-byte Token Account
        let mut data = vec![0u8; 165];

        // mint: Pubkey
        data[0..32].copy_from_slice(&[0x11; 32]);

        // owner: Pubkey
        data[32..64].copy_from_slice(&[0x22; 32]);

        // amount: u64 = 500
        let amount: u64 = 500;
        data[64..72].copy_from_slice(&amount.to_le_bytes());

        // delegate: COption<Pubkey> = Some([0x33; 32])
        data[72..76].copy_from_slice(&1u32.to_le_bytes());
        data[76..108].copy_from_slice(&[0x33; 32]);

        // state: u8 = 1 (Initialized)
        data[108] = 1;

        // is_native: COption<u64> = None
        data[109..113].copy_from_slice(&0u32.to_le_bytes());

        // delegated_amount: u64 = 100
        let delegated: u64 = 100;
        data[121..129].copy_from_slice(&delegated.to_le_bytes());

        // close_authority: COption<Pubkey> = None
        data[129..133].copy_from_slice(&0u32.to_le_bytes());

        let result = decoder.decode_account(&data).unwrap();
        assert!(result.is_some());

        let fields = result.unwrap();
        assert_eq!(fields.account_type, "TokenAccount");
        assert_eq!(
            fields.fields.get("mint"),
            Some(&DecodedValue::ChainAddress(ChainAddress::Solana(
                [0x11; 32]
            )))
        );
        assert_eq!(
            fields.fields.get("owner"),
            Some(&DecodedValue::ChainAddress(ChainAddress::Solana(
                [0x22; 32]
            )))
        );
        assert_eq!(
            fields.fields.get("amount"),
            Some(&DecodedValue::Uint64(500))
        );
        assert_eq!(
            fields.fields.get("delegate"),
            Some(&DecodedValue::ChainAddress(ChainAddress::Solana(
                [0x33; 32]
            )))
        );
        assert_eq!(fields.fields.get("state"), Some(&DecodedValue::Uint8(1)));
        assert_eq!(fields.fields.get("is_native"), Some(&DecodedValue::Null));
        assert_eq!(
            fields.fields.get("delegated_amount"),
            Some(&DecodedValue::Uint64(100))
        );
        assert_eq!(
            fields.fields.get("close_authority"),
            Some(&DecodedValue::Null)
        );
    }

    #[test]
    fn test_unknown_account_size_returns_none() {
        let decoder = SplTokenDecoder::new();

        // Not 82 or 165 bytes
        let data = vec![0u8; 50];
        let result = decoder.decode_account(&data).unwrap();
        assert!(result.is_none());

        let data = vec![0u8; 200];
        let result = decoder.decode_account(&data).unwrap();
        assert!(result.is_none());
    }
}
