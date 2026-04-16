use std::collections::HashMap;
use std::sync::Arc;

use tracing::warn;

use crate::transformations::context::DecodedAccountState;
use crate::types::chain::ChainAddress;

use super::traits::ProgramDecoder;

/// Routes raw account data to the appropriate `ProgramDecoder` by owner program
/// and produces `DecodedAccountState` for the transformation engine.
pub struct SolanaAccountDecoder {
    /// program_id bytes -> decoder
    decoders: HashMap<[u8; 32], Arc<dyn ProgramDecoder>>,
}

impl SolanaAccountDecoder {
    pub fn new(decoders: Vec<Arc<dyn ProgramDecoder>>) -> Self {
        let decoders = decoders.into_iter().map(|d| (d.program_id(), d)).collect();
        Self { decoders }
    }

    pub fn decode_account(
        &self,
        owner_program: [u8; 32],
        account_address: [u8; 32],
        data: &[u8],
        slot: u64,
        block_time: Option<i64>,
        source_name: &str,
    ) -> Option<DecodedAccountState> {
        let decoder = self.decoders.get(&owner_program)?;

        match decoder.decode_account(data) {
            Ok(Some(fields)) => Some(DecodedAccountState {
                block_number: slot,
                block_timestamp: block_time.unwrap_or(0) as u64,
                account_address: ChainAddress::Solana(account_address),
                owner_program: ChainAddress::Solana(owner_program),
                source_name: source_name.to_string(),
                account_type: fields.account_type,
                fields: fields.fields,
            }),
            Ok(None) => None,
            Err(e) => {
                warn!(
                    owner_program = hex::encode(owner_program),
                    account_address = hex::encode(account_address),
                    error = %e,
                    "failed to decode Solana account"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::decoding::traits::{
        DecodedAccountFields, DecodedEventFields, DecodedInstructionFields, SolanaDecodeError,
    };
    use crate::types::decoded::DecodedValue;

    struct MockDecoder {
        id: [u8; 32],
        result: Result<Option<DecodedAccountFields>, SolanaDecodeError>,
    }

    impl ProgramDecoder for MockDecoder {
        fn program_id(&self) -> [u8; 32] {
            self.id
        }
        fn program_name(&self) -> &str {
            "mock"
        }
        fn decode_event(
            &self,
            _discriminator: &[u8],
            _data: &[u8],
        ) -> Result<Option<DecodedEventFields>, SolanaDecodeError> {
            Ok(None)
        }
        fn decode_instruction(
            &self,
            _data: &[u8],
            _accounts: &[[u8; 32]],
        ) -> Result<Option<DecodedInstructionFields>, SolanaDecodeError> {
            Ok(None)
        }
        fn decode_account(
            &self,
            _data: &[u8],
        ) -> Result<Option<DecodedAccountFields>, SolanaDecodeError> {
            self.result.clone()
        }
        fn event_types(&self) -> Vec<String> {
            vec![]
        }
        fn instruction_types(&self) -> Vec<String> {
            vec![]
        }
        fn account_types(&self) -> Vec<String> {
            vec![]
        }
    }

    #[test]
    fn test_decode_known_account() {
        let program_id = [1u8; 32];
        let account_addr = [2u8; 32];
        let mut fields = HashMap::new();
        fields.insert("supply".to_string(), DecodedValue::Uint64(1_000_000));
        fields.insert("decimals".to_string(), DecodedValue::Uint8(6));

        let decoder = MockDecoder {
            id: program_id,
            result: Ok(Some(DecodedAccountFields {
                account_type: "Mint".to_string(),
                fields: fields.clone(),
            })),
        };

        let router = SolanaAccountDecoder::new(vec![Arc::new(decoder)]);
        let result = router.decode_account(
            program_id,
            account_addr,
            &[0u8; 82],
            500,
            Some(1_700_000_000),
            "spl_token",
        );

        assert!(result.is_some());
        let state = result.unwrap();
        assert_eq!(state.block_number, 500);
        assert_eq!(state.block_timestamp, 1_700_000_000);
        assert_eq!(state.account_address, ChainAddress::Solana(account_addr));
        assert_eq!(state.owner_program, ChainAddress::Solana(program_id));
        assert_eq!(state.source_name, "spl_token");
        assert_eq!(state.account_type, "Mint");
        assert_eq!(
            state.fields.get("supply"),
            Some(&DecodedValue::Uint64(1_000_000))
        );
        assert_eq!(state.fields.get("decimals"), Some(&DecodedValue::Uint8(6)));
    }

    #[test]
    fn test_unknown_program_skipped() {
        let decoder = MockDecoder {
            id: [1u8; 32],
            result: Ok(Some(DecodedAccountFields {
                account_type: "Mint".to_string(),
                fields: HashMap::new(),
            })),
        };

        let router = SolanaAccountDecoder::new(vec![Arc::new(decoder)]);
        // Use a different owner_program that has no decoder
        let result = router.decode_account([99u8; 32], [2u8; 32], &[0u8; 82], 500, None, "test");

        assert!(result.is_none());
    }
}
