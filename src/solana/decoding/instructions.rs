use std::collections::HashMap;
use std::sync::Arc;

use tracing::warn;

use crate::solana::raw_data::types::SolanaInstructionRecord;
use crate::transformations::context::DecodedEvent;
use crate::types::chain::{ChainAddress, LogPosition, TxId};
use crate::types::decoded::DecodedValue;

use super::traits::ProgramDecoder;

/// Routes `SolanaInstructionRecord`s to the appropriate `ProgramDecoder` by
/// program ID and produces `DecodedEvent`s for the transformation engine.
///
/// Instruction args and named accounts are merged into a single `params` map,
/// with named accounts stored as `DecodedValue::ChainAddress(ChainAddress::Solana(...))`.
pub struct SolanaInstructionDecoder {
    /// program_id bytes -> decoder
    decoders: HashMap<[u8; 32], Arc<dyn ProgramDecoder>>,
}

impl SolanaInstructionDecoder {
    pub fn new(decoders: Vec<Arc<dyn ProgramDecoder>>) -> Self {
        let decoders = decoders.into_iter().map(|d| (d.program_id(), d)).collect();
        Self { decoders }
    }

    pub fn decode_instruction_batch(
        &self,
        instructions: &[SolanaInstructionRecord],
        source_name: &str,
    ) -> Vec<DecodedEvent> {
        instructions
            .iter()
            .filter_map(|record| self.decode_single_instruction(record, source_name))
            .collect()
    }

    fn decode_single_instruction(
        &self,
        record: &SolanaInstructionRecord,
        source_name: &str,
    ) -> Option<DecodedEvent> {
        let decoder = self.decoders.get(&record.program_id)?;

        match decoder.decode_instruction(&record.data, &record.accounts) {
            Ok(Some(fields)) => {
                // Merge args and named_accounts into one params map.
                // Account names are prefixed with "accounts." to avoid
                // collisions with instruction arguments of the same name.
                let mut params = fields.args;
                for (name, pubkey) in fields.named_accounts {
                    params.insert(
                        format!("accounts.{}", name),
                        DecodedValue::ChainAddress(ChainAddress::Solana(pubkey)),
                    );
                }

                Some(DecodedEvent {
                    block_number: record.slot,
                    block_timestamp: record.block_time.unwrap_or(0) as u64,
                    transaction_id: TxId::Solana(record.transaction_signature),
                    position: LogPosition::Solana {
                        instruction_index: record.instruction_index,
                        inner_instruction_index: record.inner_instruction_index,
                    },
                    contract_address: ChainAddress::Solana(record.program_id),
                    source_name: source_name.to_string(),
                    event_name: fields.instruction_name.clone(),
                    event_signature: fields.instruction_name,
                    params,
                })
            }
            Ok(None) => None,
            Err(e) => {
                warn!(
                    program_id = hex::encode(record.program_id),
                    error = %e,
                    "failed to decode Solana instruction"
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

    struct MockDecoder {
        id: [u8; 32],
        result: Result<Option<DecodedInstructionFields>, SolanaDecodeError>,
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
            self.result.clone()
        }
        fn decode_account(
            &self,
            _data: &[u8],
        ) -> Result<Option<DecodedAccountFields>, SolanaDecodeError> {
            Ok(None)
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

    fn make_instruction_record(program_id: [u8; 32]) -> SolanaInstructionRecord {
        SolanaInstructionRecord {
            slot: 200,
            block_time: Some(1_700_000_000),
            transaction_signature: [0xBB; 64],
            program_id,
            data: vec![3, 100, 0, 0, 0, 0, 0, 0, 0],
            accounts: vec![[10u8; 32], [20u8; 32], [30u8; 32]],
            instruction_index: 1,
            inner_instruction_index: None,
        }
    }

    #[test]
    fn test_decode_known_instruction() {
        let program_id = [1u8; 32];
        let mut args = HashMap::new();
        args.insert("amount".to_string(), DecodedValue::Uint64(100));

        let mut named_accounts = HashMap::new();
        named_accounts.insert("source".to_string(), [10u8; 32]);
        named_accounts.insert("destination".to_string(), [20u8; 32]);

        let decoder = MockDecoder {
            id: program_id,
            result: Ok(Some(DecodedInstructionFields {
                instruction_name: "Transfer".to_string(),
                args,
                named_accounts,
            })),
        };

        let router = SolanaInstructionDecoder::new(vec![Arc::new(decoder)]);
        let record = make_instruction_record(program_id);
        let results = router.decode_instruction_batch(&[record], "spl_token");

        assert_eq!(results.len(), 1);
        let event = &results[0];
        assert_eq!(event.block_number, 200);
        assert_eq!(event.event_name, "Transfer");
        assert_eq!(event.event_signature, "Transfer");
        // Args are merged in
        assert_eq!(event.params.get("amount"), Some(&DecodedValue::Uint64(100)));
        // Named accounts are merged in as ChainAddress with "accounts." prefix
        assert_eq!(
            event.params.get("accounts.source"),
            Some(&DecodedValue::ChainAddress(ChainAddress::Solana(
                [10u8; 32]
            )))
        );
        assert_eq!(
            event.params.get("accounts.destination"),
            Some(&DecodedValue::ChainAddress(ChainAddress::Solana(
                [20u8; 32]
            )))
        );
    }

    #[test]
    fn test_named_accounts_as_chain_address() {
        let program_id = [1u8; 32];
        let pubkey = [42u8; 32];
        let mut named_accounts = HashMap::new();
        named_accounts.insert("authority".to_string(), pubkey);

        let decoder = MockDecoder {
            id: program_id,
            result: Ok(Some(DecodedInstructionFields {
                instruction_name: "MintTo".to_string(),
                args: HashMap::new(),
                named_accounts,
            })),
        };

        let router = SolanaInstructionDecoder::new(vec![Arc::new(decoder)]);
        let record = make_instruction_record(program_id);
        let results = router.decode_instruction_batch(&[record], "test");

        assert_eq!(results.len(), 1);
        let val = results[0].params.get("accounts.authority").unwrap();
        match val {
            DecodedValue::ChainAddress(ChainAddress::Solana(bytes)) => {
                assert_eq!(*bytes, pubkey);
            }
            other => panic!("expected ChainAddress::Solana, got {:?}", other),
        }
    }

    #[test]
    fn test_unknown_program_skipped() {
        let decoder = MockDecoder {
            id: [1u8; 32],
            result: Ok(Some(DecodedInstructionFields {
                instruction_name: "Transfer".to_string(),
                args: HashMap::new(),
                named_accounts: HashMap::new(),
            })),
        };

        let router = SolanaInstructionDecoder::new(vec![Arc::new(decoder)]);
        // Use a different program_id that has no decoder
        let record = make_instruction_record([99u8; 32]);
        let results = router.decode_instruction_batch(&[record], "test");

        assert!(results.is_empty());
    }
}
