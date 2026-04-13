use std::collections::HashMap;
use std::sync::Arc;

use tracing::warn;

use crate::solana::raw_data::types::SolanaEventRecord;
use crate::transformations::context::DecodedEvent;
use crate::types::chain::{ChainAddress, LogPosition, TxId};

use super::traits::ProgramDecoder;

/// Routes `SolanaEventRecord`s to the appropriate `ProgramDecoder` by program ID
/// and produces `DecodedEvent`s for the transformation engine.
pub struct SolanaEventDecoder {
    /// program_id bytes -> decoder
    decoders: HashMap<[u8; 32], Arc<dyn ProgramDecoder>>,
}

impl SolanaEventDecoder {
    pub fn new(decoders: Vec<Arc<dyn ProgramDecoder>>) -> Self {
        let decoders = decoders
            .into_iter()
            .map(|d| (d.program_id(), d))
            .collect();
        Self { decoders }
    }

    pub fn decode_event_batch(
        &self,
        events: &[SolanaEventRecord],
        source_name: &str,
    ) -> Vec<DecodedEvent> {
        events
            .iter()
            .filter_map(|record| self.decode_single_event(record, source_name))
            .collect()
    }

    fn decode_single_event(
        &self,
        record: &SolanaEventRecord,
        source_name: &str,
    ) -> Option<DecodedEvent> {
        let decoder = self.decoders.get(&record.program_id)?;

        match decoder.decode_event(&record.event_discriminator, &record.event_data) {
            Ok(Some(fields)) => Some(DecodedEvent {
                block_number: record.slot,
                block_timestamp: record.block_time.unwrap_or(0) as u64,
                transaction_id: TxId::Solana(record.transaction_signature),
                position: LogPosition::Solana {
                    instruction_index: record.instruction_index,
                    inner_instruction_index: record.inner_instruction_index,
                },
                contract_address: ChainAddress::Solana(record.program_id),
                source_name: source_name.to_string(),
                event_name: fields.event_name.clone(),
                event_signature: fields.event_name,
                params: fields.params,
            }),
            Ok(None) => None,
            Err(e) => {
                warn!(
                    program_id = hex::encode(record.program_id),
                    discriminator = hex::encode(record.event_discriminator),
                    error = %e,
                    "failed to decode Solana event"
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
        result: Result<Option<DecodedEventFields>, SolanaDecodeError>,
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
            self.result.clone()
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

    fn make_event_record(program_id: [u8; 32]) -> SolanaEventRecord {
        SolanaEventRecord {
            slot: 100,
            block_time: Some(1_700_000_000),
            transaction_signature: [0xAA; 64],
            program_id,
            event_discriminator: [1, 2, 3, 4, 5, 6, 7, 8],
            event_data: vec![10, 20, 30],
            log_index: 0,
            instruction_index: 2,
            inner_instruction_index: Some(5),
        }
    }

    #[test]
    fn test_decode_known_event() {
        let program_id = [1u8; 32];
        let mut params = HashMap::new();
        params.insert("amount".to_string(), DecodedValue::Uint64(42));

        let decoder = MockDecoder {
            id: program_id,
            result: Ok(Some(DecodedEventFields {
                event_name: "Transfer".to_string(),
                params: params.clone(),
            })),
        };

        let router = SolanaEventDecoder::new(vec![Arc::new(decoder)]);
        let record = make_event_record(program_id);
        let results = router.decode_event_batch(&[record], "spl_token");

        assert_eq!(results.len(), 1);
        let event = &results[0];
        assert_eq!(event.block_number, 100);
        assert_eq!(event.block_timestamp, 1_700_000_000);
        assert_eq!(event.event_name, "Transfer");
        assert_eq!(event.source_name, "spl_token");
        assert_eq!(event.contract_address, ChainAddress::Solana([1u8; 32]));
        assert_eq!(
            event.params.get("amount"),
            Some(&DecodedValue::Uint64(42))
        );
    }

    #[test]
    fn test_unknown_program_skipped() {
        let decoder = MockDecoder {
            id: [1u8; 32],
            result: Ok(Some(DecodedEventFields {
                event_name: "Transfer".to_string(),
                params: HashMap::new(),
            })),
        };

        let router = SolanaEventDecoder::new(vec![Arc::new(decoder)]);
        // Use a different program_id that has no decoder
        let record = make_event_record([99u8; 32]);
        let results = router.decode_event_batch(&[record], "test");

        assert!(results.is_empty());
    }

    #[test]
    fn test_decode_error_logged() {
        let program_id = [1u8; 32];
        let decoder = MockDecoder {
            id: program_id,
            result: Err(SolanaDecodeError::UnexpectedEof {
                needed: 8,
                available: 2,
            }),
        };

        let router = SolanaEventDecoder::new(vec![Arc::new(decoder)]);
        let record = make_event_record(program_id);
        let results = router.decode_event_batch(&[record], "test");

        assert!(results.is_empty());
    }

    #[test]
    fn test_event_signature_equals_name() {
        let program_id = [1u8; 32];
        let decoder = MockDecoder {
            id: program_id,
            result: Ok(Some(DecodedEventFields {
                event_name: "Swap".to_string(),
                params: HashMap::new(),
            })),
        };

        let router = SolanaEventDecoder::new(vec![Arc::new(decoder)]);
        let record = make_event_record(program_id);
        let results = router.decode_event_batch(&[record], "test");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event_signature, results[0].event_name);
        assert_eq!(results[0].event_signature, "Swap");
    }

    #[test]
    fn test_position_mapping() {
        let program_id = [1u8; 32];
        let decoder = MockDecoder {
            id: program_id,
            result: Ok(Some(DecodedEventFields {
                event_name: "Test".to_string(),
                params: HashMap::new(),
            })),
        };

        let router = SolanaEventDecoder::new(vec![Arc::new(decoder)]);
        let record = make_event_record(program_id);
        let results = router.decode_event_batch(&[record], "test");

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].position,
            LogPosition::Solana {
                instruction_index: 2,
                inner_instruction_index: Some(5),
            }
        );
    }
}
