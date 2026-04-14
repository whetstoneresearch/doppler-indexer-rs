//! Async task wrappers for Solana decoders.
//!
//! These functions receive raw events/instructions on a channel, decode them
//! using [`SolanaEventDecoder`] / [`SolanaInstructionDecoder`], and forward
//! decoded results to the transformation engine.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::DecoderMessage;
use crate::transformations::engine::{
    DecodedEventsMessage, RangeCompleteKind, RangeCompleteMessage,
};

use super::events::SolanaEventDecoder;
use super::instructions::SolanaInstructionDecoder;

// ---------------------------------------------------------------------------
// Event decoder task
// ---------------------------------------------------------------------------

/// Async task that receives [`DecoderMessage::SolanaEventsReady`], decodes
/// events via the routing layer, and forwards results to the transformation
/// engine.
///
/// Events are grouped by `program_id` before decoding (since each program has
/// a different decoder). Decoded events are then regrouped by
/// `(source_name, event_name)` so the transformation engine receives one
/// [`DecodedEventsMessage`] per handler trigger.
pub async fn decode_solana_events(
    decoder: Arc<SolanaEventDecoder>,
    program_names: Arc<HashMap<[u8; 32], String>>,
    mut decoder_rx: Receiver<DecoderMessage>,
    transform_tx: Option<Sender<DecodedEventsMessage>>,
    complete_tx: Option<Sender<RangeCompleteMessage>>,
) {
    while let Some(msg) = decoder_rx.recv().await {
        match msg {
            #[cfg(feature = "solana")]
            DecoderMessage::SolanaEventsReady {
                range_start,
                range_end,
                events,
                live_mode: _,
            } => {
                // Group raw events by program_id
                let mut by_program: HashMap<[u8; 32], Vec<_>> = HashMap::new();
                for event in events {
                    by_program
                        .entry(event.program_id)
                        .or_default()
                        .push(event);
                }

                // Decode per-program, then regroup by (source_name, event_name)
                let mut by_trigger: HashMap<(String, String), Vec<_>> = HashMap::new();

                for (program_id, program_events) in &by_program {
                    let source_name = match program_names.get(program_id) {
                        Some(name) => name.as_str(),
                        None => {
                            tracing::debug!(
                                program_id = hex::encode(program_id),
                                "No source name for program, skipping"
                            );
                            continue;
                        }
                    };

                    let decoded = decoder.decode_event_batch(program_events, source_name);
                    for event in decoded {
                        let key = (event.source_name.clone(), event.event_name.clone());
                        by_trigger.entry(key).or_default().push(event);
                    }
                }

                // Send one message per (source_name, event_name) group
                if let Some(ref tx) = transform_tx {
                    for ((source_name, event_name), events) in by_trigger {
                        if events.is_empty() {
                            continue;
                        }
                        if tx
                            .send(DecodedEventsMessage {
                                range_start,
                                range_end,
                                source_name,
                                event_name,
                                events,
                            })
                            .await
                            .is_err()
                        {
                            tracing::debug!("Transform events channel closed");
                            return;
                        }
                    }
                }

                // Signal event decoding complete for this range
                if let Some(ref tx) = complete_tx {
                    if tx
                        .send(RangeCompleteMessage {
                            range_start,
                            range_end,
                            kind: RangeCompleteKind::Logs,
                        })
                        .await
                        .is_err()
                    {
                        tracing::debug!("Transform complete channel closed");
                        return;
                    }
                }
            }
            DecoderMessage::AllComplete => break,
            _ => {} // Ignore EVM-specific messages
        }
    }

    tracing::info!("Solana event decoder task finished");
}

// ---------------------------------------------------------------------------
// Instruction decoder task
// ---------------------------------------------------------------------------

/// Async task that receives [`DecoderMessage::SolanaInstructionsReady`],
/// decodes instructions via the routing layer, and forwards results to the
/// transformation engine.
///
/// Instructions are decoded into [`DecodedEvent`]s (instruction args + named
/// accounts merged into params) and sent as [`DecodedEventsMessage`]s.
pub async fn decode_solana_instructions(
    decoder: Arc<SolanaInstructionDecoder>,
    program_names: Arc<HashMap<[u8; 32], String>>,
    mut decoder_rx: Receiver<DecoderMessage>,
    transform_tx: Option<Sender<DecodedEventsMessage>>,
    complete_tx: Option<Sender<RangeCompleteMessage>>,
) {
    while let Some(msg) = decoder_rx.recv().await {
        match msg {
            #[cfg(feature = "solana")]
            DecoderMessage::SolanaInstructionsReady {
                range_start,
                range_end,
                instructions,
                live_mode: _,
            } => {
                // Group raw instructions by program_id
                let mut by_program: HashMap<[u8; 32], Vec<_>> = HashMap::new();
                for instr in instructions {
                    by_program
                        .entry(instr.program_id)
                        .or_default()
                        .push(instr);
                }

                // Decode per-program, then regroup by (source_name, event_name)
                let mut by_trigger: HashMap<(String, String), Vec<_>> = HashMap::new();

                for (program_id, program_instrs) in &by_program {
                    let source_name = match program_names.get(program_id) {
                        Some(name) => name.as_str(),
                        None => {
                            tracing::debug!(
                                program_id = hex::encode(program_id),
                                "No source name for program, skipping"
                            );
                            continue;
                        }
                    };

                    let decoded =
                        decoder.decode_instruction_batch(program_instrs, source_name);
                    for event in decoded {
                        let key = (event.source_name.clone(), event.event_name.clone());
                        by_trigger.entry(key).or_default().push(event);
                    }
                }

                // Send one message per (source_name, instruction_name) group
                if let Some(ref tx) = transform_tx {
                    for ((source_name, event_name), events) in by_trigger {
                        if events.is_empty() {
                            continue;
                        }
                        if tx
                            .send(DecodedEventsMessage {
                                range_start,
                                range_end,
                                source_name,
                                event_name,
                                events,
                            })
                            .await
                            .is_err()
                        {
                            tracing::debug!("Transform events channel closed");
                            return;
                        }
                    }
                }

                // Signal instruction decoding complete for this range
                if let Some(ref tx) = complete_tx {
                    if tx
                        .send(RangeCompleteMessage {
                            range_start,
                            range_end,
                            kind: RangeCompleteKind::Instructions,
                        })
                        .await
                        .is_err()
                    {
                        tracing::debug!("Transform complete channel closed");
                        return;
                    }
                }
            }
            DecoderMessage::AllComplete => break,
            _ => {} // Ignore EVM-specific messages
        }
    }

    tracing::info!("Solana instruction decoder task finished");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::decoding::traits::{
        DecodedAccountFields, DecodedEventFields, DecodedInstructionFields, ProgramDecoder,
        SolanaDecodeError,
    };
    use crate::solana::raw_data::types::{SolanaEventRecord, SolanaInstructionRecord};
    use crate::types::decoded::DecodedValue;

    struct MockDecoder {
        id: [u8; 32],
        event_result: Option<DecodedEventFields>,
        instruction_result: Option<DecodedInstructionFields>,
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
            Ok(self.event_result.clone())
        }
        fn decode_instruction(
            &self,
            _data: &[u8],
            _accounts: &[[u8; 32]],
        ) -> Result<Option<DecodedInstructionFields>, SolanaDecodeError> {
            Ok(self.instruction_result.clone())
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
            instruction_index: 0,
            inner_instruction_index: None,
        }
    }

    fn make_instruction_record(program_id: [u8; 32]) -> SolanaInstructionRecord {
        SolanaInstructionRecord {
            slot: 200,
            block_time: Some(1_700_000_000),
            transaction_signature: [0xBB; 64],
            program_id,
            data: vec![1, 2, 3],
            accounts: vec![[10u8; 32]],
            instruction_index: 0,
            inner_instruction_index: None,
        }
    }

    #[tokio::test]
    async fn event_decoder_task_sends_grouped_messages() {
        let program_id = [1u8; 32];

        let mut params = HashMap::new();
        params.insert("amount".to_string(), DecodedValue::Uint64(42));

        let decoder = Arc::new(SolanaEventDecoder::new(vec![Arc::new(MockDecoder {
            id: program_id,
            event_result: Some(DecodedEventFields {
                event_name: "Transfer".to_string(),
                params,
            }),
            instruction_result: None,
        })]));

        let mut names = HashMap::new();
        names.insert(program_id, "spl_token".to_string());
        let program_names = Arc::new(names);

        let (decoder_tx, decoder_rx) = tokio::sync::mpsc::channel(10);
        let (transform_tx, mut transform_rx) = tokio::sync::mpsc::channel(10);
        let (complete_tx, mut complete_rx) = tokio::sync::mpsc::channel(10);

        let handle = tokio::spawn(decode_solana_events(
            decoder,
            program_names,
            decoder_rx,
            Some(transform_tx),
            Some(complete_tx),
        ));

        // Send an event batch
        decoder_tx
            .send(DecoderMessage::SolanaEventsReady {
                range_start: 0,
                range_end: 1000,
                events: vec![make_event_record(program_id)],
                live_mode: false,
            })
            .await
            .unwrap();

        // Should receive a decoded events message
        let msg = transform_rx.recv().await.unwrap();
        assert_eq!(msg.source_name, "spl_token");
        assert_eq!(msg.event_name, "Transfer");
        assert_eq!(msg.events.len(), 1);
        assert_eq!(msg.range_start, 0);
        assert_eq!(msg.range_end, 1000);

        // Should receive a completion message
        let complete = complete_rx.recv().await.unwrap();
        assert_eq!(complete.range_start, 0);
        assert_eq!(complete.range_end, 1000);
        assert_eq!(complete.kind, RangeCompleteKind::Logs);

        // Send AllComplete to stop
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn instruction_decoder_task_sends_instruction_complete() {
        let program_id = [2u8; 32];

        let mut args = HashMap::new();
        args.insert("amount".to_string(), DecodedValue::Uint64(100));

        let decoder = Arc::new(SolanaInstructionDecoder::new(vec![Arc::new(
            MockDecoder {
                id: program_id,
                event_result: None,
                instruction_result: Some(DecodedInstructionFields {
                    instruction_name: "Transfer".to_string(),
                    args,
                    named_accounts: HashMap::new(),
                }),
            },
        )]));

        let mut names = HashMap::new();
        names.insert(program_id, "spl_token".to_string());
        let program_names = Arc::new(names);

        let (decoder_tx, decoder_rx) = tokio::sync::mpsc::channel(10);
        let (transform_tx, mut transform_rx) = tokio::sync::mpsc::channel(10);
        let (complete_tx, mut complete_rx) = tokio::sync::mpsc::channel(10);

        let handle = tokio::spawn(decode_solana_instructions(
            decoder,
            program_names,
            decoder_rx,
            Some(transform_tx),
            Some(complete_tx),
        ));

        decoder_tx
            .send(DecoderMessage::SolanaInstructionsReady {
                range_start: 1000,
                range_end: 2000,
                instructions: vec![make_instruction_record(program_id)],
                live_mode: false,
            })
            .await
            .unwrap();

        let msg = transform_rx.recv().await.unwrap();
        assert_eq!(msg.source_name, "spl_token");
        assert_eq!(msg.event_name, "Transfer");
        assert_eq!(msg.events.len(), 1);

        let complete = complete_rx.recv().await.unwrap();
        assert_eq!(complete.kind, RangeCompleteKind::Instructions);

        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn decoder_task_exits_on_channel_close() {
        let decoder = Arc::new(SolanaEventDecoder::new(vec![]));
        let program_names = Arc::new(HashMap::new());

        let (decoder_tx, decoder_rx) = tokio::sync::mpsc::channel(10);
        let handle = tokio::spawn(decode_solana_events(
            decoder,
            program_names,
            decoder_rx,
            None,
            None,
        ));

        // Drop sender to close channel
        drop(decoder_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn unknown_program_events_skipped() {
        let known_id = [1u8; 32];
        let unknown_id = [99u8; 32];

        let mut params = HashMap::new();
        params.insert("x".to_string(), DecodedValue::Uint64(1));

        let decoder = Arc::new(SolanaEventDecoder::new(vec![Arc::new(MockDecoder {
            id: known_id,
            event_result: Some(DecodedEventFields {
                event_name: "Evt".to_string(),
                params,
            }),
            instruction_result: None,
        })]));

        let mut names = HashMap::new();
        names.insert(known_id, "prog".to_string());
        let program_names = Arc::new(names);

        let (decoder_tx, decoder_rx) = tokio::sync::mpsc::channel(10);
        let (transform_tx, mut transform_rx) = tokio::sync::mpsc::channel(10);
        let (complete_tx, mut complete_rx) = tokio::sync::mpsc::channel(10);

        let handle = tokio::spawn(decode_solana_events(
            decoder,
            program_names,
            decoder_rx,
            Some(transform_tx),
            Some(complete_tx),
        ));

        // Send batch with one known and one unknown program event
        decoder_tx
            .send(DecoderMessage::SolanaEventsReady {
                range_start: 0,
                range_end: 1000,
                events: vec![
                    make_event_record(known_id),
                    make_event_record(unknown_id),
                ],
                live_mode: false,
            })
            .await
            .unwrap();

        // Only the known program event should appear
        let msg = transform_rx.recv().await.unwrap();
        assert_eq!(msg.events.len(), 1);
        assert_eq!(msg.source_name, "prog");

        // Completion still sent
        let _complete = complete_rx.recv().await.unwrap();

        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();
        handle.await.unwrap();
    }
}
