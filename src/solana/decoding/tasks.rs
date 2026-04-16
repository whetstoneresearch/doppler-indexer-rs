//! Async task wrappers for Solana decoders.
//!
//! These functions receive raw events/instructions on a channel, decode them
//! using [`SolanaEventDecoder`] / [`SolanaInstructionDecoder`], and forward
//! decoded results to the transformation engine.
//!
//! Decoded events are also always written to parquet on disk so that output
//! is persisted regardless of whether the transformation channel is present.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::transformations::context::DecodedEvent;

use crate::decoding::DecoderMessage;
use crate::storage::paths::{decoded_solana_events_dir, decoded_solana_instructions_dir};
use crate::transformations::engine::{
    DecodedEventsMessage, RangeCompleteKind, RangeCompleteMessage,
};

use super::decoded_parquet::{build_decoded_event_schema, write_decoded_events_to_parquet};
use super::events::SolanaEventDecoder;
use super::instructions::SolanaInstructionDecoder;

// ---------------------------------------------------------------------------
// Stale decoded parquet cleanup
// ---------------------------------------------------------------------------

/// Remove decoded parquet files for a range that no longer appear in the
/// current decoded output. This handles IDL/decoder changes that rename or
/// remove event/instruction types: after re-decoding, the old
/// `{source}/{old_event_name}/{range}.parquet` file would otherwise remain
/// on disk as stale output.
///
/// Scans `base_dir/{source}/{event_name}/` for any file named `range_file`
/// where `(source, event_name)` is NOT in `written_groups`.
///
/// When `only_sources` is `Some`, the scan is restricted to those source
/// directories. Directories with unknown names (including old names from
/// source renames) are left untouched because we cannot distinguish an old
/// name for an in-scope program from one for an out-of-scope program
/// without reading parquet content. Source renames are cleaned up by
/// running unscoped `--decode-only --repair`.
///
/// **Callers must not invoke this when a function filter is active**, since
/// the written set would be incomplete *within* each source and cleanup
/// would delete files for out-of-scope event/instruction names.
fn cleanup_stale_decoded_parquet(
    base_dir: &Path,
    written_groups: &HashMap<(String, String), Vec<DecodedEvent>>,
    range_file: &str,
    only_sources: Option<&HashSet<String>>,
) {
    let written: HashSet<(&str, &str)> = written_groups
        .keys()
        .map(|(s, e)| (s.as_str(), e.as_str()))
        .collect();

    let Ok(source_dirs) = std::fs::read_dir(base_dir) else {
        return;
    };

    for source_entry in source_dirs.flatten() {
        let source_path = source_entry.path();
        if !source_path.is_dir() {
            continue;
        }
        let Some(source_name) = source_entry.file_name().to_str().map(String::from) else {
            continue;
        };

        // When source-scoped, only scan directories whose name matches an
        // in-scope source. We leave unknown directories alone to avoid
        // accidentally deleting data for renamed out-of-scope programs.
        if let Some(sources) = only_sources {
            if !sources.contains(source_name.as_str()) {
                continue;
            }
        }

        let Ok(event_dirs) = std::fs::read_dir(&source_path) else {
            continue;
        };

        for event_entry in event_dirs.flatten() {
            let event_path = event_entry.path();
            if !event_path.is_dir() {
                continue;
            }
            let Some(event_name) = event_entry.file_name().to_str().map(String::from) else {
                continue;
            };

            // If this (source, event_name) was written in this batch, skip
            if written.contains(&(source_name.as_str(), event_name.as_str())) {
                continue;
            }

            // Check if the stale range file exists and delete it
            let stale_path = event_path.join(range_file);
            if stale_path.exists() {
                if let Err(e) = std::fs::remove_file(&stale_path) {
                    tracing::warn!(
                        path = %stale_path.display(),
                        error = %e,
                        "Failed to remove stale decoded parquet"
                    );
                } else {
                    tracing::debug!(
                        source = source_name.as_str(),
                        event = event_name.as_str(),
                        path = %stale_path.display(),
                        "Removed stale decoded parquet"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Event decoder task
// ---------------------------------------------------------------------------

/// Async task that receives [`DecoderMessage::SolanaEventsReady`], decodes
/// events via the routing layer, writes decoded parquet to disk, and
/// optionally forwards results to the transformation engine.
///
/// Events are grouped by `program_id` before decoding (since each program has
/// a different decoder). Decoded events are then regrouped by
/// `(source_name, event_name)` so each group gets its own parquet file and
/// the transformation engine receives one [`DecodedEventsMessage`] per handler
/// trigger.
/// When `only_sources` is `Some`, stale cleanup is restricted to those
/// source directories. Unknown directories (e.g. old names from a source
/// rename) are left alone; use unscoped repair to clean those up.
/// When a `function_filter` is active, cleanup is skipped entirely because
/// the decoded output is incomplete within each source.
pub async fn decode_solana_events(
    decoder: Arc<SolanaEventDecoder>,
    program_names: Arc<HashMap<[u8; 32], String>>,
    mut decoder_rx: Receiver<DecoderMessage>,
    transform_tx: Option<Sender<DecodedEventsMessage>>,
    complete_tx: Option<Sender<RangeCompleteMessage>>,
    chain_name: String,
    function_filter: Option<Arc<HashSet<String>>>,
    only_sources: Option<Arc<HashSet<String>>>,
    live_storage: Option<crate::solana::live::storage::SolanaLiveStorage>,
    // When true (live mode + discovery-managed account reads configured), sends
    // a sentinel empty
    // DecodedEventsMessage through transform_tx after all decoded groups for a
    // slot. The bridge detects this and forwards RangeComplete to the discovery
    // loop, which sends mark_complete_only to the reader AFTER all
    // discovery-triggered account reads for the slot have been enqueued.
    signal_slot_done: bool,
) {
    let schema = build_decoded_event_schema();
    let base_dir = decoded_solana_events_dir(&chain_name);

    while let Some(msg) = decoder_rx.recv().await {
        match msg {
            #[cfg(feature = "solana")]
            DecoderMessage::SolanaEventsReady {
                range_start,
                range_end,
                events,
                live_mode,
            } => {
                // Group raw events by program_id
                let mut by_program: HashMap<[u8; 32], Vec<_>> = HashMap::new();
                for event in events {
                    by_program.entry(event.program_id).or_default().push(event);
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

                // Apply function filter: drop groups whose event_name
                // doesn't match --function scope
                if let Some(ref filter) = function_filter {
                    by_trigger.retain(|(_, event_name), _| filter.contains(event_name.as_str()));
                }

                // In live mode, skip parquet writes — decoded data flows through
                // channels to the engine, and compaction handles parquet later.
                // Mark the slot as events_decoded in live storage so the
                // compaction service sees it as complete.
                if !live_mode {
                    let range_end_inclusive = if range_end > 0 { range_end - 1 } else { 0 };
                    let range_file = format!("{}-{}.parquet", range_start, range_end_inclusive);

                    for ((source_name, event_name), events) in &by_trigger {
                        if events.is_empty() {
                            continue;
                        }
                        let output_dir = base_dir.join(source_name).join(event_name);
                        if let Err(e) = std::fs::create_dir_all(&output_dir) {
                            tracing::error!(
                                path = %output_dir.display(),
                                error = %e,
                                "Failed to create decoded events output directory"
                            );
                            continue;
                        }
                        let output_path = output_dir.join(&range_file);

                        if let Err(e) =
                            write_decoded_events_to_parquet(events, &schema, &output_path)
                        {
                            tracing::error!(
                                path = %output_path.display(),
                                error = %e,
                                "Failed to write decoded events parquet"
                            );
                        } else {
                            tracing::debug!(
                                source = source_name.as_str(),
                                event = event_name.as_str(),
                                count = events.len(),
                                path = %output_path.display(),
                                "Wrote decoded events parquet"
                            );
                        }
                    }

                    // Remove stale decoded parquet for this range. Function
                    // filtering makes the output incomplete per-source, so
                    // cleanup must be skipped. Source scoping restricts the
                    // scan to in-scope sources only.
                    if function_filter.is_none() {
                        cleanup_stale_decoded_parquet(
                            &base_dir,
                            &by_trigger,
                            &range_file,
                            only_sources.as_deref(),
                        );
                    }
                } else if let Some(ref storage) = live_storage {
                    // In live mode, mark events_decoded in slot status.
                    // range_start == range_end == slot for live ranges.
                    if let Err(e) = storage.update_status_atomic(range_start, |s| {
                        s.events_decoded = true;
                    }) {
                        tracing::warn!(
                            slot = range_start,
                            error = %e,
                            "Failed to mark events_decoded in live status"
                        );
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

                // Send a slot-done sentinel through the same channel as decoded
                // events so it follows them through the bridge → discovery
                // pipeline. The bridge converts this into a RangeComplete for
                // the discovery loop, which then sends mark_complete_only to
                // the reader after all discovery-triggered account reads for
                // the slot have been enqueued.
                if live_mode && signal_slot_done {
                    if let Some(ref tx) = transform_tx {
                        let _ = tx
                            .send(DecodedEventsMessage {
                                range_start,
                                range_end,
                                source_name: String::new(),
                                event_name: String::new(),
                                events: Vec::new(),
                            })
                            .await;
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
/// decodes instructions via the routing layer, writes decoded parquet to disk,
/// and optionally forwards results to the transformation engine.
///
/// Instructions are decoded into [`DecodedEvent`]s (instruction args + named
/// accounts merged into params) and sent as [`DecodedEventsMessage`]s.
pub async fn decode_solana_instructions(
    decoder: Arc<SolanaInstructionDecoder>,
    program_names: Arc<HashMap<[u8; 32], String>>,
    mut decoder_rx: Receiver<DecoderMessage>,
    transform_tx: Option<Sender<DecodedEventsMessage>>,
    complete_tx: Option<Sender<RangeCompleteMessage>>,
    chain_name: String,
    function_filter: Option<Arc<HashSet<String>>>,
    only_sources: Option<Arc<HashSet<String>>>,
    live_storage: Option<crate::solana::live::storage::SolanaLiveStorage>,
) {
    let schema = build_decoded_event_schema();
    let base_dir = decoded_solana_instructions_dir(&chain_name);

    while let Some(msg) = decoder_rx.recv().await {
        match msg {
            #[cfg(feature = "solana")]
            DecoderMessage::SolanaInstructionsReady {
                range_start,
                range_end,
                instructions,
                live_mode,
            } => {
                // Group raw instructions by program_id
                let mut by_program: HashMap<[u8; 32], Vec<_>> = HashMap::new();
                for instr in instructions {
                    by_program.entry(instr.program_id).or_default().push(instr);
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

                    let decoded = decoder.decode_instruction_batch(program_instrs, source_name);
                    for event in decoded {
                        let key = (event.source_name.clone(), event.event_name.clone());
                        by_trigger.entry(key).or_default().push(event);
                    }
                }

                // Apply function filter
                if let Some(ref filter) = function_filter {
                    by_trigger.retain(|(_, event_name), _| filter.contains(event_name.as_str()));
                }

                if !live_mode {
                    let range_end_inclusive = if range_end > 0 { range_end - 1 } else { 0 };
                    let range_file = format!("{}-{}.parquet", range_start, range_end_inclusive);

                    for ((source_name, event_name), events) in &by_trigger {
                        if events.is_empty() {
                            continue;
                        }
                        let output_dir = base_dir.join(source_name).join(event_name);
                        if let Err(e) = std::fs::create_dir_all(&output_dir) {
                            tracing::error!(
                                path = %output_dir.display(),
                                error = %e,
                                "Failed to create decoded instructions output directory"
                            );
                            continue;
                        }
                        let output_path = output_dir.join(&range_file);

                        if let Err(e) =
                            write_decoded_events_to_parquet(events, &schema, &output_path)
                        {
                            tracing::error!(
                                path = %output_path.display(),
                                error = %e,
                                "Failed to write decoded instructions parquet"
                            );
                        } else {
                            tracing::debug!(
                                source = source_name.as_str(),
                                instruction = event_name.as_str(),
                                count = events.len(),
                                path = %output_path.display(),
                                "Wrote decoded instructions parquet"
                            );
                        }
                    }

                    // Remove stale decoded parquet — see decode_solana_events
                    if function_filter.is_none() {
                        cleanup_stale_decoded_parquet(
                            &base_dir,
                            &by_trigger,
                            &range_file,
                            only_sources.as_deref(),
                        );
                    }
                } else if let Some(ref storage) = live_storage {
                    if let Err(e) = storage.update_status_atomic(range_start, |s| {
                        s.instructions_decoded = true;
                    }) {
                        tracing::warn!(
                            slot = range_start,
                            error = %e,
                            "Failed to mark instructions_decoded in live status"
                        );
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
    use crate::solana::live::storage::SolanaLiveStorage;
    use crate::solana::live::types::LiveSlotStatus;
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

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let chain_name = format!(
            "test-{}",
            tmp_dir.path().file_name().unwrap().to_str().unwrap()
        );

        // Create the expected output directory under a temp-like data dir
        // We need the test to write to a real dir so use the chain_name approach
        let handle = tokio::spawn(decode_solana_events(
            decoder,
            program_names,
            decoder_rx,
            Some(transform_tx),
            Some(complete_tx),
            chain_name,
            None,
            None,
            None,
            false,
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
    async fn event_decoder_task_marks_live_slot_complete() {
        let program_id = [3u8; 32];

        let mut params = HashMap::new();
        params.insert("amount".to_string(), DecodedValue::Uint64(7));

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

        let storage_root = tempfile::TempDir::new().unwrap();
        let storage = SolanaLiveStorage::with_base_dir(storage_root.path().to_path_buf());
        storage.ensure_dirs().unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.events_extracted = true;
        storage.write_status(100, &status).unwrap();

        let (decoder_tx, decoder_rx) = tokio::sync::mpsc::channel(10);
        let handle = tokio::spawn(decode_solana_events(
            decoder,
            program_names,
            decoder_rx,
            None,
            None,
            "test-live-events".to_string(),
            None,
            None,
            Some(storage.clone()),
            false,
        ));

        decoder_tx
            .send(DecoderMessage::SolanaEventsReady {
                range_start: 100,
                range_end: 100,
                events: vec![make_event_record(program_id)],
                live_mode: true,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();
        handle.await.unwrap();

        let updated = storage.read_status(100).unwrap();
        assert!(updated.events_decoded);
    }

    #[tokio::test]
    async fn instruction_decoder_task_sends_instruction_complete() {
        let program_id = [2u8; 32];

        let mut args = HashMap::new();
        args.insert("amount".to_string(), DecodedValue::Uint64(100));

        let decoder = Arc::new(SolanaInstructionDecoder::new(vec![Arc::new(MockDecoder {
            id: program_id,
            event_result: None,
            instruction_result: Some(DecodedInstructionFields {
                instruction_name: "Transfer".to_string(),
                args,
                named_accounts: HashMap::new(),
            }),
        })]));

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
            "test-solana-instr".to_string(),
            None,
            None,
            None,
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
    async fn instruction_decoder_task_marks_live_slot_complete() {
        let program_id = [4u8; 32];

        let mut args = HashMap::new();
        args.insert("amount".to_string(), DecodedValue::Uint64(9));

        let decoder = Arc::new(SolanaInstructionDecoder::new(vec![Arc::new(MockDecoder {
            id: program_id,
            event_result: None,
            instruction_result: Some(DecodedInstructionFields {
                instruction_name: "Transfer".to_string(),
                args,
                named_accounts: HashMap::new(),
            }),
        })]));

        let mut names = HashMap::new();
        names.insert(program_id, "spl_token".to_string());
        let program_names = Arc::new(names);

        let storage_root = tempfile::TempDir::new().unwrap();
        let storage = SolanaLiveStorage::with_base_dir(storage_root.path().to_path_buf());
        storage.ensure_dirs().unwrap();

        let mut status = LiveSlotStatus::collected();
        status.block_fetched = true;
        status.instructions_extracted = true;
        storage.write_status(200, &status).unwrap();

        let (decoder_tx, decoder_rx) = tokio::sync::mpsc::channel(10);
        let handle = tokio::spawn(decode_solana_instructions(
            decoder,
            program_names,
            decoder_rx,
            None,
            None,
            "test-live-instructions".to_string(),
            None,
            None,
            Some(storage.clone()),
        ));

        decoder_tx
            .send(DecoderMessage::SolanaInstructionsReady {
                range_start: 200,
                range_end: 200,
                instructions: vec![make_instruction_record(program_id)],
                live_mode: true,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();
        handle.await.unwrap();

        let updated = storage.read_status(200).unwrap();
        assert!(updated.instructions_decoded);
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
            "test-close".to_string(),
            None,
            None,
            None,
            false,
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
            "test-unknown".to_string(),
            None,
            None,
            None,
            false,
        ));

        // Send batch with one known and one unknown program event
        decoder_tx
            .send(DecoderMessage::SolanaEventsReady {
                range_start: 0,
                range_end: 1000,
                events: vec![make_event_record(known_id), make_event_record(unknown_id)],
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
