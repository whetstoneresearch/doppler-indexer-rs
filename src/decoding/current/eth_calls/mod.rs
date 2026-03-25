//! Live/current phase for eth_call decoding - processes new data as it arrives via channel.

mod events;
mod factory;
mod range_processor;
mod state;

use std::path::Path;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::eth_calls::{
    process_event_calls, process_once_calls, process_regular_calls, CallDecodeConfig,
    EthCallDecodingError, EventCallDecodeConfig,
};
use crate::decoding::types::DecoderMessage;
use crate::live::TransformRetryRequest;
use crate::transformations::{DecodedCallsMessage, RangeCompleteMessage};
use crate::types::config::raw_data::RawDataCollectionConfig;

use crate::decoding::catchup::eth_calls::catchup_decode_eth_calls;

use self::events::handle_event_calls_live;
use self::factory::{handle_once_calls_live, handle_once_file_backfilled};
use self::range_processor::{handle_block_complete, handle_regular_calls_live};
use self::state::DecoderState;

/// Live phase: Process new data as it arrives via channel.
/// Returns when AllComplete message is received or channel closes.
pub async fn decode_eth_calls_live(
    mut decoder_rx: Receiver<DecoderMessage>,
    raw_calls_dir: &Path,
    output_base: &Path,
    chain_name: &str,
    regular_configs: &[CallDecodeConfig],
    once_configs: &[CallDecodeConfig],
    event_configs: &[EventCallDecodeConfig],
    raw_data_config: &RawDataCollectionConfig,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    complete_tx: Option<&Sender<RangeCompleteMessage>>,
    transform_retry_tx: Option<&Sender<TransformRetryRequest>>,
) -> Result<(), EthCallDecodingError> {
    let state = DecoderState::new(chain_name);

    loop {
        match decoder_rx.recv().await {
            Some(DecoderMessage::EthCallsReady {
                range_start,
                range_end,
                contract_name,
                function_name,
                results,
                live_mode,
                retry_transform_after_decode,
            }) => {
                // Find the decode config for this call
                let config = regular_configs
                    .iter()
                    .find(|c| c.contract_name == contract_name && c.function_name == function_name);

                if let Some(config) = config {
                    if live_mode {
                        handle_regular_calls_live(
                            &state.live_storage,
                            range_start,
                            range_end,
                            &contract_name,
                            &function_name,
                            &results,
                            config,
                            transform_tx,
                            retry_transform_after_decode,
                        )
                        .await?;
                    } else {
                        // Historical mode: write to parquet
                        process_regular_calls(
                            &results,
                            range_start,
                            range_end,
                            config,
                            output_base,
                            transform_tx,
                        )
                        .await?;
                    }
                }
            }
            Some(DecoderMessage::OnceCallsReady {
                range_start,
                range_end,
                contract_name,
                results,
                live_mode,
                retry_transform_after_decode,
            }) => {
                let configs: Vec<&CallDecodeConfig> = once_configs
                    .iter()
                    .filter(|c| c.contract_name == contract_name)
                    .collect();

                if !configs.is_empty() {
                    if live_mode {
                        handle_once_calls_live(
                            &state.live_storage,
                            range_start,
                            range_end,
                            &contract_name,
                            &results,
                            &configs,
                            transform_tx,
                            retry_transform_after_decode,
                        )
                        .await?;
                    } else {
                        // Historical mode: write to parquet
                        process_once_calls(
                            &results,
                            range_start,
                            range_end,
                            &contract_name,
                            &configs,
                            output_base,
                            transform_tx,
                            false,
                        )
                        .await?;
                    }
                }
            }
            Some(DecoderMessage::EventCallsReady {
                range_start,
                range_end,
                contract_name,
                function_name,
                results,
                live_mode,
                retry_transform_after_decode,
            }) => {
                if let Some(config) = event_configs
                    .iter()
                    .find(|c| c.contract_name == contract_name && c.function_name == function_name)
                {
                    if live_mode {
                        handle_event_calls_live(
                            &state.live_storage,
                            range_start,
                            range_end,
                            &contract_name,
                            &function_name,
                            &results,
                            config,
                            transform_tx,
                            retry_transform_after_decode,
                        )
                        .await?;
                    } else {
                        // Historical mode: write to parquet
                        process_event_calls(
                            &results,
                            range_start,
                            range_end,
                            config,
                            output_base,
                            transform_tx,
                        )
                        .await?;
                    }
                }
            }
            Some(DecoderMessage::Reorg { orphaned, .. }) => {
                tracing::info!(
                    "Handling reorg in eth_calls decoder, deleting {} orphaned blocks",
                    orphaned.len()
                );
                for block_number in orphaned {
                    let _ = state.live_storage.delete_all_decoded_calls(block_number);
                }
            }
            Some(DecoderMessage::EthCallsBlockComplete {
                range_start,
                range_end,
                retry_transform_after_decode,
            }) => {
                handle_block_complete(
                    &state.live_storage,
                    range_start,
                    range_end,
                    retry_transform_after_decode,
                    complete_tx,
                    transform_retry_tx,
                )
                .await;
            }
            Some(DecoderMessage::OnceFileBackfilled {
                range_start,
                range_end,
                contract_name,
            }) => {
                handle_once_file_backfilled(
                    raw_calls_dir,
                    output_base,
                    range_start,
                    range_end,
                    &contract_name,
                    once_configs,
                    transform_tx,
                )
                .await?;
            }
            Some(DecoderMessage::AllComplete) => {
                // Re-run catchup to decode any new columns added by raw backfill.
                if raw_calls_dir.exists() {
                    tracing::info!("Raw collection complete, re-running decode catchup");
                    catchup_decode_eth_calls(
                        raw_calls_dir,
                        output_base,
                        regular_configs,
                        once_configs,
                        event_configs,
                        raw_data_config,
                        transform_tx,
                    )
                    .await?;
                }
                break;
            }
            None => {
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use super::decode_eth_calls_live;
    use crate::decoding::eth_calls::{CallDecodeConfig, EventCallDecodeConfig};
    use crate::decoding::{DecoderMessage, EthCallResult, EventCallResult, OnceCallResult};
    use crate::live::{LiveBlockStatus, LiveStorage, TransformRetryRequest};
    use crate::transformations::{DecodedCallsMessage, RangeCompleteKind, RangeCompleteMessage};
    use crate::types::config::eth_call::EvmType;
    use crate::types::config::raw_data::{FieldsConfig, RawDataCollectionConfig};
    use crate::types::decoded::DecodedValue;
    use alloy::primitives::U256;

    fn raw_data_config() -> RawDataCollectionConfig {
        RawDataCollectionConfig {
            parquet_block_range: None,
            rpc_batch_size: None,
            fields: FieldsConfig {
                block_fields: None,
                receipt_fields: None,
                log_fields: None,
            },
            contract_logs_only: None,
            channel_capacity: None,
            factory_channel_capacity: None,
            block_receipt_concurrency: None,
            decoding_concurrency: None,
            factory_concurrency: None,
            live_mode: None,
            reorg_depth: None,
            compaction_interval_secs: None,
            transform_retry_grace_period_secs: None,
        }
    }

    fn unique_chain_name(prefix: &str) -> String {
        format!("{}-{}", prefix, rand::random::<u64>())
    }

    fn seed_live_status(chain_name: &str, block_number: u64) -> LiveStorage {
        let storage = LiveStorage::new(chain_name);
        storage.ensure_dirs().unwrap();
        storage
            .write_status(block_number, &LiveBlockStatus::collected())
            .unwrap();
        storage
    }

    fn cleanup_chain_storage(chain_name: &str) {
        let _ = std::fs::remove_dir_all(format!("data/{}", chain_name));
    }

    fn encode_uint256(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[24..].copy_from_slice(&value.to_be_bytes());
        bytes
    }

    fn spawn_live_decoder(
        decoder_rx: mpsc::Receiver<DecoderMessage>,
        temp: TempDir,
        chain_name: String,
        regular_configs: Vec<CallDecodeConfig>,
        once_configs: Vec<CallDecodeConfig>,
        event_configs: Vec<EventCallDecodeConfig>,
        transform_tx: Option<mpsc::Sender<DecodedCallsMessage>>,
        complete_tx: Option<mpsc::Sender<RangeCompleteMessage>>,
        retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
    ) -> tokio::task::JoinHandle<Result<(), crate::decoding::eth_calls::EthCallDecodingError>> {
        let raw_dir = temp.path().join("raw");
        let output_dir = temp.path().join("decoded");
        tokio::spawn(async move {
            let _temp = temp;
            decode_eth_calls_live(
                decoder_rx,
                raw_dir.as_path(),
                output_dir.as_path(),
                &chain_name,
                &regular_configs,
                &once_configs,
                &event_configs,
                &raw_data_config(),
                transform_tx.as_ref(),
                complete_tx.as_ref(),
                retry_tx.as_ref(),
            )
            .await
        })
    }

    #[tokio::test]
    async fn eth_call_block_complete_emits_completion_signal() {
        let (decoder_tx, decoder_rx) = mpsc::channel(4);
        let (complete_tx, mut complete_rx) = mpsc::channel::<RangeCompleteMessage>(4);

        let handle = tokio::spawn(async move {
            decode_eth_calls_live(
                decoder_rx,
                Path::new("data/test/raw"),
                Path::new("data/test/decoded"),
                "test",
                &[],
                &[],
                &[],
                &raw_data_config(),
                None,
                Some(&complete_tx),
                None,
            )
            .await
        });

        decoder_tx
            .send(DecoderMessage::EthCallsBlockComplete {
                range_start: 42,
                range_end: 43,
                retry_transform_after_decode: false,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();

        let msg = complete_rx.recv().await.unwrap();
        assert_eq!(msg.range_start, 42);
        assert_eq!(msg.range_end, 43);
        assert_eq!(msg.kind, RangeCompleteKind::EthCalls);

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn eth_call_block_complete_with_retry_emits_retry_request() {
        let (decoder_tx, decoder_rx) = mpsc::channel(4);
        let (retry_tx, mut retry_rx) = mpsc::channel::<TransformRetryRequest>(4);
        let (complete_tx, mut complete_rx) = mpsc::channel::<RangeCompleteMessage>(4);

        let handle = tokio::spawn(async move {
            decode_eth_calls_live(
                decoder_rx,
                Path::new("data/test/raw"),
                Path::new("data/test/decoded"),
                "test",
                &[],
                &[],
                &[],
                &raw_data_config(),
                None,
                Some(&complete_tx),
                Some(&retry_tx),
            )
            .await
        });

        decoder_tx
            .send(DecoderMessage::EthCallsBlockComplete {
                range_start: 42,
                range_end: 43,
                retry_transform_after_decode: true,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();

        let request = retry_rx.recv().await.unwrap();
        assert_eq!(request.block_number, 42);
        assert!(request.missing_handlers.is_none());
        assert!(complete_rx.try_recv().is_err());

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn deferred_regular_calls_persist_without_forwarding_transforms() {
        let chain_name = unique_chain_name("deferred-regular");
        let storage = seed_live_status(&chain_name, 42);
        let temp = TempDir::new().unwrap();
        let (decoder_tx, decoder_rx) = mpsc::channel(8);
        let (transform_tx, mut transform_rx) = mpsc::channel::<DecodedCallsMessage>(8);
        let (retry_tx, mut retry_rx) = mpsc::channel::<TransformRetryRequest>(8);
        let (complete_tx, mut complete_rx) = mpsc::channel::<RangeCompleteMessage>(8);

        let handle = spawn_live_decoder(
            decoder_rx,
            temp,
            chain_name.clone(),
            vec![CallDecodeConfig {
                contract_name: "contract".to_string(),
                function_name: "foo".to_string(),
                output_type: EvmType::Uint256,
                _is_once: false,
                start_block: None,
            }],
            vec![],
            vec![],
            Some(transform_tx),
            Some(complete_tx),
            Some(retry_tx),
        );

        decoder_tx
            .send(DecoderMessage::EthCallsReady {
                range_start: 42,
                range_end: 43,
                contract_name: "contract".to_string(),
                function_name: "foo".to_string(),
                results: vec![EthCallResult {
                    block_number: 42,
                    block_timestamp: 1_000,
                    contract_address: [1u8; 20],
                    value: encode_uint256(5),
                }],
                live_mode: true,
                retry_transform_after_decode: true,
            })
            .await
            .unwrap();
        decoder_tx
            .send(DecoderMessage::EthCallsBlockComplete {
                range_start: 42,
                range_end: 43,
                retry_transform_after_decode: true,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();

        let request = retry_rx.recv().await.unwrap();
        assert_eq!(request.block_number, 42);
        assert!(request.missing_handlers.is_none());
        assert!(transform_rx.try_recv().is_err());
        assert!(complete_rx.try_recv().is_err());

        handle.await.unwrap().unwrap();

        let decoded = storage.read_decoded_calls(42, "contract", "foo").unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(matches!(
            decoded[0].decoded_value,
            DecodedValue::Uint256(ref value) if *value == U256::from(5)
        ));

        cleanup_chain_storage(&chain_name);
    }

    #[tokio::test]
    async fn deferred_once_calls_persist_without_forwarding_transforms() {
        let chain_name = unique_chain_name("deferred-once");
        let storage = seed_live_status(&chain_name, 51);
        let temp = TempDir::new().unwrap();
        let (decoder_tx, decoder_rx) = mpsc::channel(8);
        let (transform_tx, mut transform_rx) = mpsc::channel::<DecodedCallsMessage>(8);
        let (retry_tx, mut retry_rx) = mpsc::channel::<TransformRetryRequest>(8);

        let handle = spawn_live_decoder(
            decoder_rx,
            temp,
            chain_name.clone(),
            vec![],
            vec![CallDecodeConfig {
                contract_name: "factory".to_string(),
                function_name: "once".to_string(),
                output_type: EvmType::Uint256,
                _is_once: true,
                start_block: None,
            }],
            vec![],
            Some(transform_tx),
            None,
            Some(retry_tx),
        );

        decoder_tx
            .send(DecoderMessage::OnceCallsReady {
                range_start: 51,
                range_end: 52,
                contract_name: "factory".to_string(),
                results: vec![OnceCallResult {
                    block_number: 51,
                    block_timestamp: 2_000,
                    contract_address: [2u8; 20],
                    results: HashMap::from([("once".to_string(), encode_uint256(6))]),
                }],
                live_mode: true,
                retry_transform_after_decode: true,
            })
            .await
            .unwrap();
        decoder_tx
            .send(DecoderMessage::EthCallsBlockComplete {
                range_start: 51,
                range_end: 52,
                retry_transform_after_decode: true,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();

        let request = retry_rx.recv().await.unwrap();
        assert_eq!(request.block_number, 51);
        assert!(transform_rx.try_recv().is_err());

        handle.await.unwrap().unwrap();

        let decoded = storage.read_decoded_once_calls(51, "factory").unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].decoded_values.len(), 1);
        assert_eq!(decoded[0].decoded_values[0].0, "once");
        assert!(matches!(
            decoded[0].decoded_values[0].1,
            DecodedValue::Uint256(ref value) if *value == U256::from(6)
        ));

        cleanup_chain_storage(&chain_name);
    }

    #[tokio::test]
    async fn deferred_event_calls_persist_without_forwarding_transforms() {
        let chain_name = unique_chain_name("deferred-event");
        let storage = seed_live_status(&chain_name, 61);
        let temp = TempDir::new().unwrap();
        let (decoder_tx, decoder_rx) = mpsc::channel(8);
        let (transform_tx, mut transform_rx) = mpsc::channel::<DecodedCallsMessage>(8);
        let (retry_tx, mut retry_rx) = mpsc::channel::<TransformRetryRequest>(8);

        let handle = spawn_live_decoder(
            decoder_rx,
            temp,
            chain_name.clone(),
            vec![],
            vec![],
            vec![EventCallDecodeConfig {
                contract_name: "contract".to_string(),
                function_name: "bar".to_string(),
                output_type: EvmType::Uint256,
                start_block: None,
            }],
            Some(transform_tx),
            None,
            Some(retry_tx),
        );

        decoder_tx
            .send(DecoderMessage::EventCallsReady {
                range_start: 61,
                range_end: 62,
                contract_name: "contract".to_string(),
                function_name: "bar".to_string(),
                results: vec![EventCallResult {
                    block_number: 61,
                    block_timestamp: 3_000,
                    log_index: 7,
                    target_address: [3u8; 20],
                    value: encode_uint256(7),
                }],
                live_mode: true,
                retry_transform_after_decode: true,
            })
            .await
            .unwrap();
        decoder_tx
            .send(DecoderMessage::EthCallsBlockComplete {
                range_start: 61,
                range_end: 62,
                retry_transform_after_decode: true,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();

        let request = retry_rx.recv().await.unwrap();
        assert_eq!(request.block_number, 61);
        assert!(transform_rx.try_recv().is_err());

        handle.await.unwrap().unwrap();

        let decoded = storage
            .read_decoded_event_calls(61, "contract", "bar")
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(matches!(
            decoded[0].decoded_value,
            DecodedValue::Uint256(ref value) if *value == U256::from(7)
        ));

        cleanup_chain_storage(&chain_name);
    }

    #[tokio::test]
    async fn non_deferred_regular_calls_still_forward_transforms() {
        let chain_name = unique_chain_name("nondeferred-regular");
        let storage = seed_live_status(&chain_name, 71);
        let temp = TempDir::new().unwrap();
        let (decoder_tx, decoder_rx) = mpsc::channel(8);
        let (transform_tx, mut transform_rx) = mpsc::channel::<DecodedCallsMessage>(8);

        let handle = spawn_live_decoder(
            decoder_rx,
            temp,
            chain_name.clone(),
            vec![CallDecodeConfig {
                contract_name: "contract".to_string(),
                function_name: "foo".to_string(),
                output_type: EvmType::Uint256,
                _is_once: false,
                start_block: None,
            }],
            vec![],
            vec![],
            Some(transform_tx),
            None,
            None,
        );

        decoder_tx
            .send(DecoderMessage::EthCallsReady {
                range_start: 71,
                range_end: 72,
                contract_name: "contract".to_string(),
                function_name: "foo".to_string(),
                results: vec![EthCallResult {
                    block_number: 71,
                    block_timestamp: 4_000,
                    contract_address: [4u8; 20],
                    value: encode_uint256(8),
                }],
                live_mode: true,
                retry_transform_after_decode: false,
            })
            .await
            .unwrap();
        decoder_tx
            .send(DecoderMessage::EthCallsBlockComplete {
                range_start: 71,
                range_end: 72,
                retry_transform_after_decode: false,
            })
            .await
            .unwrap();
        decoder_tx.send(DecoderMessage::AllComplete).await.unwrap();

        let transform_msg = transform_rx.recv().await.unwrap();
        assert_eq!(transform_msg.range_start, 71);
        assert_eq!(transform_msg.source_name, "contract");
        assert_eq!(transform_msg.function_name, "foo");
        assert_eq!(transform_msg.calls.len(), 1);

        handle.await.unwrap().unwrap();

        let decoded = storage.read_decoded_calls(71, "contract", "foo").unwrap();
        assert_eq!(decoded.len(), 1);
        cleanup_chain_storage(&chain_name);
    }
}
