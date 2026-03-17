//! Live/current phase for eth_call decoding - processes new data as it arrives via channel.

use std::collections::HashSet;
use std::path::Path;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::eth_calls::column_index::{
    load_or_build_decoded_column_index, read_once_calls_from_parquet,
    read_raw_parquet_function_names,
};
use crate::decoding::eth_calls::{
    build_result_map, decode_value, process_event_calls, process_once_calls, process_regular_calls,
    CallDecodeConfig, EthCallDecodingError, EventCallDecodeConfig,
};
use crate::decoding::types::DecoderMessage;
use crate::live::LiveStorage;
use crate::live::TransformRetryRequest;
use crate::live::{LiveDecodedCall, LiveDecodedEventCall, LiveDecodedOnceCall};
use crate::transformations::{
    DecodedCall as TransformDecodedCall, DecodedCallsMessage, RangeCompleteKind,
    RangeCompleteMessage,
};
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::decoded::DecodedValue;

use crate::decoding::catchup::eth_calls::catchup_decode_eth_calls;

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
    // Live storage for live_mode=true messages
    let live_storage = LiveStorage::new(chain_name);

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
                        // Live mode: decode and write to bincode storage
                        let mut decoded_calls: Vec<LiveDecodedCall> =
                            Vec::with_capacity(results.len());
                        let mut transform_calls: Vec<TransformDecodedCall> =
                            Vec::with_capacity(results.len());

                        for result in &results {
                            match decode_value(&result.value, &config.output_type) {
                                Ok(decoded) => {
                                    // Build transform call for transformation engine
                                    transform_calls.push(TransformDecodedCall {
                                        block_number: result.block_number,
                                        block_timestamp: result.block_timestamp,
                                        contract_address: result.contract_address,
                                        source_name: contract_name.clone(),
                                        function_name: function_name.clone(),
                                        trigger_log_index: None,
                                        result: build_result_map(
                                            &decoded,
                                            &config.output_type,
                                            &function_name,
                                        ),
                                    });

                                    // Build live call for bincode storage
                                    decoded_calls.push(LiveDecodedCall {
                                        block_number: result.block_number,
                                        block_timestamp: result.block_timestamp,
                                        contract_address: result.contract_address,
                                        decoded_value: decoded.clone(),
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to decode eth_call {}/{} at block {}: address={}, raw_bytes=0x{}, error={}",
                                        contract_name, function_name, result.block_number,
                                        alloy::primitives::Address::from(result.contract_address),
                                        hex::encode(&result.value),
                                        e
                                    );
                                }
                            }
                        }

                        if !decoded_calls.is_empty() {
                            if let Err(e) = live_storage.write_decoded_calls(
                                range_start,
                                &contract_name,
                                &function_name,
                                &decoded_calls,
                            ) {
                                tracing::warn!(
                                    "Failed to write decoded eth_calls for {}/{} at block {}: {}",
                                    contract_name,
                                    function_name,
                                    range_start,
                                    e
                                );
                            }
                        }

                        // Send to transformation engine
                        if !retry_transform_after_decode {
                            if let Some(tx) = transform_tx {
                                if !transform_calls.is_empty() {
                                    let msg = DecodedCallsMessage {
                                        range_start,
                                        range_end,
                                        source_name: contract_name.clone(),
                                        function_name: function_name.clone(),
                                        calls: transform_calls,
                                    };
                                    let _ = tx.send(msg).await;
                                }
                            }
                        }
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
                // Find the once configs for this contract
                let configs: Vec<&CallDecodeConfig> = once_configs
                    .iter()
                    .filter(|c| c.contract_name == contract_name)
                    .collect();

                if !configs.is_empty() {
                    if live_mode {
                        // Live mode: decode and write to bincode storage
                        let mut decoded_once_calls: Vec<LiveDecodedOnceCall> =
                            Vec::with_capacity(results.len());
                        // Group transform calls by function name
                        let mut transform_calls_by_fn: std::collections::HashMap<
                            String,
                            Vec<TransformDecodedCall>,
                        > = std::collections::HashMap::new();

                        for result in &results {
                            let mut decoded_values: Vec<(String, DecodedValue)> = Vec::new();

                            for config in &configs {
                                if let Some(raw_value) = result.results.get(&config.function_name) {
                                    match decode_value(raw_value, &config.output_type) {
                                        Ok(decoded) => {
                                            // Build transform call for transformation engine
                                            let transform_call = TransformDecodedCall {
                                                block_number: result.block_number,
                                                block_timestamp: result.block_timestamp,
                                                contract_address: result.contract_address,
                                                source_name: contract_name.clone(),
                                                function_name: config.function_name.clone(),
                                                trigger_log_index: None,
                                                result: build_result_map(
                                                    &decoded,
                                                    &config.output_type,
                                                    &config.function_name,
                                                ),
                                            };
                                            transform_calls_by_fn
                                                .entry(config.function_name.clone())
                                                .or_default()
                                                .push(transform_call);

                                            // Build live value for bincode storage
                                            decoded_values.push((
                                                config.function_name.clone(),
                                                decoded.clone(),
                                            ));
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "Failed to decode once_call {}/{} at block {}: address={}, raw_bytes=0x{}, error={}",
                                                contract_name, config.function_name, result.block_number,
                                                alloy::primitives::Address::from(result.contract_address),
                                                hex::encode(raw_value),
                                                e
                                            );
                                        }
                                    }
                                }
                            }

                            if !decoded_values.is_empty() {
                                decoded_once_calls.push(LiveDecodedOnceCall {
                                    block_number: result.block_number,
                                    block_timestamp: result.block_timestamp,
                                    contract_address: result.contract_address,
                                    decoded_values,
                                });
                            }
                        }

                        if !decoded_once_calls.is_empty() {
                            if let Err(e) = live_storage.write_decoded_once_calls(
                                range_start,
                                &contract_name,
                                &decoded_once_calls,
                            ) {
                                tracing::warn!(
                                    "Failed to write decoded once_calls for {} at block {}: {}",
                                    contract_name,
                                    range_start,
                                    e
                                );
                            }
                        }

                        // Send to transformation engine (one message per function)
                        if !retry_transform_after_decode {
                            for (function_name, calls) in transform_calls_by_fn {
                                if let Some(tx) = transform_tx {
                                    if calls.is_empty() {
                                        continue;
                                    }
                                    let msg = DecodedCallsMessage {
                                        range_start,
                                        range_end,
                                        source_name: contract_name.clone(),
                                        function_name,
                                        calls,
                                    };
                                    let _ = tx.send(msg).await;
                                }
                            }
                        }
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
                            false, // Update index directly (no concurrent tasks)
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
                // Find the decode config for this event-triggered call
                if let Some(config) = event_configs
                    .iter()
                    .find(|c| c.contract_name == contract_name && c.function_name == function_name)
                {
                    if live_mode {
                        // Live mode: decode and write to bincode storage
                        let mut decoded_event_calls: Vec<LiveDecodedEventCall> =
                            Vec::with_capacity(results.len());
                        let mut transform_calls: Vec<TransformDecodedCall> =
                            Vec::with_capacity(results.len());

                        for result in &results {
                            match decode_value(&result.value, &config.output_type) {
                                Ok(decoded) => {
                                    // Build transform call for transformation engine
                                    transform_calls.push(TransformDecodedCall {
                                        block_number: result.block_number,
                                        block_timestamp: result.block_timestamp,
                                        contract_address: result.target_address,
                                        source_name: contract_name.clone(),
                                        function_name: function_name.clone(),
                                        trigger_log_index: Some(result.log_index),
                                        result: build_result_map(
                                            &decoded,
                                            &config.output_type,
                                            &function_name,
                                        ),
                                    });

                                    // Build live call for bincode storage
                                    decoded_event_calls.push(LiveDecodedEventCall {
                                        block_number: result.block_number,
                                        block_timestamp: result.block_timestamp,
                                        log_index: result.log_index,
                                        target_address: result.target_address,
                                        decoded_value: decoded.clone(),
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to decode event_call {}/{} at block {}: address={}, log_index={}, raw_bytes=0x{}, error={}",
                                        contract_name, function_name, result.block_number,
                                        alloy::primitives::Address::from(result.target_address),
                                        result.log_index,
                                        hex::encode(&result.value),
                                        e
                                    );
                                }
                            }
                        }

                        if !decoded_event_calls.is_empty() {
                            if let Err(e) = live_storage.write_decoded_event_calls(
                                range_start,
                                &contract_name,
                                &function_name,
                                &decoded_event_calls,
                            ) {
                                tracing::warn!(
                                    "Failed to write decoded event_calls for {}/{} at block {}: {}",
                                    contract_name,
                                    function_name,
                                    range_start,
                                    e
                                );
                            }
                        }

                        // Send to transformation engine
                        if !retry_transform_after_decode {
                            if let Some(tx) = transform_tx {
                                if !transform_calls.is_empty() {
                                    let msg = DecodedCallsMessage {
                                        range_start,
                                        range_end,
                                        source_name: contract_name.clone(),
                                        function_name: function_name.clone(),
                                        calls: transform_calls,
                                    };
                                    let _ = tx.send(msg).await;
                                }
                            }
                        }
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
                // Delete decoded data for orphaned blocks
                tracing::info!(
                    "Handling reorg in eth_calls decoder, deleting {} orphaned blocks",
                    orphaned.len()
                );
                for block_number in orphaned {
                    let _ = live_storage.delete_all_decoded_calls(block_number);
                }
            }
            Some(DecoderMessage::EthCallsBlockComplete {
                range_start,
                range_end,
                retry_transform_after_decode,
            }) => {
                if let Ok(mut status) = live_storage.read_status(range_start) {
                    status.eth_calls_decoded = true;
                    if let Err(e) = live_storage.write_status(range_start, &status) {
                        tracing::warn!(
                            "Failed to update block status after eth_call block completion: {}",
                            e
                        );
                    } else {
                        tracing::debug!("Block {} eth_calls decoded", range_start);
                    }
                }

                if !retry_transform_after_decode {
                    if let Some(tx) = complete_tx {
                        let msg = RangeCompleteMessage {
                            range_start,
                            range_end,
                            kind: RangeCompleteKind::EthCalls,
                        };
                        if let Err(e) = tx.send(msg).await {
                            tracing::warn!("Failed to send eth_call range complete: {}", e);
                        }
                    }
                }

                if retry_transform_after_decode {
                    if let Some(tx) = transform_retry_tx {
                        if let Err(e) = tx
                            .send(TransformRetryRequest {
                                block_number: range_start,
                                missing_handlers: None,
                            })
                            .await
                        {
                            tracing::warn!(
                                "Failed to queue deferred transform retry for block {}: {}",
                                range_start,
                                e
                            );
                        }
                    } else {
                        tracing::warn!(
                            "Block {} requested deferred transform retry but no retry channel is configured",
                            range_start
                        );
                    }
                }
            }
            Some(DecoderMessage::OnceFileBackfilled {
                range_start,
                range_end,
                contract_name,
            }) => {
                // A once-call file was backfilled with new columns - decode missing columns
                let configs: Vec<&CallDecodeConfig> = once_configs
                    .iter()
                    .filter(|c| c.contract_name == contract_name)
                    .collect();

                if !configs.is_empty() {
                    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
                    let raw_path = raw_calls_dir
                        .join(&contract_name)
                        .join("once")
                        .join(&file_name);

                    if raw_path.exists() {
                        // Read raw columns from parquet
                        let raw_cols: HashSet<String> = read_raw_parquet_function_names(&raw_path);

                        // Read decoded columns from index or parquet
                        let decoded_dir = output_base.join(&contract_name).join("once");
                        let raw_once_dir = raw_calls_dir.join(&contract_name).join("once");
                        let decoded_index =
                            load_or_build_decoded_column_index(&decoded_dir, &raw_once_dir);
                        let decoded_cols: HashSet<String> = decoded_index
                            .get(&file_name)
                            .map(|cols| cols.iter().cloned().collect())
                            .unwrap_or_default();

                        // Find configs that need decoding
                        let missing_configs: Vec<CallDecodeConfig> = configs
                            .iter()
                            .filter(|c| {
                                raw_cols.contains(&c.function_name)
                                    && !decoded_cols.contains(&c.function_name)
                            })
                            .cloned()
                            .cloned()
                            .collect();

                        if !missing_configs.is_empty() {
                            tracing::info!(
                                "OnceFileBackfilled: decoding {} new columns for {}/{}",
                                missing_configs.len(),
                                contract_name,
                                file_name
                            );

                            // Read raw parquet and decode the missing columns
                            let configs_ref: Vec<&CallDecodeConfig> =
                                missing_configs.iter().collect();
                            let results = read_once_calls_from_parquet(&raw_path, &configs_ref)?;
                            process_once_calls(
                                &results,
                                range_start,
                                range_end,
                                &contract_name,
                                &configs_ref,
                                output_base,
                                transform_tx,
                                false,
                            )
                            .await?;
                        }
                    }
                }
            }
            Some(DecoderMessage::AllComplete) => {
                // Re-run catchup to decode any new columns added by raw backfill.
                // Initial catchup may have decoded partial data (e.g., name, symbol)
                // while raw backfill was still adding new columns (e.g., getAssetData).
                // This second pass picks up the newly added columns with fresh indexes.
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
    use alloy::primitives::U256;
    use crate::live::{LiveBlockStatus, LiveStorage, TransformRetryRequest};
    use crate::types::decoded::DecodedValue;
    use crate::transformations::{DecodedCallsMessage, RangeCompleteKind, RangeCompleteMessage};
    use crate::types::config::eth_call::EvmType;
    use crate::types::config::raw_data::{FieldsConfig, RawDataCollectionConfig};

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
