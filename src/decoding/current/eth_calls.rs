//! Live/current phase for eth_call decoding - processes new data as it arrives via channel.

use std::collections::HashSet;
use std::path::Path;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::catchup::eth_calls::{
    load_or_build_decoded_column_index, read_once_calls_from_parquet,
    read_raw_parquet_function_names,
};
use crate::decoding::eth_calls::{
    build_result_map, decode_value, CallDecodeConfig, DecodedValue, EthCallDecodingError,
    EventCallDecodeConfig, process_event_calls, process_once_calls, process_regular_calls,
};
use crate::decoding::types::DecoderMessage;
use crate::live::{LiveDecodedCall, LiveDecodedEventCall, LiveDecodedOnceCall, LiveDecodedValue};
use crate::live::LiveStorage;
use crate::transformations::{DecodedCall as TransformDecodedCall, DecodedCallsMessage};
use crate::types::config::raw_data::RawDataCollectionConfig;

use super::super::catchup;

/// Convert DecodedValue to LiveDecodedValue for bincode serialization.
fn to_live_value(value: &DecodedValue) -> LiveDecodedValue {
    match value {
        DecodedValue::Address(a) => LiveDecodedValue::Address(*a),
        DecodedValue::Uint256(v) => LiveDecodedValue::Uint256(v.to_string()),
        DecodedValue::Int256(v) => LiveDecodedValue::Int256(v.to_string()),
        DecodedValue::Uint64(v) => LiveDecodedValue::Uint64(*v),
        DecodedValue::Int64(v) => LiveDecodedValue::Int64(*v),
        DecodedValue::Uint8(v) => LiveDecodedValue::Uint8(*v),
        DecodedValue::Int8(v) => LiveDecodedValue::Int8(*v),
        DecodedValue::Bool(v) => LiveDecodedValue::Bool(*v),
        DecodedValue::Bytes32(v) => LiveDecodedValue::Bytes32(*v),
        DecodedValue::Bytes(v) => LiveDecodedValue::Bytes(v.clone()),
        DecodedValue::String(v) => LiveDecodedValue::String(v.clone()),
        DecodedValue::NamedTuple(fields) => {
            let converted: Vec<(String, LiveDecodedValue)> = fields
                .iter()
                .map(|(name, val)| (name.clone(), to_live_value(val)))
                .collect();
            LiveDecodedValue::NamedTuple(converted)
        }
        DecodedValue::UnnamedTuple(values) => {
            let converted: Vec<LiveDecodedValue> = values.iter().map(to_live_value).collect();
            LiveDecodedValue::UnnamedTuple(converted)
        }
        DecodedValue::Array(values) => {
            let converted: Vec<LiveDecodedValue> = values.iter().map(to_live_value).collect();
            LiveDecodedValue::Array(converted)
        }
    }
}

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
            }) => {
                // Find the decode config for this call
                let config = regular_configs
                    .iter()
                    .find(|c| c.contract_name == contract_name && c.function_name == function_name);

                if let Some(config) = config {
                    if live_mode {
                        // Live mode: decode and write to bincode storage
                        let mut decoded_calls: Vec<LiveDecodedCall> = Vec::with_capacity(results.len());
                        let mut transform_calls: Vec<TransformDecodedCall> = Vec::with_capacity(results.len());

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
                                        result: build_result_map(&decoded, &config.output_type, &function_name),
                                    });

                                    // Build live call for bincode storage
                                    decoded_calls.push(LiveDecodedCall {
                                        block_number: result.block_number,
                                        block_timestamp: result.block_timestamp,
                                        contract_address: result.contract_address,
                                        decoded_value: to_live_value(&decoded),
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
                                    contract_name, function_name, range_start, e
                                );
                            }
                        }

                        // Send to transformation engine
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

                        // Update block status to mark decoding complete
                        if let Ok(mut status) = live_storage.read_status(range_start) {
                            status.decoded = true;
                            if let Err(e) = live_storage.write_status(range_start, &status) {
                                tracing::warn!("Failed to update block status after eth_call decoding: {}", e);
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
            }) => {
                // Find the once configs for this contract
                let configs: Vec<&CallDecodeConfig> = once_configs
                    .iter()
                    .filter(|c| c.contract_name == contract_name)
                    .collect();

                if !configs.is_empty() {
                    if live_mode {
                        // Live mode: decode and write to bincode storage
                        let mut decoded_once_calls: Vec<LiveDecodedOnceCall> = Vec::with_capacity(results.len());
                        // Group transform calls by function name
                        let mut transform_calls_by_fn: std::collections::HashMap<String, Vec<TransformDecodedCall>> =
                            std::collections::HashMap::new();

                        for result in &results {
                            let mut decoded_values: Vec<(String, LiveDecodedValue)> = Vec::new();

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
                                                result: build_result_map(&decoded, &config.output_type, &config.function_name),
                                            };
                                            transform_calls_by_fn
                                                .entry(config.function_name.clone())
                                                .or_default()
                                                .push(transform_call);

                                            // Build live value for bincode storage
                                            decoded_values.push((
                                                config.function_name.clone(),
                                                to_live_value(&decoded),
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
                                    contract_name, range_start, e
                                );
                            }
                        }

                        // Send to transformation engine (one message per function)
                        if let Some(tx) = transform_tx {
                            for (function_name, calls) in transform_calls_by_fn {
                                if !calls.is_empty() {
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

                        // Update block status to mark decoding complete
                        if let Ok(mut status) = live_storage.read_status(range_start) {
                            status.decoded = true;
                            if let Err(e) = live_storage.write_status(range_start, &status) {
                                tracing::warn!("Failed to update block status after once_call decoding: {}", e);
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
            }) => {
                // Find the decode config for this event-triggered call
                if let Some(config) = event_configs
                    .iter()
                    .find(|c| c.contract_name == contract_name && c.function_name == function_name)
                {
                    if live_mode {
                        // Live mode: decode and write to bincode storage
                        let mut decoded_event_calls: Vec<LiveDecodedEventCall> = Vec::with_capacity(results.len());
                        let mut transform_calls: Vec<TransformDecodedCall> = Vec::with_capacity(results.len());

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
                                        result: build_result_map(&decoded, &config.output_type, &function_name),
                                    });

                                    // Build live call for bincode storage
                                    decoded_event_calls.push(LiveDecodedEventCall {
                                        block_number: result.block_number,
                                        block_timestamp: result.block_timestamp,
                                        log_index: result.log_index,
                                        target_address: result.target_address,
                                        decoded_value: to_live_value(&decoded),
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
                                    contract_name, function_name, range_start, e
                                );
                            }
                        }

                        // Send to transformation engine
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

                        // Update block status to mark decoding complete
                        if let Ok(mut status) = live_storage.read_status(range_start) {
                            status.decoded = true;
                            if let Err(e) = live_storage.write_status(range_start, &status) {
                                tracing::warn!("Failed to update block status after event_call decoding: {}", e);
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
                    tracing::info!(
                        "Raw collection complete, re-running decode catchup"
                    );
                    catchup::catchup_decode_eth_calls(
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
