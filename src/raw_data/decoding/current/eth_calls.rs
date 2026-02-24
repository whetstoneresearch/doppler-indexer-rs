//! Live/current phase for eth_call decoding - processes new data as it arrives via channel.

use std::collections::HashSet;
use std::path::Path;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::raw_data::decoding::catchup::eth_calls::{
    load_or_build_decoded_column_index, read_once_calls_from_parquet,
    read_raw_parquet_function_names,
};
use crate::raw_data::decoding::eth_calls::{
    CallDecodeConfig, EthCallDecodingError, process_once_calls, process_regular_calls,
};
use crate::raw_data::decoding::types::DecoderMessage;
use crate::transformations::DecodedCallsMessage;
use crate::types::config::raw_data::RawDataCollectionConfig;

use super::super::catchup;

/// Live phase: Process new data as it arrives via channel.
/// Returns when AllComplete message is received or channel closes.
pub async fn decode_eth_calls_live(
    mut decoder_rx: Receiver<DecoderMessage>,
    raw_calls_dir: &Path,
    output_base: &Path,
    regular_configs: &[CallDecodeConfig],
    once_configs: &[CallDecodeConfig],
    raw_data_config: &RawDataCollectionConfig,
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
) -> Result<(), EthCallDecodingError> {
    loop {
        match decoder_rx.recv().await {
            Some(DecoderMessage::EthCallsReady {
                range_start,
                range_end,
                contract_name,
                function_name,
                results,
            }) => {
                // Find the decode config for this call
                let config = regular_configs
                    .iter()
                    .find(|c| c.contract_name == contract_name && c.function_name == function_name);

                if let Some(config) = config {
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
            Some(DecoderMessage::OnceCallsReady {
                range_start,
                range_end,
                contract_name,
                results,
            }) => {
                // Find the once configs for this contract
                let configs: Vec<&CallDecodeConfig> = once_configs
                    .iter()
                    .filter(|c| c.contract_name == contract_name)
                    .collect();

                if !configs.is_empty() {
                    process_once_calls(
                        &results,
                        range_start,
                        range_end,
                        &contract_name,
                        &configs,
                        output_base,
                        transform_tx,
                        false, // Live mode: update index directly (no concurrent tasks)
                    )
                    .await?;
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
